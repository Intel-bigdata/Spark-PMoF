#include "DataServerService.h"
#include "pmpool/client/NetworkClient.h"

ServiceConnectCallback::ServiceConnectCallback(std::shared_ptr<DataServerService> service) {
    service_ = service;
}

void ServiceConnectCallback::operator()(void *param_1, void *param_2) {
  cout << "ServiceConnectCallback" << endl;
  auto connection = static_cast<Connection*>(param_1);
  service_->setConnection(connection);
}

ServiceRecvCallback::ServiceRecvCallback(std::shared_ptr<ChunkMgr> chunkMgr, std::shared_ptr<DataServiceRequestHandler> requestHandler, std::shared_ptr<ReplicateWorker> worker) 
: chunkMgr_(chunkMgr), requestHandler_(requestHandler), worker_(worker) {}

void ServiceRecvCallback::operator()(void *param_1, void *param_2) {
  int mid = *static_cast<int*>(param_1);
  Chunk* ck = chunkMgr_->get(mid);
  auto requestReply = std::make_shared<ReplicaRequestReply>(
      reinterpret_cast<char *>(ck->buffer), ck->size,
      reinterpret_cast<Connection *>(ck->con));
  requestReply->decode();
  auto rrc = requestReply->get_rrc();
  if (rrc.type == REGISTER) {
    requestHandler_->notify(requestReply);
  } else {
    worker_->addTask(requestReply);
  }
  chunkMgr_->reclaim(ck, static_cast<Connection *>(ck->con));
}

ReplicateWorker::ReplicateWorker(std::shared_ptr<DataServerService> service)
    : service_(service) {}

int ReplicateWorker::entry() {
  std::shared_ptr<ReplicaRequestReply> requestReply;
  bool res = pendingRecvRequestQueue_.wait_dequeue_timed(
      requestReply, std::chrono::milliseconds(1000));
  if (res) {
    service_->handle_replica_msg(requestReply);
  }
  return 0;
}

void ReplicateWorker::abort() {}

void ReplicateWorker::addTask(std::shared_ptr<ReplicaRequestReply> rr) {
  pendingRecvRequestQueue_.enqueue(rr);
}

DataServerService::DataServerService(std::shared_ptr<Config> config,
                                     std::shared_ptr<Log> log)
    : config_(config), log_(log) {}

DataServerService::~DataServerService() {
  requestHandler_->reset();
  shutdownCallback.reset();
  connectCallback.reset();
  recvCallback.reset();
  sendCallback.reset();
  if (proxyCon_) {
    proxyCon_->shutdown();
  }
  if (proxyClient_) {
    proxyClient_->shutdown();
    proxyClient_.reset();
  }
}

bool DataServerService::init() {
  requestHandler_ = std::make_shared<DataServiceRequestHandler>(shared_from_this());
  requestHandler_->start();
  worker_ = std::make_shared<ReplicateWorker>(shared_from_this());
  worker_->start();
  host_ = config_->get_ip();
  port_ = config_->get_port();
  proxyClient_ = std::make_shared<Client>(1, 32);
  if ((proxyClient_->init()) != 0) {
    return -1;
  }
  int buffer_size_ = 65536;
  int buffer_number_ = 64;
  chunkMgr_ = std::make_shared<ChunkPool>(proxyClient_.get(), buffer_size_,
                                          buffer_number_);
  proxyClient_->set_chunk_mgr(chunkMgr_.get());

  shutdownCallback = std::make_shared<ServiceShutdownCallback>();
  connectCallback =
      std::make_shared<ServiceConnectCallback>(shared_from_this());
  recvCallback =
      std::make_shared<ServiceRecvCallback>(chunkMgr_, requestHandler_, worker_);
  sendCallback = std::make_shared<ServiceSendCallback>(chunkMgr_);
  proxyClient_->set_shutdown_callback(shutdownCallback.get());
  proxyClient_->set_connected_callback(connectCallback.get());
  proxyClient_->set_recv_callback(recvCallback.get());
  proxyClient_->set_send_callback(sendCallback.get());

  proxyClient_->start();
  //TODO get service ip & port from config
  int res = proxyClient_->connect(config_->get_proxy_ip().c_str(), "12340");
  std::unique_lock<std::mutex> lk(con_mtx);
  while (!proxyConnected) {
    std::cout<<"NetworkClient from " << host_ <<" wait to be connected to proxy " << config_->get_proxy_ip()<<std::endl;
    con_v.wait(lk);
  }
  registerDataServer();
  return 0;
}

void DataServerService::setConnection(Connection *con) {
  std::cout<<"NetworkClient from " << host_ <<" connected to proxy " << config_->get_proxy_ip()<<std::endl;
  std::unique_lock<std::mutex> lk(con_mtx);
  proxyCon_ = con;
  proxyConnected = true;
  con_v.notify_all();
  lk.unlock();
}

void DataServerService::registerDataServer() {
  while (true) {
    ReplicaRequestContext rc = {};
    rc.type = REGISTER;
    rc.rid = rid_++;
    rc.node = host_;
    rc.port = port_;
    auto request = std::make_shared<ReplicaRequest>(rc);
    requestHandler_->addTask(request);
    try {
      auto rrc = requestHandler_->get(request);
      break;
    } catch (const char *ex) {
      std::cout << "Failed to register data server, try again" << std::endl;
    }
  }
}

void DataServerService::handle_replica_msg(std::shared_ptr<ReplicaRequestReply> reply) {
  auto rrc = reply->get_rrc();
  if (rrc.type == REPLICATE) {
    cout << "handle replicate" << endl;
    auto rc = ReplicaRequestContext();
    rc.type = REPLICA_REPLY;
    rc.key = rrc.key;
    rc.node = rrc.node;
    rc.port = rrc.port;
    rc.rid = rrc.rid;
    rc.src_address = rrc.src_address;
    auto rr = std::make_shared<ReplicaRequest>(rc);
    std::shared_ptr<DataChannel> channel = getChannel(rrc.node, rrc.port);
    std::shared_ptr<NetworkClient> networkClient = channel->networkClient;
    std::shared_ptr<RequestHandler> requestHandler = channel->requestHandler;
    // RequestContext rc = {};
    // rc.type = PUT;
    // rc.rid = rid_++;
    // rc.size = rrc.size;
    // rc.address = 0;
    // rc.src_address = rrc.src_address;
    // rc.src_rkey = networkClient->get_rkey();
    // rc.key = rrc.key;
    // auto request = std::make_shared<Request>(rc);
    // requestHandler->addTask(request);
    // auto res = requestHandler->wait(request);
    // auto res = proxyRequestHandler_->get(pRequest);
    RequestContext dataRc = {};
    dataRc.type = REPLICATE_PUT;
    dataRc.rid = rid_++;
    dataRc.key = rrc.key;
    dataRc.size = rrc.size;
    dataRc.src_address = networkClient->get_dram_buffer(reinterpret_cast<char *>(rrc.src_address), rrc.size);
    dataRc.src_rkey = networkClient->get_rkey();
    auto dataRequest = std::make_shared<Request>(dataRc);
    requestHandler->addTask(dataRequest);
    requestHandler->wait(dataRequest);
    networkClient->reclaim_dram_buffer(rc.src_address, rc.size);
    rr->encode();
    send(rr->data_, rr->size_);
  }
}

void DataServerService::send(const char *data, uint64_t size) {
  auto chunk = chunkMgr_->get(proxyCon_);
  std::memcpy(reinterpret_cast<char *>(chunk->buffer), data, size);
  chunk->size = size;
  proxyCon_->send(chunk);
}

void DataServerService::addTask(std::shared_ptr<ReplicaRequest> request) {
  requestHandler_->addTask(request);
}

std::shared_ptr<DataChannel> DataServerService::getChannel(string node, string port) {
  std::lock_guard<std::mutex> lk(channel_mtx);
  if (channels.count(node+port)) {
    return channels.find(node+port)->second;
  } else {
    std::shared_ptr<DataChannel> channel = std::make_shared<DataChannel>();
    channel->networkClient = std::make_shared<NetworkClient>(node, port);
    channel->requestHandler = std::make_shared<RequestHandler>(channel->networkClient);
    channel->networkClient->init(channel->requestHandler);
    channel->requestHandler->start();
    channels.insert(make_pair(node+port, channel));
    return channel;
  }
}