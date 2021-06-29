#include "DataServerService.h"
#include "pmpool/client/NetworkClient.h"

ServiceConnectCallback::ServiceConnectCallback(std::shared_ptr<DataServerService> service) {
    service_ = service;
}

void ServiceConnectCallback::operator()(void *param_1, void *param_2) {
#ifdef DEBUG
  cout << "ServiceConnectCallback" << endl;
#endif
  auto connection = static_cast<Connection*>(param_1);
  service_->setConnection(connection);
}

ServiceRecvCallback::ServiceRecvCallback(
    std::shared_ptr<ChunkMgr> chunkMgr,
    std::shared_ptr<DataServiceRequestHandler> requestHandler,
    std::shared_ptr<DataServerService> service)
    : chunkMgr_(chunkMgr), requestHandler_(requestHandler), service_(service) {}

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
    service_->enqueue_recv_msg(requestReply);
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
                                     std::shared_ptr<RLog> log, 
                                     std::shared_ptr<Protocol> protocol)
    : config_(config), log_(log), protocol_(protocol) {}

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

int worker_num = 10;
bool DataServerService::init() {
  requestHandler_ = std::make_shared<DataServiceRequestHandler>(shared_from_this());
  requestHandler_->start();
  for (int i = 0; i < worker_num; i++) {
    auto worker = std::make_shared<ReplicateWorker>(shared_from_this());
    worker->start();
    workers_.push_back(std::move(worker));
  }
  /// The address is passed from start script.
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
      std::make_shared<ServiceRecvCallback>(chunkMgr_, requestHandler_, shared_from_this());
  sendCallback = std::make_shared<ServiceSendCallback>(chunkMgr_);
  proxyClient_->set_shutdown_callback(shutdownCallback.get());
  proxyClient_->set_connected_callback(connectCallback.get());
  proxyClient_->set_recv_callback(recvCallback.get());
  proxyClient_->set_send_callback(sendCallback.get());

  proxyClient_->start();
  int res = build_connection();
  if (res == -1) {
    log_->get_console_log()->info("Failed to register data server!");
    return false;
  }
  registerDataServer();
  return true;
}

int DataServerService::build_connection() {
  std::vector<std::string> proxy_addrs = config_->get_proxy_addrs();
  for (int i = 0; i< proxy_addrs.size(); i++) {
    auto res = build_connection(proxy_addrs[i]);
    if (res == 0) {
      return 0;
    }
  }
  log_->get_console_log()->info("Failed to connect to an active proxy!");
  return -1;
}

int DataServerService::build_connection(std::string proxy_addr) {
  // reset to false to consider the possible re-connection to a new active proxy.
  proxyConnected = false;
  std::string replica_service_port = config_->get_replica_service_port();
  log_->get_console_log()->info("Trying to connect to " + proxy_addr + ":" + replica_service_port);
  // res can be 0 even though remote proxy is shut down.
  int res = proxyClient_->connect(proxy_addr.c_str(), replica_service_port.c_str());
  if (res == -1) {
    return -1;
  }
  // wait for ConnectedCallback to be executed.
  std::unique_lock<std::mutex> lk(con_mtx);
  while (!proxyConnected) {
    if (con_v.wait_for(lk, std::chrono::seconds(3)) == std::cv_status::timeout) {
      break;
    }
  }
  if (!proxyConnected) {
    return -1;
  }
  log_->get_console_log()->info("Successfully connected to active proxy: " + proxy_addr);
  return 0;
}

void DataServerService::setConnection(Connection *con) {
  std::unique_lock<std::mutex> lk(con_mtx);
  proxyCon_ = con;
  proxyConnected = true;
  con_v.notify_all();
  lk.unlock();
}

void DataServerService::handle_replica_msg(std::shared_ptr<ReplicaRequestReply> reply) {
  auto rrc = reply->get_rrc();
  if (rrc.type == REPLICATE) {
    auto rc = ReplicaRequestContext();
    rc.type = REPLICA_REPLY;
    rc.key = rrc.key;
    rc.rid = rrc.rid;
    rc.size = rrc.size;
    rc.src_address = rrc.src_address;
    for (auto node : rrc.nodes) {
      rc.node = node;
      auto rr = std::make_shared<ReplicaRequest>(rc);
      std::shared_ptr<DataChannel> channel = getChannel(node.getIp(), node.getPort());
      std::shared_ptr<NetworkClient> networkClient = channel->networkClient;
      std::shared_ptr<RequestHandler> requestHandler = channel->requestHandler;
      RequestContext dataRc = {};
      dataRc.type = REPLICATE_PUT;
      dataRc.rid = rid_++;
      dataRc.key = rrc.key;
      dataRc.size = rrc.size;
      try {
        dataRc.src_address = networkClient->get_dram_buffer(
            reinterpret_cast<char *>(rrc.src_address), rrc.size);
      } catch (const char *msg) {
        std::cout << "Replication work: " << msg << std::endl;
      }
      dataRc.src_rkey = networkClient->get_rkey();
      auto dataRequest = std::make_shared<Request>(dataRc);
      requestHandler->addTask(dataRequest);
      requestHandler->wait(dataRequest);
      networkClient->reclaim_dram_buffer(dataRc.src_address, dataRc.size);
      rr->encode();
      send(rr->data_, rr->size_);
    }
    protocol_->reclaim_dram_buffer(rrc.key);
  }else if(rrc.type == REPLICATE_DIRECT){
    auto rc = ReplicaRequestContext();
    rc.type = REPLICA_REPLY;
    rc.key = rrc.key;
    rc.rid = rrc.rid;
    rc.src_address = rrc.src_address;
    for (auto node : rrc.nodes) {
      rc.node = node;
      auto rr = std::make_shared<ReplicaRequest>(rc);
      std::shared_ptr<DataChannel> channel = getChannel(node.getIp(), node.getPort());
      std::shared_ptr<NetworkClient> networkClient = channel->networkClient;
      std::shared_ptr<RequestHandler> requestHandler = channel->requestHandler;
      RequestContext dataRc = {};
      dataRc.type = REPLICATE_PUT;
      dataRc.rid = rid_++;
      dataRc.key = rrc.key;
      dataRc.size = rrc.size;
      //Get data address by allocatorProxy
      auto bml = protocol_->getAllocatorProxy()->get_cached_chunk(rrc.key);
      uint64_t address;
      if (bml.size() == 1) {
        address = protocol_->getAllocatorProxy()->get_virtual_address(bml[0].address);
      } else {
        fprintf(stderr, "key %lu has zero or more than one BlockMeta\n",
                rrc.key);
        return;
      }
      try {
        dataRc.src_address = networkClient->get_dram_buffer(
            reinterpret_cast<char *>(address), rrc.size);
      } catch (const char *msg) {
        std::cout << "Replication work: " << msg << std::endl;
      }
      dataRc.src_rkey = networkClient->get_rkey();
      auto dataRequest = std::make_shared<Request>(dataRc);
      requestHandler->addTask(dataRequest);
      requestHandler->wait(dataRequest);
      networkClient->reclaim_dram_buffer(dataRc.src_address, dataRc.size);
      rr->encode();
      send(rr->data_, rr->size_);
    }
    protocol_->reclaim_dram_buffer(rrc.key);
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

void DataServerService::enqueue_recv_msg(std::shared_ptr<ReplicaRequestReply> reply) {
  auto rrc = reply->get_rrc();
  workers_[rrc.rid % worker_num]->addTask(reply);
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

void DataServerService::registerDataServer() {
  while (true) {
    ReplicaRequestContext rc = {};
    rc.type = REGISTER;
    rc.rid = rid_++;
    rc.node = {host_, port_};
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

void DataServerService::registerDataServer(std::string proxy_addr) {
  build_connection(proxy_addr);
  while (true) {
    ReplicaRequestContext rc = {};
    rc.type = REGISTER;
    rc.rid = rid_++;
    rc.node = {host_, port_};
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