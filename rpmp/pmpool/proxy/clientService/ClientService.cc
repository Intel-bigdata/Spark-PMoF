#include <iostream>

#include "pmpool/proxy/clientService/ClientService.h"
#include "pmpool/proxy/metastore/JsonUtil.h"

using namespace std;

ProxyRecvCallback::ProxyRecvCallback(std::shared_ptr<ClientService> service,
                                     std::shared_ptr<ChunkMgr> chunkMgr)
    : service_(service), chunkMgr_(chunkMgr) {}

void ProxyRecvCallback::operator()(void* param_1, void* param_2) {
  int mid = *static_cast<int*>(param_1);
  auto chunk = chunkMgr_->get(mid);
  auto request = std::make_shared<ProxyRequest>(
      reinterpret_cast<char*>(chunk->buffer), chunk->size,
      reinterpret_cast<Connection*>(chunk->con));
  request->decode();
  ProxyRequestMsg* requestMsg = (ProxyRequestMsg*)(request->getData());
  if (requestMsg->type != 0) {
    service_->enqueue_recv_msg(request);
  } else {
    std::cout << "[RecvCallback::RecvCallback][" << requestMsg->type
              << "] size is " << chunk->size << std::endl;
    for (int i = 0; i < chunk->size; i++) {
      printf("%X ", *(request->getData() + i));
    }
    printf("\n");
  }
  chunkMgr_->reclaim(chunk, static_cast<Connection*>(chunk->con));
}

ProxySendCallback::ProxySendCallback(std::shared_ptr<ChunkMgr> chunkMgr)
    : chunkMgr_(chunkMgr) {}

void ProxySendCallback::operator()(void* param_1, void* param_2) {
  int mid = *static_cast<int*>(param_1);
  auto chunk = chunkMgr_->get(mid);
  auto connection = static_cast<Connection*>(chunk->con);
  chunkMgr_->reclaim(chunk, connection);
}

Worker::Worker(std::shared_ptr<ClientService> service) : service_(service) {}

void Worker::addTask(std::shared_ptr<ProxyRequest> request) {
  pendingRecvRequestQueue_.enqueue(request);
}

void Worker::abort() {}

int Worker::entry() {
  std::shared_ptr<ProxyRequest> request;
  bool res = pendingRecvRequestQueue_.wait_dequeue_timed(
      request, std::chrono::milliseconds(1000));
  if (res) {
    service_->handle_recv_msg(request);
  }
  return 0;
}

ClientService::ClientService(std::shared_ptr<Config> config, std::shared_ptr<RLog> log, std::shared_ptr<Proxy> proxyServer, std::shared_ptr<MetastoreFacade> metastore) :
 config_(config), log_(log) ,proxyServer_(proxyServer), metastore_(metastore){}

ClientService::~ClientService() {
  // for (auto worker : workers_) {
  //   worker->stop();
  //   worker->join();
  // }
  worker_->stop();
  worker_->join();
}

void ClientService::constructJobStatus(Json::Value record, uint64_t key){
  Json::Value root;
  Json::Value data;
  data[0][NODE] = record[NODE];
  data[0][STATUS] = record[STATUS];
  data[0][SIZE] = record[SIZE];
  root["data"] = data;
  string json_str = rootToString(root);
  #ifdef DEBUG
  cout<<"ClientService::constructJobStatus::json_str:"<<json_str<<endl;
  #endif
  metastore_->set(to_string(key), json_str);
}

/**
 * Add data's location and status
 **/
void ClientService::addRecords(uint64_t key, unordered_set<PhysicalNode, PhysicalNodeHash> nodes){
  #ifdef DEBUG
  cout<<"ClientService::addRecords::key: "<<key<<endl;
  #endif
  Json::Value root;
  Json::Value data;
  int index = 0;
  for(auto node: nodes){
    data[index][NODE] = node.getIp();
    data[index][STATUS] = PENDING;
    data[index][SIZE] = "0";
    index++;
  }
  root["data"] = data;
  string json_str = rootToString(root);
  metastore_->set(to_string(key), json_str);
}

void ClientService::enqueue_recv_msg(std::shared_ptr<ProxyRequest> request) {
  worker_->addTask(request);
  // ProxyRequestContext rc = request->get_rc();
  // workers_[rc.rid % 4]->addTask(request);
}

void ClientService::handle_recv_msg(std::shared_ptr<ProxyRequest> request) {
  ProxyRequestContext rc = request->get_rc();
  auto rrc = ProxyRequestReplyContext();
  switch(rc.type) {
    case GET_HOSTS: {
      unordered_set<PhysicalNode, PhysicalNodeHash> nodes = proxyServer_->getNodes(rc.key);
#ifdef DEBUG
      for(auto node: nodes){
        cout<<"Get_HOSTS::"<<node.getIp()<<endl;
      }
#endif
      rrc.type = rc.type;
      rrc.key = rc.key;
      rrc.success = 0;
      rrc.rid = rc.rid;
      rrc.nodes = nodes;
      rrc.con = rc.con;
      std::shared_ptr<ProxyRequestReply> requestReply =
          std::make_shared<ProxyRequestReply>(rrc);

      requestReply->encode();
      auto ck = chunkMgr_->get(rrc.con);
      memcpy(reinterpret_cast<char*>(ck->buffer), requestReply->data_,
             requestReply->size_);
      ck->size = requestReply->size_;
      std::unique_lock<std::mutex> lk(prrcMtx);
      prrcMap_[rc.key] = requestReply;
      lk.unlock();
      rrc.con->send(ck);
      addRecords(rc.key, nodes);
      break;
    }
    case GET_REPLICA: {
      auto nodes = proxyServer_->getReplica(rc.key);
      rrc.type = rc.type;
      rrc.key = rc.key;
      rrc.success = 0;
      rrc.rid = rc.rid;
      rrc.nodes = nodes;
      rrc.con = rc.con;
      std::shared_ptr<ProxyRequestReply> requestReply =
          std::make_shared<ProxyRequestReply>(rrc);

      requestReply->encode();
      auto ck = chunkMgr_->get(rrc.con);
      memcpy(reinterpret_cast<char*>(ck->buffer), requestReply->data_,
             requestReply->size_);
      ck->size = requestReply->size_;
      rrc.con->send(ck);
      break;
    }
  }
}

bool ClientService::startService() {
  dataServerPort_ = config_->get_port();
  int worker_number = config_->get_network_worker_num();
  int buffer_number = config_->get_network_buffer_num();
  int buffer_size = config_->get_network_buffer_size();
  server_ = std::make_shared<Server>(worker_number, buffer_number);
  if(server_->init() != 0){
    cout<<"HPNL server init failed"<<endl;
    return false;
  }
  chunkMgr_ = std::make_shared<ChunkPool>(server_.get(), buffer_size, buffer_number);
  server_->set_chunk_mgr(chunkMgr_.get());

  recvCallback_ = std::make_shared<ProxyRecvCallback>(shared_from_this(), chunkMgr_);
  sendCallback_ = std::make_shared<ProxySendCallback>(chunkMgr_);
  shutdownCallback_ = std::make_shared<ProxyShutdownCallback>();
  connectCallback_ = std::make_shared<ProxyConnectCallback>();

  // for (int i = 0; i < 4; i++) {
  //   auto worker = std::make_shared<Worker>(shared_from_this());
  //   worker->start();
  //   workers_.push_back(std::move(worker));
  // }
  worker_ = std::make_shared<Worker>(shared_from_this());
  worker_->start();

  server_->set_recv_callback(recvCallback_.get());
  server_->set_send_callback(sendCallback_.get());
  server_->set_connected_callback(connectCallback_.get());
  server_->set_shutdown_callback(shutdownCallback_.get());

  server_->start();
  server_->listen(config_->get_current_proxy_addr().c_str(), config_->get_client_service_port().c_str());
  log_->get_console_log()->info("Proxy client service started at {0}:{1}", config_->get_current_proxy_addr(), config_->get_client_service_port());
  return true;
}

void ClientService::notifyClient(uint64_t key) {
  std::unique_lock<std::mutex> lk(prrcMtx);
  if (prrcMap_.count(key)) {
    auto reply = prrcMap_[key];
    auto rrc = reply->get_rrc();
    auto ck = chunkMgr_->get(rrc.con);
    memcpy(reinterpret_cast<char*>(ck->buffer), reply->data_, reply->size_);
    ck->size = reply->size_;
    rrc.con->send(ck);
    prrcMap_.erase(key);
  } else {
    cout << "Proxy request reply does not exist" << endl;
  }
}

void ClientService::wait() {
  server_->wait();
}
