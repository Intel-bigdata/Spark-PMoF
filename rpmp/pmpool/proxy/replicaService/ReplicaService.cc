#include <iostream>

#include "ReplicaService.h"

using namespace std;

ReplicaRecvCallback::ReplicaRecvCallback(std::shared_ptr<ReplicaService> service,
                                     std::shared_ptr<ChunkMgr> chunkMgr)
    : service_(service), chunkMgr_(chunkMgr) {}

void ReplicaRecvCallback::operator()(void* param_1, void* param_2) {
  cout << "ReplicaRecvCallback " << endl;
  int mid = *static_cast<int*>(param_1);
  auto chunk = chunkMgr_->get(mid);
  auto request = std::make_shared<ReplicaRequest>(
      reinterpret_cast<char*>(chunk->buffer), chunk->size,
      reinterpret_cast<Connection*>(chunk->con));
  request->decode();
  service_->enqueue_recv_msg(request);
  chunkMgr_->reclaim(chunk, static_cast<Connection*>(chunk->con));
}

ReplicaSendCallback::ReplicaSendCallback(std::shared_ptr<ChunkMgr> chunkMgr)
    : chunkMgr_(chunkMgr) {}

void ReplicaSendCallback::operator()(void* param_1, void* param_2) {
  int mid = *static_cast<int*>(param_1);
  auto chunk = chunkMgr_->get(mid);
  auto connection = static_cast<Connection*>(chunk->con);
  chunkMgr_->reclaim(chunk, connection);
}

ReplicaWorker::ReplicaWorker(std::shared_ptr<ReplicaService> service) : service_(service) {}

void ReplicaWorker::addTask(std::shared_ptr<ReplicaRequest> request) {
  pendingRecvRequestQueue_.enqueue(request);
}

void ReplicaWorker::abort() {}

int ReplicaWorker::entry() {
  std::shared_ptr<ReplicaRequest> request;
  bool res = pendingRecvRequestQueue_.wait_dequeue_timed(
      request, std::chrono::milliseconds(1000));
  if (res) {
    service_->handle_recv_msg(request);
  }
  return 0;
}

ReplicaService::ReplicaService(std::shared_ptr<Config> config, std::shared_ptr<Log> log, std::shared_ptr<Proxy> proxyServer) :
 config_(config), log_(log), proxyServer_(proxyServer) {}

ReplicaService::~ReplicaService() {
    worker_->stop();
    worker_->join();
}

void ReplicaService::enqueue_recv_msg(std::shared_ptr<ReplicaRequest> request) {
  worker_->addTask(request);
}

void ReplicaService::handle_recv_msg(std::shared_ptr<ReplicaRequest> request) {
  ReplicaRequestContext rc = request->get_rc();
  auto rrc = ReplicaRequestReplyContext();
  switch(rc.type) {
    case REGISTER: {
      PhysicalNode* physicalNode = new PhysicalNode(rc.node);
      proxyServer_->addNode(physicalNode);
      rrc.type = rc.type;
      rrc.success = 0;
      rrc.rid = rc.rid;
      rrc.con = rc.con;
      dataServerConnections_.insert(pair<std::string, Connection*>(rc.node, rc.con));
      std::shared_ptr<ReplicaRequestReply> requestReply = std::make_shared<ReplicaRequestReply>(rrc);
      requestReply->encode();
      auto ck = chunkMgr_->get(rrc.con);
      memcpy(reinterpret_cast<char*>(ck->buffer), requestReply->data_,
             requestReply->size_);
      ck->size = requestReply->size_;
      rrc.con->send(ck);
      break;
    }
    case REPLICATE: {
      cout << "put reply" << endl;
      rc.type = REPLICATE;
      rc.rid = rid_++;
      rc.key = rrc.key;
      rc.node = rrc.node;
      rc.src_address = rrc.src_address;
      auto request = std::make_shared<ReplicaRequest>(rc);
      request->encode();
      auto ck = chunkMgr_->get(rrc.con);
      memcpy(reinterpret_cast<char*>(ck->buffer), request->data_,
             request->size_);
      ck->size = request->size_;
      std::unique_lock<std::mutex> lk(prrcMtx);
      prrcMap_[rc.key] = request;
      lk.unlock();
      rrc.con->send(ck);
      break;
    }
    case REPLICA_REPLY : {
      cout << "replica reply" << endl;
      addReplica(rc.key, rc.node);
      if (getReplica(rc.key).size() >= 2) {
        auto request = prrcMap_[rc.key];
        auto rc = request->get_rc();
        // rrc.type = rc.type;
        // rrc.success = 0;
        // rrc.rid = rc.rid;
        // rrc.con = rc.con;
        // std::shared_ptr<ProxyRequestReply> requestReply =
        //     std::make_shared<ProxyRequestReply>(rrc);
        // requestReply->encode();
        auto ck = chunkMgr_->get(rc.con);
        memcpy(reinterpret_cast<char*>(ck->buffer), request->data_,
               request->size_);
        ck->size = request->size_;
        rrc.con->send(ck);
        break;
      }
    }
  }
}

bool ReplicaService::startService() {
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

  recvCallback_ = std::make_shared<ReplicaRecvCallback>(shared_from_this(), chunkMgr_);
  sendCallback_ = std::make_shared<ReplicaSendCallback>(chunkMgr_);
  shutdownCallback_ = std::make_shared<ReplicaShutdownCallback>();
  connectCallback_ = std::make_shared<ReplicaConnectCallback>();

  worker_ = std::make_shared<ReplicaWorker>(shared_from_this());
  worker_->start();

  server_->set_recv_callback(recvCallback_.get());
  server_->set_send_callback(sendCallback_.get());
  server_->set_connected_callback(connectCallback_.get());
  server_->set_shutdown_callback(shutdownCallback_.get());

  server_->start();
  //TODO replica service port
  server_->listen(config_->get_proxy_ip().c_str(), "12340");
  log_->get_console_log()->info("ReplicaService started at {0}", config_->get_proxy_ip());
  return true;
}

void ReplicaService::wait() {
  server_->wait();
}

void ReplicaService::addReplica(uint64_t key, std::string node) {
  // proxyServer_->addReplica(key, node);
}

void ReplicaService::removeReplica(uint64_t key) {
  std::lock_guard<std::mutex> lk(replica_mtx);
  replicaMap_.erase(key);
}

std::unordered_set<std::string> ReplicaService::getReplica(uint64_t key) {
  std::lock_guard<std::mutex> lk(replica_mtx);
  return replicaMap_[key];
}
