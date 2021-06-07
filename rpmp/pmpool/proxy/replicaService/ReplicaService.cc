#include <iostream>

#include "ReplicaService.h"
#include "pmpool/proxy/metastore/JsonUtil.h"

using namespace std;

ReplicaRecvCallback::ReplicaRecvCallback(std::shared_ptr<ReplicaService> service,
                                     std::shared_ptr<ChunkMgr> chunkMgr)
    : service_(service), chunkMgr_(chunkMgr) {}

void ReplicaRecvCallback::operator()(void* param_1, void* param_2) {
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

ReplicaService::ReplicaService(std::shared_ptr<Config> config, std::shared_ptr<RLog> log, std::shared_ptr<Proxy> proxyServer, std::shared_ptr<MetastoreFacade> metastore) :
 config_(config), log_(log), proxyServer_(proxyServer), metastore_(metastore) {}

ReplicaService::~ReplicaService() {
  worker_->stop();
  worker_->join();
}

void ReplicaService::enqueue_recv_msg(std::shared_ptr<ReplicaRequest> request) {
  worker_->addTask(request);
}

/**
 * Update data status once it's been put to the node successfully
 **/
void ReplicaService::updateRecord(uint64_t key, PhysicalNode node){
  string rawJson = metastore_->get(to_string(key));
  #ifdef DEBUG
  cout<<rawJson<<endl;
  #endif
  const auto rawJsonLength = static_cast<int>(rawJson.length());
  JSONCPP_STRING err;
  Json::Value root;
  
  Json::CharReaderBuilder builder;
  const std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
  if (!reader->parse(rawJson.c_str(), rawJson.c_str() + rawJsonLength, &root,
                      &err)) {
    std::cout << "Error occurred in UpdateRecord." << std::endl;
  }

  Json::Value recordArray = root["data"];
  Json::ArrayIndex size = recordArray.size(); 
  Json::Value data;
  
  for(Json::ArrayIndex i = 0; i < size; i++){
    data[i][NODE] = recordArray[i][NODE];
    if(data[i][NODE] == node.getIp()){
      data[i][STATUS] = VALID;
    }else{
      data[i][STATUS] = recordArray[i][STATUS];
    }
  }

  root["data"] = data;
  string json_str = rootToString(root);
  metastore_->set(to_string(key), json_str);
}

void ReplicaService::handle_recv_msg(std::shared_ptr<ReplicaRequest> request) {
  ReplicaRequestContext rc = request->get_rc();
  auto rrc = ReplicaRequestReplyContext();
  switch(rc.type) {
    case REPLICATE: {
      //The message received means the data has been put to node, change status from pending to valid
      uint32_t replicaNum = dataReplica_ < proxyServer_->getNodeNum() ? dataReplica_ : proxyServer_->getNodeNum();
      uint32_t minReplica = replicaNum < minReplica_ ? replicaNum : minReplica_;
      if (getReplica(rc.key).size() != 0) {
        removeReplica(rc.key);
      }
      addReplica(rc.key, rc.node);
      updateRecord(rc.key, rc.node);
      unordered_set<PhysicalNode, PhysicalNodeHash> nodes = proxyServer_->getNodes(rc.key);
      if (nodes.count(rc.node)) {
        nodes.erase(rc.node);
      }

      rrc.type = REPLICATE;
      rrc.rid = rid_++;
      rrc.key = rc.key;
      rrc.size = rc.size;
      rrc.nodes = nodes;
      rrc.con = rc.con;
      rrc.src_address = rc.src_address;
      auto reply = std::make_shared<ReplicaRequestReply>(rrc);
      reply->encode();
      auto ck = chunkMgr_->get(rrc.con);
      memcpy(reinterpret_cast<char *>(ck->buffer), reply->data_, reply->size_);
      ck->size = reply->size_;
      rrc.con->send(ck);
      if (getReplica(rc.key).size() >= minReplica) {
        proxyServer_->notifyClient(rc.key);
      }
      break;
    }
    case REPLICA_REPLY : {
      uint32_t replicaNum = dataReplica_ < proxyServer_->getNodeNum() ? dataReplica_ : proxyServer_->getNodeNum();
      uint32_t minReplica = replicaNum < minReplica_ ? replicaNum : minReplica_;
      addReplica(rc.key, rc.node);
      updateRecord(rc.key, rc.node);
      if (getReplica(rc.key).size() == minReplica) {
        proxyServer_->notifyClient(rc.key);
      }
      break;
    }
  }
}

bool ReplicaService::startService() {
  int worker_number = config_->get_network_worker_num();
  int buffer_number = config_->get_network_buffer_num();
  int buffer_size = config_->get_network_buffer_size();
  minReplica_ = config_->get_data_minReplica();
  dataReplica_ = config_->get_data_replica();
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
  server_->listen(config_->get_current_proxy_addr().c_str(), config_->get_replica_service_port().c_str());
  log_->get_console_log()->info("ReplicaService started at {0}:{1}", config_->get_current_proxy_addr(), config_->get_replica_service_port());
  return true;
}

void ReplicaService::wait() {
  server_->wait();
}

void ReplicaService::addReplica(uint64_t key, PhysicalNode node) {
  proxyServer_->addReplica(key, node);
}

void ReplicaService::removeReplica(uint64_t key) {
  proxyServer_->removeReplica(key);
}

std::unordered_set<PhysicalNode, PhysicalNodeHash> ReplicaService::getReplica(uint64_t key) {
  return proxyServer_->getReplica(key);
}
