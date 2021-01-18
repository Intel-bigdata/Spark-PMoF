#include "HPNL/Callback.h"
#include "HPNL/Connection.h"
#include "HPNL/ChunkMgr.h"
#include "HPNL/Server.h"

#include <iostream>
#include <string>
#include "Proxy.h"

using namespace std;

Proxy::Proxy(std::shared_ptr<Config> config, std::shared_ptr<Log> log) :
 config_(config), log_(log) {}

Proxy::~Proxy() {
    // worker_->stop();
    // worker_->join();
}

bool Proxy::launchServer() {
  //TODO get load balance from config
  loadBalanceFactor_ = 5;
  consistentHash_ = std::make_shared<ConsistentHash>();
  dataServerPort_ = config_->get_port();
  dataReplica_ = config_->get_data_replica();
  clientService_ = std::make_shared<ClientService>(config_, log_, shared_from_this());
  clientService_->startService();
  replicaService_ = std::make_shared<ReplicaService>(config_, log_, shared_from_this());
  replicaService_->startService();
}

void Proxy::addNode(PhysicalNode* physicalNode) {
  consistentHash_->addNode(*physicalNode, loadBalanceFactor_);
}

vector<PhysicalNode> Proxy::getNodes(uint64_t key) {
  return consistentHash_->getNodes(key, dataReplica_);
}

uint32_t Proxy::getNodeNum() {
  return consistentHash_->getNodeNum();
}

void Proxy::addReplica(uint64_t key, std::string node, std::string port) {
  std::lock_guard<std::mutex> lk(replica_mtx);
  replicaMap_[key].insert(node+port);
}

void Proxy::removeReplica(uint64_t key) {
  std::lock_guard<std::mutex> lk(replica_mtx);
  replicaMap_.erase(key);
}

std::unordered_set<std::string> Proxy::getReplica(uint64_t key) {
  std::lock_guard<std::mutex> lk(replica_mtx);
  cout << "replica map size: " << replicaMap_[key].size() << endl;
  return replicaMap_[key];
}

void Proxy::notifyClient(uint64_t key) {
  clientService_->notifyClient(key);
}

void Proxy::wait() {
    clientService_->wait();
    replicaService_->wait();
}