#include "HPNL/Callback.h"
#include "HPNL/Connection.h"
#include "HPNL/ChunkMgr.h"
#include "HPNL/Server.h"

#include <iostream>
#include <string>
#include <thread>
#include "Proxy.h"

#include "hiredis/hiredis.h"
#include "pmpool/proxy/metastore/Redis.h"
#include "json/json.h"

using namespace std;

Proxy::Proxy(std::shared_ptr<Config> config, std::shared_ptr<Log> log, std::shared_ptr<Redis> redis) :
 config_(config), log_(log), redis_(redis) {}

Proxy::~Proxy() {
    // worker_->stop();
    // worker_->join();
}

bool Proxy::launchServer(string current_host_ip) {
    if (isActiveProxy(current_host_ip)) {
      return launchActiveService();
    }
  return launchStandbyService();
}

bool isActiveProxy(string current_host_ip) {
  if (current_host_ip.empty()) {
    return true;
  }
  vector<string> proxies = config_->get_proxy_ips();
  // Only proxy node will trigger the launch. So if there is
  // only one proxy configured, the current node is active proxy.
  if (proxies.size() == 1) {
    return true;
  }
  if (std::find(proxies.begin(), proxies.end(),
                current_host_ip) == proxies.end()) {
    log_->get_file_log()->error("Incorrect host ip is given!");
    return false;
  }
  // All proxy nodes share same config file. The first node
  // in the config will serve as active proxy. Other nodes
  // will be standby.
  if (proxies[0] == current_host_ip) {
    return true;
  }
  return false;
}

bool Proxy::launchActiveService() {
  nodeManager_ = std::make_shared<NodeManager>(config_, log_, redis_);
  nodeManager_->init();
  loadBalanceFactor_ = config_->get_load_balance_factor();
  consistentHash_ = std::make_shared<ConsistentHash>();
  dataServerPort_ = config_->get_port();
  dataReplica_ = config_->get_data_replica();
  clientService_ = std::make_shared<ClientService>(config_, log_, shared_from_this());
  clientService_->startService();
  replicaService_ = std::make_shared<ReplicaService>(config_, log_, shared_from_this());
  replicaService_->startService();
  return true;
}

bool Proxy::launchStandbyService() {
  vector<string> proxies = config_->get_proxy_ips();

}

void Proxy::addNode(PhysicalNode physicalNode) {
  consistentHash_->addNode(physicalNode, loadBalanceFactor_);
}

unordered_set<PhysicalNode, PhysicalNodeHash> Proxy::getNodes(uint64_t key) {
  return consistentHash_->getNodes(key, dataReplica_);
}

uint32_t Proxy::getNodeNum() {
  return consistentHash_->getNodeNum();
}

void Proxy::addReplica(uint64_t key, PhysicalNode node) {
  std::lock_guard<std::mutex> lk(replica_mtx);
  replicaMap_[key].insert(node);
}

void Proxy::removeReplica(uint64_t key) {
  std::lock_guard<std::mutex> lk(replica_mtx);
  replicaMap_.erase(key);
}

std::unordered_set<PhysicalNode,PhysicalNodeHash> Proxy::getReplica(uint64_t key) {
  std::lock_guard<std::mutex> lk(replica_mtx);
  return replicaMap_[key];
}

void Proxy::notifyClient(uint64_t key) {
  clientService_->notifyClient(key);
}

void Proxy::wait() {
    clientService_->wait();
    replicaService_->wait();
}