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

Proxy::Proxy(std::shared_ptr<Config> config, std::shared_ptr<Log> log, std::shared_ptr<Redis> redis, string currentHostAddr) :
 config_(config), log_(log), redis_(redis), currentHostAddr_(currentHostAddr) {}

Proxy::~Proxy() {
    // worker_->stop();
    // worker_->join();
}

bool Proxy::launchServer() {
  if (isActiveProxy(currentHostAddr_)) {
    return launchActiveService();
  }
  return launchStandbyService();
}

/**
 * Judge whether the current proxy is active according to proxy
 * hosts order in the configuration.
 *
 * @param currentHostAddr the host address of current proxy.
 * @return  true if the current proxy should be active.
 */
bool isActiveProxy(string currentHostAddr) {
  if (currentHostAddr.empty()) {
    return true;
  }
  vector<string> proxies = config_->get_proxy_addrs();
  // Only proxy node will trigger the launch. So if there is
  // only one proxy configured, the current node is active proxy.
  if (proxies.size() == 1) {
    return true;
  }
  if (std::find(proxies.begin(), proxies.end(),
                currentHostAddr) == proxies.end()) {
    log_->get_file_log()->error("Incorrect host address is given!");
    return false;
  }
  // All proxy nodes share same config file. The first node
  // in the config will serve as active proxy. Other nodes
  // will be standby.
  if (proxies[0] == currentHostAddr) {
    return true;
  }
  return false;
}

/// TODO: add fencing service to make sure only one proxy is active.
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
  vector<string> proxies = config_->get_proxy_addrs();
  heartbeatClient_ = std::make_shared<HeartbeatClient>(config_, log_);
  /// TODO: init with speculated active proxy address.
  heartbeatClient_->init();
  std::shared_ptr<ActiveProxyShutdownCallback> shutdownCallback =
      std::make_shared<ActiveProxyShutdownCallback>(shared_from_this());
  heartbeatClient_->set_shutdown_callback(shutdownCallback.get());
}

/**
 * According to the configuration order, if the proxy prior to the current proxy
 * was active recently and it is dead now, the current proxy should become active.
 */
bool Proxy::shouldBecomeActiveProxy() {
  vector<string> proxies = config_->get_proxy_addrs();
  string lastActiveProxy = getLastActiveProxy();
  std::vector<string>::iterator iter;
  iter = std::find(proxies.begin(), proxies.end(), lastActiveProxy);
  if (iter != proxies.end()) {
    iter++;
    return *iter == currentHostAddr_;
  }
  return false;
}

/**
 * Get last active proxy according to HeartbeatClient's successfully built connection previously.
 */
bool Proxy::getLastActiveProxy() {
  heartbeatClient_->getActiveProxyAddr();
}

void ActiveProxyShutdownCallback::ActiveProxyShutdownCallback(std::shared_ptr<Proxy> proxy) {
  proxy_ = proxy;
}

/**
 * Shut down callback function as a watch service. The current host should take action to
 * launch active proxy service if predefined condition is met.
 */
void ActiveProxyShutdownCallback::operator()(void* param_1, void* param_2) {
  if (proxy_->shouldBecomeActiveProxy()) {
    proxy_->stopStandbyService();
    proxy_->launchActiveService();
  } else {
    /// TODO: wait for a while. New active proxy needs some time to launch services.
    /// connect to new active proxy.
    int res = proxy_->build_connection_with_new_active_proxy();
    if (res == 0) {
      return;
    }
    /// No active proxy is running. The current proxy should become active.
    proxy_->stopStandbyService();
    proxy_->launchActiveService();
  }
}

/**
 * Standby proxy. Used to connect to new active proxy.
 */
int Proxy::build_connection_with_new_active_proxy() {
  return heartbeatClient_-> build_connection_with_exclusion(currentHostAddr_);
}

/**
 * TODO: needs to avoid cyclic calling.
 * ActiveProxyShutdownCallback -> stopStandbyService -> ActiveProxyShutdownCallback.
 */
void Proxy::stopStandbyService() {
  heartbeatClient_->reset();
  heartbeatClient_.reset();
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

/// TODO: for standby service, the below two services are not created.
void Proxy::wait() {
    clientService_->wait();
    replicaService_->wait();
}