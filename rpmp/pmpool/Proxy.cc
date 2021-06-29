#include "HPNL/Callback.h"
#include "HPNL/Connection.h"
#include "HPNL/ChunkMgr.h"
#include "HPNL/Server.h"

#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>
#include "Proxy.h"

#include "json/json.h"

#include "pmpool/proxy/NodeManager.h"

Proxy::Proxy(std::shared_ptr<Config> config, std::shared_ptr<RLog> log, std::string currentHostAddr) :
 config_(config), log_(log), currentHostAddr_(currentHostAddr) {}

Proxy::~Proxy() {
    // worker_->stop();
    // worker_->join();
}

bool Proxy::launchServer() {
  if (isActiveProxy(currentHostAddr_)) {
    return launchActiveService();
  }
  // If no active proxy is found at start time, the current proxy should be active.
  if (!launchStandbyService()) {
    stopStandbyService();
    return launchActiveService();
  }
  return true;
}

/**
 * Judge whether the current proxy should be active at start time according to proxy
 * hosts order in the configuration.
 *
 * @param currentHostAddr   the host address of current proxy.
 * @return  true if the current proxy should be active.
 */
bool Proxy::isActiveProxy(std::string currentHostAddr) {
  // Directly launch proxy case.
  if (currentHostAddr.empty()) {
    return true;
  }
  std::vector<std::string> proxies = config_->get_proxy_addrs();
  // Only proxy node will trigger the launch. So if there is
  // only one proxy configured, the current node is active proxy.
  if (proxies.size() == 1) {
    return true;
  }
  if (std::find(proxies.begin(), proxies.end(),
                currentHostAddr) == proxies.end()) {
    log_->get_console_log()->error("Incorrect proxy address is configured for current host!");
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

/** Terminate the launching if key service is not launched as expected.
 *  TODO: add fencing service to make sure only one proxy is active.
 */
bool Proxy::launchActiveService() {
  log_->get_console_log()->info("Launch active proxy services..");
  string metastore_type = config_->get_metastore_type();
  metastore_ = std::make_shared<MetastoreFacade>(config_, log_, metastore_type);
  if (!metastore_->connect()) {
    return false;
  }
  std::shared_ptr<NodeManager> nodeManager = std::make_shared<NodeManager>(config_, log_, shared_from_this(), metastore_);
  if (!nodeManager->startService()) {
    return false;
  }
  loadBalanceFactor_ = config_->get_load_balance_factor();
  consistentHash_ = std::make_shared<ConsistentHash>();
  dataServerPort_ = config_->get_port();
  dataReplica_ = config_->get_data_replica();
  clientService_ = std::make_shared<ClientService>(config_, log_, shared_from_this(), metastore_);
  if (!clientService_->startService()) {
    return false;
  }
  replicaService_ = std::make_shared<ReplicaService>(config_, log_, shared_from_this(), metastore_);
  if (!replicaService_->startService()) {
    return false;
  }
  tracker_ = std::make_shared<Tracker>(config_, log_, shared_from_this(), metastore_, replicaService_);
  int wait_nodes_connect_timeout = config_->get_node_connect_timeout();
  int64_t startTime = nodeManager->getCurrentTime();
  while(!nodeManager->allConnected()){
    sleep(5);
    int64_t currentTime = nodeManager->getCurrentTime();
    if(currentTime - startTime > wait_nodes_connect_timeout * 1000){
      break;
    }
  }
  tracker_->scheduleUnfinishedTasks();
  return true;
}

bool Proxy::launchStandbyService() {
  log_->get_console_log()->info("Launch standby proxy services..");
  std::vector<std::string> proxies = config_->get_proxy_addrs();
  heartbeatClient_ = std::make_shared<HeartbeatClient>(config_, log_);
  // To avoid unnecessarily trying to connect to itself.
  heartbeatClient_->setExcludedProxy(currentHostAddr_);
  int res = heartbeatClient_->init();
  if (res == -1) {
    return false;
  }
  std::shared_ptr<ActiveProxyShutdownCallback> shutdownCallback =
      std::make_shared<ActiveProxyShutdownCallback>(shared_from_this());
  heartbeatClient_->set_active_proxy_shutdown_callback(shutdownCallback.get());
  return true;
}

/**
 * According to the configuration order, if the proxy prior to the current proxy
 * was active recently but it is dead now, the current proxy should become active.
 */
bool Proxy::shouldBecomeActiveProxy() {
  std::vector<std::string> proxies = config_->get_proxy_addrs();
  std::string lastActiveProxy = getLastActiveProxy();
  std::vector<std::string>::iterator iter;
  iter = std::find(proxies.begin(), proxies.end(), lastActiveProxy);
  if (iter != proxies.end()) {
    /// TODO: in a loop style.
    iter++;
    return *iter == currentHostAddr_;
  }
  return false;
}

/**
 * Get last active proxy according to HeartbeatClient's successfully built connection previously.
 */
std::string Proxy::getLastActiveProxy() {
  log_->get_console_log()->info("Last active proxy addr: {0}", heartbeatClient_->getActiveProxyAddr());
  return heartbeatClient_->getActiveProxyAddr();
}

std::shared_ptr<HeartbeatClient> Proxy::getHeartbeatClient() {
  return heartbeatClient_;
}

ActiveProxyShutdownCallback::ActiveProxyShutdownCallback(std::shared_ptr<Proxy> proxy) {
  proxy_ = proxy;
  heartbeatTimeoutInSec_ = proxy_->getHeartbeatClient()->get_heartbeat_timeout();
}

/**
 * Shutdown callback used to watch active proxy state. The current proxy should take action to
 * launch active proxy service if predefined condition is met.
 */
void ActiveProxyShutdownCallback::operator()(void* param_1, void* param_2) {
  if (proxy_->shouldBecomeActiveProxy()) {
    proxy_->stopStandbyService();
    if (!proxy_->launchActiveService()) {
      std::cout << "Failed to launch active proxy services!\n";
      return;
    }
  } else {
    /// wait for time required by candidate proxy to detect the disconnection and to set up active proxy services.
    /// New active proxy needs some time to launch services.
    const int waitTime = heartbeatTimeoutInSec_ + 1;
    sleep(waitTime);
    int res = proxy_->build_connection_with_new_active_proxy();
    if (res == 0) {
      return;
    }
    /// No active proxy is running. The current proxy should become active.
    proxy_->stopStandbyService();
    if (!proxy_->launchActiveService()) {
      std::cout << "Failed to launch active proxy services!\n";
    }
  }
}

/**
 * Standby proxy. Used to connect to new active proxy.
 */
int Proxy::build_connection_with_new_active_proxy() {
  return heartbeatClient_-> build_connection_with_exclusion(currentHostAddr_);
}

void Proxy::stopStandbyService() {
  log_->get_console_log()->info("Shutting down standby services..");
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

// TODO: get location from Metastore.
std::unordered_set<PhysicalNode,PhysicalNodeHash> Proxy::getReplica(uint64_t key) {
  std::lock_guard<std::mutex> lk(replica_mtx);
  return replicaMap_[key];
}

void Proxy::notifyClient(uint64_t key) {
  clientService_->notifyClient(key);
}

void Proxy::wait() {
  while (true) {
    sleep(10);
  }
}
