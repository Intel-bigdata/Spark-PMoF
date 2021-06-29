#include "pmpool/DataServer.h"

#include "pmpool/AllocatorProxy.h"
#include "pmpool/Callback.h"
#include "pmpool/Config.h"
#include "pmpool/Digest.h"
#include "pmpool/HeartbeatClient.h"
#include "pmpool/RLog.h"
#include "pmpool/NetworkServer.h"
#include "pmpool/Protocol.h"

DataServer::DataServer(std::shared_ptr<Config> config, std::shared_ptr<RLog> log)
    : config_(config), log_(log) {}

int DataServer::init() {
  networkServer_ = std::make_shared<NetworkServer>(config_, log_);
  CHK_ERR("network server init", networkServer_->init());
  log_->get_file_log()->info("network server initialized.");

  allocatorProxy_ =
      std::make_shared<AllocatorProxy>(config_, log_, networkServer_);
  CHK_ERR("allocator proxy init", allocatorProxy_->init());
  log_->get_file_log()->info("allocator proxy initialized.");

  protocol_ = std::make_shared<Protocol>(config_, log_, networkServer_,
                                         allocatorProxy_);
  CHK_ERR("protocol init", protocol_->init());
  log_->get_file_log()->info("protocol initialized.");

  networkServer_->start();
  log_->get_file_log()->info("network server started.");
  log_->get_console_log()->info("RPMP started.");

  /// initialize heartbeat client
  heartbeatClient_ = std::make_shared<HeartbeatClient>(config_, log_);
  CHK_ERR("heartbeat client init", heartbeatClient_->init());
  log_->get_console_log()->info("heartbeat client initialized");

  std::shared_ptr<ConnectionShutdownCallback> shutdownCallback =
      std::make_shared<ConnectionShutdownCallback>(heartbeatClient_, protocol_->getDataService());
  heartbeatClient_->set_active_proxy_shutdown_callback(shutdownCallback.get());

  return 0;
}

void DataServer::wait() { networkServer_->wait(); }

ConnectionShutdownCallback::ConnectionShutdownCallback(std::shared_ptr<HeartbeatClient> heartbeatClient,
    std::shared_ptr<DataServerService> dataService) {
  heartbeatClient_ = heartbeatClient;
  dataService_ = dataService;
}

/**
 * Shutdown callback used to watch active proxy state. The current rpmp server should try to build connection with
 * a new active proxy.
 */
void ConnectionShutdownCallback::operator()(void* param_1, void* param_2) {
  // Some time is needed for new active proxy setup.
  const int waitTime = heartbeatClient_->get_heartbeat_timeout() + 1;
  sleep(waitTime);
  int res = heartbeatClient_->build_connection();
  int attempts = 0;
  while (res != 0 && attempts < 10) {
    res = heartbeatClient_->build_connection();
    attempts++;
  }
  if (res != 0) {
    return;
  }
  // TODO: combine two communication paths (HeartbeatClient/DataServerService) into single one.
  // re-register to new active proxy.
  string activeProxyAddr = heartbeatClient_->getActiveProxyAddr();
  dataService_->registerDataServer(activeProxyAddr);
}
