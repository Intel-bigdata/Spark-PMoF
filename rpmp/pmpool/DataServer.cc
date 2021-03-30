/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/DataServer.cc
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Thursday, November 7th 2019, 3:48:52 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#include "pmpool/DataServer.h"

#include "pmpool/AllocatorProxy.h"
#include "pmpool/Config.h"
#include "pmpool/Digest.h"
#include "pmpool/Log.h"
#include "pmpool/NetworkServer.h"
#include "pmpool/Protocol.h"

DataServer::DataServer(std::shared_ptr<Config> config, std::shared_ptr<Log> log)
    : config_(config), log_(log) {}

int DataServer::init() {
  /// initialize heartbeat client
  heartbeatClient_ = std::make_shared<HeartbeatClient>(config_, log_);
  CHK_ERR("heartbeat client init", heartbeatClient_->init());
  log->get_console_log()->info("heartbeat client initialized");
  std::shared_ptr<ActiveProxyShutdownCallback> shutdownCallback =
      std::make_shared<ActiveProxyShutdownCallback>(heartbeatClient_);
  heartbeatClient_->set_active_proxy_shutdown_callback(shutdownCallback.get());

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
  log_->get_console_log()->info("RPMP started...");
  return 0;
}

void DataServer::wait() { networkServer_->wait(); }

ActiveProxyShutdownCallback::ActiveProxyShutdownCallback(std::shared_ptr<HeartbeatClient> heartbeatClient) {
  heartbeatClient_ = heartbeatClient;
}

/**
 * Shutdown callback used to watch active proxy state. The current rpmp server should try to build connection with
 * a new active proxy.
 */
void ActiveProxyShutdownCallback::operator()(void* param_1, void* param_2) {
  /// TODO: wait for 5s, can be optimized.
  /// New active proxy needs some time to launch services.
  sleep(5);
  int res = heartbeatClient_->build_connection();
  int attempts = 0;
  while (res != 0 && attempts < 10) {
    log_->get_console_log()->info("Try to find an active proxy again..");
    res = heartbeatClient_->build_connection();
    attempts++;
  }
  if (res != 0) {
    log_->get_console_log()->info("Cannot find an active proxy!");
  }
}
