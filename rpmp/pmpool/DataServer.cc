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
  networkServer_ = std::make_shared<NetworkServer>(config_, log_);
  CHK_ERR("network server init", networkServer_->init());
  log_->get_file_log()->info("network server initialized.");

  allocatorProxy_ =
      std::make_shared<AllocatorProxy>(config_, log_, networkServer_);
  CHK_ERR("allocator proxy init", allocatorProxy_->init());
  log_->get_file_log()->info("allocator proxy initialized.");

  // dataService_ = std::make_shared<DataServerService>(config_, log_);
  // dataService_->init();
  // log_->get_console_log()->info("Data service initialized.");

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
