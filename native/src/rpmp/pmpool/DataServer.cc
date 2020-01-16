/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/DataServer.cc
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Thursday, November 7th 2019, 3:48:52 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#include "pmpool/DataServer.h"

#include "AllocatorProxy.h"
#include "Config.h"
#include "Digest.h"
#include "NetworkServer.h"
#include "Protocol.h"

DataServer::DataServer(Config *config) : config_(config) {}

int DataServer::init() {
  networkServer_ = std::make_shared<NetworkServer>(config_);
  networkServer_->init();

  allocatorProxy_ =
      std::make_shared<AllocatorProxy>(config_, networkServer_.get());
  allocatorProxy_->init();

  protocol_ = std::make_shared<Protocol>(config_, networkServer_.get(),
                                         allocatorProxy_.get());
  protocol_->init();

  networkServer_->start();

  std::cout << "remote persistent memory pool started......" << std::endl;

  return 0;
}

void DataServer::wait() { networkServer_->wait(); }
