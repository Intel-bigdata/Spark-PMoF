/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/DataServer.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Thursday, November 7th 2019, 3:48:52 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_DATASERVER_H_
#define PMPOOL_DATASERVER_H_

#include <HPNL/ChunkMgr.h>
#include <HPNL/Server.h>
#include "pmpool/HeartbeatClient.h"

#include <memory>

class Config;
class Protocol;
class Digest;
class DataList;
class AllocatorProxy;
class NetworkServer;
class Log;
class HeartbeatClient;

/**
 * @brief DataServer is designed as distributed remote memory pool.
 * DataServer on every node communicated with each other to guarantee data
 * consistency.
 *
 */
class DataServer {
 public:
  DataServer() = delete;
  explicit DataServer(std::shared_ptr<Config> config, std::shared_ptr<Log> log);
  int init();
  void wait();

 private:
  std::shared_ptr<Config> config_;
  std::shared_ptr<Log> log_;
  std::shared_ptr<NetworkServer> networkServer_;
  std::shared_ptr<AllocatorProxy> allocatorProxy_;
  std::shared_ptr<Protocol> protocol_;
  std::shared_ptr<HeartbeatClient> heartbeatClient_;
};

/**
 * A callback to take action when the built connection is shut down.
 */
class ActiveProxyShutdownCallback : public Callback {
public:
    explicit ActiveProxyShutdownCallback(std::shared_ptr<HeartbeatClient> heartbeatClient);
    ~ActiveProxyShutdownCallback() override = default;
    void operator()(void* param_1, void* param_2);

private:
    std::shared_ptr<HeartbeatClient> heartbeatClient_;
};

#endif  // PMPOOL_DATASERVER_H_
