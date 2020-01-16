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

#include <HPNL/Server.h>
#include <HPNL/ChunkMgr.h>

#include <memory>

class Config;
class Protocol;
class Digest;
class DataList;
class AllocatorProxy;
class NetworkServer;

class DataServer {
 public:
  explicit DataServer(Config* config);
  int init();
  void wait();
 private:
  Config* config_;
  std::shared_ptr<NetworkServer> networkServer_;
  std::shared_ptr<AllocatorProxy> allocatorProxy_;
  std::shared_ptr<Protocol> protocol_;
};

#endif  // PMPOOL_DATASERVER_H_
