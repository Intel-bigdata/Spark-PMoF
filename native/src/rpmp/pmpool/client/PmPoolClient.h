/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/client/PmPoolClient.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool/client
 * Created Date: Friday, December 13th 2019, 3:43:04 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_CLIENT_PMPOOLCLIENT_H_
#define PMPOOL_CLIENT_PMPOOLCLIENT_H_

#define BUFFER_SIZE 65536
#define INITIAL_BUFFER_NUMBER 64

#include <HPNL/Callback.h>
#include <HPNL/ChunkMgr.h>
#include <HPNL/Client.h>
#include <HPNL/Connection.h>

#include <atomic>
#include <condition_variable>  // NOLINT
#include <functional>
#include <future>  // NOLINT
#include <iostream>
#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "../Common.h"
#include "../ThreadWrapper.h"

class NetworkClient;
class RequestHandler;
class Function;

using std::atomic;
using std::make_shared;
using std::shared_ptr;
using std::string;

class PmPoolClient {
 public:
  PmPoolClient(string remote_address, string remote_port);
  ~PmPoolClient();
  int init();
  void begin_tx();
  uint64_t alloc(uint64_t size);
  int free(uint64_t address);
  int write(uint64_t address, char *data, uint64_t size);
  uint64_t write(char *data, uint64_t size);
  int read(uint64_t address, char *data, uint64_t size);
  int read(uint64_t address, char* data, uint64_t size, std::function<void(int)> func);
  void end_tx();
  void shutdown();
  void wait();

 private:
  shared_ptr<RequestHandler> requestHandler_;
  shared_ptr<NetworkClient> networkClient_;
  atomic<uint64_t> rid_ = {0};
  std::mutex tx_mtx;
  std::condition_variable tx_con;
  bool tx_finished;
  std::mutex op_mtx;
  bool op_finished;
};

#endif  // PMPOOL_CLIENT_PMPOOLCLIENT_H_
