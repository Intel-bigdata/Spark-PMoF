#ifndef RPMP_REPLICASERVICE_H
#define RPMP_REPLICASERVICE_H

#include <HPNL/Callback.h>
#include <HPNL/ChunkMgr.h>
#include <HPNL/Connection.h>
#include <HPNL/Server.h>

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <atomic>

#include "pmpool/Config.h"
#include "pmpool/Log.h"
#include "pmpool/ThreadWrapper.h"
#include "ReplicaEvent.h"
#include "pmpool/queue/blockingconcurrentqueue.h"
#include "pmpool/queue/concurrentqueue.h"
#include "pmpool/Proxy.h"

using moodycamel::BlockingConcurrentQueue;

class ReplicaService;

class ReplicaRecvCallback : public Callback {
 public:
  ReplicaRecvCallback() = delete;
  explicit ReplicaRecvCallback(std::shared_ptr<ReplicaService> service,
                               std::shared_ptr<ChunkMgr> chunkMgr);
  ~ReplicaRecvCallback() override = default;
  void operator()(void* param_1, void* param_2) override;

 private:
  std::shared_ptr<ChunkMgr> chunkMgr_;
  std::shared_ptr<ReplicaService> service_;
};

class ReplicaSendCallback : public Callback {
 public:
  ReplicaSendCallback() = delete;
  explicit ReplicaSendCallback(std::shared_ptr<ChunkMgr> chunkMgr);
  ~ReplicaSendCallback() override = default;
  void operator()(void* param_1, void* param_2) override;

 private:
  std::shared_ptr<ChunkMgr> chunkMgr_;
};

class ReplicaShutdownCallback : public Callback {
 public:
  ReplicaShutdownCallback() = default;
  ~ReplicaShutdownCallback() override = default;

  void operator()(void* param_1, void* param_2) override {
    cout << "replica service::ShutdownCallback::operator" << endl;
  }
};

class ReplicaConnectCallback : public Callback {
 public:
  ReplicaConnectCallback() = default;
  void operator()(void* param_1, void* param_2) override {
    cout << "replica service::ConnectCallback::operator" << endl;
  }
};

class ReplicaWorker : public ThreadWrapper {
 public:
  ReplicaWorker(std::shared_ptr<ReplicaService> service);
  int entry() override;
  void abort() override;
  void addTask(std::shared_ptr<ReplicaRequest> msg);

 private:
  std::shared_ptr<ReplicaService> service_;
  BlockingConcurrentQueue<std::shared_ptr<ReplicaRequest>>
      pendingRecvRequestQueue_;
};

class ReplicaService : public std::enable_shared_from_this<ReplicaService> {
 public:
  explicit ReplicaService(std::shared_ptr<Config> config,
                          std::shared_ptr<Log> log,
                          std::shared_ptr<Proxy> proxyServer);
  ~ReplicaService();
  bool startService();
  void enqueue_recv_msg(std::shared_ptr<ReplicaRequest> msg);
  void handle_recv_msg(std::shared_ptr<ReplicaRequest> msg);
  void wait();

 private:
  // void enqueue_finalize_msg(std::shared_ptr<ProxyRequestReply> reply);
  // void handle_finalize_msg(std::shared_ptr<ProxyRequestReply> reply);
  void addReplica(uint64_t key, std::string node);
  std::unordered_set<std::string> getReplica(uint64_t key);
  void removeReplica(uint64_t key);
  std::shared_ptr<ReplicaWorker> worker_;
  std::shared_ptr<ChunkMgr> chunkMgr_;
  std::shared_ptr<Config> config_;
  std::shared_ptr<Log> log_;
  std::shared_ptr<Server> server_;
  std::shared_ptr<ReplicaRecvCallback> recvCallback_;
  std::shared_ptr<ReplicaSendCallback> sendCallback_;
  std::shared_ptr<ReplicaConnectCallback> connectCallback_;
  std::shared_ptr<ReplicaShutdownCallback> shutdownCallback_;

  atomic<uint64_t> rid_ = {0};
  std::string dataServerPort_;
  uint32_t dataReplica_;
  std::unordered_map<std::string, Connection*> dataServerConnections_;
  std::unordered_map<uint64_t, std::unordered_set<std::string>> replicaMap_;
  std::mutex replica_mtx;
  std::unordered_map<uint64_t, std::shared_ptr<ReplicaRequest>> prrcMap_;
  std::mutex prrcMtx;
  std::shared_ptr<Proxy> proxyServer_;
};

#endif  // RPMP_REPLICASERVICE_H
