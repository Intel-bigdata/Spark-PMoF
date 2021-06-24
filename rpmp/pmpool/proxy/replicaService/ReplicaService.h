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
#include "pmpool/RLog.h"
#include "pmpool/ThreadWrapper.h"
#include "ReplicaEvent.h"
#include "pmpool/queue/blockingconcurrentqueue.h"
#include "pmpool/queue/concurrentqueue.h"
#include "pmpool/Proxy.h"
#include "pmpool/proxy/metastore/MetastoreFacade.h"

#include "json/json.h"

const string JOB_STATUS = "JOB_STATUS";
const string NODES = "NODES";
const string NODE = "NODE";
const string STATUS = "STATUS";
const string VALID = "VALID";
const string INVALID = "INVALID";
const string PENDING = "PENDING";
const string SIZE = "SIZE";

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
#ifdef DEBUG
    cout << "ReplicaService::ReplicaShutdownCallback::operator() is called." << endl;
#endif
  }
};

class ReplicaConnectCallback : public Callback {
 public:
  ReplicaConnectCallback() = default;
  void operator()(void* param_1, void* param_2) override;
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
                          std::shared_ptr<RLog> log,
                          std::shared_ptr<Proxy> proxyServer,
                          std::shared_ptr<MetastoreFacade> metastore);
  ~ReplicaService();
  bool startService();
  void enqueue_recv_msg(std::shared_ptr<ReplicaRequest> msg);
  void handle_recv_msg(std::shared_ptr<ReplicaRequest> msg);
  void wait();
  Connection* getConnection(string node);
  ChunkMgr* getChunkMgr();

 private:
  void addReplica(uint64_t key, PhysicalNode node);
  std::unordered_set<PhysicalNode, PhysicalNodeHash> getReplica(uint64_t key);
  void removeReplica(uint64_t key);
  void updateRecord(uint64_t key, PhysicalNode node, uint64_t size);
  std::shared_ptr<ReplicaWorker> worker_;
  std::shared_ptr<ChunkMgr> chunkMgr_;
  std::shared_ptr<Config> config_;
  std::shared_ptr<RLog> log_;
  std::shared_ptr<Server> server_;
  std::shared_ptr<MetastoreFacade> metastore_;
  std::shared_ptr<ReplicaRecvCallback> recvCallback_;
  std::shared_ptr<ReplicaSendCallback> sendCallback_;
  std::shared_ptr<ReplicaConnectCallback> connectCallback_;
  std::shared_ptr<ReplicaShutdownCallback> shutdownCallback_;

  atomic<uint64_t> rid_ = {0};
  std::string dataServerPort_;
  uint32_t dataReplica_;
  uint32_t minReplica_;
  std::unordered_map<std::string, Connection*> dataServerConnections_;
  std::unordered_map<uint64_t, std::unordered_set<std::string>> replicaMap_;
  std::mutex replica_mtx;
  std::unordered_map<uint64_t, std::shared_ptr<ReplicaRequest>> prrcMap_;
  std::mutex prrcMtx;
  std::shared_ptr<Proxy> proxyServer_;
  map<string, Connection*> node2Connection;
};

#endif  // RPMP_REPLICASERVICE_H
