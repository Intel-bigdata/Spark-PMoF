#ifndef SPARK_PMOF_NODEMANAGER_H
#define SPARK_PMOF_NODEMANAGER_H

#include <HPNL/Callback.h>
#include <HPNL/ChunkMgr.h>
#include <HPNL/Connection.h>

#include "pmpool/proxy/ConsistentHash.h"
#include "pmpool/proxy/NodeManager.h"
#include "pmpool/HeartbeatEvent.h"
#include "pmpool/ThreadWrapper.h"
#include "pmpool/queue/blockingconcurrentqueue.h"
#include "pmpool/queue/concurrentqueue.h"
#include "pmpool/Config.h"
#include "pmpool/RLog.h"
#include "pmpool/Proxy.h"
#include "pmpool/proxy/XXHash.h"
#include "pmpool/proxy/IHash.h"
#include "pmpool/proxy/metastore/MetastoreFacade.h"

#include "json/json.h"
#include <chrono>

using moodycamel::BlockingConcurrentQueue;

struct NodeStatus{
  string status;
  int last_heartbeat;
};

class NodeManager;
class Proxy;

class NodeManagerRecvCallback : public Callback {
public:
  NodeManagerRecvCallback() = delete;
  explicit NodeManagerRecvCallback(std::shared_ptr<NodeManager> nodeManager, std::shared_ptr<ChunkMgr> chunkMgr);
  ~NodeManagerRecvCallback() override = default;
  void operator()(void* param_1, void* param_2) override;

private:
  std::shared_ptr<ChunkMgr> chunkMgr_;
  std::shared_ptr<NodeManager> nodeManager_;
};

class NodeManagerSendCallback : public Callback {
public:
  NodeManagerSendCallback() = delete;
  explicit NodeManagerSendCallback(std::shared_ptr<ChunkMgr> chunkMgr);
  ~NodeManagerSendCallback() override = default;
  void operator()(void* param_1, void* param_2) override;

private:
  std::shared_ptr<ChunkMgr> chunkMgr_;
};

class NodeManagerConnectCallback:public Callback{
public:
  NodeManagerConnectCallback() = default;
  ~NodeManagerConnectCallback() override = default;

  void operator()(void* param_1, void* param_2) override{
    #ifdef DEBUG
    cout<<"NodeManager::ConnectCallback::operator"<<endl;
    #endif
  }
};

class NodeManagerShutdownCallback:public Callback{
public:
  NodeManagerShutdownCallback() = default;
  ~NodeManagerShutdownCallback() override = default;

  /**
   * Currently, nothing to be done when node manager is shutdown
   */
  void operator()(void* param_1, void* param_2) override{
    #ifdef DEBUG
    cout<<"NodeManager::ShutdownCallback::operator"<<endl;
    #endif
  }
};

class NodeManagerWorker : public ThreadWrapper {
public:
  NodeManagerWorker(std::shared_ptr<NodeManager> nodeManager);
  int entry() override;
  void abort() override;
  void addTask(std::shared_ptr<HeartbeatRequest> request);

private:
  std::shared_ptr<NodeManager> nodeManager_;
  BlockingConcurrentQueue<std::shared_ptr<HeartbeatRequest>> pendingRecvRequestQueue_;
};

class NodeManager : public std::enable_shared_from_this<NodeManager> {
public:
  NodeManager() = delete;
  NodeManager(std::shared_ptr<Config> config, std::shared_ptr <RLog> log, std::shared_ptr<Proxy> proxyServer, std::shared_ptr<MetastoreFacade> metastore);
  ~NodeManager();
  void wait();
  void printNodeStatus();
  uint64_t get_rkey();
  std::shared_ptr <ChunkMgr> get_chunk_mgr();
  void send(char *data, uint64_t size, Connection *con);
  void enqueue_recv_msg(std::shared_ptr<HeartbeatRequest> request);
  void handle_recv_msg(std::shared_ptr<HeartbeatRequest> request);
  bool startService();
  bool allConnected();
  int64_t getCurrentTime();

private:
  const string NODE_STATUS = "NODE_STATUS";
  const string HOST = "HOST";
  const string TIME = "TIME";
  const string PORT = "PORT";
  const string STATUS = "STATUS";
  const string LIVE = "LIVE";
  const string DEAD = "DEAD";
  std::shared_ptr <std::map<string, NodeStatus>> nodeMap_;
  std::map<uint64_t, string> *hashToNode_;
  std::shared_ptr <NodeManagerWorker> worker_;
  std::shared_ptr <Config> config_;
  std::shared_ptr <RLog> log_;
  std::shared_ptr <MetastoreFacade> metastore_;
  std::shared_ptr <Server> server_;
  std::shared_ptr <ChunkMgr> chunkMgr_;
  std::shared_ptr <NodeManagerRecvCallback> recvCallback_;
  std::shared_ptr <NodeManagerSendCallback> sendCallback_;
  std::shared_ptr <NodeManagerConnectCallback> connectCallback_;
  std::shared_ptr <NodeManagerShutdownCallback> shutdownCallback_;
  std::shared_ptr <Proxy> proxy_;
  void nodeDead(string host);
  void nodeConnect(string host, string port);
  int checkNode();
  bool hostExists(string host);
  void addOrUpdateRecord(Json::Value record);
  void constructNodeStatus(Json::Value record);
};
#endif //SPARK_PMOF_NODEMANAGER_H
