#ifndef SPARK_PMOF_HEARTBEAT_H
#define SPARK_PMOF_HEARTBEAT_H

#include <utility>
#include <memory>
#include <string>
#include <vector>

#include <HPNL/Callback.h>
#include <HPNL/Client.h>
#include "pmpool/ThreadWrapper.h"
#include "pmpool/HeartbeatEvent.h"

#include "pmpool/queue/blockingconcurrentqueue.h"
#include "pmpool/queue/concurrentqueue.h"

class Connection;
class ChunkMgr;
class HeartbeatClient;
class Config;
class RLog;

using std::string;
using std::vector;
using std::make_shared;
using moodycamel::BlockingConcurrentQueue;

class HeartbeatRequestHandler : public ThreadWrapper {
public:
  explicit HeartbeatRequestHandler(std::shared_ptr<HeartbeatClient> heartbeatClient);
  ~HeartbeatRequestHandler();
  void addTask(std::shared_ptr<HeartbeatRequest> request);
  void reset();
  int entry() override;
  void abort() override {}
  void notify(std::shared_ptr<HeartbeatRequestReply> requestReply);
  int get(std::shared_ptr<HeartbeatRequest> request);
  int heartbeat(uint64_t host_hash);

private:
  std::shared_ptr<HeartbeatClient> heartbeatClient_;
  int heartbeatInterval_;
  int heartbeatTimeoutInSec_;
  BlockingConcurrentQueue<std::shared_ptr<HeartbeatRequest>> pendingRequestQueue_;
  uint64_t total_num = 0;
  uint64_t begin = 0;
  uint64_t end = 0;
  uint64_t time = 0;
  struct InflightHeartbeatRequestContext {
    std::mutex mtx_reply;
    std::condition_variable cv_reply;
    std::mutex mtx_returned;
    std::chrono::time_point<std::chrono::steady_clock> start;
    bool op_finished = false;
    bool op_failed = false;
    InflightHeartbeatRequestContext() { start = std::chrono::steady_clock::now(); }
    HeartbeatRequestReplyContext requestReplyContext;
    HeartbeatRequestReplyContext &get_rrc() { return requestReplyContext; }
  };
  std::unordered_map<uint64_t, std::shared_ptr<HeartbeatRequestHandler::InflightHeartbeatRequestContext>>
          inflight_;
  std::mutex inflight_mtx_;

  std::shared_ptr<HeartbeatRequestHandler::InflightHeartbeatRequestContext> inflight_insert_or_get(
          std::shared_ptr<HeartbeatRequest>);
  void inflight_erase(std::shared_ptr<HeartbeatRequest> request);
  void handleRequest(std::shared_ptr<HeartbeatRequest> request);
};

class HeartbeatShutdownCallback : public Callback {
public:
  explicit HeartbeatShutdownCallback() {}
  ~HeartbeatShutdownCallback() override = default;
  void operator()(void* param_1, void* param_2);
};

class HeartbeatConnectedCallback : public Callback {
public:
  explicit HeartbeatConnectedCallback(std::shared_ptr<HeartbeatClient> heartbeatClient);
  ~HeartbeatConnectedCallback() override = default;
  void operator()(void* param_1, void* param_2);
private:
  std::shared_ptr<HeartbeatClient> heartbeatClient_;
};

class HeartbeatRecvCallback : public Callback {
public:
  HeartbeatRecvCallback();
  HeartbeatRecvCallback(std::shared_ptr<ChunkMgr> chunkMgr, std::shared_ptr<HeartbeatRequestHandler> requestHandler);
  ~HeartbeatRecvCallback() override = default;
  void operator()(void* param_1, void* param_2);

private:
  std::shared_ptr<ChunkMgr> chunkMgr_;
  std::shared_ptr<HeartbeatRequestHandler> requestHandler_;
  std::mutex mtx;
};

class HeartbeatSendCallback : public Callback {

public:
  explicit HeartbeatSendCallback(std::shared_ptr<ChunkMgr> chunkMgr) : chunkMgr_(chunkMgr) {}
  ~HeartbeatSendCallback() override = default;
  void operator()(void* param_1, void* param_2);

private:
  std::shared_ptr<ChunkMgr> chunkMgr_;
};

class HeartbeatClient: public std::enable_shared_from_this<HeartbeatClient>{
public:
  HeartbeatClient(std::shared_ptr<Config> config, std::shared_ptr<RLog> log);
  ~HeartbeatClient();
  int init();
  int heartbeat();
  int breakdown();
  void send(const char* data, uint64_t size);
  void setConnection(Connection* connection);
  int initHeartbeatClient();
  int build_connection();
  int build_connection_with_exclusion(std::string excludedProxy);
  int build_connection(std::string proxy_addr, std::string heartbeat_port);
  void set_active_proxy_shutdown_callback(Callback* shutdownCallback);
  std::string getActiveProxyAddr();
  void onActiveProxyShutdown();
  void shutdown();
  void shutdown(Connection* conn);
  void wait();
  void reset();
  // heartbeat in sec.
  int get_heartbeat_interval();
  // timeout in sec.
  int get_heartbeat_timeout();
  void setExcludedProxy(std::string proxyAddr);

private:
  atomic<uint64_t> rid_ = {0};
  std::string host_ip_;
  uint64_t host_ip_hash_;

  std::shared_ptr<HeartbeatRequestHandler> heartbeatRequestHandler_;
  std::shared_ptr<Config> config_;
  std::shared_ptr<RLog> log_;
  int heartbeatInterval_;
  // looks 5s for timeout is suitable. May need to be tuned for large scale cluster.
  const int heartbeatTimeout_ = 5;
  std::shared_ptr<Client> client_;
  std::shared_ptr<ChunkMgr> chunkMgr_;

  std::shared_ptr<HeartbeatShutdownCallback> shutdownCallback;
  std::shared_ptr<HeartbeatConnectedCallback> connectedCallback;
  std::shared_ptr<HeartbeatRecvCallback> recvCallback;
  std::shared_ptr<HeartbeatSendCallback> sendCallback;
  Connection* heartbeat_connection_;
  std::string activeProxyAddr_;

  std::mutex con_mtx;
  std::condition_variable con_v;
  bool connected_;
  bool isTerminated_;

  Callback* activeProxyShutdownCallback_;
  std::string excludedProxy_;
};

#endif //SPARK_PMOF_HEARTBEAT_H
