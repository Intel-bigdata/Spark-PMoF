#ifndef RPMP_PROXYCLIENT_H
#define RPMP_PROXYCLIENT_H

#include <HPNL/Callback.h>
#include <HPNL/Client.h>

#include <atomic>
#include <condition_variable>  // NOLINT
#include <cstring>
#include <future>  // NOLINT
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "pmpool/ThreadWrapper.h"
#include "pmpool/ProxyEvent.h"
#include "pmpool/queue/blockingconcurrentqueue.h"
#include "pmpool/queue/concurrentqueue.h"

using namespace std;
using moodycamel::BlockingConcurrentQueue;
using moodycamel::BlockingConcurrentQueue;
using std::atomic;
using std::condition_variable;
using std::future;
using std::make_shared;
using std::mutex;
using std::promise;
using std::shared_ptr;
using std::string;
using std::unique_lock;
using std::unordered_map;

class ProxyClient;
class Connection;
class ChunkMgr;

class ProxyRequestHandler : public ThreadWrapper {
 public:
  explicit ProxyRequestHandler(std::shared_ptr<ProxyClient> proxyClient);
  ~ProxyRequestHandler();
  void addTask(std::shared_ptr<ProxyRequest> request);
  // add replica request to wait for replication response from proxy
  void addRequest(std::shared_ptr<ProxyRequest> request);
  void reset();
  int entry() override;
  void abort() override {}
  void notify(std::shared_ptr<ProxyRequestReply> requestReply);
  // wait proxy response and get reply
  ProxyRequestReplyContext get(std::shared_ptr<ProxyRequest> request);
  string getAddress(uint64_t hashValue);

 private:
  std::shared_ptr<ProxyClient> proxyClient_;
  BlockingConcurrentQueue<std::shared_ptr<ProxyRequest>> pendingRequestQueue_;
  uint64_t total_num = 0;
  uint64_t begin = 0;
  uint64_t end = 0;
  uint64_t time = 0;
  struct InflightProxyRequestContext {
    std::mutex mtx_reply;
    std::condition_variable cv_reply;
    std::mutex mtx_returned;
    std::chrono::time_point<std::chrono::steady_clock> start;
    bool op_finished = false;
    bool op_failed = false;
    InflightProxyRequestContext() { start = std::chrono::steady_clock::now(); }
    ProxyRequestReplyContext requestReplyContext;
    ProxyRequestReplyContext &get_rrc() { return requestReplyContext; }
    vector<string> hosts;
  };
  std::unordered_map<uint64_t, std::shared_ptr<ProxyRequestHandler::InflightProxyRequestContext>>
      inflight_;
  std::mutex inflight_mtx_;

  std::shared_ptr<ProxyRequestHandler::InflightProxyRequestContext> inflight_insert_or_get(
      std::shared_ptr<ProxyRequest>);
  void inflight_erase(std::shared_ptr<ProxyRequest> request);
  void handleRequest(std::shared_ptr<ProxyRequest> request);
};

class ProxyClientShutdownCallback : public Callback {
public:
  explicit ProxyClientShutdownCallback() {}
  ~ProxyClientShutdownCallback() override = default;
  void operator()(void* param_1, void* param_2) {};
};


class ProxyClientConnectCallback : public Callback {
public:
  explicit ProxyClientConnectCallback(std::shared_ptr<ProxyClient> proxyClient);
  ~ProxyClientConnectCallback() override = default;
  void operator()(void* param_1, void* param_2);

private:
  std::shared_ptr<ProxyClient> proxyClient_;
};

class ProxyClientRecvCallback : public Callback {
public:
  ProxyClientRecvCallback(std::shared_ptr<ChunkMgr> chunkMgr, std::shared_ptr<ProxyRequestHandler> requestHandler);
  ~ProxyClientRecvCallback() override = default;
  void operator()(void* param_1, void* param_2);

private:
  std::shared_ptr<ChunkMgr> chunkMgr_;
  std::shared_ptr<ProxyRequestHandler> requestHandler_;
  std::mutex mtx;
};

class ProxyClientSendCallback : public Callback {

public:
  explicit ProxyClientSendCallback(std::shared_ptr<ChunkMgr> chunkMgr) : chunkMgr_(chunkMgr) {}
  ~ProxyClientSendCallback() override = default;
  void operator()(void* param_1, void* param_2) {
    auto buffer_id_ = *static_cast<int *>(param_1);
    auto ck = chunkMgr_->get(buffer_id_);
    chunkMgr_->reclaim(ck, static_cast<Connection *>(ck->con));
  }

private:
  std::shared_ptr<ChunkMgr> chunkMgr_;
};

class ProxyClient : public std::enable_shared_from_this<ProxyClient> {
 public:
  ProxyClient(const string &proxy_address, const string &proxy_port);
  ProxyClient() = delete;
  ~ProxyClient();
  int initProxyClient();
  void send(const char* data, uint64_t size);
  void setConnection(Connection* connection);

  int build_connection();
  int build_connection(string proxy_addr, string proxy_port);
  void onActiveProxyShutdown();
  void set_active_proxy_shutdown_callback(Callback* activeProxyDisconnectedCallback);

  void addTask(std::shared_ptr<ProxyRequest> request);
  ProxyRequestReplyContext get(std::shared_ptr<ProxyRequest> request);
  void addRequest(std::shared_ptr<ProxyRequest> request);

  void shutdown();
  void wait();
  void reset();

 private:
  std::vector<string> proxy_addrs_;
  std::string proxy_port_;
  std::shared_ptr<ProxyRequestHandler> proxyRequestHandler_;
  std::shared_ptr<Client> client_;
  std::shared_ptr<ChunkMgr> chunkMgr_;

  std::shared_ptr<ProxyClientShutdownCallback> shutdownCallback;
  std::shared_ptr<ProxyClientConnectCallback> connectCallback;
  std::shared_ptr<ProxyClientRecvCallback> recvCallback;
  std::shared_ptr<ProxyClientSendCallback> sendCallback;
  Connection* proxy_connection_;
  std::mutex con_mtx;
  std::condition_variable con_v;
  bool connected_;

  Callback* activeProxyDisconnectedCallback_;
};

/**
 * A callback to take action when the built connection is shut down.
 */
class ActiveProxyDisconnectedCallback : public Callback {
public:
    explicit ActiveProxyDisconnectedCallback(std::shared_ptr<ProxyClient> proxyClient);
    ~ActiveProxyDisconnectedCallback() override = default;
    void operator()(void* param_1, void* param_2);

private:
    std::shared_ptr<ProxyClient> proxyClient_;
};

#endif //RPMP_PROXYCLIENT_H
