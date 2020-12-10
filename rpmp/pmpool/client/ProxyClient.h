#ifndef RPMP_PROXYCLIENT_H
#define RPMP_PROXYCLIENT_H

#include <cstring>

#include "HPNL/Callback.h"
#include "HPNL/ChunkMgr.h"
#include "HPNL/Client.h"
#include "HPNL/Connection.h"

#include <iostream>
#include <thread>
#include <unistd.h>
#include <mutex>
#include <condition_variable>

#include "pmpool/buffer/CircularBuffer.h"
#include "pmpool/ThreadWrapper.h"
#include "pmpool/ProxyEvent.h"
#include "pmpool/queue/blockingconcurrentqueue.h"

class ProxyClient;
class Connection;
class ChunkMgr;

#define MSG_SIZE 4096


#define BUFFER_SIZE (65536 * 2)
#define BUFFER_NUM 128

using namespace std;
using moodycamel::BlockingConcurrentQueue;

class ProxyRequestHandler : public ThreadWrapper {
 public:
  explicit ProxyRequestHandler(std::shared_ptr<ProxyClient> proxyClient);
  ~ProxyRequestHandler();
  void addTask(std::shared_ptr<ProxyRequest> request);
  void addTask(std::shared_ptr<ProxyRequest> request, std::function<void()> func);
  void reset();
  int entry() override;
  void abort() override {}
  void notify(std::shared_ptr<ProxyRequestReply> requestReply);
  // string wait(std::shared_ptr<ProxyRequest> request);
  ProxyRequestReplyContext get(std::shared_ptr<ProxyRequest> request);
  string getAddress(uint64_t hashValue);

 private:
  std::shared_ptr<ProxyClient> proxyClient_;
  BlockingConcurrentQueue<std::shared_ptr<ProxyRequest>> pendingRequestQueue_;
  unordered_map<uint64_t, std::function<void()>> callback_map;
  uint64_t total_num = 0;
  uint64_t begin = 0;
  uint64_t end = 0;
  uint64_t time = 0;
  struct InflightRequestContext {
    std::mutex mtx_reply;
    std::condition_variable cv_reply;
    std::mutex mtx_returned;
    std::chrono::time_point<std::chrono::steady_clock> start;
    bool op_finished = false;
    bool op_failed = false;
    InflightRequestContext() { start = std::chrono::steady_clock::now(); }
    ProxyRequestReplyContext requestReplyContext;
    ProxyRequestReplyContext &get_rrc() { return requestReplyContext; }
    vector<string> hosts;
  };
  std::unordered_map<uint64_t, std::shared_ptr<InflightRequestContext>>
      inflight_;
  std::mutex inflight_mtx_;
  long expectedReturnType;

  std::shared_ptr<InflightRequestContext> inflight_insert_or_get(
      std::shared_ptr<ProxyRequest>);
  void inflight_erase(std::shared_ptr<ProxyRequest> request);
  void handleRequest(std::shared_ptr<ProxyRequest> request);
};

class ProxyClientShutdownCallback : public Callback {
public:
  explicit ProxyClientShutdownCallback() {}
  ~ProxyClientShutdownCallback() override = default;
  void operator()(void* param_1, void* param_2);

private:
  Client* client;
};


class ProxyClientConnectedCallback : public Callback {
public:
  explicit ProxyClientConnectedCallback(std::shared_ptr<ProxyClient> proxyClient) : proxyClient_(proxyClient) {}
  ~ProxyClientConnectedCallback() override = default;
  void operator()(void* param_1, void* param_2);

private:
  std::shared_ptr<ProxyClient> proxyClient_;
};

class ProxyClientRecvCallback : public Callback {
public:
  // ProxyClientRecvCallback(Client* client_, ChunkMgr* chunkMgr_, ProxyClient* proxyClient_) : client(client_), chunkMgr(chunkMgr_), proxyClient(proxyClient_) {}
  ProxyClientRecvCallback(std::shared_ptr<ChunkMgr> chunkMgr, std::shared_ptr<ProxyRequestHandler> requestHandler) 
      : chunkMgr_(chunkMgr), requestHandler_(requestHandler) {}
  ~ProxyClientRecvCallback() override = default;
  void operator()(void* param_1, void* param_2);

private:
  // Client* client_;
  std::shared_ptr<ChunkMgr> chunkMgr_;
  std::shared_ptr<ProxyRequestHandler> requestHandler_;
};

class ProxyClientSendCallback : public Callback {

public:
  explicit ProxyClientSendCallback(std::shared_ptr<ChunkMgr> chunkMgr_) : chunkMgr(chunkMgr_) {}
  ~ProxyClientSendCallback() override = default;
  void operator()(void* param_1, void* param_2);

private:
  std::shared_ptr<ChunkMgr> chunkMgr;
};

class ProxyClient : public std::enable_shared_from_this<ProxyClient> {
 public:
  ProxyClient(const string& proxy_address, const string& proxy_port);
  int initProxyClient(std::shared_ptr<ProxyRequestHandler> requestHandler);
  // string getAddress(uint64_t hashValue);
  // string getAddress(string key);
  void send(const char* data, uint64_t size);
  void received(char* data);
  void setConnection(Connection* connection);
  // void addTask(std::shared_ptr<ProxyRequest> request);
  void shutdown();
  void wait();
  void reset();

 private:
  string proxy_address_;
  string proxy_port_;
  std::shared_ptr<Client> client_;
  std::shared_ptr<ChunkMgr> chunkMgr_;

  std::shared_ptr<ProxyClientShutdownCallback> shutdownCallback;
  std::shared_ptr<ProxyClientConnectedCallback> connectedCallback;
  std::shared_ptr<ProxyClientRecvCallback> recvCallback;
  std::shared_ptr<ProxyClientSendCallback> sendCallback;
  Connection* proxy_connection_;
  std::mutex mtx;
  std::condition_variable cv;
  bool received_ = false;
  string s_data;

  bool connected_;
  shared_ptr<CircularBuffer> circularBuffer_;
  atomic<uint64_t> buffer_id_{0};
  atomic<uint64_t> rid_ = {0};
  // std::shared_ptr<ProxyRequestHandler> requestHandler_;
};

#endif //RPMP_PROXYCLIENT_H
