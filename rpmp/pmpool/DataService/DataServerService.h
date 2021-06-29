#ifndef RPMP_DATASERVICE_H
#define RPMP_DATASERVICE_H

#include <HPNL/Callback.h>
#include <HPNL/ChunkMgr.h>
#include <HPNL/Connection.h>
#include <HPNL/Server.h>
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
#include "pmpool/queue/blockingconcurrentqueue.h"
#include "pmpool/queue/concurrentqueue.h"
#include "pmpool/Config.h"
#include "pmpool/RLog.h"
#include "pmpool/proxy/replicaService/ReplicaEvent.h"
#include "pmpool/Protocol.h"
#include "pmpool/AllocatorProxy.h"

using moodycamel::BlockingConcurrentQueue;

class DataServerService;
class NetworkClient;
class RequestHandler;
class ReplicateWorker;

struct DataChannel {
  std::shared_ptr<NetworkClient> networkClient;
  std::shared_ptr<RequestHandler> requestHandler;
};

class ServiceShutdownCallback : public Callback {
public:
  explicit ServiceShutdownCallback() {}
  ~ServiceShutdownCallback() override = default;
  void operator()(void* param_1, void* param_2) {};
};


class ServiceConnectCallback : public Callback {
public:
  explicit ServiceConnectCallback(std::shared_ptr<DataServerService> service);
  ~ServiceConnectCallback() override = default;
  void operator()(void* param_1, void* param_2);

private:
  std::shared_ptr<DataServerService> service_;
};

class ServiceRecvCallback : public Callback {
public:
  ServiceRecvCallback(std::shared_ptr<ChunkMgr> chunkMgr, std::shared_ptr<DataServiceRequestHandler> requestHandler, std::shared_ptr<DataServerService> service);
  ~ServiceRecvCallback() override = default;
  void operator()(void* param_1, void* param_2);

private:
  std::shared_ptr<ChunkMgr> chunkMgr_;
  std::shared_ptr<DataServiceRequestHandler> requestHandler_;
  std::shared_ptr<DataServerService> service_;
  std::mutex mtx;
};

class ServiceSendCallback : public Callback {

public:
  explicit ServiceSendCallback(std::shared_ptr<ChunkMgr> chunkMgr) : chunkMgr_(chunkMgr) {}
  ~ServiceSendCallback() override = default;
  void operator()(void* param_1, void* param_2) {
    auto buffer_id_ = *static_cast<int *>(param_1);
    auto ck = chunkMgr_->get(buffer_id_);
    chunkMgr_->reclaim(ck, static_cast<Connection *>(ck->con));
  }

private:
  std::shared_ptr<ChunkMgr> chunkMgr_;
};

class DataServerService : public std::enable_shared_from_this<DataServerService> {
public:
 explicit DataServerService(std::shared_ptr<Config> config,
                            std::shared_ptr<RLog> log,
                            std::shared_ptr<Protocol> protocol);
 ~DataServerService();
 bool init();  // connect to proxy server
 int build_connection();
 int build_connection(std::string proxy_addr);
 void setConnection(Connection* connection);
 void send(const char* data, uint64_t size);
 void addTask(std::shared_ptr<ReplicaRequest> request);
 void registerDataServer();
 void registerDataServer(std::string proxy_addr);
 void enqueue_recv_msg(std::shared_ptr<ReplicaRequestReply> repy);
 void handle_replica_msg(std::shared_ptr<ReplicaRequestReply> msg);
 // get RPMP channel
 std::shared_ptr<DataChannel> getChannel(string node, string port);
private:
 std::string host_;
 std::string port_;
 atomic<uint64_t> rid_ = {0};
 std::shared_ptr<DataServiceRequestHandler> requestHandler_;
 std::vector<std::shared_ptr<ReplicateWorker>> workers_;
 std::shared_ptr<ChunkMgr> chunkMgr_;
 std::shared_ptr<ServiceShutdownCallback> shutdownCallback;
 std::shared_ptr<ServiceConnectCallback> connectCallback;
 std::shared_ptr<ServiceRecvCallback> recvCallback;
 std::shared_ptr<ServiceSendCallback> sendCallback;
 std::shared_ptr<Config> config_;
 std::shared_ptr<RLog> log_;
 std::shared_ptr<Protocol> protocol_;

 std::shared_ptr<Client> proxyClient_;
 Connection* proxyCon_;
 bool proxyConnected = false;
 std::mutex con_mtx;
 std::condition_variable con_v;
 std::map<string, std::shared_ptr<DataChannel>> channels;
 std::mutex channel_mtx;
};

class DataServiceRequestHandler : public ThreadWrapper {
 public:
  explicit DataServiceRequestHandler(std::shared_ptr<DataServerService> service)
      : service_(service) {}
  ~DataServiceRequestHandler() = default;
  void addTask(std::shared_ptr<ReplicaRequest> request) {
    pendingRequestQueue_.enqueue(request);
  }
  void reset() {
    this->stop();
    this->join();
  }
  int entry() override {
    std::shared_ptr<ReplicaRequest> request;
    bool res = pendingRequestQueue_.wait_dequeue_timed(
        request, std::chrono::milliseconds(1000));
    if (res) {
      handleRequest(request);
    }
    return 0;
  }
  void abort() override {}
  void notify(std::shared_ptr<ReplicaRequestReply> requestReply) {
    const std::lock_guard<std::mutex> lock(inflight_mtx_);
    auto rid = requestReply->get_rrc().rid;
    if (inflight_.count(rid) == 0) {
      return;
    }
    auto ctx = inflight_[rid];
    unique_lock<mutex> lk(ctx->mtx_reply);
    ctx->op_finished = true;
    auto rrc = requestReply->get_rrc();
    ctx->requestReplyContext = rrc;
    ctx->cv_reply.notify_one();
  }

  ReplicaRequestReplyContext get(
      std::shared_ptr<ReplicaRequest> request) {
    auto ctx = inflight_insert_or_get(request);
    unique_lock<mutex> lk(ctx->mtx_reply);
    while (!ctx->cv_reply.wait_for(lk, 5ms, [ctx, request] {
      auto current = std::chrono::steady_clock::now();
      auto elapse = current - ctx->start;
      if (elapse > 10s) {
        ctx->op_failed = true;
        fprintf(
            stderr, "DataServerService::Request [TYPE %ld] spent %ld s, time out\n",
            request->requestContext_.type,
            std::chrono::duration_cast<std::chrono::seconds>(elapse).count());
        return true;
      }
      return ctx->op_finished;
    })) {
    }
    auto res = ctx->get_rrc();
    if (ctx->op_failed) {
      throw "Get request reply failed!";
    }
    inflight_erase(request);
    return res;
  }

 private:
  std::shared_ptr<DataServerService> service_;
  BlockingConcurrentQueue<std::shared_ptr<ReplicaRequest>> pendingRequestQueue_;
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
    ReplicaRequestReplyContext requestReplyContext;
    ReplicaRequestReplyContext &get_rrc() { return requestReplyContext; }
  };
  std::unordered_map<uint64_t, std::shared_ptr<DataServiceRequestHandler::InflightProxyRequestContext>> inflight_;
  std::mutex inflight_mtx_;

  std::shared_ptr<DataServiceRequestHandler::InflightProxyRequestContext>
  inflight_insert_or_get(std::shared_ptr<ReplicaRequest> request) {
    const std::lock_guard<std::mutex> lock(inflight_mtx_);
    auto rid = request->requestContext_.rid;
    if (inflight_.find(rid) == inflight_.end()) {
      auto ctx =
          std::make_shared<DataServiceRequestHandler::InflightProxyRequestContext>();
      inflight_.emplace(rid, ctx);
      return ctx;
    } else {
      auto ctx = inflight_[rid];
      return ctx;
    }
  }

  void inflight_erase(std::shared_ptr<ReplicaRequest> request) {
    const std::lock_guard<std::mutex> lock(inflight_mtx_);
    inflight_.erase(request->requestContext_.rid);
  }

  void handleRequest(std::shared_ptr<ReplicaRequest> request) {
    auto ctx = inflight_insert_or_get(request);
    ReplicaOpType rt = request->get_rc().type;
    request->encode();
    service_->send(reinterpret_cast<char *>(request->data_),
                       request->size_);
  }
};

class ReplicateWorker : public ThreadWrapper {
 public:
  ReplicateWorker() = delete;
  ReplicateWorker(std::shared_ptr<DataServerService> service);
  ~ReplicateWorker() override = default;
  int entry() override;
  void abort() override;
  void addTask(std::shared_ptr<ReplicaRequestReply> requestReply);

 private:
  std::shared_ptr<DataServerService> service_;
  BlockingConcurrentQueue<std::shared_ptr<ReplicaRequestReply>> pendingRecvRequestQueue_;
};

#endif //RPMP_DATASERVICE_H
