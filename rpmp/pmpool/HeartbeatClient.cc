#include "pmpool/HeartbeatClient.h"
#include "pmpool/proxy/XXHash.h"
#include "pmpool/proxy/IHash.h"

#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <unistd.h>
#include <thread>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <array>

HeartbeatRequestHandler::HeartbeatRequestHandler(std::shared_ptr<HeartbeatClient> heartbeatClient)
        : heartbeatClient_(heartbeatClient) {
}

HeartbeatRequestHandler::~HeartbeatRequestHandler() {

}

void HeartbeatRequestHandler::reset() {
  this->stop();
  this->join();
  heartbeatClient_.reset();
}

void HeartbeatRequestHandler::addTask(std::shared_ptr<HeartbeatRequest> request) {
  pendingRequestQueue_.enqueue(request);
}

int HeartbeatRequestHandler::entry() {
  std::shared_ptr<HeartbeatRequest> request;
  bool res = pendingRequestQueue_.wait_dequeue_timed(
          request, std::chrono::milliseconds(1000));
  if (res) {
    handleRequest(request);
  }
  return 0;
}

std::shared_ptr<HeartbeatRequestHandler::InflightHeartbeatRequestContext>
HeartbeatRequestHandler::inflight_insert_or_get(std::shared_ptr<HeartbeatRequest> request) {
  const std::lock_guard<std::mutex> lock(inflight_mtx_);
  auto rid = request->requestContext_.rid;
  if (inflight_.find(rid) == inflight_.end()) {
    auto ctx = std::make_shared<HeartbeatRequestHandler::InflightHeartbeatRequestContext>();
    inflight_.emplace(rid, ctx);
    return ctx;
  } else {
    auto ctx = inflight_[rid];
    return ctx;
  }
}

void HeartbeatRequestHandler::inflight_erase(std::shared_ptr<HeartbeatRequest> request) {
  const std::lock_guard<std::mutex> lock(inflight_mtx_);
  inflight_.erase(request->requestContext_.rid);
}

int HeartbeatRequestHandler::get(std::shared_ptr<HeartbeatRequest> request) {
  auto ctx = inflight_insert_or_get(request);
  unique_lock<mutex> lk(ctx->mtx_reply);
  while (!ctx->cv_reply.wait_for(lk, 5ms, [ctx, request] {
    auto current = std::chrono::steady_clock::now();
    auto elapse = current - ctx->start;
    if (elapse > 10s) {
      ctx->op_failed = true;
      fprintf(stderr, "Request [TYPE %ld] spent %ld s, time out\n",
              request->requestContext_.type,
              std::chrono::duration_cast<std::chrono::seconds>(elapse).count());
      return true;
    }
    return ctx->op_finished;
  })) {
  }
  auto res = ctx->get_rrc();
  if (ctx->op_failed) {
    throw;
  }
  inflight_erase(request);
  return res.success;
}

void HeartbeatRequestHandler::notify(std::shared_ptr<HeartbeatRequestReply> requestReply) {
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

void HeartbeatRequestHandler::handleRequest(std::shared_ptr<HeartbeatRequest> request) {
  inflight_insert_or_get(request);
  request->encode();
  heartbeatClient_->send(reinterpret_cast<char *>(request->data_), request->size_);
}

HeartbeatConnectCallback::HeartbeatConnectCallback(std::shared_ptr<HeartbeatClient> heartbeatClient) {
  heartbeatClient_ = heartbeatClient;
}

void HeartbeatConnectCallback::operator()(void *param_1, void *param_2) {
  auto connection = static_cast<Connection*>(param_1);
  heartbeatClient_->setConnection(connection);
}

void HeartbeatSendCallback::operator()(void *param_1, void *param_2) {
  int mid = *static_cast<int *>(param_1);
  auto chunk = chunkMgr_->get(mid);
  auto connection = static_cast<Connection *>(chunk->con);
  chunkMgr_->reclaim(chunk, connection);
}

HeartbeatRecvCallback::HeartbeatRecvCallback(std::shared_ptr<ChunkMgr> chunkMgr, std::shared_ptr<HeartbeatRequestHandler> requestHandler)
        : chunkMgr_(chunkMgr), requestHandler_(requestHandler) {}

void HeartbeatRecvCallback::operator()(void *param_1, void *param_2) {
  int mid = *static_cast<int*>(param_1);
  Chunk* ck = chunkMgr_->get(mid);
  auto requestReply = std::make_shared<HeartbeatRequestReply>(
          reinterpret_cast<char *>(ck->buffer), ck->size,
          reinterpret_cast<Connection *>(ck->con));
  requestReply->decode();
  requestHandler_->notify(requestReply);
  chunkMgr_->reclaim(ck, static_cast<Connection *>(ck->con));
}

std::string exec(const char* cmd) {
  std::array<char, 128> buffer;
  std::string result;
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);
  if (!pipe) {
    throw std::runtime_error("popen() failed!");
  }
  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }
  return result;
}

HeartbeatClient::HeartbeatClient(std::shared_ptr<Config> config, std::shared_ptr<Log> log)
        : config_(config), log_(log) {
  XXHash *hashFactory = new XXHash();
  std::string result = exec("ip a");

  for(std::string node: config->get_nodes()){
    if (result.find(node) != std::string::npos){
      host_ip_ = node;
      host_ip_hash_ = hashFactory->hash(host_ip_);
    }
  }
}

HeartbeatClient::~HeartbeatClient() {
}

void HeartbeatClient::setConnection(Connection *connection) {
  std::unique_lock<std::mutex> lk(con_mtx);
  heartbeat_connection_ = connection;
  connected_ = true;
  con_v.notify_all();
  lk.unlock();
}

int HeartbeatClient::heartbeat() {
  int heartbeatInterval = config_->get_heartbeat_interval();
  while(true){
    sleep(heartbeatInterval);
    #ifdef DEBUG
    cout<<"I'm alive"<<endl;
    #endif
    HeartbeatRequestContext hrc = {};
    hrc.type = HEARTBEAT;
    hrc.rid = rid_++;
    hrc.host_ip_hash = host_ip_hash_;

    auto heartbeatRequest = std::make_shared<HeartbeatRequest>(hrc);
    heartbeatRequestHandler_->addTask(heartbeatRequest);
    heartbeatRequestHandler_->get(heartbeatRequest);
  }
}

void HeartbeatClient::send(const char *data, uint64_t size) {
  auto chunk = chunkMgr_->get(heartbeat_connection_);
  std::memcpy(reinterpret_cast<char *>(chunk->buffer), data, size);
  chunk->size = size;
  heartbeat_connection_->send(chunk);
}

int HeartbeatClient::init(){
  heartbeatRequestHandler_ = make_shared<HeartbeatRequestHandler>(shared_from_this());
  auto res = initHeartbeatClient();
  heartbeatRequestHandler_->start();
  if (res != -1){
    std::thread t_heartbeat(&HeartbeatClient::heartbeat, shared_from_this());
    t_heartbeat.detach();
  }
  return res;
}

int HeartbeatClient::initHeartbeatClient() {
  client_ = std::make_shared<Client>(1, 32);
  if ((client_->init()) != 0) {
    return -1;
  }
  int buffer_size_ = 65536;
  int buffer_number_ = 64;
  chunkMgr_ = std::make_shared<ChunkPool>(client_.get(), buffer_size_,
                                          buffer_number_);

  client_->set_chunk_mgr(chunkMgr_.get());

  shutdownCallback = std::make_shared<HeartbeatShutdownCallback>();
  connectCallback =
          std::make_shared<HeartbeatConnectCallback>(shared_from_this());
  recvCallback = std::make_shared<HeartbeatRecvCallback>(chunkMgr_, heartbeatRequestHandler_);
  sendCallback = std::make_shared<HeartbeatSendCallback>(chunkMgr_);

  client_->set_shutdown_callback(shutdownCallback.get());
  client_->set_connected_callback(connectCallback.get());
  client_->set_recv_callback(recvCallback.get());
  client_->set_send_callback(sendCallback.get());

  client_->start();
  string proxy_address = config_->get_proxy_ip();
  string heartbeat_port = config_->get_heartbeat_port();
  log_->get_console_log()->info(proxy_address);
  log_->get_console_log()->info(heartbeat_port);
  int res = client_->connect(proxy_address.c_str(), heartbeat_port.c_str());

  unique_lock<mutex> lk(con_mtx);
  while (!connected_) {
    con_v.wait(lk);
  }

  return 0;
}

void HeartbeatClient::shutdown() {
  client_->shutdown();
}

void HeartbeatClient::wait() {
  client_->wait();
}

void HeartbeatClient::reset(){
  shutdownCallback.reset();
  connectCallback.reset();
  recvCallback.reset();
  sendCallback.reset();
  if (heartbeat_connection_ != nullptr) {
    heartbeat_connection_->shutdown();
  }
  if (client_) {
    client_->shutdown();
    client_.reset();
  }
}
