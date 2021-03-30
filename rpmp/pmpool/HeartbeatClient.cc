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
  const int heartbeatInverval = heartbeatClient_->get_heartbeat_interval();
  // timeout in sec depneds on the configured heartbeat interval.
  const std::chrono::seconds timeoutInSec(2 * heartbeatInverval);
  while (!ctx->cv_reply.wait_for(lk, 5ms, [ctx, request, timeoutInSec] {
    auto current = std::chrono::steady_clock::now();
    auto elapse = current - ctx->start;
    if (elapse > timeoutInSec) {
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
    throw "Failed to send heart beat to active proxy.";
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

HeartbeatConnectedCallback::HeartbeatConnectedCallback(std::shared_ptr<HeartbeatClient> heartbeatClient) {
  heartbeatClient_ = heartbeatClient;
}

void HeartbeatConnectedCallback::operator()(void *param_1, void *param_2) {
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

/**
 * TODO: let RPMP server find new active proxy in shutdown call back function.
 */
void HeartbeatShutdownCallback::operator()(void* param_1, void* param_2) {
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
  heartbeatInterval_ = config->get_heartbeat_interval();
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
    try {
      heartbeatRequestHandler_->get(heartbeatRequest);
    } catch (char const* e) {
      log_->get_console_log()->info(e);
      shutdown();
      break;
    }
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
  if (res != -1){
    heartbeatRequestHandler_->start();
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
  const int buffer_size = 65536;
  const int buffer_number = 64;
  chunkMgr_ = std::make_shared<ChunkPool>(client_.get(), buffer_size,
                                          buffer_number);

  client_->set_chunk_mgr(chunkMgr_.get());

  shutdownCallback = std::make_shared<HeartbeatShutdownCallback>();
  connectedCallback =
          std::make_shared<HeartbeatConnectedCallback>(shared_from_this());
  recvCallback = std::make_shared<HeartbeatRecvCallback>(chunkMgr_, heartbeatRequestHandler_);
  sendCallback = std::make_shared<HeartbeatSendCallback>(chunkMgr_);

  client_->set_shutdown_callback(shutdownCallback.get());
  client_->set_connected_callback(connectedCallback.get());
  client_->set_recv_callback(recvCallback.get());
  client_->set_send_callback(sendCallback.get());

  client_->start();
  return build_connection();
}

/**
 * For standby proxy, it is impossible to connect to itself since heartbeat listen port is not in service.
 */
int HeartbeatClient::build_connection() {
  vector<string> proxy_addrs = config_->get_proxy_addrs();
  string heartbeat_port = config_->get_heartbeat_port();
  if (!excludedProxy_.empty()) {
    return build_connection_with_exclusion(excludedProxy_);
  }
  for (int i = 0; i< proxy_addrs.size(); i++) {
    log_->get_console_log()->info("Trying to connect to " + proxy_addrs[i] + ":" + heartbeat_port);
    auto res = build_connection(proxy_addrs[i], heartbeat_port);
    if (res == 0) {
      return 0;
    }
  }
  log_->get_console_log()->info("Failed to connect to an active proxy!");
  return -1;
}

/**
 * For standby proxy use, try to connect to all other proxies.
 */
int HeartbeatClient::build_connection_with_exclusion(string excludedProxy) {
  vector<string> proxy_addrs = config_->get_proxy_addrs();
  string heartbeat_port = config_->get_heartbeat_port();
  for (int i = 0; i < proxy_addrs.size(); i++) {
    // Skip excluded proxy.
    if (proxy_addrs[i] == excludedProxy) {
      continue;
    }
    log_->get_console_log()->info("Trying to connect to " + proxy_addrs[i] + ":" + heartbeat_port);
    auto res = build_connection(proxy_addrs[i], heartbeat_port);
    if (res == 0) {
      return 0;
    }
  }
  log_->get_console_log()->info("Failed to connect to an active proxy!");
  return -1;
}

int HeartbeatClient::build_connection(string proxy_addr, string heartbeat_port) {
  // reset to false to consider the possible re-connection to a new active proxy.
  connected_ = false;
  // res can be 0 even though remote proxy is shut down.
  int res = client_->connect(proxy_addr.c_str(), heartbeat_port.c_str());
  log_->get_console_log()->info("Debug..{0}", res);
  if (res == -1) {
    return -1;
  }
  // wait for ConnectedCallback to be executed.
  unique_lock<mutex> lk(con_mtx);
  // TODO: looks not a loop.
  while (!connected_) {
    log_->get_console_log()->info("Debug..1");
    // TODO: sometimes segmentation fault occurs.
    if (con_v.wait_for(lk, std::chrono::seconds(3)) == std::cv_status::timeout) {
      break;
    }
  }
  if (!connected_) {
    log_->get_console_log()->info("Debug..2");
    return -1;
  }
  log_->get_console_log()->info("Successfully connected to active proxy: " + proxy_addr);
  activeProxyAddr_ = proxy_addr;
  return 0;
}

string HeartbeatClient::getActiveProxyAddr() {
  return activeProxyAddr_;
}

// For standby proxy use.
void::HeartbeatClient::set_active_proxy_shutdown_callback(Callback* activeProxyShutdownCallback) {
  activeProxyShutdownCallback_ = activeProxyShutdownCallback;
//  client_->set_shutdown_callback(shutdownCallback.get());
}

///TODO: looks client should not be shutdown.
void HeartbeatClient::shutdown() {
  client_->shutdown();
  if (activeProxyShutdownCallback_) {
    activeProxyShutdownCallback_->operator()(nullptr, nullptr);
  }
}

void HeartbeatClient::shutdown(Connection* conn) {
  client_->shutdown(conn);
}

void HeartbeatClient::wait() {
  client_->wait();
}

void HeartbeatClient::reset(){
  shutdownCallback.reset();
  connectedCallback.reset();
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

int HeartbeatClient::get_heartbeat_interval() {
  return heartbeatInterval_;
}

// Directly letting standby proxy try to connect to itself can sometimes cause issues.
// Also it is for efficiency consideration.
void HeartbeatClient::setExcludedProxy(string proxyAddr) {
  excludedProxy_ = proxyAddr;
}

