/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/client/NetworkClient.cc
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool/client
 * Created Date: Monday, December 16th 2019, 1:16:16 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#include "pmpool/client/NetworkClient.h"

#include <HPNL/Callback.h>
#include <HPNL/ChunkMgr.h>
#include <HPNL/Connection.h>

#include "pmpool/Event.h"
#include "pmpool/buffer/CircularBuffer.h"
using namespace std::chrono_literals;

uint64_t timestamp_now() {
  return std::chrono::high_resolution_clock::now().time_since_epoch() /
         std::chrono::milliseconds(1);
}

RequestHandler::RequestHandler(std::shared_ptr<NetworkClient> networkClient)
    : networkClient_(networkClient) {}

RequestHandler::~RequestHandler() {
#ifdef DEBUG
  std::cout << "RequestHandler destructed" << std::endl;
#endif
}

void RequestHandler::reset() {
  this->stop();
  this->join();
  networkClient_.reset();
#ifdef DEBUG
  std::cout << "Callback map is "
            << (callback_map.empty() ? "empty" : "not empty") << std::endl;
  std::cout << "inflight map is " << (inflight_.empty() ? "empty" : "not empty")
            << std::endl;
#endif
}

void RequestHandler::addTask(std::shared_ptr<Request> request) {
  pendingRequestQueue_.enqueue(request);
}

void RequestHandler::addTask(std::shared_ptr<Request> request,
                             std::function<void()> func) {
  callback_map[request->get_rc().rid] = func;
  pendingRequestQueue_.enqueue(request);
}

int RequestHandler::entry() {
  std::shared_ptr<Request> request;
  bool res = pendingRequestQueue_.wait_dequeue_timed(
      request, std::chrono::milliseconds(1000));
  if (res) {
    handleRequest(request);
  }
  return 0;
}

std::shared_ptr<RequestHandler::InflightRequestContext>
RequestHandler::inflight_insert_or_get(std::shared_ptr<Request> request) {
  const std::lock_guard<std::mutex> lock(inflight_mtx_);
  auto rid = request->requestContext_.rid;
  if (inflight_.find(rid) == inflight_.end()) {
    auto ctx = std::make_shared<InflightRequestContext>();
    inflight_.emplace(rid, ctx);
    return ctx;
  } else {
    auto ctx = inflight_[rid];
    return ctx;
  }
}

void RequestHandler::inflight_erase(std::shared_ptr<Request> request) {
  const std::lock_guard<std::mutex> lock(inflight_mtx_);
  inflight_.erase(request->requestContext_.rid);
}

uint64_t RequestHandler::wait(std::shared_ptr<Request> request) {
  auto ctx = inflight_insert_or_get(request);
  unique_lock<mutex> lk(ctx->mtx_reply);
  while (!ctx->cv_reply.wait_for(lk, 5ms, [ctx, request] {
    auto current = std::chrono::steady_clock::now();
    auto elapse = current - ctx->start;
    if (elapse > 10s) {  // tried 10s and found 8 process * 8 threads request
                         // will still go timeout, need to fix
      ctx->op_failed = true;
      fprintf(stderr, "NetworkClient::wait::Request [TYPE %ld][Key %ld] spent %ld s, time out\n",
              request->requestContext_.type, request->requestContext_.key,
              std::chrono::duration_cast<std::chrono::seconds>(elapse).count());
      return true;
    }
    return ctx->op_finished;
  })) {
  }
  uint64_t res = 0;
  if (ctx->op_failed) {
    res = -1;
  }
  inflight_erase(request);
  return res;
}

RequestReplyContext RequestHandler::get(std::shared_ptr<Request> request) {
  auto ctx = inflight_insert_or_get(request);
  unique_lock<mutex> lk(ctx->mtx_reply);
  while (!ctx->cv_reply.wait_for(lk, 5ms, [ctx, request] {
    auto current = std::chrono::steady_clock::now();
    auto elapse = current - ctx->start;
    if (elapse > 10s) {  // tried 10s and found 8 process * 8 threads request
                         // will still go timeout, need to fix
      ctx->op_failed = true;
      fprintf(stderr, "NetworkClient::get::Request [TYPE %ld] spent %ld s, time out\n",
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
  return res;
}

void RequestHandler::notify(std::shared_ptr<RequestReply> requestReply) {
  const std::lock_guard<std::mutex> lock(inflight_mtx_);
  auto rid = requestReply->get_rrc().rid;
  if (inflight_.count(rid) == 0) {
    return;
  }
  auto ctx = inflight_[rid];
  ctx->op_finished = true;
  auto rrc = requestReply->get_rrc();
  ctx->requestReplyContext = rrc;
  if (callback_map.count(ctx->requestReplyContext.rid) != 0) {
    callback_map[ctx->requestReplyContext.rid]();
    callback_map.erase(ctx->requestReplyContext.rid);
  } else {
    ctx->cv_reply.notify_one();
  }
}

void RequestHandler::handleRequest(std::shared_ptr<Request> request) {
  auto ctx = inflight_insert_or_get(request);
  OpType rt = request->get_rc().type;
  switch (rt) {
    case ALLOC: {
      expectedReturnType = ALLOC_REPLY;
      request->encode();
      networkClient_->send(reinterpret_cast<char *>(request->data_),
                           request->size_);
      break;
    }
    case FREE: {
      expectedReturnType = FREE_REPLY;
      request->encode();
      networkClient_->send(reinterpret_cast<char *>(request->data_),
                           request->size_);
      break;
    }
    case WRITE: {
      expectedReturnType = WRITE_REPLY;
      request->encode();
      networkClient_->send(reinterpret_cast<char *>(request->data_),
                           request->size_);
      break;
    }
    case READ: {
      expectedReturnType = READ_REPLY;
      request->encode();
      networkClient_->send(reinterpret_cast<char *>(request->data_),
                           request->size_);
      break;
    }
    case PUT: {
      expectedReturnType = PUT_REPLY;
      request->encode();
      networkClient_->send(reinterpret_cast<char *>(request->data_),
                           request->size_);
      break;
    }
    case REPLICATE_PUT: {
      expectedReturnType = REPLICATE_PUT_REPLY;
      request->encode();
      networkClient_->send(reinterpret_cast<char *>(request->data_),
                           request->size_);
      break;
    }
    case GET: {
      expectedReturnType = GET_REPLY;
      request->encode();
      networkClient_->send(reinterpret_cast<char *>(request->data_),
                           request->size_);
      break;
    }
    case GET_META: {
      expectedReturnType = GET_META_REPLY;
      request->encode();
      networkClient_->send(reinterpret_cast<char *>(request->data_),
                           request->size_);
      break;
    }
    default: {}
  }
}

ClientConnectedCallback::ClientConnectedCallback(
    std::shared_ptr<NetworkClient> networkClient) {
  networkClient_ = networkClient;
}

void ClientConnectedCallback::operator()(void *param_1, void *param_2) {
  auto con = static_cast<Connection *>(param_1);
  networkClient_->connected(con);
}

ClientRecvCallback::ClientRecvCallback(
    std::shared_ptr<ChunkMgr> chunkMgr,
    std::shared_ptr<RequestHandler> requestHandler)
    : chunkMgr_(chunkMgr), requestHandler_(requestHandler) {}

void ClientRecvCallback::operator()(void *param_1, void *param_2) {
  int mid = *static_cast<int *>(param_1);
  auto ck = chunkMgr_->get(mid);

  auto requestReply = std::make_shared<RequestReply>(
      reinterpret_cast<char *>(ck->buffer), ck->size,
      reinterpret_cast<Connection *>(ck->con));
  requestReply->decode();
  requestHandler_->notify(requestReply);
  chunkMgr_->reclaim(ck, static_cast<Connection *>(ck->con));
}

NetworkClient::NetworkClient(const string &remote_address,
                             const string &remote_port)
    : NetworkClient(remote_address, remote_port, 1, 32, 65536, 64) {}

NetworkClient::NetworkClient(const string &remote_address,
                             const string &remote_port, int worker_num,
                             int buffer_num_per_con, int buffer_size,
                             int init_buffer_num)
    : remote_address_(remote_address),
      remote_port_(remote_port),
      worker_num_(worker_num),
      buffer_num_per_con_(buffer_num_per_con),
      buffer_size_(buffer_size),
      init_buffer_num_(init_buffer_num),
      connected_(false) {}

NetworkClient::~NetworkClient() {
#ifdef DUBUG
  std::cout << "NetworkClient destructed" << std::endl;
#endif
}

int NetworkClient::init(std::shared_ptr<RequestHandler> requestHandler) {
  client_ = std::make_shared<Client>(worker_num_, buffer_num_per_con_);
  if ((client_->init()) != 0) {
    return -1;
  }
  chunkMgr_ = std::make_shared<ChunkPool>(client_.get(), buffer_size_,
                                          init_buffer_num_);

  client_->set_chunk_mgr(chunkMgr_.get());

  shutdownCallback = std::make_shared<ClientShutdownCallback>();
  connectedCallback =
      std::make_shared<ClientConnectedCallback>(shared_from_this());
  recvCallback =
      std::make_shared<ClientRecvCallback>(chunkMgr_, requestHandler);
  sendCallback = std::make_shared<ClientSendCallback>(chunkMgr_);

  client_->set_shutdown_callback(shutdownCallback.get());
  client_->set_connected_callback(connectedCallback.get());
  client_->set_recv_callback(recvCallback.get());
  client_->set_send_callback(sendCallback.get());

  client_->start();
  int res = client_->connect(remote_address_.c_str(), remote_port_.c_str());
  unique_lock<mutex> lk(con_mtx);
  auto start = std::chrono::steady_clock::now();
  while (!con_v.wait_for(lk, 50ms, [start, this] {
    auto current = std::chrono::steady_clock::now();
    auto elapse = current - start;
    if (elapse > 10s) {
      fprintf(stderr, "Client connection spent %ld s, time out\n",
              std::chrono::duration_cast<std::chrono::seconds>(elapse).count());
      return true;
    }
    return this->connected_;
  })) {
  }
  if (!connected_) {
    return -1;
  }

  circularBuffer_ =
      make_shared<CircularBuffer>(1024 * 1024, 512, false, shared_from_this());
  return 0;
}

void NetworkClient::shutdown() { client_->shutdown(); }

void NetworkClient::wait() { client_->wait(); }

void NetworkClient::reset() {
  circularBuffer_.reset();
  shutdownCallback.reset();
  connectedCallback.reset();
  recvCallback.reset();
  sendCallback.reset();
  if (con_ != nullptr) {
    con_->shutdown();
  }
  if (client_) {
    client_->shutdown();
    client_.reset();
  }
}

std::shared_ptr<ChunkMgr> NetworkClient::get_chunkMgr() { return chunkMgr_; }

Chunk *NetworkClient::register_rma_buffer(char *rma_buffer, uint64_t size) {
  return client_->reg_rma_buffer(rma_buffer, size, buffer_id_++);
}

void NetworkClient::unregister_rma_buffer(int buffer_id) {
  client_->unreg_rma_buffer(buffer_id);
}

uint64_t NetworkClient::get_dram_buffer(const char *data, uint64_t size) {
  char *dest = circularBuffer_->get(size);
  if (data) {
    memcpy(dest, data, size);
  }
  return (uint64_t)dest;
}

void NetworkClient::reclaim_dram_buffer(uint64_t src_address, uint64_t size) {
  circularBuffer_->put(reinterpret_cast<char *>(src_address), size);
}

uint64_t NetworkClient::get_rkey() {
  return circularBuffer_->get_rma_chunk()->mr->key;
}

void NetworkClient::connected(Connection *con) {
  std::cout<<"NetworkClient from "<<this->getRemoteAddress()<<":" << this->getRemotePort()<<" connected to server"<<std::endl;
  std::unique_lock<std::mutex> lk(con_mtx);
  con_ = con;
  connected_ = true;
  con_v.notify_all();
  lk.unlock();
}

void NetworkClient::send(char *data, uint64_t size) {
  auto ck = chunkMgr_->get(con_);
  std::memcpy(reinterpret_cast<char *>(ck->buffer), data, size);
  ck->size = size;
#ifdef DEBUG
  RequestMsg *requestMsg = (RequestMsg *)(data);
  std::cout << "[NetworkClient::send][" << requestMsg->type << "] size is "
            << size << std::endl;
  for (int i = 0; i < size; i++) {
    printf("%X ", *(data + i));
  }
  printf("\n");
#endif
  con_->send(ck);
}

void NetworkClient::read(std::shared_ptr<Request> request) {
  RequestContext rc = request->get_rc();
}

string NetworkClient::getRemoteAddress(){
  return remote_address_;
}

string NetworkClient::getRemotePort(){
  return remote_port_;
}
