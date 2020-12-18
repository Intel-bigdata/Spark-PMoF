
#include "pmpool/client/ProxyClient.h"

ProxyRequestHandler::ProxyRequestHandler(std::shared_ptr<ProxyClient> proxyClient)
    : proxyClient_(proxyClient) {}

ProxyRequestHandler::~ProxyRequestHandler() {
#ifdef DEBUG
  std::cout << "RequestHandler destructed" << std::endl;
#endif
}

void ProxyRequestHandler::reset() {
  this->stop();
  this->join();
  proxyClient_.reset();
#ifdef DEBUG
  std::cout << "Callback map is "
            << (callback_map.empty() ? "empty" : "not empty") << std::endl;
  std::cout << "inflight map is " << (inflight_.empty() ? "empty" : "not empty")
            << std::endl;
#endif
}

void ProxyRequestHandler::addTask(std::shared_ptr<ProxyRequest> request) {
  pendingRequestQueue_.enqueue(request);
}

int ProxyRequestHandler::entry() {
  std::shared_ptr<ProxyRequest> request;
  bool res = pendingRequestQueue_.wait_dequeue_timed(
      request, std::chrono::milliseconds(1000));
  if (res) {
    handleRequest(request);
  }
  return 0;
}

std::shared_ptr<ProxyRequestHandler::InflightProxyRequestContext>
ProxyRequestHandler::inflight_insert_or_get(std::shared_ptr<ProxyRequest> request) {
  const std::lock_guard<std::mutex> lock(inflight_mtx_);
  auto rid = request->requestContext_.rid;
  if (inflight_.find(rid) == inflight_.end()) {
    auto ctx = std::make_shared<ProxyRequestHandler::InflightProxyRequestContext>();
    inflight_.emplace(rid, ctx);
    return ctx;
  } else {
    auto ctx = inflight_[rid];
    return ctx;
  }
}

void ProxyRequestHandler::inflight_erase(std::shared_ptr<ProxyRequest> request) {
  const std::lock_guard<std::mutex> lock(inflight_mtx_);
  inflight_.erase(request->requestContext_.rid);
}

string ProxyRequestHandler::get(std::shared_ptr<ProxyRequest> request) {
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
  return res.hosts[0];
}

void ProxyRequestHandler::notify(std::shared_ptr<ProxyRequestReply> requestReply) {
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

void ProxyRequestHandler::handleRequest(std::shared_ptr<ProxyRequest> request) {
  auto ctx = inflight_insert_or_get(request);
  ProxyOpType rt = request->get_rc().type;
  request->encode();
  proxyClient_->send(reinterpret_cast<char *>(request->data_), request->size_);
}

ProxyClientConnectCallback::ProxyClientConnectCallback(std::shared_ptr<ProxyClient> proxyClient) {
    proxyClient_ = proxyClient;
}

void ProxyClientConnectCallback::operator()(void *param_1, void *param_2) {
  cout << "ProxyClientConnectCallback" << endl;
  auto connection = static_cast<Connection*>(param_1);
  proxyClient_->setConnection(connection);
}

ProxyClientRecvCallback::ProxyClientRecvCallback(std::shared_ptr<ChunkMgr> chunkMgr, std::shared_ptr<ProxyRequestHandler> requestHandler) 
: chunkMgr_(chunkMgr), requestHandler_(requestHandler) {}

void ProxyClientRecvCallback::operator()(void *param_1, void *param_2) {
  int mid = *static_cast<int*>(param_1);
  Chunk* ck = chunkMgr_->get(mid);
  auto requestReply = std::make_shared<ProxyRequestReply>(
      reinterpret_cast<char *>(ck->buffer), ck->size,
      reinterpret_cast<Connection *>(ck->con));
  requestReply->decode();
  requestHandler_->notify(requestReply);
  chunkMgr_->reclaim(ck, static_cast<Connection *>(ck->con));
}

ProxyClient::ProxyClient(const string &proxy_address, const string &proxy_port) 
:proxy_address_(proxy_address), proxy_port_(proxy_port) {}

ProxyClient::~ProxyClient() {
#ifdef DUBUG
  std::cout << "NetworkClient destructed" << std::endl;
#endif
}

void ProxyClient::setConnection(Connection *connection) {
  std::unique_lock<std::mutex> lk(con_mtx);
  proxy_connection_ = connection;
  connected_ = true;
  con_v.notify_all();
  lk.unlock();
}

void ProxyClient::send(const char *data, uint64_t size) {
  auto chunk = chunkMgr_->get(proxy_connection_);
  std::memcpy(reinterpret_cast<char *>(chunk->buffer), data, size);
  chunk->size = size;
  proxy_connection_->send(chunk);
}

int ProxyClient::initProxyClient(std::shared_ptr<ProxyRequestHandler> requestHandler) {
  client_ = std::make_shared<Client>(1, 32);
  if ((client_->init()) != 0) {
    return -1;
  }
  int buffer_size_ = 65536;
  int buffer_number_ = 64;
  chunkMgr_ = std::make_shared<ChunkPool>(client_.get(), buffer_size_,
                                          buffer_number_);

  client_->set_chunk_mgr(chunkMgr_.get());

  shutdownCallback = std::make_shared<ProxyClientShutdownCallback>();
  connectCallback =
      std::make_shared<ProxyClientConnectCallback>(shared_from_this());
  recvCallback = std::make_shared<ProxyClientRecvCallback>(chunkMgr_, requestHandler);
  sendCallback = std::make_shared<ProxyClientSendCallback>(chunkMgr_);

  client_->set_shutdown_callback(shutdownCallback.get());
  client_->set_connected_callback(connectCallback.get());
  client_->set_recv_callback(recvCallback.get());
  client_->set_send_callback(sendCallback.get());

  client_->start();
  int res = client_->connect(proxy_address_.c_str(), proxy_port_.c_str());
  unique_lock<mutex> lk(con_mtx);
  while (!connected_) {
    con_v.wait(lk);
  }

  return 0;
}

void ProxyClient::shutdown() {
  client_->shutdown();
}

void ProxyClient::wait() {
  client_->wait();
}

void ProxyClient::reset(){
  shutdownCallback.reset();
  connectCallback.reset();
  recvCallback.reset();
  sendCallback.reset();
  if (proxy_connection_ != nullptr) {
    proxy_connection_->shutdown();
  }
  if (client_) {
    client_->shutdown();
    client_.reset();
  }
}
