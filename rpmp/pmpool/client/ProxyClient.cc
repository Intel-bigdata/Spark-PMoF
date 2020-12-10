
#include "pmpool/client/ProxyClient.h"
#include "pmpool/client/NetworkClient.h"

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

void ProxyRequestHandler::addTask(std::shared_ptr<ProxyRequest> request,
                             std::function<void()> func) {
  callback_map[request->get_rc().rid] = func;
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

std::shared_ptr<ProxyRequestHandler::InflightRequestContext>
ProxyRequestHandler::inflight_insert_or_get(std::shared_ptr<ProxyRequest> request) {
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

void ProxyRequestHandler::inflight_erase(std::shared_ptr<ProxyRequest> request) {
  const std::lock_guard<std::mutex> lock(inflight_mtx_);
  inflight_.erase(request->requestContext_.rid);
}

ProxyRequestReplyContext ProxyRequestHandler::get(std::shared_ptr<ProxyRequest> request) {
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
  return res;
}

void ProxyRequestHandler::notify(std::shared_ptr<ProxyRequestReply> requestReply) {
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

void ProxyRequestHandler::handleRequest(std::shared_ptr<ProxyRequest> request) {
  auto ctx = inflight_insert_or_get(request);
  ProxyOpType rt = request->get_rc().type;
  request->encode();
  proxyClient_->send(reinterpret_cast<char *>(request->data_), request->size_);
}

void ProxyClientShutdownCallback::operator()(void *param_1, void *param_2) {
  cout<<"PRoxyClient::ProxyClientShutdownCallback::operator"<<endl;
  client->shutdown();
}

void ProxyClientConnectedCallback::operator()(void *param_1, void *param_2) {
  cout<<"ProxyClient::ProxyClientConnectedCallback::operator"<<endl;
  auto connection = static_cast<Connection*>(param_1);
  proxyClient_->setConnection(connection);
}

int counter2 = 0;

void ProxyClientRecvCallback::operator()(void *param_1, void *param_2) {
  cout<<"ProxyClient::ProxyClientRecvCallback::operator"<<endl;
  int mid = *static_cast<int*>(param_1);
  Chunk* ck = chunkMgr_->get(mid);
  auto requestReply = std::make_shared<ProxyRequestReply>(
      reinterpret_cast<char *>(ck->buffer), ck->size,
      reinterpret_cast<Connection *>(ck->con));
  requestReply->decode();
  requestHandler_->notify(requestReply);
  chunkMgr_->reclaim(ck, static_cast<Connection *>(ck->con));
}

void ProxyClientSendCallback::operator()(void *param_1, void *param_2) {
  cout<<"Client::SendCallback::operator"<<endl;
  /**
  int mid = *static_cast<int*>(param_1);
  Chunk* chunk = chunkMgr->get(mid);
  auto connection = static_cast<Connection*>(chunk->con);
  chunkMgr->reclaim(chunk, connection);
   **/
}

ProxyClient::ProxyClient(const string &proxy_address, const string &proxy_port) 
:proxy_address_(proxy_address), proxy_port_(proxy_port) {}

void ProxyClient::setConnection(Connection *connection) {
  std::cout<<"connected to proxy server" << proxy_address_ <<std::endl;
  std::unique_lock<std::mutex> lk(mtx);
  proxy_connection_ = connection;
  connected_ = true;
  cv.notify_all();
  lk.unlock();
}

void ProxyClient::send(const char *data, uint64_t size) {
  // if(chunkMgr_ == nullptr){
  //   cout<<"ProxyClient::send: chunkMgr_ is null"<<endl;
  // }
  auto chunk = chunkMgr_->get(proxy_connection_);
  std::memcpy(reinterpret_cast<char *>(chunk->buffer), data, size);
  chunk->size = size;
  proxy_connection_->send(chunk);
}


// string ProxyRequestHandler::getAddress(uint64_t hashValue){
//   ProxyRequestContext rc = {};
//   rc.type = GET_HOSTS;
//   rc.rid = rid_++;
//   rc.key = hashValue;
//   auto request = std::make_shared<ProxyRequest>(rc);
//   requestHandler_->addTask(request);
//   auto res = requestHandler_->get(request);
//   return res.host;

// }

int ProxyClient::initProxyClient(std::shared_ptr<ProxyRequestHandler> requestHandler) {
  client_ = std::make_shared<Client>(1, 16);
  if ((client_->init()) != 0) {
    return -1;
  }
  int buffer_size_ = 65536;
  int buffer_number_ = 128;
  chunkMgr_ = std::make_shared<ChunkPool>(client_.get(), buffer_size_,
                                          buffer_number_);

  client_->set_chunk_mgr(chunkMgr_.get());

  shutdownCallback = std::make_shared<ProxyClientShutdownCallback>();
  connectedCallback =
      std::make_shared<ProxyClientConnectedCallback>(shared_from_this());
  recvCallback = std::make_shared<ProxyClientRecvCallback>(chunkMgr_, requestHandler);
  sendCallback = std::make_shared<ProxyClientSendCallback>(chunkMgr_);

  client_->set_shutdown_callback(shutdownCallback.get());
  client_->set_connected_callback(connectedCallback.get());
  client_->set_recv_callback(recvCallback.get());
  client_->set_send_callback(sendCallback.get());

  client_->start();
  int res = client_->connect(proxy_address_.c_str(), proxy_port_.c_str());
  unique_lock<mutex> lk(mtx);
  while (!connected_) {
    cv.wait(lk);
  }

  circularBuffer_ =
      make_shared<CircularBuffer>(1024 * 1024, 512, false);
  // requestHandler_->start();
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
  connectedCallback.reset();
  recvCallback.reset();
  sendCallback.reset();
  // requestHandler_->reset();
  if (proxy_connection_ != nullptr) {
    proxy_connection_->shutdown();
  }
  if (client_) {
    client_->shutdown();
    client_.reset();
  }
}