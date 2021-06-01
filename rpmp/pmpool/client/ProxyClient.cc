#include <boost/program_options.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

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

ProxyRequestReplyContext ProxyRequestHandler::get(std::shared_ptr<ProxyRequest> request) {
  auto ctx = inflight_insert_or_get(request);
  unique_lock<mutex> lk(ctx->mtx_reply);
  while (!ctx->cv_reply.wait_for(lk, 5ms, [ctx, request] {
    auto current = std::chrono::steady_clock::now();
    auto elapse = current - ctx->start;
    // TODO: timeout set too long?
    if (elapse > 30s) {
      ctx->op_failed = true;
      fprintf(stderr, "ProxyClient::Request [TYPE %ld] spent %ld s, time out\n",
              request->requestContext_.type,
              std::chrono::duration_cast<std::chrono::seconds>(elapse).count());
      // TODO: throw exception from here?
      // Throw the exception and ask for building connection with active proxy.
      // throw "TIMEOUT occurred in sending request to active proxy.";
      return true;
    }
    return ctx->op_finished;
  })) {
  }
  auto res = ctx->get_rrc();
  if (ctx->op_failed) {
    throw "Failed to send request to active proxy.";
  }
  inflight_erase(request);
  return res;
}

void ProxyRequestHandler::notify(std::shared_ptr<ProxyRequestReply> requestReply) {
  const std::lock_guard<std::mutex> lock(inflight_mtx_);
  auto rid = requestReply->get_rrc().rid;
  if (inflight_.count(rid) == 0) {
    cout << "No exist reply to notify: " << rid << endl;
    return;
  }
  auto ctx = inflight_[rid];
  unique_lock<mutex> lk(ctx->mtx_reply);
  ctx->op_finished = true;
  auto rrc = requestReply->get_rrc();
  ctx->requestReplyContext = rrc;
  ctx->cv_reply.notify_one();
}

void ProxyRequestHandler::addRequest(std::shared_ptr<ProxyRequest> request) {
  inflight_insert_or_get(request);
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

ProxyClientRecvCallback::ProxyClientRecvCallback(std::shared_ptr<ChunkMgr> chunkMgr,
                                                 std::shared_ptr<ProxyRequestHandler> requestHandler)
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

/**
 * In RPMP HA mode, proxy_address is a string containing multiple proxies with "," separated.
 */
ProxyClient::ProxyClient(const string &proxy_address, const string &proxy_port) {
  vector<string> proxies;
  boost::split(proxies, proxy_address, boost::is_any_of(","), boost::token_compress_on);
  proxy_addrs_ = proxies;
  proxy_port_ = proxy_port;
}

ProxyClient::~ProxyClient() {
#ifdef DUBUG
  std::cout << "ProxyClient destructed" << std::endl;
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

int ProxyClient::initProxyClient() {
  proxyRequestHandler_ = make_shared<ProxyRequestHandler>(shared_from_this());
  proxyRequestHandler_->start();

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
  recvCallback = std::make_shared<ProxyClientRecvCallback>(chunkMgr_, proxyRequestHandler_);
  sendCallback = std::make_shared<ProxyClientSendCallback>(chunkMgr_);

  client_->set_shutdown_callback(shutdownCallback.get());
  client_->set_connected_callback(connectCallback.get());
  client_->set_recv_callback(recvCallback.get());
  client_->set_send_callback(sendCallback.get());

  client_->start();

  std::shared_ptr<ActiveProxyDisconnectedCallback> callbackPtr =
      std::make_shared<ActiveProxyDisconnectedCallback>(shared_from_this());
  set_active_proxy_shutdown_callback(callbackPtr.get());
  return build_connection();
}

int ProxyClient::build_connection() {
  const int MAX_LOOPS = 5;
  int loop = 0;
  while (loop++ < MAX_LOOPS) {
    for (int i = 0; i < proxy_addrs_.size(); i++) {
      cout << "Trying to connect to " << proxy_addrs_[i] << ":" << proxy_port_ << endl;
      auto res = build_connection(proxy_addrs_[i], proxy_port_);
      if (res == 0) {
        return 0;
      }
    }
  }
  cout << "Failed to connect to an active proxy!" << endl;
  return -1;
}

int ProxyClient::build_connection(string proxy_addr, string proxy_port) {
  // reset to false to consider the possible re-connection to a new active proxy.
  connected_ = false;
  // res can be 0 even though remote proxy is shut down.
  int res = client_->connect(proxy_addr.c_str(), proxy_port.c_str());
  if (res == -1) {
    return -1;
  }
  // wait for ConnectedCallback to be executed.
  unique_lock<mutex> lk(con_mtx);
  while (!connected_) {
    if (con_v.wait_for(lk, std::chrono::seconds(3)) == std::cv_status::timeout) {
      break;
    }
  }
  if (!connected_) {
    return -1;
  }
  cout << "Successfully connected to active proxy: " + proxy_addr << endl;
  // Looks no need to keep this addr.
//  activeProxyAddr_ = proxy_addr;
  return 0;
}

void ProxyClient::addTask(std::shared_ptr<ProxyRequest> request) {
  proxyRequestHandler_->addTask(request);
}

/**
 * Catch exception and build connection with new active proxy.
 */
ProxyRequestReplyContext ProxyClient::get(std::shared_ptr<ProxyRequest> request) {
  try {
    return proxyRequestHandler_->get(request);
  } catch (char const* e) {
    onActiveProxyShutdown();
    if (connected_) {
      // Ignore the exception case in below after new proxy connection is built.
      // The possibility of this case is very low.
      return proxyRequestHandler_->get(request);
    }
  }
}

void ProxyClient::addRequest(std::shared_ptr<ProxyRequest> request) {
  return  proxyRequestHandler_->addRequest(request);
}

void ProxyClient::set_active_proxy_shutdown_callback(Callback* activeProxyDisconnectedCallback) {
  activeProxyDisconnectedCallback_ = activeProxyDisconnectedCallback;
}

/**
 * Actions to be token when active proxy is unreachable.
 */
void ProxyClient::onActiveProxyShutdown() {
  connected_ = false;
  if (proxy_connection_ != nullptr) {
    proxy_connection_->shutdown();
  }
  if (activeProxyDisconnectedCallback_) {
    activeProxyDisconnectedCallback_->operator()(nullptr, nullptr);
  }
  if (connected_) {
    cout << "Connected to a new active proxy." << endl;
  } else {
    cout << "No active proxy is found." << endl;
  }
}

void ProxyClient::shutdown() {
  client_->shutdown();
}

void ProxyClient::wait() {
  client_->wait();
}

void ProxyClient::reset(){
  proxyRequestHandler_->reset();
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

ActiveProxyDisconnectedCallback::ActiveProxyDisconnectedCallback(std::shared_ptr<ProxyClient> proxyClient) {
  proxyClient_ = proxyClient;
}

/**
 * Shutdown callback used to watch active proxy state. The current proxy should take action to
 * launch active proxy service if predefined condition is met.
 */
void ActiveProxyDisconnectedCallback::operator()(void* param_1, void* param_2) {
  proxyClient_->build_connection();
}
