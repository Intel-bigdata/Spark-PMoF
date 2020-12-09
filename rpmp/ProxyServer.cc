#include "HPNL/Callback.h"
#include "HPNL/Connection.h"
#include "HPNL/ChunkMgr.h"
#include "HPNL/Server.h"

#include <iostream>
#include <string>
// #include "pmpool/proxy/ConsistentHash.h"
#include "ProxyServer.h"
// #include "pmpool/Event.h"

using namespace std;

ConsistentHash<PhysicalNode> *consistentHash;

class ShutdownCallback:public Callback{
public:
    ShutdownCallback() = default;
    ~ShutdownCallback() override = default;

    /**
     * Currently, nothing to be done when proxy is shutdown
     */
    void operator()(void* param_1, void* param_2) override{
      cout<<"ProxyServer::ShutdownCallback::operator"<<endl;
    }
};

class RecvCallback : public Callback {
public:
    explicit RecvCallback(std::shared_ptr<Worker> worker, ChunkMgr* chunkMgr_) : worker_(worker), chunkMgr(chunkMgr_) {}
    ~RecvCallback() override = default;
    void operator()(void* param_1, void* param_2) override {
      int mid = *static_cast<int*>(param_1);
      auto chunk = chunkMgr->get(mid);
      auto request =
      std::make_shared<ProxyRequest>(reinterpret_cast<char *>(chunk->buffer), chunk->size,
                                reinterpret_cast<Connection *>(chunk->con));
      request->decode();
      ProxyRequestMsg *requestMsg = (ProxyRequestMsg *)(request->getData());
      if (requestMsg->type != 0) {
        worker_->addTask(request);
        // proxyServer_->enqueue_recv_msg(request);
      } else {
        std::cout << "[RecvCallback::RecvCallback][" << requestMsg->type
                  << "] size is " << chunk->size << std::endl;
        for (int i = 0; i < chunk->size; i++)
        {
          printf("%X ", *(request->getData() + i));
        }
        printf("\n");
      }
      chunkMgr->reclaim(chunk, static_cast<Connection *>(chunk->con));
    }

private:
    ChunkMgr* chunkMgr;
    std::shared_ptr<Worker> worker_;
};

/**
 * The SendCallback is mainly for the reclaim of chunk
 */
class SendCallback : public Callback {
public:
    explicit SendCallback(ChunkMgr* chunkMgr_) : chunkMgr(chunkMgr_) {}
    ~SendCallback() override = default;
    void operator()(void* param_1, void* param_2) override {
      int mid = *static_cast<int*>(param_1);
      Chunk* chunk = chunkMgr->get(mid);
      auto connection = static_cast<Connection*>(chunk->con);
      chunkMgr->reclaim(chunk, connection);
    }

private:
    ChunkMgr* chunkMgr;
};

class ConnectCallback : public Callback {
  public:
  explicit ConnectCallback() {}
  void operator()(void* param_1, void* param_2) override {
    cout << "ProxyServer::ConnectCallback::operator" << endl;
  }
};

Worker::Worker(std::shared_ptr<ProxyServer> proxyServer) : proxyServer_(proxyServer) {}

void Worker::addTask(std::shared_ptr<ProxyRequest> request) {
  pendingRecvRequestQueue_.enqueue(request);
}

void Worker::abort() {}

int Worker::entry() {
  std::shared_ptr<ProxyRequest> request;
  bool res = pendingRecvRequestQueue_.wait_dequeue_timed(
      request, std::chrono::milliseconds(1000));
  if (res) {
    proxyServer_->handle_recv_msg(request);
  }
  return 0;
}

ProxyServer::ProxyServer(std::shared_ptr<Config> config, std::shared_ptr<Log> log) :
 config_(config), log_(log) {}

void ProxyServer::enqueue_recv_msg(std::shared_ptr<ProxyRequest> request) {
  worker->addTask(request);
}

void ProxyServer::handle_recv_msg(std::shared_ptr<ProxyRequest> request) {
  ProxyRequestContext rc = request->get_rc();
  cout << "handle_recv_msg: " << rc.rid << endl;
  string node = consistentHash->getNode(rc.key).getKey();
  cout << "get node from consistent hash: " << node << endl;
  auto rrc = ProxyRequestReplyContext();
  rrc.type = rc.type;
  rrc.success = 0;
  rrc.rid = rc.rid;
  rrc.host = const_cast<char*> (node.c_str());
  // memcpy(rrc.host, node.c_str(), node.length());
  rrc.con = rc.con;
  std::shared_ptr<ProxyRequestReply> requestReply = std::make_shared<ProxyRequestReply>(rrc);
  requestReply->encode();
  auto ck = chunkMgr->get(rrc.con);

  memcpy(ck->buffer, requestReply->data_, requestReply->size_);
  ck->size = requestReply->size_;
  rrc.con->send(ck);
}

bool ProxyServer::launchServer() {
  /**
   * Set the number of virtual nodes for load balance
   */
  int loadBalanceFactor = 5;
  /**
   * The nodes should be come from config, hard code for temp use
   */
  consistentHash = new ConsistentHash<PhysicalNode>();
  vector<string> nodes = config_->get_nodes();
  for (string node : nodes) {
    PhysicalNode* physicalNode = new PhysicalNode(node);
    consistentHash->addNode(*physicalNode, loadBalanceFactor);
  }

  int worker_number = config_->get_network_worker_num();
  int buffer_number = config_->get_network_buffer_num();
  int buffer_size = config_->get_network_buffer_size();
  auto server = new Server(worker_number, buffer_number);
  if(server->init() != 0){
    cout<<"HPNL server init failed"<<endl;
    return false;
  }

  chunkMgr = new ChunkPool(server, buffer_size, buffer_number);
  server->set_chunk_mgr(chunkMgr);

  worker = std::make_shared<Worker>(shared_from_this());
  worker->start();

  auto recvCallback = new RecvCallback(worker, chunkMgr);
  auto sendCallback = new SendCallback(chunkMgr);
  auto shutdownCallback = new ShutdownCallback();
  auto connectedCallback = new ConnectCallback();

  server->set_recv_callback(recvCallback);
  server->set_send_callback(sendCallback);
  server->set_connected_callback(connectedCallback);
  server->set_shutdown_callback(shutdownCallback);

  server->start();
  server->listen(config_->get_proxy_ip().c_str(), config_->get_proxy_port().c_str());
  log_->get_file_log()->info("Proxy server started at ", config_->get_proxy_ip(), ":", config_->get_proxy_port());
  server->wait();
  return true;
}

int main(int argc, char* argv[]){
  std::shared_ptr<Config> config = std::make_shared<Config>();
  if (argc > 1) {
    CHK_ERR("init config", config->init(argc, argv));
  } else {
    config->readFromFile();
  }
  std::shared_ptr<Log> log = std::make_shared<Log>(config.get());
  // ProxyServer* proxyServer;
  std::shared_ptr<ProxyServer> proxyServer = std::make_shared<ProxyServer>(config, log);
  proxyServer->launchServer();
  return 0;
}
