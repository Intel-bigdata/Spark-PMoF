#include "HPNL/Callback.h"
#include "HPNL/Connection.h"
#include "HPNL/ChunkMgr.h"
#include "HPNL/Server.h"

#include <iostream>
#include <string>
#include "Proxy.h"

using namespace std;

Proxy::Proxy(std::shared_ptr<Config> config, std::shared_ptr<Log> log) :
 config_(config), log_(log) {}

Proxy::~Proxy() {
    // worker_->stop();
    // worker_->join();
}

void Proxy::enqueue_recv_msg(std::shared_ptr<ProxyRequest> request) {
  // worker_->addTask(request);
}

void Proxy::handle_recv_msg(std::shared_ptr<ProxyRequest> request) {
  ProxyRequestContext rc = request->get_rc();
  vector<string> nodes = consistentHash_->getNodes(rc.key, dataReplica_);
  auto rrc = ProxyRequestReplyContext();
  rrc.type = rc.type;
  rrc.success = 0;
  rrc.rid = rc.rid;
  rrc.hosts = nodes;
  rrc.dataServerPort = dataServerPort_;
  rrc.con = rc.con;
  std::shared_ptr<ProxyRequestReply> requestReply = std::make_shared<ProxyRequestReply>(rrc);

  requestReply->encode();
  auto ck = chunkMgr_->get(rrc.con);
  memcpy(reinterpret_cast<char *>(ck->buffer), requestReply->data_, requestReply->size_);
  ck->size = requestReply->size_;
  rrc.con->send(ck);
}

bool Proxy::launchServer() {
  int loadBalanceFactor = 5;
  consistentHash_ = std::make_shared<ConsistentHash>();
  vector<string> nodes = config_->get_nodes();
  for (string node : nodes) {
    PhysicalNode* physicalNode = new PhysicalNode(node);
    consistentHash_->addNode(*physicalNode, loadBalanceFactor);
  }
  dataServerPort_ = config_->get_port();
  dataReplica_ = config_->get_data_replica();
  clientService_ = std::make_shared<ClientService>(config_, log_, shared_from_this());
  clientService_->startService();
}

vector<string> Proxy::getNodes(uint64_t key) {
  return consistentHash_->getNodes(key, dataReplica_);
}

void Proxy::wait() {
    clientService_->wait();
}