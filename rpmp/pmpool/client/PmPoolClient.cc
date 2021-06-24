/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/client/PmPoolClient.cc
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool/client
 * Created Date: Friday, December 13th 2019, 3:44:08 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#include "pmpool/client/PmPoolClient.h"

#include "NetworkClient.h"
#include "pmpool/Digest.h"
#include "pmpool/Event.h"
#include "pmpool/client/ProxyClient.h"

/**
 * In RPMP HA mode, proxy_address is a string containing multiple proxies with "," separated.
 */
PmPoolClient::PmPoolClient(const string &proxy_address, const string &proxy_port) {
  tx_finished = true;
  op_finished = false;
  proxyClient_ = make_shared<ProxyClient>(proxy_address, proxy_port);
}

PmPoolClient::~PmPoolClient() {
  map<string, std::shared_ptr<Channel>>::iterator itr;
  for (itr = channels.begin(); itr != channels.end(); ++itr){
    itr->second->requestHandler->reset();
    itr->second->networkClient->reset();
  }
  proxyClient_->reset();

#ifdef DEBUG
  std::cout << "PmPoolClient destructed" << std::endl;
#endif
}

std::shared_ptr<Channel> PmPoolClient::getChannel(PhysicalNode node) {
  std::unique_lock<std::mutex> lk(channel_mtx);
  if (channels.count(node.getKey())) {
    return channels.find(node.getKey())->second;
  } else if (deadNodes.find(node.getKey()) != deadNodes.end()) {
    throw "RPMP channel is dead";
  } else {
    std::shared_ptr<Channel> channel = std::make_shared<Channel>();
    channel->networkClient =
        std::make_shared<NetworkClient>(node.getIp(), node.getPort());
    channel->requestHandler =
        std::make_shared<RequestHandler>(channel->networkClient);
    int res = channel->networkClient->init(channel->requestHandler);
    if (res) {
      deadNodes.insert(node.getKey());
      throw "Failed to init RPMP client";
    }
    channel->requestHandler->start();
    channels.insert(make_pair(node.getKey(), channel));
    return channel;
  }
}

int PmPoolClient::init() {
  auto res = proxyClient_->initProxyClient();
  if (res) {
    throw "Failed to init proxy client";
  }
  return res;
}

void PmPoolClient::begin_tx() {
  std::unique_lock<std::mutex> lk(tx_mtx);
  while (!tx_finished) {
    tx_con.wait(lk);
  }
  tx_finished;
}

uint64_t PmPoolClient::alloc(uint64_t size) {
  // RequestContext rc = {};
  // rc.type = ALLOC;
  // rc.rid = rid_++;
  // rc.size = size;
  // auto request = std::make_shared<Request>(rc);
  // requestHandler_->addTask(request);
  // return requestHandler_->get(request).address;
  return 0;
}

int PmPoolClient::free(uint64_t address) {
  // RequestContext rc = {};
  // rc.type = FREE;
  // rc.rid = rid_++;
  // rc.address = address;
  // auto request = std::make_shared<Request>(rc);
  // requestHandler_->addTask(request);
  // return requestHandler_->get(request).success;
  return 0;
}

void PmPoolClient::shutdown() {
  map<string, std::shared_ptr<Channel>>::iterator itr;
  for (itr = channels.begin(); itr != channels.end(); ++itr){      
    itr->second->networkClient->shutdown(); 
  }
  proxyClient_->shutdown();
}

void PmPoolClient::wait() {
  map<string, std::shared_ptr<Channel>>::iterator itr;
  for (itr = channels.begin(); itr != channels.end(); ++itr){
    itr->second->networkClient->wait();
  }
  proxyClient_->wait();
}

int PmPoolClient::write(uint64_t address, const char *data, uint64_t size) {
  // RequestContext rc = {};
  // rc.type = WRITE;
  // rc.rid = rid_++;
  // rc.size = size;
  // rc.address = address;
  // // allocate memory for RMA read from client.
  // rc.src_address = networkClient_->get_dram_buffer(data, rc.size);
  // rc.src_rkey = networkClient_->get_rkey();
  // auto request = std::make_shared<Request>(rc);
  // requestHandler_->addTask(request);
  // auto res = requestHandler_->get(request).success;
  // networkClient_->reclaim_dram_buffer(rc.src_address, rc.size);
  return 0;
}

uint64_t PmPoolClient::write(const char *data, uint64_t size) {
  // RequestContext rc = {};
  // rc.type = WRITE;
  // rc.rid = rid_++;
  // rc.size = size;
  // rc.address = 0;
  // // allocate memory for RMA read from client.
  // rc.src_address = networkClient_->get_dram_buffer(data, rc.size);
  // rc.src_rkey = networkClient_->get_rkey();
  // auto request = std::make_shared<Request>(rc);
  // requestHandler_->addTask(request);
  // auto res = requestHandler_->get(request).address;
  // networkClient_->reclaim_dram_buffer(rc.src_address, rc.size);
  return 0;
}

int PmPoolClient::read(uint64_t address, char *data, uint64_t size) {
  // RequestContext rc = {};
  // rc.type = READ;
  // rc.rid = rid_++;
  // rc.size = size;
  // rc.address = address;
  // // allocate memory for RMA read from client.
  // rc.src_address = networkClient_->get_dram_buffer(nullptr, rc.size);
  // rc.src_rkey = networkClient_->get_rkey();
  // auto request = std::make_shared<Request>(rc);
  // requestHandler_->addTask(request);
  // auto res = requestHandler_->get(request).success;
  // if (!res) {
  //   memcpy(data, reinterpret_cast<char *>(rc.src_address), size);
  // }
  // networkClient_->reclaim_dram_buffer(rc.src_address, rc.size);
  return 0;
}

int PmPoolClient::read(uint64_t address, char *data, uint64_t size,
                       std::function<void(int)> func) {
  // RequestContext rc = {};
  // rc.type = READ;
  // rc.rid = rid_++;
  // rc.size = size;
  // rc.address = address;
  // // allocate memory for RMA read from client.
  // rc.src_address = networkClient_->get_dram_buffer(nullptr, rc.size);
  // rc.src_rkey = networkClient_->get_rkey();
  // auto request = std::make_shared<Request>(rc);
  // requestHandler_->addTask(request, [&] {
  //   auto res = requestHandler_->get(request).success;
  //   if (res) {
  //     memcpy(data, reinterpret_cast<char *>(rc.src_address), size);
  //   }
  //   networkClient_->reclaim_dram_buffer(rc.src_address, rc.size);
  //   func(res);
  // });
  return 0;
}

void PmPoolClient::end_tx() {
  std::lock_guard<std::mutex> lk(tx_mtx);
  tx_finished = true;
  tx_con.notify_one();
}

uint64_t PmPoolClient::put(const string &key, const char *value,
                           uint64_t size) {
  uint64_t key_uint;
  Digest::computeKeyHash(key, &key_uint);

  ProxyRequestContext prc = {};
  prc.type = GET_HOSTS;
  prc.rid = rid_++;
  prc.key = key_uint;
  auto pRequest = std::make_shared<ProxyRequest>(prc);
  proxyClient_->addTask(pRequest);
  ProxyRequestReplyContext prrc = proxyClient_->get(pRequest);
  proxyClient_->addRequest(pRequest);
  std::shared_ptr<Channel> channel = getChannel(*prrc.nodes.begin());
  std::shared_ptr<NetworkClient> networkClient = channel->networkClient;
  std::shared_ptr<RequestHandler> requestHandler = channel->requestHandler;

  RequestContext rc = {};
  rc.type = PUT;
  rc.rid = prc.rid;
  rc.size = size;
  rc.address = 0;
  // allocate memory for RDMA read from data server. 
#ifdef DEBUG
  std::cout << "[PmPoolClient::put start] " << key << "-" << rc.size
    << ", hashkey is " << key_uint << std::endl;
#endif
  rc.src_address = networkClient->get_dram_buffer(value, rc.size);
  rc.src_rkey = networkClient->get_rkey();
#ifdef DEBUG
  std::cout << "[PmPoolClient::put] " << key << "-" << rc.size
    << ", hashkey is " << key_uint << std::endl;
#endif
  rc.key = key_uint;
  auto request = std::make_shared<Request>(rc);
  requestHandler->addTask(request);
  auto res = proxyClient_->get(pRequest).success;
  networkClient->reclaim_dram_buffer(rc.src_address, rc.size);
#ifdef DEBUG
  fprintf(stderr, "[PUT]key is %s, length is %ld, content is \n", key.c_str(),
      size);
  for (int i = 0; i < 100; i++) {
    fprintf(stderr, "%X ", *(value + i));
  }
  fprintf(stderr, " ...\n");
#endif
  return res;
}

uint64_t PmPoolClient::get(const string &key, char *value, uint64_t size) {
  uint64_t key_uint;
  Digest::computeKeyHash(key, &key_uint);

  ProxyRequestContext prc = {};
  prc.type = GET_REPLICA;
  prc.rid = rid_++;
  prc.key = key_uint;
  auto pRequest = std::make_shared<ProxyRequest>(prc);
  proxyClient_->addTask(pRequest);
  ProxyRequestReplyContext prrc = proxyClient_->get(pRequest);
  std::shared_ptr<Channel> channel;
  for (auto node : prrc.nodes) {
    std::unique_lock<std::mutex> lk(channel_mtx);
    if (deadNodes.find(node.getKey()) != deadNodes.end()) {
      continue;
    }
    lk.unlock();
    try {
      channel = getChannel(node);
      break;
    } catch (const char *msg) {
      std::cout << msg << std::endl;
    }
  }
  if (!channel) {
    std::cout << "No channel available" << std::endl;
    return -1;
  }
  std::shared_ptr<NetworkClient> networkClient = channel->networkClient;
  std::shared_ptr<RequestHandler> requestHandler = channel->requestHandler;

  RequestContext rc = {};
  rc.type = GET;
  rc.rid = prc.rid;
  rc.size = size;
  rc.address = 0;
  // allocate memory for RMA write from data server.
  rc.src_address = networkClient->get_dram_buffer(nullptr, rc.size);
  rc.src_rkey = networkClient->get_rkey();
#ifdef DEBUG
  std::cout << "[PmPoolClient::get] " << key << "-" << rc.size
    << ", hashkey is " << key_uint << std::endl;
#endif
  rc.key = key_uint;
  auto request = std::make_shared<Request>(rc);
  requestHandler->addTask(request);
  auto res = requestHandler->wait(request);
  memcpy(value, reinterpret_cast<char *>(rc.src_address), rc.size);
  networkClient->reclaim_dram_buffer(rc.src_address, rc.size);

#ifdef DEBUG
  fprintf(stderr, "[GET]key is %s, length is %ld, content is \n", key.c_str(),
      size);
  for (int i = 0; i < 100; i++) {
    fprintf(stderr, "%X ", *(value + i));
  }
  fprintf(stderr, " ...\n");
#endif
  return res;
}

vector<block_meta> PmPoolClient::getMeta(const string &key) {
  uint64_t key_uint;
  Digest::computeKeyHash(key, &key_uint);

  ProxyRequestContext prc = {};
  prc.type = GET_REPLICA;
  prc.rid = rid_++;
  prc.key = key_uint;
  auto pRequest = std::make_shared<ProxyRequest>(prc);
  proxyClient_->addTask(pRequest);
  ProxyRequestReplyContext prrc = proxyClient_->get(pRequest);
  std::shared_ptr<Channel> channel = getChannel(*prrc.nodes.begin());
  std::shared_ptr<NetworkClient> networkClient = channel->networkClient;
  std::shared_ptr<RequestHandler> requestHandler = channel->requestHandler;

  RequestContext rc = {};
  rc.type = GET_META;
  rc.rid = prc.rid;
  rc.address = 0;
  rc.key = key_uint;
  auto request = std::make_shared<Request>(rc);
  requestHandler->addTask(request);
  auto rrc = requestHandler->get(request);
  return rrc.bml;
}

int PmPoolClient::del(const string &key) {
  uint64_t key_uint;
  Digest::computeKeyHash(key, &key_uint);

  ProxyRequestContext prc = {};
  prc.type = GET_REPLICA;
  prc.rid = rid_++;
  prc.key = key_uint;
  auto pRequest = std::make_shared<ProxyRequest>(prc);
  proxyClient_->addTask(pRequest);
  ProxyRequestReplyContext prrc = proxyClient_->get(pRequest);
  std::shared_ptr<Channel> channel = getChannel(*prrc.nodes.begin());
  std::shared_ptr<NetworkClient> networkClient = channel->networkClient;
  std::shared_ptr<RequestHandler> requestHandler = channel->requestHandler;

  RequestContext rc = {};
  rc.type = DELETE;
  rc.rid = prc.rid;
  rc.key = key_uint;
  auto request = std::make_shared<Request>(rc);
  requestHandler->addTask(request);
  auto res = requestHandler->get(request).success;
  return res;
}
