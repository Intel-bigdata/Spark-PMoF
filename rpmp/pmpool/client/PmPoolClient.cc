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
#include "pmpool/Protocol.h"
#include "pmpool/client/ProxyClient.h"


PmPoolClient::PmPoolClient(const string &remote_address,
    const string &remote_port) {
  tx_finished = true;
  op_finished = false;

  proxyClient_ = make_shared<ProxyClient>();   

  thread t1(&ProxyClient::initProxyClient, proxyClient_);
  t1.detach();

  string node1_address = "172.168.0.209";
  string node1_port = "12346";

  Channel channel1;
  channel1.networkClient = make_shared<NetworkClient>(node1_address, node1_port); 
  channel1.requestHandler = make_shared<RequestHandler>(channel1.networkClient);
  channels.insert(pair<string, Channel>(node1_address, channel1));

  string node2_address = "172.168.0.40";
  string node2_port = "12346";

  Channel channel2;
  channel2.networkClient = make_shared<NetworkClient>(node2_address, node2_port);
  channel2.requestHandler = make_shared<RequestHandler>(channel2.networkClient);
  channels.insert(pair<string, Channel>(node2_address, channel2));

}

PmPoolClient::~PmPoolClient() {
  /**
    requestHandler_->reset();
    networkClient_->reset();
   **/
  map<string, Channel>::iterator itr;
  for (itr = channels.begin(); itr != channels.end(); ++itr){
    itr->second.requestHandler->reset();
    itr->second.networkClient->reset();
  }

#ifdef DEBUG
  std::cout << "PmPoolClient destructed" << std::endl;
#endif
}


int PmPoolClient::init() {

  map<string, Channel>::iterator itr;
  std::vector<int> ress;

  for (itr = channels.begin(); itr != channels.end(); ++itr){
    int res = itr->second.networkClient->init(itr->second.requestHandler); 
    itr->second.requestHandler->start(); 
    ress.push_back(res);
  }                                                           

  int sum = 0;
  for (int i: ress){
    sum += i;
  }

  if (sum < 0 )
    return -1;

  return 0;

}

void PmPoolClient::begin_tx() {
  std::unique_lock<std::mutex> lk(tx_mtx);
  while (!tx_finished) {
    tx_con.wait(lk);
  }
  tx_finished;
}

uint64_t PmPoolClient::alloc(uint64_t size) {
  /**
    RequestContext rc = {};
    rc.type = ALLOC;
    rc.rid = rid_++;
    rc.size = size;
    auto request = std::make_shared<Request>(rc);
    requestHandler_->addTask(request);
    return requestHandler_->get(request).address;
   **/
  return 0;
}

int PmPoolClient::free(uint64_t address) {
  /**
    RequestContext rc = {};
    rc.type = FREE;
    rc.rid = rid_++;
    rc.address = address;
    auto request = std::make_shared<Request>(rc);
    requestHandler_->addTask(request);
    return requestHandler_->get(request).success;
   **/
  return 0;
}

void PmPoolClient::shutdown() {
  map<string, Channel>::iterator itr;
  for (itr = channels.begin(); itr != channels.end(); ++itr){      
    itr->second.networkClient->shutdown(); 
  }                                                                
}

void PmPoolClient::wait() {
  /**
  map<string, Channel>::iterator itr;
  for (itr = channels.begin(); itr != channels.end(); ++itr){
    itr->second.networkClient->wait();
  }
  **/
}

int PmPoolClient::write(uint64_t address, const char *data, uint64_t size) {
  /**
    RequestContext rc = {};
    rc.type = WRITE;
    rc.rid = rid_++;
    rc.size = size;
    rc.address = address;
  // allocate memory for RMA read from client.
  rc.src_address = networkClient_->get_dram_buffer(data, rc.size);
  rc.src_rkey = networkClient_->get_rkey();
  auto request = std::make_shared<Request>(rc);
  requestHandler_->addTask(request);
  auto res = requestHandler_->get(request).success;
  networkClient_->reclaim_dram_buffer(rc.src_address, rc.size);
   **/
  return 0;
}

uint64_t PmPoolClient::write(const char *data, uint64_t size) {
  /**
    RequestContext rc = {};
    rc.type = WRITE;
    rc.rid = rid_++;
    rc.size = size;
    rc.address = 0;
  // allocate memory for RMA read from client.
  rc.src_address = networkClient_->get_dram_buffer(data, rc.size);
  rc.src_rkey = networkClient_->get_rkey();
  auto request = std::make_shared<Request>(rc);
  requestHandler_->addTask(request);
  auto res = requestHandler_->get(request).address;
  networkClient_->reclaim_dram_buffer(rc.src_address, rc.size);
   **/
  return 0;
}

int PmPoolClient::read(uint64_t address, char *data, uint64_t size) {
  /**
    RequestContext rc = {};
    rc.type = READ;
    rc.rid = rid_++;
    rc.size = size;
    rc.address = address;
  // allocate memory for RMA read from client.
  rc.src_address = networkClient_->get_dram_buffer(nullptr, rc.size);
  rc.src_rkey = networkClient_->get_rkey();
  auto request = std::make_shared<Request>(rc);
  requestHandler_->addTask(request);
  auto res = requestHandler_->get(request).success;
  if (!res) {
  memcpy(data, reinterpret_cast<char *>(rc.src_address), size);
  }
  networkClient_->reclaim_dram_buffer(rc.src_address, rc.size);
   **/
  return 0;
}

int PmPoolClient::read(uint64_t address, char *data, uint64_t size,
    std::function<void(int)> func) {
  /**
    RequestContext rc = {};
    rc.type = READ;
    rc.rid = rid_++;
    rc.size = size;
    rc.address = address;
  // allocate memory for RMA read from client.
  rc.src_address = networkClient_->get_dram_buffer(nullptr, rc.size);
  rc.src_rkey = networkClient_->get_rkey();
  auto request = std::make_shared<Request>(rc);
  requestHandler_->addTask(request, [&] {
  auto res = requestHandler_->get(request).success;
  if (res) {
  memcpy(data, reinterpret_cast<char *>(rc.src_address), size);
  }
  networkClient_->reclaim_dram_buffer(rc.src_address, rc.size);
  func(res);
  });
   **/
  return 0;
}

void PmPoolClient::end_tx() {
  std::lock_guard<std::mutex> lk(tx_mtx);
  tx_finished = true;
  tx_con.notify_one();
}

int counter = 0;

uint64_t PmPoolClient::put(const string &key, const char *value,
    uint64_t size) {
  unique_lock<mutex> lk(mtx);
  uint64_t key_uint;
  Digest::computeKeyHash(key, &key_uint);
  string address = proxyClient_->getAddress(key_uint);
  map<string, Channel>::iterator itr = channels.find(address);
  shared_ptr<NetworkClient> networkClient = itr->second.networkClient;
  shared_ptr<RequestHandler> requestHandler = itr->second.requestHandler;
  RequestContext rc = {};
  rc.type = PUT;
  rc.rid = rid_++;
  rc.size = size;
  rc.address = 0;
  // allocate memory for RMA read from client.
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
  auto res = requestHandler->wait(request);
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
  unique_lock<mutex> lk(mtx);
  uint64_t key_uint;
  Digest::computeKeyHash(key, &key_uint);

  string address = proxyClient_->getAddress(key_uint);
  map<string, Channel>::iterator itr = channels.find(address);
  shared_ptr<NetworkClient> networkClient = itr->second.networkClient;   
  shared_ptr<RequestHandler> requestHandler = itr->second.requestHandler;

  RequestContext rc = {};
  rc.type = GET;
  rc.rid = rid_++;
  rc.size = size;
  rc.address = 0;
  // allocate memory for RMA read from client.
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

  string address = proxyClient_->getAddress(key_uint); 
  map<string, Channel>::iterator itr = channels.find(address);
  shared_ptr<NetworkClient> networkClient = itr->second.networkClient;
  shared_ptr<RequestHandler> requestHandler = itr->second.requestHandler;

  RequestContext rc = {};
  rc.type = GET_META;
  rc.rid = rid_++;
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

  string address = proxyClient_->getAddress(key_uint); 
  map<string, Channel>::iterator itr = channels.find(address);
  shared_ptr<NetworkClient> networkClient = itr->second.networkClient;
  shared_ptr<RequestHandler> requestHandler = itr->second.requestHandler;

  RequestContext rc = {};
  rc.type = DELETE;
  rc.rid = rid_++;
  rc.key = key_uint;
  auto request = std::make_shared<Request>(rc);
  requestHandler->addTask(request);
  auto res = requestHandler->get(request).success;
  return res;
}
