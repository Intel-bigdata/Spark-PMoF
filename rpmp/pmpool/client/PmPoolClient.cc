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

PmPoolClient::PmPoolClient(const string &remote_address,
                           const string &remote_port) {
  tx_finished = true;
  op_finished = false;
  networkClient_ = make_shared<NetworkClient>(remote_address, remote_port);
  requestHandler_ = make_shared<RequestHandler>(networkClient_);
}

PmPoolClient::~PmPoolClient() {
  requestHandler_->reset();
  networkClient_->reset();

#ifdef DEBUG
  std::cout << "PmPoolClient destructed" << std::endl;
#endif
}

int PmPoolClient::init() {
  auto res = networkClient_->init(requestHandler_);
  requestHandler_->start();
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
  RequestContext rc = {};
  rc.type = ALLOC;
  rc.rid = rid_++;
  rc.size = size;
  auto request = std::make_shared<Request>(rc);
  requestHandler_->addTask(request);
  return requestHandler_->get(request)->address;
}

int PmPoolClient::free(uint64_t address) {
  RequestContext rc = {};
  rc.type = FREE;
  rc.rid = rid_++;
  rc.address = address;
  auto request = std::make_shared<Request>(rc);
  requestHandler_->addTask(request);
  return requestHandler_->get(request)->success;
}

void PmPoolClient::shutdown() { networkClient_->shutdown(); }

void PmPoolClient::wait() { networkClient_->wait(); }

int PmPoolClient::write(uint64_t address, const char *data, uint64_t size) {
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
  auto res = requestHandler_->get(request)->success;
  networkClient_->reclaim_dram_buffer(rc.src_address, rc.size);
  return res;
}

uint64_t PmPoolClient::write(const char *data, uint64_t size) {
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
  auto res = requestHandler_->get(request)->address;
  networkClient_->reclaim_dram_buffer(rc.src_address, rc.size);
  return res;
}

int PmPoolClient::read(uint64_t address, char *data, uint64_t size) {
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
  auto res = requestHandler_->get(request)->success;
  if (!res) {
    memcpy(data, reinterpret_cast<char *>(rc.src_address), size);
  }
  networkClient_->reclaim_dram_buffer(rc.src_address, rc.size);
  return res;
}

int PmPoolClient::read(uint64_t address, char *data, uint64_t size,
                       std::function<void(int)> func) {
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
    auto res = requestHandler_->get(request)->success;
    if (res) {
      memcpy(data, reinterpret_cast<char *>(rc.src_address), size);
    }
    networkClient_->reclaim_dram_buffer(rc.src_address, rc.size);
    func(res);
  });
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
  RequestContext rc = {};
  rc.type = PUT;
  rc.rid = rid_++;
  rc.size = size;
  rc.address = 0;
  // allocate memory for RMA read from client.
  rc.src_address = networkClient_->get_dram_buffer(value, rc.size);
  rc.src_rkey = networkClient_->get_rkey();
#ifdef DEBUG
  std::cout << "[PmPoolClient::put] " << rc.src_rkey << "-" << rc.src_address
            << ":" << rc.size << std::endl;
#endif
  rc.key = key_uint;
  auto request = std::make_shared<Request>(rc);
  requestHandler_->addTask(request);
  auto res = requestHandler_->wait(request);
  networkClient_->reclaim_dram_buffer(rc.src_address, rc.size);
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
  RequestContext rc = {};
  rc.type = GET;
  rc.rid = rid_++;
  rc.size = size;
  rc.address = 0;
  // allocate memory for RMA read from client.
  rc.src_address = networkClient_->get_dram_buffer(nullptr, rc.size);
  rc.src_rkey = networkClient_->get_rkey();
#ifdef DEBUG
  std::cout << "[PmPoolClient::get] " << rc.src_rkey << "-" << rc.src_address
            << ":" << rc.size << std::endl;
#endif
  rc.key = key_uint;
  auto request = std::make_shared<Request>(rc);
  requestHandler_->addTask(request);
  auto res = requestHandler_->wait(request);
  memcpy(value, reinterpret_cast<char *>(rc.src_address), rc.size);
  networkClient_->reclaim_dram_buffer(rc.src_address, rc.size);

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
  RequestContext rc = {};
  rc.type = GET_META;
  rc.rid = rid_++;
  rc.address = 0;
  rc.key = key_uint;
  auto request = std::make_shared<Request>(rc);
  requestHandler_->addTask(request);
  auto rrc = requestHandler_->get(request);
  if (rrc->type == GET_META_REPLY) {
    return rrc->bml;
  } else {
    std::string err_msg =
        "GetMeta function got " + std::to_string(rrc->type) + " msg.";
    std::cerr << err_msg << std::endl;
    throw;
  }
}

int PmPoolClient::del(const string &key) {
  uint64_t key_uint;
  Digest::computeKeyHash(key, &key_uint);
  RequestContext rc = {};
  rc.type = DELETE;
  rc.rid = rid_++;
  rc.key = key_uint;
  auto request = std::make_shared<Request>(rc);
  requestHandler_->addTask(request);
  auto res = requestHandler_->get(request)->success;
  return res;
}
