/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/client/PmPoolClient.cc
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool/client
 * Created Date: Friday, December 13th 2019, 3:44:08 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#include "pmpool/client/PmPoolClient.h"

#include "../Digest.h"
#include "../Protocol.h"
#include "../Request.h"
#include "NetworkClient.h"

PmPoolClient::PmPoolClient(string remote_address, string remote_port) {
  tx_finished = true;
  op_finished = false;
  networkClient_ = make_shared<NetworkClient>(remote_address, remote_port);
  requestHandler_ = make_shared<RequestHandler>(networkClient_.get());
}

PmPoolClient::~PmPoolClient() {}

int PmPoolClient::init() { networkClient_->init(requestHandler_.get()); }

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
  Request request(rc);
  requestHandler_->addTask(&request);
  requestHandler_->wait();
  return requestHandler_->get().address;
}

int PmPoolClient::free(uint64_t address) {
  RequestContext rc = {};
  rc.type = FREE;
  rc.rid = rid_++;
  rc.address = address;
  Request request(rc);
  requestHandler_->addTask(&request);
  requestHandler_->wait();
  return requestHandler_->get().success;
}

void PmPoolClient::shutdown() { networkClient_->shutdown(); }

void PmPoolClient::wait() { networkClient_->wait(); }

int PmPoolClient::write(uint64_t address, char *data, uint64_t size) {
  RequestContext rc = {};
  rc.type = WRITE;
  rc.rid = rid_++;
  rc.size = size;
  rc.address = address;
  // allocate memory for RMA read from client.
  rc.src_address = networkClient_->get_dram_buffer(data, rc.size);
  rc.src_rkey = networkClient_->get_rkey();
  Request request(rc);
  requestHandler_->addTask(&request);
  requestHandler_->wait();
  auto res = requestHandler_->get().success;
  networkClient_->reclaim_dram_buffer(rc.src_address, rc.size);
  return res;
}

uint64_t PmPoolClient::write(char *data, uint64_t size) {
  RequestContext rc = {};
  rc.type = WRITE;
  rc.rid = rid_++;
  rc.size = size;
  rc.address = 0;
  // allocate memory for RMA read from client.
  rc.src_address = networkClient_->get_dram_buffer(data, rc.size);
  rc.src_rkey = networkClient_->get_rkey();
  Request request(rc);
  requestHandler_->addTask(&request);
  requestHandler_->wait();
  auto res = requestHandler_->get().address;
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
  Request request(rc);
  requestHandler_->addTask(&request);
  requestHandler_->wait();
  auto res = requestHandler_->get().success;
  if (res) {
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
  Request request(rc);
  requestHandler_->addTask(&request, [&] {
    auto res = requestHandler_->get().success;
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
