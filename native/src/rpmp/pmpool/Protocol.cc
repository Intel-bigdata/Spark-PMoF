/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/Protocol.cc
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Thursday, November 7th 2019, 3:48:52 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#include "pmpool/Protocol.h"

#include <assert.h>

#include "AllocatorProxy.h"
#include "Config.h"
#include "Digest.h"
#include "NetworkServer.h"
#include "Request.h"

// uint64_t timestamp_now() {
//   return std::chrono::high_resolution_clock::now().time_since_epoch() /
//          std::chrono::milliseconds(1);
// }

RecvCallback::RecvCallback(Protocol *protocol, ChunkMgr *chunkMgr)
    : protocol_(protocol), chunkMgr_(chunkMgr) {}

void RecvCallback::operator()(void *buffer_id, void *buffer_size) {
  auto buffer_id_ = *static_cast<int *>(buffer_id);
  Chunk *ck = chunkMgr_->get(buffer_id_);
  assert(*static_cast<uint64_t *>(buffer_size) == ck->size);
  Request *request = new Request(reinterpret_cast<char *>(ck->buffer), ck->size,
                                 reinterpret_cast<Connection *>(ck->con));
  request->decode();
  protocol_->enqueue_recv_msg(request);
  chunkMgr_->reclaim(ck, static_cast<Connection *>(ck->con));
}

ReadCallback::ReadCallback(Protocol *protocol) : protocol_(protocol) {}

void ReadCallback::operator()(void *buffer_id, void *buffer_size) {
  auto buffer_id_ = *static_cast<int *>(buffer_id);
  protocol_->enqueue_rma_msg(buffer_id_);
}

SendCallback::SendCallback(ChunkMgr *chunkMgr) : chunkMgr_(chunkMgr) {}

void SendCallback::operator()(void *buffer_id, void *buffer_size) {
  auto buffer_id_ = *static_cast<int *>(buffer_id);
  auto ck = chunkMgr_->get(buffer_id_);
  chunkMgr_->reclaim(ck, static_cast<Connection *>(ck->con));
}

WriteCallback::WriteCallback(Protocol *protocol) : protocol_(protocol) {}

void WriteCallback::operator()(void *buffer_id, void *buffer_size) {
  auto buffer_id_ = *static_cast<int *>(buffer_id);
  protocol_->enqueue_rma_msg(buffer_id_);
}

RecvWorker::RecvWorker(Protocol *protocol, int index)
    : protocol_(protocol), index_(index) {
  init = false;
}

int RecvWorker::entry() {
  if (!init) {
    set_affinity(index_);
    init = true;
  }
  Request *request;
  bool res = pendingRecvRequestQueue_.wait_dequeue_timed(
      request, std::chrono::milliseconds(1000));
  if (res) {
    protocol_->handle_recv_msg(request);
  }
  return 0;
}

void RecvWorker::abort() {}

void RecvWorker::addTask(Request *request) {
  pendingRecvRequestQueue_.enqueue(request);
}

ReadWorker::ReadWorker(Protocol *protocol, int index)
    : protocol_(protocol), index_(index) {
  init = false;
}

int ReadWorker::entry() {
  if (!init) {
    set_affinity(index_);
    init = true;
  }
  RequestReply *requestReply;
  bool res = pendingReadRequestQueue_.wait_dequeue_timed(
      requestReply, std::chrono::milliseconds(1000));
  if (res) {
    protocol_->handle_rma_msg(requestReply);
  }
  return 0;
}

void ReadWorker::abort() {}

void ReadWorker::addTask(RequestReply *rr) {
  pendingReadRequestQueue_.enqueue(rr);
}

FinalizeWorker::FinalizeWorker(Protocol *protocol) : protocol_(protocol) {}

int FinalizeWorker::entry() {
  RequestReply *requestReply;
  bool res = pendingRequestReplyQueue_.wait_dequeue_timed(
      requestReply, std::chrono::milliseconds(1000));
  if (res) {
    protocol_->handle_finalize_msg(requestReply);
  }
  return 0;
}

void FinalizeWorker::abort() {}

void FinalizeWorker::addTask(RequestReply *requestReply) {
  pendingRequestReplyQueue_.enqueue(requestReply);
}

Protocol::Protocol(Config *config, NetworkServer *server,
                   AllocatorProxy *allocatorProxy)
    : config_(config), networkServer_(server), allocatorProxy_(allocatorProxy) {
  time = 0;
}

int Protocol::init() {
  recvCallback_ =
      std::make_shared<RecvCallback>(this, networkServer_->get_chunk_mgr());
  sendCallback_ =
      std::make_shared<SendCallback>(networkServer_->get_chunk_mgr());
  readCallback_ = std::make_shared<ReadCallback>(this);
  writeCallback_ = std::make_shared<WriteCallback>(this);

  for (int i = 0; i < config_->get_pool_size(); i++) {
    auto recvWorker = new RecvWorker(this, config_->get_affinities_()[i] - 1);
    recvWorker->start();
    recvWorkers_.push_back(recvWorker);
  }

  finalizeWorker_ = make_shared<FinalizeWorker>(this);
  finalizeWorker_->start();

  for (int i = 0; i < config_->get_pool_size(); i++) {
    auto readWorker = new ReadWorker(this, config_->get_affinities_()[i]);
    readWorker->start();
    readWorkers_.push_back(readWorker);
  }

  networkServer_->set_recv_callback(recvCallback_.get());
  networkServer_->set_send_callback(sendCallback_.get());
  networkServer_->set_read_callback(readCallback_.get());
  networkServer_->set_write_callback(writeCallback_.get());
  return 0;
}

void Protocol::enqueue_recv_msg(Request *request) {
  RequestContext rc = request->get_rc();
  if (rc.address != 0) {
    auto wid = GET_WID(rc.address);
    recvWorkers_[wid]->addTask(request);
  } else {
    recvWorkers_[rc.rid % config_->get_pool_size()]->addTask(request);
  }
}

void Protocol::handle_recv_msg(Request *request) {
  RequestContext rc = request->get_rc();
  RequestReplyContext rrc;
  switch (rc.type) {
    case ALLOC: {
      uint64_t addr = allocatorProxy_->allocate_and_write(
          rc.size, nullptr, rc.rid % config_->get_pool_size());
      auto wid = GET_WID(addr);
      assert(wid == rc.rid % config_->get_pool_size());
      rrc.type = ALLOC_REPLY;
      rrc.success = 0;
      rrc.rid = rc.rid;
      rrc.address = addr;
      rrc.size = rc.size;
      rrc.con = rc.con;
      RequestReply *requestReply = new RequestReply(rrc);
      requestReply->encode();
      enqueue_finalize_msg(requestReply);
      break;
    }
    case FREE: {
      rrc.type = FREE_REPLY;
      rrc.success = allocatorProxy_->release(rc.address);
      rrc.rid = rc.rid;
      rrc.address = rc.address;
      rrc.size = rc.size;
      rrc.con = rc.con;
      RequestReply *requestReply = new RequestReply(rrc);
      requestReply->encode();
      enqueue_finalize_msg(requestReply);
      break;
    }
    case WRITE: {
      rrc.type = WRITE_REPLY;
      rrc.success = 0;
      rrc.rid = rc.rid;
      rrc.address = rc.address;
      rrc.src_address = rc.src_address;
      rrc.src_rkey = rc.src_rkey;
      rrc.size = rc.size;
      rrc.con = rc.con;
      networkServer_->get_dram_buffer(&rrc);
      RequestReply *requestReply = new RequestReply(rrc);

      std::unique_lock<std::mutex> lk(rrcMtx_);
      rrcMap_[rrc.ck->buffer_id] = requestReply;
      lk.unlock();
      networkServer_->read(requestReply);
      break;
    }
    case READ: {
      rrc.type = READ_REPLY;
      rrc.success = 0;
      rrc.rid = rc.rid;
      rrc.address = rc.address;
      rrc.src_address = rc.src_address;
      rrc.src_rkey = rc.src_rkey;
      rrc.size = rc.size;
      rrc.con = rc.con;
      rrc.dest_address = allocatorProxy_->get_virtual_address(rrc.address);
      rrc.ck = nullptr;
      Chunk *base_ck = allocatorProxy_->get_rma_chunk(rrc.address);
      networkServer_->get_pmem_buffer(&rrc, base_ck);
      RequestReply *requestReply = new RequestReply(rrc);
      requestReply->encode();

      std::unique_lock<std::mutex> lk(rrcMtx_);
      rrcMap_[rrc.ck->buffer_id] = requestReply;
      lk.unlock();
      networkServer_->write(requestReply);
      break;
    }
    default: { break; }
  }

  delete request;
}

void Protocol::enqueue_finalize_msg(RequestReply *requestReply) {
  finalizeWorker_->addTask(requestReply);
}

void Protocol::handle_finalize_msg(RequestReply *requestReply) {
  RequestReplyContext rrc = requestReply->get_rrc();
  networkServer_->send(reinterpret_cast<char *>(requestReply->data_),
                       requestReply->size_, rrc.con);
}

void Protocol::enqueue_rma_msg(uint64_t buffer_id) {
  std::unique_lock<std::mutex> lk(rrcMtx_);
  RequestReply *requestReply = rrcMap_[buffer_id];
  lk.unlock();
  RequestReplyContext rrc = requestReply->get_rrc();
  if (rrc.address != 0) {
    auto wid = GET_WID(rrc.address);
    readWorkers_[wid]->addTask(requestReply);
  } else {
    readWorkers_[rrc.rid % config_->get_pool_size()]->addTask(requestReply);
  }
}

void Protocol::handle_rma_msg(RequestReply *requestReply) {
  RequestReplyContext &rrc = requestReply->get_rrc();
  switch (rrc.type) {
    case WRITE_REPLY: {
      char *buffer = static_cast<char *>(rrc.ck->buffer);
      if (rrc.address == 0) {
        rrc.address = allocatorProxy_->allocate_and_write(
            rrc.size, buffer, rrc.rid % config_->get_pool_size());
      } else {
        allocatorProxy_->write(rrc.address, buffer, rrc.size);
      }
      networkServer_->reclaim_dram_buffer(&rrc);
      break;
    }
    case READ_REPLY: {
      networkServer_->reclaim_pmem_buffer(&rrc);
      break;
    }
    default: { break; }
  }
  requestReply->encode();
  enqueue_finalize_msg(requestReply);
}
