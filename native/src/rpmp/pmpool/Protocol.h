/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/Protocol.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Thursday, November 7th 2019, 3:48:52 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_PROTOCOL_H_
#define PMPOOL_PROTOCOL_H_

#include <HPNL/Callback.h>
#include <HPNL/ChunkMgr.h>
#include <HPNL/Connection.h>

#include <cassert>
#include <chrono>  // NOLINT
#include <cstring>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "flatbuffers/flatbuffers.h"

#include "Request.h"
#include "ThreadWrapper.h"
#include "queue/blockingconcurrentqueue.h"
#include "queue/concurrentqueue.h"

class Digest;
class AllocatorProxy;
class Protocol;
class NetworkServer;
class Config;

using moodycamel::BlockingConcurrentQueue;
using std::make_shared;

struct MessageHeader {
  MessageHeader() {}
  MessageHeader(uint8_t msg_type, uint64_t sequence_id) {
    msg_type_ = msg_type;
    sequence_id_ = sequence_id;
  }
  uint8_t msg_type_;
  uint64_t sequence_id_;
  int msg_size;
};

class RecvCallback : public Callback {
 public:
  RecvCallback(Protocol *protocol, ChunkMgr *chunkMgr);
  ~RecvCallback() override = default;
  void operator()(void *buffer_id, void *buffer_size) override;

 private:
  Protocol *protocol_;
  ChunkMgr *chunkMgr_;
};

class SendCallback : public Callback {
 public:
  explicit SendCallback(ChunkMgr *chunkMgr);
  ~SendCallback() override = default;
  void operator()(void *buffer_id, void *buffer_size) override;

 private:
  ChunkMgr *chunkMgr_;
};

class ReadCallback : public Callback {
 public:
  explicit ReadCallback(Protocol *protocol);
  ~ReadCallback() override = default;
  void operator()(void *buffer_id, void *buffer_size) override;

 private:
  Protocol *protocol_;
};

class WriteCallback : public Callback {
 public:
  explicit WriteCallback(Protocol *protocol);
  ~WriteCallback() override = default;
  void operator()(void *buffer_id, void *buffer_size) override;

 private:
  Protocol *protocol_;
};

class RecvWorker : public ThreadWrapper {
 public:
  RecvWorker(Protocol *protocol, int index);
  ~RecvWorker() override = default;
  int entry() override;
  void abort() override;
  void addTask(Request *request);

 private:
  Protocol *protocol_;
  int index_;
  bool init;
  BlockingConcurrentQueue<Request *> pendingRecvRequestQueue_;
};

class ReadWorker : public ThreadWrapper {
 public:
  ReadWorker(Protocol *protocol, int index);
  ~ReadWorker() override = default;
  int entry() override;
  void abort() override;
  void addTask(RequestReply *requestReply);

 private:
  Protocol *protocol_;
  int index_;
  bool init;
  BlockingConcurrentQueue<RequestReply*> pendingReadRequestQueue_;
};

class FinalizeWorker : public ThreadWrapper {
 public:
  explicit FinalizeWorker(Protocol *protocol);
  ~FinalizeWorker() override = default;
  int entry() override;
  void abort() override;
  void addTask(RequestReply *requestReply);

 private:
  Protocol *protocol_;
  BlockingConcurrentQueue<RequestReply *> pendingRequestReplyQueue_;
};

class Protocol {
 public:
  Protocol() = default;
  Protocol(Config *config, NetworkServer *server,
           AllocatorProxy *allocatorProxy);
  ~Protocol() = default;
  int init();

  friend class RecvCallback;
  friend class RecvWorker;

  void enqueue_recv_msg(Request *request);
  void handle_recv_msg(Request *request);
  void enqueue_finalize_msg(RequestReply *requestReply);
  void handle_finalize_msg(RequestReply *requestReply);
  void enqueue_rma_msg(uint64_t buffer_id);
  void handle_rma_msg(RequestReply *requestReply);

 public:
  Config *config_;

 private:
  NetworkServer *networkServer_;
  AllocatorProxy *allocatorProxy_;
  std::shared_ptr<RecvCallback> recvCallback_;
  std::shared_ptr<SendCallback> sendCallback_;
  std::shared_ptr<ReadCallback> readCallback_;
  std::shared_ptr<WriteCallback> writeCallback_;
  BlockingConcurrentQueue<Chunk *> recvMsgQueue_;
  BlockingConcurrentQueue<Chunk *> readMsgQueue_;
  std::vector<RecvWorker *> recvWorkers_;
  std::shared_ptr<FinalizeWorker> finalizeWorker_;
  std::vector<ReadWorker *> readWorkers_;
  std::mutex rrcMtx_;
  std::unordered_map<uint64_t, RequestReply *> rrcMap_;
  uint64_t time;
};

#endif  // PMPOOL_PROTOCOL_H_
