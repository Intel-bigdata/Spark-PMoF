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

#include "pmpool/Event.h"
#include "pmpool/ThreadWrapper.h"
#include "pmpool/queue/blockingconcurrentqueue.h"
#include "pmpool/queue/concurrentqueue.h"

class Digest;
class AllocatorProxy;
class Protocol;
class NetworkServer;
class Config;
class RLog;
class DataServerService;

using moodycamel::BlockingConcurrentQueue;
using std::make_shared;

struct MessageHeader {
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
  RecvCallback() = delete;
  RecvCallback(std::shared_ptr<Protocol> protocol,
               std::shared_ptr<ChunkMgr> chunkMgr);
  ~RecvCallback() override = default;
  void operator()(void *buffer_id, void *buffer_size) override;

 private:
  std::shared_ptr<Protocol> protocol_;
  std::shared_ptr<ChunkMgr> chunkMgr_;
};

class SendCallback : public Callback {
 public:
  SendCallback() = delete;
  explicit SendCallback(
      std::shared_ptr<ChunkMgr> chunkMgr,
      std::unordered_map<uint64_t, std::shared_ptr<RequestReply>> rrcMap);
  ~SendCallback() override = default;
  void operator()(void *buffer_id, void *buffer_size) override;

 private:
  std::shared_ptr<ChunkMgr> chunkMgr_;
  std::unordered_map<uint64_t, std::shared_ptr<RequestReply>> rrcMap_;
};

class ReadCallback : public Callback {
 public:
  ReadCallback() = delete;
  explicit ReadCallback(std::shared_ptr<Protocol> protocol);
  ~ReadCallback() override = default;
  void operator()(void *buffer_id, void *buffer_size) override;

 private:
  std::shared_ptr<Protocol> protocol_;
};

class WriteCallback : public Callback {
 public:
  WriteCallback() = delete;
  explicit WriteCallback(std::shared_ptr<Protocol> protocol);
  ~WriteCallback() override = default;
  void operator()(void *buffer_id, void *buffer_size) override;

 private:
  std::shared_ptr<Protocol> protocol_;
};

class RecvWorker : public ThreadWrapper {
 public:
  RecvWorker() = delete;
  RecvWorker(std::shared_ptr<Protocol> protocol, int index);
  ~RecvWorker() override = default;
  int entry() override;
  void abort() override;
  void addTask(std::shared_ptr<Request> request);

 private:
  std::shared_ptr<Protocol> protocol_;
  int index_;
  bool init;
  BlockingConcurrentQueue<std::shared_ptr<Request>> pendingRecvRequestQueue_;
};

class ReadWorker : public ThreadWrapper {
 public:
  ReadWorker() = delete;
  ReadWorker(std::shared_ptr<Protocol> protocol, int index);
  ~ReadWorker() override = default;
  int entry() override;
  void abort() override;
  void addTask(std::shared_ptr<RequestReply> requestReply);

 private:
  std::shared_ptr<Protocol> protocol_;
  int index_;
  bool init;
  BlockingConcurrentQueue<std::shared_ptr<RequestReply>>
      pendingReadRequestQueue_;
};

class FinalizeWorker : public ThreadWrapper {
 public:
  FinalizeWorker() = delete;
  explicit FinalizeWorker(std::shared_ptr<Protocol> protocol);
  ~FinalizeWorker() override = default;
  int entry() override;
  void abort() override;
  void addTask(std::shared_ptr<RequestReply> requestReply);

 private:
  std::shared_ptr<Protocol> protocol_;
  BlockingConcurrentQueue<std::shared_ptr<RequestReply>>
      pendingRequestReplyQueue_;
};

/**
 * @brief Protocol connect NetworkServer and AllocatorProtocol to achieve
 * network and storage co-design. Protocol maitains three queues: recv queue,
 * finalize queue and rma queue. One thread per queue to handle specific event.
 * recv queue-> to handle receive event.
 * finalize queue-> to handle finalization event.
 * rma queue-> to handle remote memory access event.
 */
class Protocol : public std::enable_shared_from_this<Protocol> {
 public:
  Protocol() = delete;
  Protocol(std::shared_ptr<Config> config, std::shared_ptr<RLog> log,
           std::shared_ptr<NetworkServer> server,
           std::shared_ptr<AllocatorProxy> allocatorProxy);
  ~Protocol();
  int init();

  friend class RecvCallback;
  friend class RecvWorker;

  void enqueue_recv_msg(std::shared_ptr<Request> request);
  void handle_recv_msg(std::shared_ptr<Request> request);

  void enqueue_finalize_msg(std::shared_ptr<RequestReply> requestReply);
  void handle_finalize_msg(std::shared_ptr<RequestReply> requestReply);

  void enqueue_rma_msg(uint64_t buffer_id);
  void handle_rma_msg(std::shared_ptr<RequestReply> requestReply);

  void reclaim_dram_buffer(uint64_t key);

  std::shared_ptr<DataServerService> getDataService();

 public:
  std::shared_ptr<Config> config_;
  std::shared_ptr<RLog> log_;
  uint64_t num_requests_ = 0;
  AllocatorProxy* getAllocatorProxy();

 private:
  std::shared_ptr<NetworkServer> networkServer_;
  std::shared_ptr<AllocatorProxy> allocatorProxy_;
  std::shared_ptr<DataServerService> dataService_;

  std::shared_ptr<RecvCallback> recvCallback_;
  std::shared_ptr<SendCallback> sendCallback_;
  std::shared_ptr<ReadCallback> readCallback_;
  std::shared_ptr<WriteCallback> writeCallback_;

  BlockingConcurrentQueue<Chunk *> recvMsgQueue_;
  BlockingConcurrentQueue<Chunk *> readMsgQueue_;

  std::vector<std::shared_ptr<RecvWorker>> recvWorkers_;
  std::shared_ptr<FinalizeWorker> finalizeWorker_;
  std::vector<std::shared_ptr<ReadWorker>> readWorkers_;

  std::mutex rrcMtx_;
  std::unordered_map<uint64_t, std::shared_ptr<RequestReply>> rrcMap_;
  uint64_t time;

  std::mutex replicateMtx_;
  std::map<uint64_t, std::shared_ptr<RequestReply>> replicateMap_;
};

#endif  // PMPOOL_PROTOCOL_H_
