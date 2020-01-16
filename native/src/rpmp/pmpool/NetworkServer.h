/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/NetworkServer.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Tuesday, December 10th 2019, 3:14:59 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_NETWORKSERVER_H_
#define PMPOOL_NETWORKSERVER_H_

#include <HPNL/ChunkMgr.h>
#include <HPNL/Connection.h>
#include <HPNL/Server.h>

#include <atomic>
#include <memory>

#include "RmaBufferRegister.h"

class CircularBuffer;
class Config;
class RequestReply;
class RequestReplyContext;

class NetworkServer : public RmaBufferRegister {
 public:
  explicit NetworkServer(Config *config);
  int init();
  int start();
  void wait();
  Chunk *register_rma_buffer(char *rma_buffer, uint64_t size) override;
  void unregister_rma_buffer(int buffer_id) override;
  void get_dram_buffer(RequestReplyContext* rrc);
  void reclaim_dram_buffer(RequestReplyContext* rrc);
  void get_pmem_buffer(RequestReplyContext* rrc, Chunk* ck);
  void reclaim_pmem_buffer(RequestReplyContext* rrc);
  ChunkMgr *get_chunk_mgr();
  void set_recv_callback(Callback *callback);
  void set_send_callback(Callback *callback);
  void set_read_callback(Callback *callback);
  void set_write_callback(Callback *callback);
  void send(char *data, uint64_t size, Connection *con);
  void read(RequestReply *rrc);
  void write(RequestReply *rrc);

 private:
  Config *config_;
  std::shared_ptr<Server> server_;
  std::shared_ptr<ChunkMgr> chunkMgr_;
  std::shared_ptr<CircularBuffer> circularBuffer_;
  std::atomic<uint64_t> buffer_id_{0};
  uint64_t time;
};

#endif  // PMPOOL_NETWORKSERVER_H_
