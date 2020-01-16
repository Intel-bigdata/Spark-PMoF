/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/Request.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Friday, December 13th 2019, 3:43:30 pm
 * Author: root
 *
 * Copyright (c) Intel
 */

#ifndef PMPOOL_REQUEST_H_
#define PMPOOL_REQUEST_H_

#include <HPNL/ChunkMgr.h>
#include <HPNL/Connection.h>

#include <future>  // NOLINT

#include "pmpool/base.h"

using std::future;
using std::promise;

class RequestHandler;
class ClientRecvCallback;
class Protocol;

enum OpType : uint32_t {
  ALLOC = 1,
  FREE,
  PREPARE,
  WRITE,
  READ,
  REPLY = 1 << 16,
  ALLOC_REPLY,
  FREE_REPLY,
  PREPARE_REPLY,
  WRITE_REPLY,
  READ_REPLY
};

struct RequestReplyContext {
  OpType type;
  uint32_t success;
  uint64_t rid;
  uint64_t address;
  uint64_t src_address;
  uint64_t dest_address;
  uint64_t src_rkey;
  uint64_t size;
  Connection* con;
  Chunk* ck;
};

template <class T>
inline void encode_(T* t, char* data, uint64_t* size) {
  assert(t != nullptr);
  memcpy(data, t, sizeof(t));
  *size = sizeof(t);
}

template <class T>
inline void decode_(T* t, char* data, uint64_t size) {
  assert(t != nullptr);
  assert(size == sizeof(t));
  memcpy(t, data, size);
}

class RequestReply {
 public:
  explicit RequestReply(RequestReplyContext requestReplyContext);
  RequestReply(char* data, uint64_t size, Connection* con);
  ~RequestReply();
  RequestReplyContext& get_rrc();
  void decode();
  void encode();

 private:
  friend Protocol;
  char data_[100];
  uint64_t size_;
  RequestReplyMsg requestReplyMsg_;
  RequestReplyContext requestReplyContext_;
};

typedef promise<RequestReplyContext> Promise;
typedef future<RequestReplyContext> Future;

struct RequestContext {
  OpType type;
  uint64_t rid;
  uint64_t address;
  uint64_t src_address;
  uint64_t src_rkey;
  uint64_t size;
  Connection* con;
};

class Request {
 public:
  explicit Request(RequestContext requestContext);
  Request(char* data, uint64_t size, Connection* con);
  ~Request();
  RequestContext& get_rc();
  void encode();
  void decode();

 private:
  friend RequestHandler;
  friend ClientRecvCallback;
  char data_[100];
  uint64_t size_;
  RequestMsg requestMsg_;
  RequestContext requestContext_;
};

#endif  // PMPOOL_REQUEST_H_
