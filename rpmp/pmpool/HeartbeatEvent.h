
#ifndef SPARK_PMOF_HEARTBEATEVENT_H
#define SPARK_PMOF_HEARTBEATEVENT_H

#include <HPNL/ChunkMgr.h>
#include <HPNL/Connection.h>

#include <future>  // NOLINT
#include <vector>

#include "pmpool/Base.h"
#include "pmpool/PmemAllocator.h"

using std::future;
using std::promise;
using std::vector;

class HeartbeatRequestHandler;
class HeartbeatRecvCallback;
class Protocol;

enum HeartbeatOpType : uint32_t {
  HEARTBEAT = 1,
  HEARTBEAT_REPLY = 1 << 16,
};

/**
 * @brief Define two types of event in this file: Request, RequestReply
 * Request: a event that client creates and sends to server.
 * RequestReply: a event that server creates and sends to client.
 * RequestContext and RequestReplyContext include the context information of the
 * previous two events.
 */
struct HeartbeatRequestReplyContext {
  HeartbeatOpType type;
  uint32_t success;
  uint64_t rid;
  Connection* con;
  Chunk* ck;
};

class HeartbeatRequestReply {
public:
  HeartbeatRequestReply() = delete;
  explicit HeartbeatRequestReply(HeartbeatRequestReplyContext& requestReplyContext);
  HeartbeatRequestReply(char* data, uint64_t size, Connection* con);
  ~HeartbeatRequestReply();
  HeartbeatRequestReplyContext& get_rrc();
  void set_rrc(HeartbeatRequestReplyContext& rrc);
  void decode();
  void encode();
  char* data_ = nullptr;
  uint64_t size_ = 0;

private:
  std::mutex data_lock_;
  friend Protocol;

  // RequestReplyMsg requestReplyMsg_;
  HeartbeatRequestReplyContext requestReplyContext_;
};

struct HeartbeatRequestContext {
  HeartbeatOpType type;
  uint64_t rid = 0;
  uint64_t host_ip_hash;
  uint64_t port;
  Connection* con;
};

class HeartbeatRequest {
public:
  HeartbeatRequest() = delete;
  explicit HeartbeatRequest(HeartbeatRequestContext requestContext);
  HeartbeatRequest(char* data, uint64_t size, Connection* con);
  ~HeartbeatRequest();
  HeartbeatRequestContext& get_rc();
  void encode();
  void decode();
  //#ifdef DEBUG
  char* getData() { return data_; }
  uint64_t getSize() { return size_; }
  //#endif

private:
  std::mutex data_lock_;
  friend HeartbeatRequestHandler;
  friend HeartbeatRecvCallback;
  char* data_;
  uint64_t size_;
  HeartbeatRequestContext requestContext_;
};

#endif //SPARK_PMOF_HEARTBEATEVENT_H
