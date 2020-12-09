#ifndef PMPOOL_PROXYEVENT_H_
#define PMPOOL_PROXYEVENT_H_

#include <HPNL/ChunkMgr.h>
#include <HPNL/Connection.h>

#include <vector>

#include "pmpool/Base.h"
#include "pmpool/PmemAllocator.h"

using std::vector;

class ClientRecvCallback;
class ProxyClient;
class ProxyRequestHandler;
class ProxyServer;
class RecvCallback;

enum ProxyOpType : uint32_t {
  GET_HOSTS = 1
};

/**
 * @brief Define two types of event in this file: Request, RequestReply
 * Request: a event that client creates and sends to server.
 * RequestReply: a event that server creates and sends to client.
 * RequestContext and RequestReplyContext include the context information of the
 * previous two events.
 */
struct ProxyRequestReplyContext {
  ProxyOpType type;
  uint32_t success;
  uint64_t rid;
  uint64_t key;
  Connection* con;
  Chunk* ck;
  string host;
};

class ProxyRequestReply {
 public:
  ProxyRequestReply() = delete;
  explicit ProxyRequestReply(ProxyRequestReplyContext& requestReplyContext);
  ProxyRequestReply(char* data, uint64_t size, Connection* con);
  ~ProxyRequestReply();
  ProxyRequestReplyContext& get_rrc();
  void set_rrc(ProxyRequestReplyContext& rrc);
  void decode();
  void encode();

 private:
  std::mutex data_lock_;
  friend RecvCallback;
  friend ProxyServer;
  char* data_ = nullptr;
  uint64_t size_ = 0;
  // RequestReplyMsg requestReplyMsg_;
  ProxyRequestReplyContext requestReplyContext_;
};

struct ProxyRequestContext {
  ProxyOpType type;
  uint64_t rid;
  uint64_t key;
  Connection* con;
};

class ProxyRequest {
 public:
  ProxyRequest() = delete;
  explicit ProxyRequest(ProxyRequestContext requestContext);
  ProxyRequest(char* data, uint64_t size, Connection* con);
  ~ProxyRequest();
  ProxyRequestContext& get_rc();
  void encode();
  void decode();
  //#ifdef DEBUG
  char* getData() { return data_; }
  uint64_t getSize() { return size_; }
  //#endif

 private:
  std::mutex data_lock_;
  friend ClientRecvCallback;
  friend ProxyRequestHandler;
  friend ProxyClient;
  char* data_;
  uint64_t size_;
  ProxyRequestContext requestContext_;
};

#endif  // PMPOOL_PROXYEVENT_H_
