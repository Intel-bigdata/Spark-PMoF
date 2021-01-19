#ifndef PMPOOL_PROXYEVENT_H_
#define PMPOOL_PROXYEVENT_H_

#include <HPNL/ChunkMgr.h>
#include <HPNL/Connection.h>

#include <vector>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/vector.hpp>

#include "pmpool/Base.h"
#include "pmpool/proxy/PhysicalNode.h"

using std::vector;

class ProxyClientRecvCallback;
class ProxyRequestHandler;
class ProxyServer;
class ClientService;
class Proxy;

enum ProxyOpType : uint32_t {
  GET_HOSTS = 1,
  P_PUT,
  P_PUT_REPLY,
  P_GET,
  P_GET_REPLY
};

struct ProxyRequestMsg {
  template<class Archive>
  void serialize(Archive& ar, const unsigned int version) {
    ar &type;
    ar &rid;
    ar &key;
  }
  uint32_t type;
  uint64_t rid;
  uint64_t key;
};

struct ProxyRequestReplyMsg {
  template<class Archive>
  void serialize(Archive& ar, const unsigned int version) {
    ar &type;
    ar &success;
    ar &rid;
    ar &key;
    // ar &hosts;
    // ar &ports;
    ar &nodes;
  }
  uint32_t type;
  uint32_t success;
  uint64_t rid;
  uint64_t key;
  // vector<std::string> hosts;
  // vector<std::string> ports;
  vector<PhysicalNode> nodes;
};

struct ProxyRequestReplyContext {
  ProxyOpType type;
  uint32_t success;
  uint64_t rid;
  uint64_t key;
  Connection* con;
  // vector<std::string> hosts;
  // vector<std::string> ports;
  vector<PhysicalNode> nodes;
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
  friend ProxyServer;
  friend ClientService;
  friend Proxy;
  char* data_ = nullptr;
  uint64_t size_ = 0;
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
  char* getData() { return data_; }
  uint64_t getSize() { return size_; }

 private:
  std::mutex data_lock_;
  friend ProxyClientRecvCallback;
  friend ProxyRequestHandler;
  char* data_;
  uint64_t size_;
  ProxyRequestContext requestContext_;
};

#endif  // PMPOOL_PROXYEVENT_H_