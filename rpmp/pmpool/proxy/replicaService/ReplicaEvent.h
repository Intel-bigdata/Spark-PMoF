#ifndef PMPOOL_REPLICAEVENT_H_
#define PMPOOL_REPLICAEVENT_H_

#include <HPNL/ChunkMgr.h>
#include <HPNL/Connection.h>

#include <stdint.h>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/unordered_set.hpp>
#include <vector>
#include <unordered_set>
#include <sstream>

#include "pmpool/proxy/PhysicalNode.h"

using std::vector;

// class ProxyClientRecvCallback;
// class ProxyRequestHandler;
class ProxyServer;
// class DataServiceRequestHandler;
class Protocol;
class ReplicaService;
class DataServiceRequestHandler;
class DataServerService;

enum ReplicaOpType : uint32_t { REGISTER, REPLICATE, REPLICA_REPLY, REPLICATE_DIRECT };

struct ReplicaRequestMsg {
  template <class Archive>
  void serialize(Archive& ar, const unsigned int version) {
    ar& type;
    ar& rid;
    ar& key;
    ar& size;
    ar& node;
    ar& src_address;
    ar& des_address;
  }
  uint32_t type;
  uint64_t rid;
  uint64_t key;
  uint64_t size;
  PhysicalNode node;
  uint64_t src_address;
  uint64_t des_address;
};

struct ReplicaRequestReplyMsg {
  template <class Archive>
  void serialize(Archive& ar, const unsigned int version) {
    ar& type;
    ar& success;
    ar& rid;
    ar& key;
    ar& size;
    ar& nodes;
    ar& src_address;
  }
  uint32_t type;
  uint32_t success;
  uint64_t rid;
  uint64_t key;
  uint64_t size;
  std::unordered_set<PhysicalNode, PhysicalNodeHash> nodes;
  uint64_t src_address;
};

struct ReplicaRequestReplyContext {
  ReplicaOpType type;
  uint32_t success;
  uint64_t rid;
  uint64_t key;
  uint64_t size;
  std::unordered_set<PhysicalNode, PhysicalNodeHash> nodes;
  uint64_t src_address;
  Connection* con;
};

class ReplicaRequestReply {
 public:
  char* data_ = nullptr;
  uint64_t size_ = 0;
  ReplicaRequestReply() = delete;
  explicit ReplicaRequestReply(ReplicaRequestReplyContext& requestReplyContext)
      : data_(nullptr), size_(0), requestReplyContext_(requestReplyContext){};
  ReplicaRequestReply(char* data, uint64_t size, Connection* con)
      : size_(size) {
    data_ = static_cast<char*>(std::malloc(size_));
    memcpy(data_, data, size_);
    requestReplyContext_ = ReplicaRequestReplyContext();
    requestReplyContext_.con = con;
  };
  ~ReplicaRequestReply() {
    const std::lock_guard<std::mutex> lock(data_lock_);
    if (data_ != nullptr) {
      std::free(data_);
      data_ = nullptr;
    }
  };
  ReplicaRequestReplyContext& get_rrc() { return requestReplyContext_; };
  void set_rrc(ReplicaRequestReplyContext& rrc) {
    memcpy(&requestReplyContext_, &rrc, sizeof(ReplicaRequestReplyContext));
  };
  void encode() {
    const std::lock_guard<std::mutex> lock(data_lock_);
    ReplicaRequestReplyMsg requestReplyMsg;
    requestReplyMsg.type = (ReplicaOpType)requestReplyContext_.type;
    requestReplyMsg.success = requestReplyContext_.success;
    requestReplyMsg.rid = requestReplyContext_.rid;
    requestReplyMsg.key = requestReplyContext_.key;
    requestReplyMsg.size = requestReplyContext_.size;
    requestReplyMsg.nodes = requestReplyContext_.nodes;
    requestReplyMsg.src_address = requestReplyContext_.src_address;
    std::ostringstream os;
    boost::archive::text_oarchive ao(os);
    ao << requestReplyMsg;
    size_ = os.str().length() + 1;
    data_ = static_cast<char*>(std::malloc(size_));
    memcpy(data_, os.str().c_str(), size_);
  };
  void decode() {
    const std::lock_guard<std::mutex> lock(data_lock_);
    if (data_ == nullptr) {
      std::string err_msg = "Decode with null data";
      std::cerr << err_msg << std::endl;
      throw;
    }
    ReplicaRequestReplyMsg requestReplyMsg;
    std::string str(data_);
    std::istringstream is(str);
    boost::archive::text_iarchive ia(is);
    ia >> requestReplyMsg;
    requestReplyContext_.type = (ReplicaOpType)requestReplyMsg.type;
    requestReplyContext_.success = requestReplyMsg.success;
    requestReplyContext_.rid = requestReplyMsg.rid;
    requestReplyContext_.key = requestReplyMsg.key;
    requestReplyContext_.size = requestReplyMsg.size;
    requestReplyContext_.nodes = requestReplyMsg.nodes;
    requestReplyContext_.src_address = requestReplyMsg.src_address;
  };

 private:
  std::mutex data_lock_;
    friend ProxyServer;
    friend Protocol;
    friend ReplicaService;
  ReplicaRequestReplyContext requestReplyContext_;
};

struct ReplicaRequestContext {
  ReplicaOpType type;
  uint64_t rid;
  uint64_t key;
  uint64_t size;
  PhysicalNode node;
  uint64_t src_address;
  uint64_t des_address;
  Connection* con;
  Chunk *ck;
};

class ReplicaRequest {
 public:
  ReplicaRequest() = delete;
  explicit ReplicaRequest(ReplicaRequestContext requestContext)
      : data_(nullptr), size_(0), requestContext_(requestContext){};
  ReplicaRequest(char* data, uint64_t size, Connection* con) : size_(size) {
    data_ = static_cast<char*>(std::malloc(size));
    memcpy(data_, data, size_);
    requestContext_.con = con;
  };
  ~ReplicaRequest() {
    const std::lock_guard<std::mutex> lock(data_lock_);
    if (data_ != nullptr) {
      std::free(data_);
      data_ = nullptr;
    }
  };
  ReplicaRequestContext& get_rc() { return requestContext_; };
  void encode() {
    const std::lock_guard<std::mutex> lock(data_lock_);
    ReplicaRequestMsg requestMsg;
    requestMsg.type = requestContext_.type;
    requestMsg.rid = requestContext_.rid;
    requestMsg.key = requestContext_.key;
    requestMsg.size = requestContext_.size;
    requestMsg.node = requestContext_.node;
    requestMsg.src_address = requestContext_.src_address;
    requestMsg.des_address = requestContext_.des_address;
    std::ostringstream os;
    boost::archive::text_oarchive ao(os);
    ao << requestMsg;
    size_ = os.str().length() + 1;
    data_ = static_cast<char*>(std::malloc(size_));
    memcpy(data_, os.str().c_str(), size_);
  };
  void decode() {
    const std::lock_guard<std::mutex> lock(data_lock_);
    if (data_ == nullptr) {
      std::string err_msg = "Decode with null data";
      std::cerr << err_msg << std::endl;
      throw;
    }
    ReplicaRequestMsg requestMsg;
    std::string str(data_);
    std::istringstream is(str);
    boost::archive::text_iarchive ia(is);
    ia >> requestMsg;
    requestContext_.type = (ReplicaOpType)requestMsg.type;
    requestContext_.rid = requestMsg.rid;
    requestContext_.key = requestMsg.key;
    requestContext_.size = requestMsg.size;
    requestContext_.node = requestMsg.node;
    requestContext_.src_address = requestMsg.src_address;
    requestContext_.des_address = requestMsg.des_address;
  };
  char* getData() { return data_; }
  uint64_t getSize() { return size_; }

 private:
  std::mutex data_lock_;
  //   friend ProxyClientRecvCallback;
  //   friend ProxyRequestHandler;
  //   friend DataServiceRequestHandler;
    friend Protocol;
  friend ReplicaService;
  friend DataServiceRequestHandler;
  friend DataServerService;
  char* data_;
  uint64_t size_;
  ReplicaRequestContext requestContext_;
};

#endif  // PMPOOL_REPLICAEVENT_H_
