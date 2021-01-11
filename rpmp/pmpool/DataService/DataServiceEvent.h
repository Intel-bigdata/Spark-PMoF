#include <HPNL/ChunkMgr.h>
#include <HPNL/Connection.h>

#include <vector>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/vector.hpp>

class DataServiceRequestHandler;

enum DataServiceOpType : uint32_t {
  REGISTER = 1
};

struct DataServiceRequestMsg {
  template<class Archive>
  void serialize(Archive& ar, const unsigned int version) {
    ar &type;
    ar &rid;
    ar &key;
    ar &node;
  }
  uint32_t type;
  uint64_t rid;
  uint64_t key;
  std::string node;
};

struct DataServiceRequestReplyMsg {
  template<class Archive>
  void serialize(Archive& ar, const unsigned int version) {
    ar &type;
    ar &success;
    ar &rid;
    ar &key;
  }
  uint32_t type;
  uint32_t success;
  uint64_t rid;
  uint64_t key;
};

struct DataServiceRequestReplyContext {
  DataServiceOpType type;
  uint32_t success;
  uint64_t rid;
  uint64_t key;
  Connection* con;
};

class DataServiceRequestReply {
 public:
  DataServiceRequestReply() = delete;
  explicit DataServiceRequestReply(DataServiceRequestReplyContext& dataServiceReplyContext);
  DataServiceRequestReply(char* data, uint64_t size, Connection* con);
  ~DataServiceRequestReply();
  DataServiceRequestReplyContext& get_rrc();
  void set_rrc(DataServiceRequestReplyContext& rrc);
  void decode();
  void encode();

 private:
  std::mutex data_lock_;
  char* data_ = nullptr;
  uint64_t size_ = 0;
  DataServiceRequestReplyContext dataServiceReplyContext_;
};

struct DataServiceRequestContext {
  DataServiceOpType type;
  uint64_t rid;
  uint64_t key;
  Connection* con;
  std::string node;
};

class DataServiceRequest {
 public:
  DataServiceRequest() = delete;
  explicit DataServiceRequest(DataServiceRequestContext dataServiceContext);
  DataServiceRequest(char* data, uint64_t size, Connection* con);
  ~DataServiceRequest();
  DataServiceRequestContext& get_rc();
  void encode();
  void decode();
  char* getData() { return data_; }
  uint64_t getSize() { return size_; }

 private:
  friend DataServiceRequestHandler;
  std::mutex data_lock_;
  char* data_;
  uint64_t size_;
  DataServiceRequestContext dataServiceRequestContext_;
};