#include <sstream>

#include "pmpool/DataService/DataServiceEvent.h"

DataServiceRequest::DataServiceRequest(DataServiceRequestContext dataServiceRequestContext)
    : data_(nullptr), size_(0), dataServiceRequestContext_(dataServiceRequestContext) {}

DataServiceRequest::DataServiceRequest(char *data, uint64_t size, Connection *con) : size_(size) {
  data_ = static_cast<char *>(std::malloc(size));
  memcpy(data_, data, size_);
  dataServiceRequestContext_.con = con;
}

DataServiceRequest::~DataServiceRequest() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  if (data_ != nullptr) {
    std::free(data_);
    data_ = nullptr;
  }
}

DataServiceRequestContext &DataServiceRequest::get_rc() { return dataServiceRequestContext_; }

void DataServiceRequest::encode() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  DataServiceOpType rt = dataServiceRequestContext_.type;
//   assert(rt == REGISTER);
  size_ = sizeof(DataServiceRequestMsg);
  data_ = static_cast<char *>(std::malloc(sizeof(DataServiceRequestMsg)));
  DataServiceRequestMsg *requestMsg = (DataServiceRequestMsg *)data_;
  requestMsg->type = dataServiceRequestContext_.type;
  requestMsg->rid = dataServiceRequestContext_.rid;
  requestMsg->key = dataServiceRequestContext_.key;
}

void DataServiceRequest::decode() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  assert(size_ == sizeof(DataServiceRequestMsg));
  DataServiceRequestMsg *requestMsg = (DataServiceRequestMsg *)data_;
  dataServiceRequestContext_.type = (DataServiceOpType)requestMsg->type;
  dataServiceRequestContext_.rid = requestMsg->rid;
  dataServiceRequestContext_.key = requestMsg->key;
}

DataServiceRequestReply::DataServiceRequestReply(DataServiceRequestReplyContext &dataServiceReplyContext)
    : data_(nullptr), size_(0), dataServiceReplyContext_(dataServiceReplyContext) {}

DataServiceRequestReply::DataServiceRequestReply(char *data, uint64_t size, Connection *con)
    : size_(size) {
  data_ = static_cast<char *>(std::malloc(size_));
  memcpy(data_, data, size_);
  dataServiceReplyContext_ = DataServiceRequestReplyContext();
  dataServiceReplyContext_.con = con;
}

DataServiceRequestReply::~DataServiceRequestReply() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  if (data_ != nullptr) {
    std::free(data_);
    data_ = nullptr;
  }
}

DataServiceRequestReplyContext &DataServiceRequestReply::get_rrc() { return dataServiceReplyContext_; }

void DataServiceRequestReply::set_rrc(DataServiceRequestReplyContext &rrc) {
  memcpy(&dataServiceReplyContext_, &rrc, sizeof(DataServiceRequestReplyContext));
}

void DataServiceRequestReply::encode() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  DataServiceRequestReplyMsg requestReplyMsg;
  requestReplyMsg.type = (DataServiceOpType)dataServiceReplyContext_.type;
  requestReplyMsg.success = dataServiceReplyContext_.success;
  requestReplyMsg.rid = dataServiceReplyContext_.rid;
  requestReplyMsg.key = dataServiceReplyContext_.key;
  std::ostringstream os;
  boost::archive::text_oarchive ao(os);
  ao << requestReplyMsg;
  size_ = os.str().length()+1;
  data_ = static_cast<char *>(std::malloc(size_));
  memcpy(data_, os.str().c_str(), size_);
}

void DataServiceRequestReply::decode() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  if (data_ == nullptr) {
    std::string err_msg = "Decode with null data";
    std::cerr << err_msg << std::endl;
    throw;
  }
  DataServiceRequestReplyMsg requestReplyMsg;
  std::string str(data_);
  std::istringstream is(str);
  boost::archive::text_iarchive ia(is);
  ia >> requestReplyMsg;
  dataServiceReplyContext_.type = (DataServiceOpType)requestReplyMsg.type;
  dataServiceReplyContext_.success = requestReplyMsg.success;
  dataServiceReplyContext_.rid = requestReplyMsg.rid;
  dataServiceReplyContext_.key = requestReplyMsg.key;
}
