#include <sstream>

#include "pmpool/ProxyEvent.h"

ProxyRequest::ProxyRequest(ProxyRequestContext requestContext)
    : data_(nullptr), size_(0), requestContext_(requestContext) {}

ProxyRequest::ProxyRequest(char *data, uint64_t size, Connection *con) : size_(size) {
  data_ = static_cast<char *>(std::malloc(size));
  memcpy(data_, data, size_);
  requestContext_.con = con;
}

ProxyRequest::~ProxyRequest() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  if (data_ != nullptr) {
    std::free(data_);
    data_ = nullptr;
  }
}

ProxyRequestContext &ProxyRequest::get_rc() { return requestContext_; }

void ProxyRequest::encode() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  ProxyOpType rt = requestContext_.type;
  // assert(rt == GET_HOSTS);
  size_ = sizeof(ProxyRequestMsg);
  data_ = static_cast<char *>(std::malloc(sizeof(ProxyRequestMsg)));
  ProxyRequestMsg *requestMsg = (ProxyRequestMsg *)data_;
  requestMsg->type = requestContext_.type;
  requestMsg->rid = requestContext_.rid;
  requestMsg->key = requestContext_.key;
}

void ProxyRequest::decode() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  assert(size_ == sizeof(ProxyRequestMsg));
  ProxyRequestMsg *requestMsg = (ProxyRequestMsg *)data_;
  requestContext_.type = (ProxyOpType)requestMsg->type;
  requestContext_.rid = requestMsg->rid;
  requestContext_.key = requestMsg->key;
}

ProxyRequestReply::ProxyRequestReply(ProxyRequestReplyContext &requestReplyContext)
    : data_(nullptr), size_(0), requestReplyContext_(requestReplyContext) {}

ProxyRequestReply::ProxyRequestReply(char *data, uint64_t size, Connection *con)
    : size_(size) {
  data_ = static_cast<char *>(std::malloc(size_));
  memcpy(data_, data, size_);
  requestReplyContext_ = ProxyRequestReplyContext();
  requestReplyContext_.con = con;
}

ProxyRequestReply::~ProxyRequestReply() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  if (data_ != nullptr) {
    std::free(data_);
    data_ = nullptr;
  }
}

ProxyRequestReplyContext &ProxyRequestReply::get_rrc() { return requestReplyContext_; }

void ProxyRequestReply::set_rrc(ProxyRequestReplyContext &rrc) {
  memcpy(&requestReplyContext_, &rrc, sizeof(ProxyRequestReplyContext));
}

void ProxyRequestReply::encode() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  ProxyRequestReplyMsg requestReplyMsg;
  requestReplyMsg.type = (ProxyOpType)requestReplyContext_.type;
  requestReplyMsg.success = requestReplyContext_.success;
  requestReplyMsg.rid = requestReplyContext_.rid;
  requestReplyMsg.key = requestReplyContext_.key;
  requestReplyMsg.nodes = requestReplyContext_.nodes;
  std::ostringstream os;
  boost::archive::text_oarchive ao(os);
  ao << requestReplyMsg;
  size_ = os.str().length()+1;
  data_ = static_cast<char *>(std::malloc(size_));
  memcpy(data_, os.str().c_str(), size_);
}

void ProxyRequestReply::decode() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  // memcpy(&requestReplyMsg_, data_, size_);
  if (data_ == nullptr) {
    std::string err_msg = "Decode with null data";
    std::cerr << err_msg << std::endl;
    throw;
  }
  ProxyRequestReplyMsg requestReplyMsg;
  std::string str(data_);
  std::istringstream is(str);
  boost::archive::text_iarchive ia(is);
  ia >> requestReplyMsg;
  // ProxyRequestReplyMsg *requestReplyMsg = (ProxyRequestReplyMsg *)data_;
  requestReplyContext_.type = (ProxyOpType)requestReplyMsg.type;
  requestReplyContext_.success = requestReplyMsg.success;
  requestReplyContext_.rid = requestReplyMsg.rid;
  requestReplyContext_.key = requestReplyMsg.key;
  requestReplyContext_.nodes = requestReplyMsg.nodes;
}
