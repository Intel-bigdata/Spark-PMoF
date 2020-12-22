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
  OpType rt = requestContext_.type;
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
  requestContext_.type = (OpType)requestMsg->type;
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
  std::cout << "encode reply" << std::endl;
  const std::lock_guard<std::mutex> lock(data_lock_);
  ProxyRequestReplyMsg requestReplyMsg;
  requestReplyMsg.type = (OpType)requestReplyContext_.type;
  requestReplyMsg.success = requestReplyContext_.success;
  requestReplyMsg.rid = requestReplyContext_.rid;
  requestReplyMsg.key = requestReplyContext_.key;
  requestReplyMsg.hosts = requestReplyContext_.hosts;
  requestReplyMsg.dataServerPort = requestReplyContext_.dataServerPort;
  std::ostringstream os;
  boost::archive::text_oarchive ao(os);
  ao << requestReplyMsg;
  size_ = os.str().length()+1;
  data_ = static_cast<char *>(std::malloc(size_));
  memcpy(data_, os.str().c_str(), size_);

  // auto msg_size = sizeof(requestReplyMsg);
  // size_ = msg_size;

  // size_t host_length = requestReplyContext_.host.length() + 1;
  // size_ += host_length;
  // data_ = static_cast<char *>(std::malloc(size_));
  // memcpy(data_, &requestReplyMsg, msg_size);
  // memcpy(data_ + msg_size, requestReplyContext_.host.c_str(), host_length);
}

void ProxyRequestReply::decode() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  // memcpy(&requestReplyMsg_, data_, size_);
  if (data_ == nullptr) {
    std::string err_msg = "Decode with null data";
    std::cerr << err_msg << std::endl;
    throw;
  }
  std::cout << "decode hosts1" << std::endl;
  ProxyRequestReplyMsg requestReplyMsg;
  std::string str(data_);
  std::istringstream is(str);
  boost::archive::text_iarchive ia(is);
  ia >> requestReplyMsg;
  // ProxyRequestReplyMsg *requestReplyMsg = (ProxyRequestReplyMsg *)data_;
  requestReplyContext_.type = (OpType)requestReplyMsg.type;
  requestReplyContext_.success = requestReplyMsg.success;
  requestReplyContext_.rid = requestReplyMsg.rid;
  requestReplyContext_.key = requestReplyMsg.key;
  requestReplyContext_.hosts = requestReplyMsg.hosts;
  requestReplyContext_.dataServerPort = requestReplyMsg.dataServerPort;
  std::cout << "decode hosts" << std::endl;
  for (auto host : requestReplyContext_.hosts) {
    std::cout << "get host: " << host << std::endl;
  }
  // uint64_t host_size = size_ - sizeof(ProxyRequestReplyMsg);
  // char* tmp = static_cast<char*>(malloc(host_size));
  // memcpy(tmp, data_ + sizeof(ProxyRequestReplyMsg), host_size);
  // requestReplyContext_.host = std::string(tmp);
  // std::free(tmp);
}
