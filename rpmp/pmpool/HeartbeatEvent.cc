
#include "pmpool/HeartbeatEvent.h"

#include "pmpool/buffer/CircularBuffer.h"

HeartbeatRequest::HeartbeatRequest(HeartbeatRequestContext requestContext)
        : data_(nullptr), size_(0), requestContext_(requestContext) {}

HeartbeatRequest::HeartbeatRequest(char *data, uint64_t size, Connection *con) : size_(size) {
  data_ = static_cast<char *>(std::malloc(size));
  memcpy(data_, data, size_);
  requestContext_.con = con;
}

HeartbeatRequest::~HeartbeatRequest() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  if (data_ != nullptr) {
    std::free(data_);
    data_ = nullptr;
  }
}

HeartbeatRequestContext &HeartbeatRequest::get_rc() { return requestContext_; }

void HeartbeatRequest::encode() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  HeartbeatOpType rt = requestContext_.type;
  size_ = sizeof(HeartbeatRequestMsg);
  data_ = static_cast<char *>(std::malloc(sizeof(HeartbeatRequestMsg)));
  HeartbeatRequestMsg *requestMsg = (HeartbeatRequestMsg *)data_;
  requestMsg->type = requestContext_.type;
  requestMsg->rid = requestContext_.rid;
  requestMsg->host_ip_hash = requestContext_.host_ip_hash;
  requestMsg->port = requestContext_.port;
}

void HeartbeatRequest::decode() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  HeartbeatRequestMsg *requestMsg = (HeartbeatRequestMsg *)data_;
  requestContext_.type = (HeartbeatOpType)requestMsg->type;
  requestContext_.rid = requestMsg->rid;
  requestContext_.host_ip_hash = requestMsg->host_ip_hash;
  requestContext_.port = requestMsg->port;
}

HeartbeatRequestReply::HeartbeatRequestReply(HeartbeatRequestReplyContext &requestReplyContext)
        : data_(nullptr), size_(0), requestReplyContext_(requestReplyContext) {}

HeartbeatRequestReply::HeartbeatRequestReply(char *data, uint64_t size, Connection *con)
        : size_(size) {
  data_ = static_cast<char *>(std::malloc(size_));
  memcpy(data_, data, size_);
  requestReplyContext_ = HeartbeatRequestReplyContext();
  requestReplyContext_.con = con;
}

HeartbeatRequestReply::~HeartbeatRequestReply() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  if (data_ != nullptr) {
    std::free(data_);
    data_ = nullptr;
  }
}

HeartbeatRequestReplyContext &HeartbeatRequestReply::get_rrc() { return requestReplyContext_; }

void HeartbeatRequestReply::set_rrc(HeartbeatRequestReplyContext &rrc) {
  memcpy(&requestReplyContext_, &rrc, sizeof(HeartbeatRequestReplyContext));
  uint32_t bml_size = 0;
}

void HeartbeatRequestReply::encode() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  HeartbeatRequestReplyMsg requestReplyMsg;
  requestReplyMsg.type = (HeartbeatOpType)requestReplyContext_.type;
  requestReplyMsg.success = requestReplyContext_.success;
  requestReplyMsg.rid = requestReplyContext_.rid;
  auto msg_size = sizeof(requestReplyMsg);
  size_ = msg_size;

  data_ = static_cast<char *>(std::malloc(size_));
  memcpy(data_, &requestReplyMsg, msg_size);
}

void HeartbeatRequestReply::decode() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  // memcpy(&requestReplyMsg_, data_, size_);
  if (data_ == nullptr) {
    std::string err_msg = "Decode with null data";
    std::cerr << err_msg << std::endl;
    throw;
  }
  HeartbeatRequestReplyMsg *requestReplyMsg = (HeartbeatRequestReplyMsg *)data_;
  requestReplyContext_.type = (HeartbeatOpType)requestReplyMsg->type;
  requestReplyContext_.success = requestReplyMsg->success;
  requestReplyContext_.rid = requestReplyMsg->rid;
}

