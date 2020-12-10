/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/Request.cc
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Thursday, December 12th 2019, 1:36:18 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#include "pmpool/Event.h"

#include "pmpool/buffer/CircularBuffer.h"

Request::Request(RequestContext requestContext)
    : data_(nullptr), size_(0), requestContext_(requestContext) {}

Request::Request(char *data, uint64_t size, Connection *con) : size_(size) {
  data_ = static_cast<char *>(std::malloc(size));
  memcpy(data_, data, size_);
  requestContext_.con = con;
}

Request::~Request() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  if (data_ != nullptr) {
    std::free(data_);
    data_ = nullptr;
  }
}

RequestContext &Request::get_rc() { return requestContext_; }

void Request::encode() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  OpType rt = requestContext_.type;
  // assert(rt == ALLOC || rt == FREE || rt == WRITE || rt == READ);
  size_ = sizeof(RequestMsg);
  data_ = static_cast<char *>(std::malloc(sizeof(RequestMsg)));
  RequestMsg *requestMsg = (RequestMsg *)data_;
  requestMsg->type = requestContext_.type;
  requestMsg->rid = requestContext_.rid;
  requestMsg->address = requestContext_.address;
  requestMsg->src_address = requestContext_.src_address;
  requestMsg->src_rkey = requestContext_.src_rkey;
  requestMsg->size = requestContext_.size;
  requestMsg->key = requestContext_.key;
}

void Request::decode() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  assert(size_ == sizeof(RequestMsg));
  RequestMsg *requestMsg = (RequestMsg *)data_;
  requestContext_.type = (OpType)requestMsg->type;
  requestContext_.rid = requestMsg->rid;
  requestContext_.address = requestMsg->address;
  requestContext_.src_address = requestMsg->src_address;
  requestContext_.src_rkey = requestMsg->src_rkey;
  requestContext_.size = requestMsg->size;
  requestContext_.key = requestMsg->key;
}

RequestReply::RequestReply(RequestReplyContext &requestReplyContext)
    : data_(nullptr), size_(0), requestReplyContext_(requestReplyContext) {}

RequestReply::RequestReply(char *data, uint64_t size, Connection *con)
    : size_(size) {
  data_ = static_cast<char *>(std::malloc(size_));
  memcpy(data_, data, size_);
  requestReplyContext_ = RequestReplyContext();
  requestReplyContext_.con = con;
}

RequestReply::~RequestReply() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  if (data_ != nullptr) {
    std::free(data_);
    data_ = nullptr;
  }
}

RequestReplyContext &RequestReply::get_rrc() { return requestReplyContext_; }

void RequestReply::set_rrc(RequestReplyContext &rrc) {
  memcpy(&requestReplyContext_, &rrc, sizeof(RequestReplyContext));
  uint32_t bml_size = 0;
}

void RequestReply::encode() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  RequestReplyMsg requestReplyMsg;
  requestReplyMsg.type = (OpType)requestReplyContext_.type;
  requestReplyMsg.success = requestReplyContext_.success;
  requestReplyMsg.rid = requestReplyContext_.rid;
  requestReplyMsg.address = requestReplyContext_.address;
  requestReplyMsg.size = requestReplyContext_.size;
  requestReplyMsg.key = requestReplyContext_.key;
  auto msg_size = sizeof(requestReplyMsg);
  size_ = msg_size;

  /// copy data from block metadata list
  uint32_t bml_size = 0;
  if (!requestReplyContext_.bml.empty()) {
    bml_size = sizeof(block_meta) * requestReplyContext_.bml.size();
    size_ += bml_size;
  }
  data_ = static_cast<char *>(std::malloc(size_));
  memcpy(data_, &requestReplyMsg, msg_size);
  if (bml_size != 0) {
    memcpy(data_ + msg_size, &requestReplyContext_.bml[0], bml_size);
  }
}

void RequestReply::decode() {
  const std::lock_guard<std::mutex> lock(data_lock_);
  // memcpy(&requestReplyMsg_, data_, size_);
  if (data_ == nullptr) {
    std::string err_msg = "Decode with null data";
    std::cerr << err_msg << std::endl;
    throw;
  }
  RequestReplyMsg *requestReplyMsg = (RequestReplyMsg *)data_;
  requestReplyContext_.type = (OpType)requestReplyMsg->type;
  requestReplyContext_.success = requestReplyMsg->success;
  requestReplyContext_.rid = requestReplyMsg->rid;
  requestReplyContext_.address = requestReplyMsg->address;
  requestReplyContext_.size = requestReplyMsg->size;
  requestReplyContext_.key = requestReplyMsg->key;
  if (size_ > sizeof(RequestReplyMsg)) {
    auto bml_size = size_ - sizeof(RequestReplyMsg);
    requestReplyContext_.bml.resize(bml_size / sizeof(block_meta));
    memcpy(&requestReplyContext_.bml[0], data_ + sizeof(RequestReplyMsg),
           bml_size);
  }
}
