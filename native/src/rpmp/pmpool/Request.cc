/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/Request.cc
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Thursday, December 12th 2019, 1:36:18 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#include "pmpool/Request.h"

#include "pmpool/buffer/CircularBuffer.h"

Request::Request(RequestContext requestContext)
    : size_(0), requestContext_(requestContext) {}

Request::Request(char *data, uint64_t size, Connection *con) : size_(size) {
  memcpy(data_, data, size_);
  requestContext_.con = con;
}

Request::~Request() {}

RequestContext &Request::get_rc() { return requestContext_; }

void Request::encode() {
  OpType rt = requestContext_.type;
  assert(rt == ALLOC || rt == FREE || rt == WRITE || rt == READ);
  requestMsg_.type = requestContext_.type;
  requestMsg_.rid = requestContext_.rid;
  requestMsg_.address = requestContext_.address;
  requestMsg_.src_address = requestContext_.src_address;
  requestMsg_.src_rkey = requestContext_.src_rkey;
  requestMsg_.size = requestContext_.size;

  size_ = sizeof(requestMsg_);
  memcpy(data_, &requestMsg_, size_);
}

void Request::decode() {
  assert(size_ == sizeof(requestMsg_));
  memcpy(&requestMsg_, data_, size_);
  requestContext_.type = (OpType)requestMsg_.type;
  requestContext_.rid = requestMsg_.rid;
  requestContext_.address = requestMsg_.address;
  requestContext_.src_address = requestMsg_.src_address;
  requestContext_.src_rkey = requestMsg_.src_rkey;
  requestContext_.size = requestMsg_.size;
}

RequestReply::RequestReply(RequestReplyContext requestReplyContext)
    : size_(0), requestReplyContext_(requestReplyContext) {}

RequestReply::RequestReply(char *data, uint64_t size, Connection *con)
    : size_(size) {
  memcpy(data_, data, size_);
  requestReplyContext_.con = con;
}

RequestReply::~RequestReply() {}

RequestReplyContext &RequestReply::get_rrc() { return requestReplyContext_; }

void RequestReply::encode() {
  requestReplyMsg_.type = (OpType)requestReplyContext_.type;
  requestReplyMsg_.success = requestReplyContext_.success;
  requestReplyMsg_.rid = requestReplyContext_.rid;
  requestReplyMsg_.address = requestReplyContext_.address;
  requestReplyMsg_.size = requestReplyContext_.size;
  size_ = sizeof(requestReplyMsg_);
  memcpy(data_, &requestReplyMsg_, size_);
}

void RequestReply::decode() {
  // std::cout << "size_ " << size_ << " request reply msg size "
  //           << sizeof(requestReplyMsg_) << std::endl;
  assert(size_ == sizeof(requestReplyMsg_));
  memcpy(&requestReplyMsg_, data_, size_);
  requestReplyContext_.type = (OpType)requestReplyMsg_.type;
  requestReplyContext_.success = requestReplyMsg_.success;
  requestReplyContext_.rid = requestReplyMsg_.rid;
  requestReplyContext_.address = requestReplyMsg_.address;
  requestReplyContext_.size = requestReplyMsg_.size;
}
