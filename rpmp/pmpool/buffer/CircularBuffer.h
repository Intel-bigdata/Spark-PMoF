/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/buffer/CircularBuffer.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool/buffer
 * Created Date: Monday, December 23rd 2019, 2:31:42 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_BUFFER_CIRCULARBUFFER_H_
#define PMPOOL_BUFFER_CIRCULARBUFFER_H_

#include <assert.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>  // NOLINT
#include <iostream>
#include <mutex>  // NOLINT
#include <vector>

#include "pmpool/Common.h"
#include "pmpool/NetworkServer.h"
#include "pmpool/RmaBufferRegister.h"

#define p2align(x, a) (((x) + (a)-1) & ~((a)-1))

class CircularBuffer {
 public:
  CircularBuffer() = delete;
  CircularBuffer(const CircularBuffer &) = delete;
  CircularBuffer(uint64_t buffer_size, uint32_t buffer_num,
                 bool is_server = false)
      : buffer_size_(buffer_size), buffer_num_(buffer_num) {
    // init();
  }
  CircularBuffer(uint64_t buffer_size, uint32_t buffer_num, bool is_server,
                 std::shared_ptr<RmaBufferRegister> rbr)
      : rbr_(rbr), buffer_size_(buffer_size), buffer_num_(buffer_num) {
    // init();
  }
  void try_init() {
    std::lock_guard<std::mutex> lk(lock_);
    if (!initialized) init();
  }
  void init() {
    uint64_t total = buffer_num_ * buffer_size_;
    buffer_ = static_cast<char *>(mmap(0, buffer_num_ * buffer_size_,
                                       PROT_READ | PROT_WRITE,
                                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));

    // for the consideration of high performance,
    // we'd better do memory paging before starting service.
    // if (is_server) {
    //  for (uint64_t i = 0; i < total; i++) {
    //    buffer_[i] = 0;
    //  }
    // }

    if (rbr_) {
      ck_ = rbr_->register_rma_buffer(buffer_, buffer_num_ * buffer_size_);
#ifdef DEBUG
      printf("[CircularBuffer::Register_RMA_Buffer] range is %ld - %ld\n",
             (uint64_t)buffer_,
             (uint64_t)(buffer_ + buffer_num_ * buffer_size_));
#endif
    }

    bits = new uint8_t[buffer_num_]();
    initialized = true;
  }

  ~CircularBuffer() {
    if (!initialized) return;
    if (ck_ != nullptr) {
      rbr_->unregister_rma_buffer(ck_->buffer_id);
    }
    munmap(buffer_, buffer_num_ * buffer_size_);
    buffer_ = nullptr;
    delete[] bits;
#ifdef DEBUG
    std::cout << "CircularBuffer destructed" << std::endl;
#endif
  }

  char *get(uint64_t bytes) {
    try_init();
    uint64_t offset = 0;
    bool res = get(bytes, &offset);
    if (res == false) {
      return nullptr;
    }
    return buffer_ + offset * buffer_size_;
  }

  void put(const char *data, uint64_t bytes) {
    try_init();
    assert((data - buffer_) % buffer_size_ == 0);
    uint64_t offset = (data - buffer_) / buffer_size_;
    put(offset, bytes);
  }

  void dump() {
    try_init();
    std::cout << "********************************************" << std::endl;
    for (int i = 0; i < buffer_num_; i++) {
      std::cout << bits[i] << " ";
    }
    std::cout << std::endl;
    std::cout << "********************************************" << std::endl;
  }

  bool get(uint64_t bytes, uint64_t *offset) {
    uint32_t alloc_num = p2align(bytes, buffer_size_) / buffer_size_;
    if (alloc_num > buffer_num_) {
      return false;
    }
    std::lock_guard<std::mutex> lk(lock_);
    uint64_t found = -1;
    for (int i = 0; i < buffer_num_; i++) {
      auto index = (i + optimal_start) % buffer_num_;
      auto res = check_availability(index, alloc_num);
      if (res) {
        set_occupied(index, alloc_num);
        found = index;
        optimal_start = (index + alloc_num) % buffer_num_;
        break;
      }
    }
    if (found == -1) {
      std::cerr << "Can't find a " << alloc_num << " * " << buffer_size_
                << " sized buffer. " << std::endl;
      throw "Cannot get buffer";
    } else {
      *offset = found;
#ifdef DEBUG
      printf(
          "[circular buffer get] alloc_num is %lu, offset is %lu, optimal "
          "start is %lu\n",
          alloc_num, *offset, optimal_start);
#endif
      return true;
    }
  }

  void put(uint64_t offset, uint64_t bytes) {
    uint32_t alloc_num = p2align(bytes, buffer_size_) / buffer_size_;
    std::lock_guard<std::mutex> lk(lock_);
    set_available(offset, alloc_num);
  }

  Chunk *get_rma_chunk() {
    try_init();
    return ck_;
  }
  uint64_t get_offset(uint64_t data) {
    try_init();
    return (data - (uint64_t)buffer_);
  }

 private:
  char *buffer_;
  uint64_t buffer_size_;
  uint64_t buffer_num_;
  std::shared_ptr<RmaBufferRegister> rbr_;
  Chunk *ck_ = nullptr;
  uint8_t *bits;
  bool initialized = false;
  std::mutex lock_;
  uint64_t optimal_start = 0;

  bool check_availability(uint64_t index, uint64_t alloc_num) {
    for (int i = 0; i < alloc_num; i++) {
      if ((index + i) >= buffer_num_) {
        return false;  // since we can't choose multiple unadjacent blocks,
                       // return false here.
      }
      auto j = (index + i) % buffer_num_;
      if (bits[j] == 1) return false;
    }
    return true;
  }

  void set_occupied(uint64_t index, uint64_t alloc_num) {
    for (int i = 0; i < alloc_num; i++) {
      auto j = (index + i) % buffer_num_;
      bits[j] = 1;
    }
  }

  void set_available(uint64_t index, uint64_t alloc_num) {
    for (int i = 0; i < alloc_num; i++) {
      auto j = (index + i) % buffer_num_;
      bits[j] = 0;
    }
  }
};

#endif  // PMPOOL_BUFFER_CIRCULARBUFFER_H_
