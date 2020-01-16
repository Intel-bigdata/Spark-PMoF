/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/AllocatorProxy.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Tuesday, December 10th 2019, 12:53:48 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_ALLOCATORPROXY_H_
#define PMPOOL_ALLOCATORPROXY_H_

#include <atomic>
#include <string>
#include <vector>

#include "Allocator.h"
#include "Config.h"
#include "DataServer.h"
#include "PmemAllocator.h"

using std::atomic;
using std::string;
using std::vector;

class AllocatorProxy {
 public:
  AllocatorProxy(Config *config, NetworkServer *networkServer)
      : config_(config) {
    vector<string> paths = config_->get_pool_paths();
    vector<uint64_t> sizes = config_->get_pool_sizes();
    assert(paths.size() == sizes.size());
    for (int i = 0; i < paths.size(); i++) {
      DiskInfo *diskInfo = new DiskInfo(paths[i], sizes[i]);
      diskInfos_.push_back(diskInfo);
      allocators_.push_back(new PmemObjAllocator(diskInfo, networkServer, i));
    }
  }

  ~AllocatorProxy() {
    for (int i = 0; i < config_->get_pool_paths().size(); i++) {
      delete allocators_[i];
      delete diskInfos_[i];
    }
    allocators_.clear();
    diskInfos_.clear();
  }

  int init() {
    for (int i = 0; i < diskInfos_.size(); i++) {
      allocators_[i]->init();
    }
    return 0;
  }

  uint64_t allocate_and_write(uint64_t size, const char* content = nullptr,
                              int index = -1) {
    uint64_t addr = 0;
    if (index < 0) {
      int random_index = buffer_id_++ % diskInfos_.size();
      addr = allocators_[random_index]->allocate_and_write(size, content);
    } else {
      addr = allocators_[index % diskInfos_.size()]->allocate_and_write(
          size, content);
    }
  }

  int write(uint64_t address, const char *content, uint64_t size) {
    uint32_t wid = GET_WID(address);
    return allocators_[wid]->write(address, content, size);
  }

  int release(uint64_t address) {
    uint32_t wid = GET_WID(address);
    return allocators_[wid]->release(address);
  }

  int release_all() {
    for (int i = 0; i < diskInfos_.size(); i++) {
      allocators_[i]->release_all();
    }
    return 0;
  }

  int dump_all() {
    for (int i = 0; i < diskInfos_.size(); i++) {
      allocators_[i]->dump_all();
    }
    return 0;
  }

  uint64_t get_virtual_address(uint64_t address) {
    uint32_t wid = GET_WID(address);
    return allocators_[wid]->get_virtual_address(address);
  }

  Chunk* get_rma_chunk(uint64_t address) {
    uint32_t wid = GET_WID(address);
    return allocators_[wid]->get_rma_chunk();
  }

 private:
  Config *config_;
  vector<Allocator *> allocators_;
  vector<DiskInfo *> diskInfos_;
  atomic<uint64_t> buffer_id_{0};
};

#endif  // PMPOOL_ALLOCATORPROXY_H_
