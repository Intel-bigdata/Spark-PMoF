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
#include <cstdlib>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "Allocator.h"
#include "Base.h"
#include "Config.h"
#include "DataServer.h"
#include "Log.h"
#include "PmemAllocator.h"

using std::atomic;
using std::make_shared;
using std::string;
using std::unordered_map;
using std::vector;

/**
 * @brief Allocator proxy schedule fairly to guarantee event to be assigned to
 * different allocators.
 *
 */
class AllocatorProxy {
 public:
  AllocatorProxy() = delete;
  AllocatorProxy(std::shared_ptr<Config> config, std::shared_ptr<Log> log,
                 std::shared_ptr<NetworkServer> networkServer)
      : config_(config), log_(log) {
    vector<string> paths = config_->get_pool_paths();
    vector<uint64_t> sizes = config_->get_pool_sizes();
    assert(paths.size() == sizes.size());
    for (int i = 0; i < paths.size(); i++) {
      auto diskInfo = std::make_shared<DiskInfo>(paths[i], sizes[i]);
      diskInfos_.push_back(diskInfo);
      allocators_.push_back(
          std::make_shared<PmemObjAllocator>(log_, diskInfo, networkServer, i));
    }
  }

  ~AllocatorProxy() {
    allocators_.clear();
    diskInfos_.clear();
  }

  int init() {
    for (int i = 0; i < diskInfos_.size(); i++) {
      allocators_[i]->init();
    }
    return 0;
  }

  uint64_t allocate_and_write(uint64_t size, const char *content = nullptr,
                              int index = -1) {
    uint64_t addr = 0;
    if (index < 0) {
      int random_index = buffer_id_++ % diskInfos_.size();
      addr = allocators_[random_index]->allocate_and_write(size, content);
    } else {
      addr = allocators_[index % diskInfos_.size()]->allocate_and_write(
          size, content);
    }
    return addr;
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

  Chunk *get_rma_chunk(uint64_t address) {
    uint32_t wid = GET_WID(address);
    return allocators_[wid]->get_rma_chunk();
  }

  void cache_chunk(uint64_t key, uint64_t address, uint64_t size, int r_key) {
    block_meta bm = {address, size, r_key};
    cache_chunk(key, bm);
  }

  void cache_chunk(uint64_t key, block_meta bm) {
    if (kv_meta_map.count(key)) {
      // kv_meta_map[key].push_back(bm);
      kv_meta_map.erase(key);
    }
    vector<block_meta> bml;
    bml.push_back(bm);
    kv_meta_map[key] = bml;
  }

  vector<block_meta> get_cached_chunk(uint64_t key) {
    if (kv_meta_map.count(key)) {
      return kv_meta_map[key];
    }
    return vector<block_meta>();
  }

  void del_chunk(uint64_t key) {
    if (kv_meta_map.count(key)) {
      kv_meta_map.erase(key);
    }
  }

 private:
  std::shared_ptr<Config> config_;
  std::shared_ptr<Log> log_;
  vector<std::shared_ptr<Allocator>> allocators_;
  vector<std::shared_ptr<DiskInfo>> diskInfos_;
  atomic<uint64_t> buffer_id_{0};
  unordered_map<uint64_t, vector<block_meta>> kv_meta_map;
};

#endif  // PMPOOL_ALLOCATORPROXY_H_
