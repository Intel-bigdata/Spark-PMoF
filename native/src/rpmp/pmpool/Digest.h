/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/Digest.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Thursday, November 7th 2019, 3:48:52 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_DIGEST_H_
#define PMPOOL_DIGEST_H_

#include <HPNL/ChunkMgr.h>

#include <cstdint>

struct KeyEntry {
  KeyEntry() = default;
  KeyEntry(uint16_t shuffle_id_, uint16_t map_id_, uint16_t reduce_id_)
      : shuffle_id(shuffle_id_), map_id(map_id_), reduce_id(reduce_id_) {}
  uint16_t shuffle_id;
  uint16_t map_id;
  uint16_t reduce_id;
};

struct DataEntry {
  uint64_t address;
  uint64_t size;
  uint64_t rkey;
  int buffer_id;
  Chunk *ck;
  DataEntry *next;
};

struct DataEntries {
  uint16_t size;
  DataEntry *first;
  DataEntry *last;
};

class Digest {
 public:
  Digest() = default;
  static void computeKeyHash(KeyEntry *keyEntry, uint64_t *hash);
};

#endif  // PMPOOL_DIGEST_H_
