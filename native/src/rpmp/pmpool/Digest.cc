/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/Digest.cc
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool
 * Created Date: Thursday, November 7th 2019, 3:48:52 pm
 * Author: root
 * 
 * Copyright (c) 2019 Intel
 */

#include "pmpool/Digest.h"

#include <iostream>
#include <array>

#include "xxhash/xxhash.h"
#include "xxhash/xxhash.hpp"

void Digest::computeKeyHash(KeyEntry *keyEntry, uint64_t *hash) {
  uint16_t shuffle_id = keyEntry->shuffle_id;
  uint16_t map_id = keyEntry->map_id;
  uint16_t reduce_id = keyEntry->reduce_id;

  std::array<uint16_t, 4> key = {shuffle_id, map_id, reduce_id};
  *hash = xxh::xxhash<64>(key);
}
