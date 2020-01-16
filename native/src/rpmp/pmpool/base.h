/*
 * Filename: /mnt/spark-pmof/tool/rpmp/pmpool/fb/Encoder.h
 * Path: /mnt/spark-pmof/tool/rpmp/pmpool/fb
 * Created Date: Friday, December 27th 2019, 3:05:51 pm
 * Author: root
 *
 * Copyright (c) 2019 Intel
 */

#ifndef PMPOOL_BASE_H_
#define PMPOOL_BASE_H_

#include <assert.h>
#include <stdint.h>
#include <string.h>

struct RequestMsg {
  uint32_t type;
  uint64_t rid;
  uint64_t address;
  uint64_t src_address;
  uint64_t src_rkey;
  uint64_t size;
};

struct RequestReplyMsg {
  uint32_t type;
  uint32_t success;
  uint64_t rid;
  uint64_t address;
  uint64_t size;
};

#endif  // PMPOOL_BASE_H_
