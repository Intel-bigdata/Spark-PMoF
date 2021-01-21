#ifndef PMPOOL_PROXY_XXHASH_H_
#define PMPOOL_PROXY_XXHASH_H_

#include "include/xxhash/xxhash.hpp"
// #include "pmpool/proxy/IHash.h"


class XXHash {
  public:
    unsigned long hash(string key){
      uint64_t key_i = xxh::xxhash<64>(key);
      return key_i;
    }
};

#endif
