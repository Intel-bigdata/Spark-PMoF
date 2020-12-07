#ifndef PMPOOL_PROXY_IHASH_H_
#define PMPOOL_PROXY_IHASH_H_

#include <string>

class IHash{
  public:
    virtual uint64_t hash(string key){}
};

#endif
