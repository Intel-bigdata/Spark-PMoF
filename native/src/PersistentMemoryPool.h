#ifndef PMPOOL_H
#define PMPOOL_H

#include <thread>
#include <string>
#include "WorkQueue.h"
#include "Request.h"

/* This class is used to make PM as a huge pool
 * All shuffle files will be stored in the same pool
 *
 * Use a hashmap to locate string key to a pmem block 
 * */

using namespace std;

template <typename T>
class PMPool {
public:
    std::hash<T> hash_fn;

    PMEMobjpool *pmpool;

    TOID(struct HashMapRoot) hashMapRoot;
    const char* dev;
    mutex mtx;

    PMPool(const char* dev, long size);
    ~PMPool();
    long getRootAddr();
    void setBlock(T key, long size, char* data, bool clean);
    void getBlock(MemoryBlock* mb, T key);
    void getBlockIndex(BlockInfo *block_info, T key);
    long getBlockSize(T key);
    void deleteBlock(T key);
private:
    void setBlock(size_t key, long size, char* data, bool clean);
    void getBlock(MemoryBlock* mb, size_t key);
    void getBlockIndex(BlockInfo *block_info, size_t key);
    long getBlockSize(size_t key);
    void deleteBlock(size_t key);
};

#endif
