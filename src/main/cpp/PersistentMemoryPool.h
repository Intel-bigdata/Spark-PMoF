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

    std::thread worker;
    WorkQueue<void*> request_queue;
    bool stop;

    TOID(struct HashMapRoot) hashMapRoot;
    const char* dev;

    PMPool(const char* dev, long size);
    ~PMPool();
    long getRootAddr();

    long setBlock(T key, long size, char* data, bool clean);
    long setBlock(size_t key, long size, char* data, bool clean);

    long getBlock(MemoryBlock* mb, T key);
    long getBlock(MemoryBlock* mb, size_t key);

    long getBlockIndex(BlockInfo *block_info, T key);
    long getBlockIndex(BlockInfo *block_info, size_t key);

    long getBlockSize(T key);
    long getBlockSize(size_t key);

    long deleteBlock(T key);
    long deleteBlock(size_t key);
private:
    void process();
};

#endif
