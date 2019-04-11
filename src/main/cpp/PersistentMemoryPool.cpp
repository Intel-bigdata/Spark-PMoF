#include "PersistentMemoryPool.h"
#include <functional>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <fstream>

template<typename T>
PMPool<T>::PMPool(const char* dev, long size):
    stop(false),
    dev(dev),
    worker(&PMPool<T>::process, this) {

  const char *pool_layout_name = "pmempool_sso";
  cout << "PMPOOL is " << dev << ", pid is " << getpid() << endl;

  pmpool = pmemobj_open(dev, pool_layout_name);
  if (pmpool == NULL) {
      pmpool = pmemobj_create(dev, pool_layout_name, size, S_IRUSR | S_IWUSR);
  }
  if (pmpool == NULL) {
      cerr << "Failed to create pool, kill process, errmsg: " << pmemobj_errormsg() << endl; 
      exit(-1);
  }

  hashMapRoot = POBJ_ROOT(pmpool, struct HashMapRoot);
}

template<typename T>
PMPool<T>::~PMPool() {
    while(request_queue.size() > 0) {
        fprintf(stderr, "%s request queue size is %d\n", dev, request_queue.size());
        sleep(1);
    }
    fprintf(stderr, "%s request queue size is %d\n", dev, request_queue.size());
    stop = true;
    worker.join();
    pmemobj_close(pmpool);
}

template<typename T>
long PMPool<T>::getRootAddr() {
    return (long)pmpool;
}

template<typename T>
void PMPool<T>::process() {
    Request *cur_req;
    while(!stop) {
        cur_req = (Request*)request_queue.dequeue();
        if (cur_req != nullptr) {
            cur_req->exec();
        }
    }
}

template<typename T>
long PMPool<T>::setBlock(T key, long size, char* data, bool clean) {
    size_t key_i = hash_fn(key);
    return setBlock(key_i, size, data, clean);
}

template<typename T>
long PMPool<T>::setBlock(size_t key, long size, char* data, bool clean) {
    WriteRequest write_request(this, key, size, data, clean);
    request_queue.enqueue((void*)&write_request);
    //write_request.exec();
    return write_request.getResult();
}

template<typename T>
long PMPool<T>::getBlock(MemoryBlock* mb, T key) {
    size_t key_i = hash_fn(key);
    return getBlock(mb, key_i);
}

template<typename T>
long PMPool<T>::getBlock(MemoryBlock* mb, size_t key) {
    ReadRequest read_request(this, mb, key);
    read_request.exec();
    return read_request.getResult();
}
    
template<typename T>
long PMPool<T>::getBlockIndex(BlockInfo *blockInfo, T key) {
    size_t key_i = hash_fn(key);
    return getBlockIndex(blockInfo, key_i);
}

template<typename T>
long PMPool<T>::getBlockIndex(BlockInfo *blockInfo, size_t key) {
    MetaRequest meta_request(this, blockInfo, key);
    meta_request.exec();
    return meta_request.getResult();
}

template<typename T>
long PMPool<T>::getBlockSize(T key) {
    size_t key_i = hash_fn(key);
    return getBlockSize(key_i);
}

template<typename T>
long PMPool<T>::getBlockSize(size_t key) {
    SizeRequest size_request(this, key);
    size_request.exec();
    return size_request.getResult();
}

template<typename T>
long PMPool<T>::deleteBlock(T key) {
    size_t key_i = hash_fn(key);
    return deleteBlock(key_i);
}

template<typename T>
long PMPool<T>::deleteBlock(size_t key) {
    DeleteRequest delete_request(this, key);
    request_queue.enqueue((void*)&delete_request);
    return delete_request.getResult();

}

template class PMPool<string>;
