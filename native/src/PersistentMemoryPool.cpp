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
    dev(dev) {

  const char *pool_layout_name = "pmempool_sso";

  int sds_write_value = 0;
  pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

  pmpool = pmemobj_open(dev, pool_layout_name);
  if (pmpool == NULL) {
      cerr << "Failed to open pool, try to create pool" << endl; 
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
    pmemobj_close(pmpool);
}

template<typename T>
long PMPool<T>::getRootAddr() {
    return (long)pmpool;
}

template<typename T>
void PMPool<T>::setBlock(T key, long size, char* data, bool clean) {
    size_t key_i = hash_fn(key);
    setBlock(key_i, size, data, clean);
}

template<typename T>
void PMPool<T>::setBlock(size_t key, long size, char* data, bool clean) {
    WriteRequest write_request(this, key, size, data, clean);
    lock_guard<mutex> lk(mtx);
    write_request.exec();
}

template<typename T>
void PMPool<T>::getBlock(MemoryBlock* mb, T key) {
    size_t key_i = hash_fn(key);
    getBlock(mb, key_i);
}

template<typename T>
void PMPool<T>::getBlock(MemoryBlock* mb, size_t key) {
    ReadRequest read_request(this, mb, key);
    lock_guard<mutex> lk(mtx);
    read_request.exec();
}
    
template<typename T>
void PMPool<T>::getBlockIndex(BlockInfo *blockInfo, T key) {
    size_t key_i = hash_fn(key);
    getBlockIndex(blockInfo, key_i);
}

template<typename T>
void PMPool<T>::getBlockIndex(BlockInfo *blockInfo, size_t key) {
    MetaRequest meta_request(this, blockInfo, key);
    lock_guard<mutex> lk(mtx);
    meta_request.exec();
}

template<typename T>
long PMPool<T>::getBlockSize(T key) {
    size_t key_i = hash_fn(key);
    return getBlockSize(key_i);
}

template<typename T>
long PMPool<T>::getBlockSize(size_t key) {
    long data_length;
    SizeRequest size_request(this, &data_length, key);
    lock_guard<mutex> lk(mtx);
    size_request.exec();
    return data_length;
}

template<typename T>
void PMPool<T>::deleteBlock(T key) {
    size_t key_i = hash_fn(key);
    deleteBlock(key_i);
}

template<typename T>
void PMPool<T>::deleteBlock(size_t key) {
    DeleteRequest delete_request(this, key);
    lock_guard<mutex> lk(mtx);
    delete_request.exec();

}

template class PMPool<string>;
