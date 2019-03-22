#include "lib_jni_pmdk.h"
#include "PmemBuffer.h"
#include "PersistentMemoryPool.h"

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeOpenDevice
  (JNIEnv *env, jclass obj, jstring path, jint maxStage, jint maxMap, jlong size) {
    const char *CStr = env->GetStringUTFChars(path, 0);
    PMPool* pmpool = new PMPool(CStr, maxStage, maxMap, size);
    env->ReleaseStringUTFChars(path, CStr);
    return (long)pmpool;
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeSetMapPartition
  (JNIEnv *env, jclass obj, jlong pmpool, jint partitionNum, jint stageId, jint mapId, jint partitionId, jlong size, jbyteArray data, jboolean clean) {
    char* buf = new char[size];
    env->GetByteArrayRegion(data, 0, size, reinterpret_cast<jbyte*>(buf));
    long addr = ((PMPool*)pmpool)->setMapPartition(partitionNum, stageId, mapId, partitionId, size, buf, clean);
    delete buf;
    return addr;
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeSetReducePartition
  (JNIEnv *env, jclass obj, jlong pmpool, jint partitionNum, jint stageId, jint partitionId, jlong size, jbyteArray data, jboolean clean) {
    char* buf = new char[size];
    env->GetByteArrayRegion(data, 0, size, reinterpret_cast<jbyte*>(buf));
    long addr = ((PMPool*)pmpool)->setReducePartition(partitionNum, stageId, partitionId, size, buf, clean);
    delete buf;
    return addr;
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeGetMapPartition
  (JNIEnv *env, jclass obj, jlong pmpool, jint stageId, jint mapId, jint partitionId) {
    MemoryBlock mb;
    long size = ((PMPool*)pmpool)->getMapPartition(&mb, stageId, mapId, partitionId);
    jbyteArray data = env->NewByteArray(size);
    env->SetByteArrayRegion(data, 0, size, (jbyte*)(mb.buf));
    return data;
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeGetReducePartition
  (JNIEnv *env, jclass obj, jlong pmpool, jint stageId, jint mapId, jint partitionId) {
    MemoryBlock mb;
    long size = ((PMPool*)pmpool)->getReducePartition(&mb, stageId, mapId, partitionId);
    jbyteArray data = env->NewByteArray(size);
    env->SetByteArrayRegion(data, 0, size, (jbyte*)(mb.buf));
    return data;
}

JNIEXPORT jlongArray JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeGetMapPartitionBlockInfo
  (JNIEnv *env, jclass obj, jlong pmpool, jint stageId, jint mapId, jint partitionId) {
    BlockInfo blockInfo;
    int length = ((PMPool*)pmpool)->getMapPartitionBlockInfo(&blockInfo, stageId, mapId, partitionId);
    if (length == 0) {
      return env->NewLongArray(0);
    }
    jlongArray data = env->NewLongArray(length);
    env->SetLongArrayRegion(data, 0, length, (jlong*)(blockInfo.data));
    return data;
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeGetMapPartitionSize
  (JNIEnv *env, jclass obj, jlong pmpool, jint stageId, jint mapId, jint partitionId) {
    return ((PMPool*)pmpool)->getMapPartitionSize(stageId, mapId, partitionId);
  }

JNIEXPORT jint JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeCloseDevice
  (JNIEnv *env, jclass obj, jlong pmpool) {
    delete (PMPool*)pmpool;
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeGetRoot
  (JNIEnv *env, jclass obj, jlong pmpool) {
  return ((PMPool*)pmpool)->getRootAddr();
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeNewPmemBuffer
  (JNIEnv *env, jobject obj) {
  return (long)(new PmemBuffer());
}

JNIEXPORT jint JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeLoadPmemBuffer
  (JNIEnv *env, jobject obj, jlong pmBuffer, jlong addr, jint len) {
  ((PmemBuffer*)pmBuffer)->load((char*)addr, len);
}

JNIEXPORT jint JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeReadPmemBuffer
  (JNIEnv *env, jobject obj, jlong pmBuffer, jbyteArray data, jint len) {
  jboolean isCopy = JNI_FALSE;
  jbyte* ret_data = env->GetByteArrayElements(data, &isCopy);
  int read_len = ((PmemBuffer*)pmBuffer)->read((char*)ret_data, len);
  if (isCopy == JNI_TRUE) {
    env->ReleaseByteArrayElements(data, ret_data, 0);
  }
  return read_len;
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeDeletePmemBuffer
  (JNIEnv *env, jobject obj, jlong pmBuffer) {
  delete (PmemBuffer*)pmBuffer;
}
