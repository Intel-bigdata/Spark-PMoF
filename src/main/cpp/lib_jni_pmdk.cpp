#include "lib_jni_pmdk.h"
#include "PmemBuffer.h"
#include "PersistentMemoryPool.h"

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeOpenDevice
  (JNIEnv *env, jclass obj, jstring path, jlong size) {
    const char *CStr = env->GetStringUTFChars(path, 0);
    PMPool<string>* pmpool = new PMPool<string>(CStr, size);
    env->ReleaseStringUTFChars(path, CStr);
    return (long)pmpool;
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeSetBlock
  (JNIEnv *env, jclass obj, jlong pmpool, jstring key, jlong pmBuffer, jboolean set_clean) {
    int size = ((PmemBuffer*)pmBuffer)->getRemaining();
    char* buf = ((PmemBuffer*)pmBuffer)->getDataForFlush(size);
    if (buf == nullptr) {
      return -1;
    }
    const char *CStr = env->GetStringUTFChars(key, 0);
    string key_str(CStr);
    long addr = static_cast<PMPool<string>*>((void*)pmpool)->setBlock(key_str, size, buf, set_clean);
    return addr;
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeGetBlock
  (JNIEnv *env, jclass obj, jlong pmpool, jstring key) {
    MemoryBlock mb;
    const char *CStr = env->GetStringUTFChars(key, 0);
    string key_str(CStr);
    long size = static_cast<PMPool<string>*>((void*)pmpool)->getBlock(&mb, key_str);
    jbyteArray data = env->NewByteArray(size);
    env->SetByteArrayRegion(data, 0, size, (jbyte*)(mb.buf));
    return data;
}

JNIEXPORT jlongArray JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeGetBlockIndex
  (JNIEnv *env, jclass obj, jlong pmpool, jstring key) {
    BlockInfo blockInfo;
    const char *CStr = env->GetStringUTFChars(key, 0);
    string key_str(CStr);
    int length = static_cast<PMPool<string>*>((void*)pmpool)->getBlockIndex(&blockInfo, key_str);
    if (length == 0) {
      return env->NewLongArray(0);
    }
    jlongArray data = env->NewLongArray(length);
    env->SetLongArrayRegion(data, 0, length, (jlong*)(blockInfo.data));
    return data;
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeGetBlockSize
  (JNIEnv *env, jclass obj, jlong pmpool, jstring key) {
    const char *CStr = env->GetStringUTFChars(key, 0);
    string key_str(CStr);
    return static_cast<PMPool<string>*>((void*)pmpool)->getBlockSize(key_str);
  }

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeDeleteBlock
  (JNIEnv *env, jclass obj, jlong pmpool, jstring key) {
    const char *CStr = env->GetStringUTFChars(key, 0);
    string key_str(CStr);
    return static_cast<PMPool<string>*>((void*)pmpool)->deleteBlock(key_str);
  }

JNIEXPORT jint JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeCloseDevice
  (JNIEnv *env, jclass obj, jlong pmpool) {
    delete static_cast<PMPool<string>*>((void*)pmpool);
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeGetRoot
  (JNIEnv *env, jclass obj, jlong pmpool) {
  return static_cast<PMPool<string>*>((void*)pmpool)->getRootAddr();
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeNewPmemBuffer
  (JNIEnv *env, jobject obj) {
  return (long)(new PmemBuffer());
}

JNIEXPORT jint JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeLoadPmemBuffer
  (JNIEnv *env, jobject obj, jlong pmBuffer, jlong addr, jint len) {
  ((PmemBuffer*)pmBuffer)->load((char*)addr, len);
}

JNIEXPORT jint JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeReadBytesFromPmemBuffer
  (JNIEnv *env, jobject obj, jlong pmBuffer, jbyteArray data, jint off, jint len) {
  jboolean isCopy = JNI_FALSE;
  jbyte* ret_data = env->GetByteArrayElements(data, &isCopy);
  int read_len = ((PmemBuffer*)pmBuffer)->read((char*)ret_data + off, len);
  if (isCopy == JNI_TRUE) {
    env->ReleaseByteArrayElements(data, ret_data, 0);
  }
  return read_len;
}

JNIEXPORT jint JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeWriteBytesToPmemBuffer
  (JNIEnv *env, jobject obj, jlong pmBuffer, jbyteArray data, jint off, jint len) {
  jboolean isCopy = JNI_FALSE;
  jbyte* ret_data = env->GetByteArrayElements(data, &isCopy);
  int read_len = ((PmemBuffer*)pmBuffer)->write((char*)ret_data + off, len);
  if (isCopy == JNI_TRUE) {
    env->ReleaseByteArrayElements(data, ret_data, 0);
  }
  return read_len;
}

JNIEXPORT jint JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeGetPmemBufferRemaining
  (JNIEnv *env, jobject obj, jlong pmBuffer) {
  ((PmemBuffer*)pmBuffer)->getRemaining();
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeGetPmemBufferDataAddr
  (JNIEnv *env, jobject obj, jlong pmBuffer) {
  return (long)(((PmemBuffer*)pmBuffer)->getDataAddr());
}

JNIEXPORT jint JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeCleanPmemBuffer
  (JNIEnv *env, jobject obj, jlong pmBuffer) {
  ((PmemBuffer*)pmBuffer)->clean();
}

JNIEXPORT jint JNICALL Java_org_apache_spark_storage_pmof_PmemBuffer_nativeDeletePmemBuffer
  (JNIEnv *env, jobject obj, jlong pmBuffer) {
  delete (PmemBuffer*)pmBuffer;
  return 0;
}
