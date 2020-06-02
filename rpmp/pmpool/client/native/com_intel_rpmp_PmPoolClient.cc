/*
 * Filename:
 * /mnt/spark-pmof/tool/rpmp/pmpool/client/native/PmPoolClientNative.cc Path:
 * /mnt/spark-pmof/tool/rpmp/pmpool/client/native Created Date: Monday, February
 * 24th 2020, 9:23:22 pm Author: root
 *
 * Copyright (c) 2020 Intel
 */
#include <memory>

#include <jni.h>
#include "pmpool/client/PmPoolClient.h"
#include "pmpool/client/native/concurrent_map.h"

static jint JNI_VERSION = JNI_VERSION_1_8;
static jclass io_exception_class;
static jclass illegal_argument_exception_class;
static arrow::jni::ConcurrentMap<std::shared_ptr<PmPoolClient>> handler_holder_;

jclass CreateGlobalClassReference(JNIEnv *env, const char *class_name) {
  jclass local_class = env->FindClass(class_name);
  jclass global_class = (jclass)env->NewGlobalRef(local_class);
  env->DeleteLocalRef(local_class);
  if (global_class == nullptr) {
    std::string error_message =
        "Unable to createGlobalClassReference for" + std::string(class_name);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }
  return global_class;
}

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM *vm, void *reserved) {
  JNIEnv *env;
  if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }

  io_exception_class = CreateGlobalClassReference(env, "Ljava/io/IOException;");
  illegal_argument_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalArgumentException;");

  return JNI_VERSION;
}

std::shared_ptr<PmPoolClient> GetClient(JNIEnv *env, jlong id) {
  auto handler = handler_holder_.Lookup(id);
  if (!handler) {
    std::string error_message = "invalid handler id " + std::to_string(id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }
  return handler;
}

JNIEXPORT jlong JNICALL Java_com_intel_rpmp_PmPoolClient_nativeOpenPmPoolClient(
    JNIEnv *env, jobject obj, jstring address, jstring port) {
  const char *remote_address = env->GetStringUTFChars(address, 0);
  const char *remote_port = env->GetStringUTFChars(port, 0);

  auto client = std::make_shared<PmPoolClient>(remote_address, remote_port);
  client->begin_tx();
  client->init();
  client->end_tx();

  env->ReleaseStringUTFChars(address, remote_address);
  env->ReleaseStringUTFChars(port, remote_port);

  return handler_holder_.Insert(std::move(client));
}

JNIEXPORT jlong JNICALL Java_com_intel_rpmp_PmPoolClient_nativeAlloc(
    JNIEnv *env, jobject obj, jlong size, jlong objectId) {
  auto client = GetClient(env, objectId);
  client->begin_tx();
  uint64_t address = client->alloc(size);
  client->end_tx();
  return address;
}

JNIEXPORT jint JNICALL Java_com_intel_rpmp_PmPoolClient_nativeFree(
    JNIEnv *env, jobject obj, jlong address, jlong objectId) {
  auto client = GetClient(env, objectId);
  client->begin_tx();
  int success = client->free(address);
  client->end_tx();
  return success;
}

JNIEXPORT jint JNICALL Java_com_intel_rpmp_PmPoolClient_nativeWrite(
    JNIEnv *env, jobject obj, jlong address, jstring data, jlong size,
    jlong objectId) {
  const char *raw_data = env->GetStringUTFChars(data, 0);

  auto client = GetClient(env, objectId);
  client->begin_tx();
  int success = client->write(address, raw_data, size);
  client->end_tx();

  env->ReleaseStringUTFChars(data, raw_data);

  return success;
}
JNIEXPORT jlong JNICALL
Java_com_intel_rpmp_PmPoolClient_nativeAllocAndWriteWithString(
    JNIEnv *env, jobject obj, jstring data, jlong size, jlong objectId) {
  const char *raw_data = env->GetStringUTFChars(data, 0);

  auto client = GetClient(env, objectId);
  client->begin_tx();
  uint64_t address = client->write(raw_data, size);
  client->end_tx();

  env->ReleaseStringUTFChars(data, raw_data);
  return address;
}

JNIEXPORT jlong JNICALL
Java_com_intel_rpmp_PmPoolClient_nativeAllocateAndWriteWithByteBuffer(
    JNIEnv *env, jobject obj, jobject data, jlong size, jlong objectId) {
  char *raw_data = static_cast<char *>((*env).GetDirectBufferAddress(data));
  auto client = GetClient(env, objectId);
  client->begin_tx();
  uint64_t address = client->write(raw_data, size);
  client->end_tx();
  return address;
}

JNIEXPORT jlong JNICALL Java_com_intel_rpmp_PmPoolClient_nativePut(
    JNIEnv *env, jobject obj, jstring key, jobject data, jlong size,
    jlong objectId) {
  char *raw_data = static_cast<char *>((*env).GetDirectBufferAddress(data));
  const char *raw_key = env->GetStringUTFChars(key, 0);
  auto client = GetClient(env, objectId);
  client->begin_tx();
  auto address = client->put(raw_key, raw_data, size);
  client->end_tx();
  env->ReleaseStringUTFChars(key, raw_key);
  return address;
}

JNIEXPORT jlong JNICALL Java_com_intel_rpmp_PmPoolClient_nativeGet(
    JNIEnv *env, jobject obj, jstring key, jlong size, jobject data,
    jlong objectId) {
  char *raw_data = static_cast<char *>((*env).GetDirectBufferAddress(data));
  const char *raw_key = env->GetStringUTFChars(key, 0);
  auto client = GetClient(env, objectId);
  client->begin_tx();
  auto address = client->get(raw_key, raw_data, size);
  client->end_tx();
  env->ReleaseStringUTFChars(key, raw_key);
  return address;
}

JNIEXPORT jlongArray JNICALL Java_com_intel_rpmp_PmPoolClient_nativeGetMeta(
    JNIEnv *env, jobject obj, jstring key, jlong objectId) {
  const char *raw_key = env->GetStringUTFChars(key, 0);
  auto client = GetClient(env, objectId);
  client->begin_tx();
  auto bml = client->getMeta(raw_key);
  client->end_tx();
  env->ReleaseStringUTFChars(key, raw_key);
  int longCArraySize = bml.size() * 3;
  if (longCArraySize == 0) {
    return nullptr;
  }
  auto longCArray = new uint64_t[longCArraySize]();
  int i = 0;
  for (auto bm : bml) {
    longCArray[i++] = bm.address;
    longCArray[i++] = bm.size;
    longCArray[i++] = bm.r_key;
  }
  jlongArray longJavaArray = env->NewLongArray(longCArraySize);
  env->SetLongArrayRegion(longJavaArray, 0, longCArraySize,
                          reinterpret_cast<jlong *>(longCArray));
  delete[] longCArray;
  return longJavaArray;
}

JNIEXPORT jint JNICALL Java_com_intel_rpmp_PmPoolClient_nativeRemove(
    JNIEnv *env, jobject obj, jstring key, jlong objectId) {
  const char *raw_key = env->GetStringUTFChars(key, 0);
  auto client = GetClient(env, objectId);
  client->begin_tx();
  int res = client->del(raw_key);
  client->end_tx();
  env->ReleaseStringUTFChars(key, raw_key);
  return res;
}

JNIEXPORT jint JNICALL Java_com_intel_rpmp_PmPoolClient_nativeRead(
    JNIEnv *env, jobject obj, jlong address, jlong size, jobject data,
    jlong objectId) {
  char *raw_data = static_cast<char *>((*env).GetDirectBufferAddress(data));
  auto client = GetClient(env, objectId);
  client->begin_tx();
  int success = client->read(address, raw_data, size);
  client->end_tx();
  return success;
}

JNIEXPORT void JNICALL Java_com_intel_rpmp_PmPoolClient_nativeShutdown(
    JNIEnv *env, jobject obj, jlong objectId) {
  auto client = GetClient(env, objectId);
  client->shutdown();
}

JNIEXPORT void JNICALL Java_com_intel_rpmp_PmPoolClient_nativeWaitToStop(
    JNIEnv *env, jobject obj, jlong objectId) {
  auto client = GetClient(env, objectId);
  client->wait();
}

JNIEXPORT void JNICALL Java_com_intel_rpmp_PmPoolClient_nativeDispose(
    JNIEnv *env, jobject obj, jlong objectId) {
  handler_holder_.Erase(objectId);
}

#ifdef __cplusplus
}
#endif
