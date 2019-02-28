/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class lib_jni_pmdk */

#ifndef _Included_lib_jni_pmdk
#define _Included_lib_jni_pmdk
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     lib_jni_pmdk
 * Method:    nativeOpenDevice
 * Signature: (Ljava/lang/String;IIJ)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeOpenDevice
  (JNIEnv *, jclass, jstring, jint maxStage, jint maxMap, jlong size);

/*
 * Class:     lib_jni_pmdk
 * Method:    nativeSetMapPartition
 * Signature: (JIIIIJ[BZ)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeSetMapPartition
  (JNIEnv *, jclass, jlong, jint, jint, jint, jint, jlong, jbyteArray, jboolean);

/*
 * Class:     lib_jni_pmdk
 * Method:    nativeSetReducePartition
 * Signature: (JIIIIJ[BZ)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeSetReducePartition
  (JNIEnv *, jclass, jlong, jint, jint, jint, jlong, jbyteArray, jboolean);

/*
 * Class:     lib_jni_pmdk
 * Method:    nativeGetPartition
 * Signature: (JIII)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeGetMapPartition
  (JNIEnv *, jclass, jlong, jint, jint, jint);

/*
 * Class:     lib_jni_pmdk
 * Method:    nativeGetPartition
 * Signature: (JIII)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeGetReducePartition
  (JNIEnv *, jclass, jlong, jint, jint, jint);

/*
 * Class:     lib_jni_pmdk
 * Method:    nativeCloseDevice
 * Signature: (J)J
 */
JNIEXPORT jint JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeCloseDevice
  (JNIEnv *, jclass, jlong);


JNIEXPORT jlong JNICALL Java_org_apache_spark_storage_pmof_PersistentMemoryPool_nativeGetRoot
  (JNIEnv *, jclass, jlong);

#ifdef __cplusplus
}
#endif
#endif
