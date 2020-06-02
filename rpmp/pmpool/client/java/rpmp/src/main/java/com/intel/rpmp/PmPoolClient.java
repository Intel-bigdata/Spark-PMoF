package com.intel.rpmp;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PmPoolClient
 *
 */
public class PmPoolClient {
  private static final Logger LOG = LoggerFactory.getLogger(PmPoolClient.class);

  public PmPoolClient(String remote_address, String remote_port) throws IOException {
    JniUtils.getInstance();
    LOG.info("create PmPoolClient instance, remote address is " + remote_address
        + ", remote port is " + remote_port);
    objectId = nativeOpenPmPoolClient(remote_address, remote_port);
  }

  public long alloc(long size) {
    return nativeAlloc(size, objectId);
  }

  public int free(long address) {
    return nativeFree(address, objectId);
  }

  public int write(long address, String data, long size) {
    return nativeWrite(address, data, size, objectId);
  }

  public long write(String data, long size) {
    return nativeAllocAndWriteWithString(data, size, objectId);
  }

  public long write(ByteBuffer data, long size) {
    return nativeAllocAndWriteWithByteBuffer(data, size, objectId);
  }

  public int read(long address, long size, ByteBuffer byteBuffer) {
    return nativeRead(address, size, byteBuffer, objectId);
  }

  public long put(String key, ByteBuffer data, long size) {
    return nativePut(key, data, size, objectId);
  }

  public long get(String key, long size, ByteBuffer data) {
    return nativeGet(key, size, data, objectId);
  }

  public long[] getMeta(String key) {
    long[] res = nativeGetMeta(key, objectId);
    if (res == null) {
      return new long[0];
    } else {
      return res;
    }
  }

  public int del(String key) throws IOException {
    throw new IOException("Delete " + key);
    // return nativeRemove(key, objectId);
  }

  public void shutdown() {
    nativeShutdown(objectId);
  }

  public void waitToStop() {
    nativeWaitToStop(objectId);
  }

  public void dispose() {
    nativeDispose(objectId);
  }

  private ByteBuffer convertToByteBuffer(long address, int length) throws IOException {
    Class<?> classDirectByteBuffer;
    try {
      classDirectByteBuffer = Class.forName("java.nio.DirectByteBuffer");
    } catch (ClassNotFoundException e) {
      throw new IOException("java.nio.DirectByteBuffer class not found");
    }
    Constructor<?> constructor;
    try {
      constructor = classDirectByteBuffer.getDeclaredConstructor(long.class, int.class);
    } catch (NoSuchMethodException e) {
      throw new IOException("java.nio.DirectByteBuffer constructor not found");
    }
    constructor.setAccessible(true);
    ByteBuffer byteBuffer;
    try {
      byteBuffer = (ByteBuffer) constructor.newInstance(address, length);
    } catch (Exception e) {
      throw new IOException("java.nio.DirectByteBuffer exception: " + e.toString());
    }

    return byteBuffer;
  }

  private native long nativeOpenPmPoolClient(String remote_address, String remote_port);

  private native long nativeAlloc(long size, long objectId);

  private native int nativeFree(long address, long objectId);

  private native int nativeWrite(long address, String data, long size, long objectId);

  private native long nativeAllocAndWriteWithString(String data, long size, long objectId);

  private native long nativeAllocAndWriteWithByteBuffer(ByteBuffer data, long size, long objectId);

  private native long nativePut(String key, ByteBuffer data, long size, long objectId);

  private native long nativeGet(String key, long size, ByteBuffer data, long objectId);

  private native long[] nativeGetMeta(String key, long objectId);

  private native int nativeRemove(String key, long objectId);

  private native int nativeRead(long address, long size, ByteBuffer byteBuffer, long objectId);

  private native void nativeShutdown(long objectId);

  private native void nativeWaitToStop(long objectId);

  private native void nativeDispose(long objectId);

  private long objectId;
}
