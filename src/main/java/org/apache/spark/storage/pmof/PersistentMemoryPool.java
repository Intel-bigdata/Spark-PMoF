package org.apache.spark.storage.pmof;

import org.apache.spark.storage.pmof.PmemBuffer;
public class PersistentMemoryPool {
    static {
        System.load("/usr/local/lib/libjnipmdk.so");
    }
    private static native long nativeOpenDevice(String path, long size);
    private static native long nativeSetBlock(long deviceHandler, String key, long pmemBufferHandler, boolean clean);
    private static native byte[] nativeGetBlock(long deviceHandler, String key);
    private static native long[] nativeGetBlockIndex(long deviceHandler, String key);
    private static native long nativeGetBlockSize(long deviceHandler, String key);
    private static native long nativeDeleteBlock(long deviceHandler, String key);
    private static native long nativeGetRoot(long deviceHandler);
    private static native int nativeCloseDevice(long deviceHandler);
  
    private static final long DEFAULT_PMPOOL_SIZE = 0L;

    private String device;
    private long deviceHandler;

    PersistentMemoryPool(String path, long pool_size) {
      this.device = path; 
      pool_size = pool_size == -1 ? DEFAULT_PMPOOL_SIZE : pool_size;
      this.deviceHandler = nativeOpenDevice(path, pool_size);
    }

    public long setPartition(String key, PmemBuffer buf, boolean set_clean) {
      return nativeSetBlock(this.deviceHandler, key, buf.getNativeObject(), set_clean);
    }

    public byte[] getPartition(String key) {
      return nativeGetBlock(this.deviceHandler, key);
    }

    public long[] getPartitionBlockInfo(String key) {
      return nativeGetBlockIndex(this.deviceHandler, key);
    }

    public long getPartitionSize(String key) {
      return nativeGetBlockSize(this.deviceHandler, key);
    }

    public long deletePartition(String key) {
      return nativeDeleteBlock(this.deviceHandler, key);
    }

    public long getRootAddr() {
        return nativeGetRoot(this.deviceHandler);
    }

    public void close() {
      nativeCloseDevice(this.deviceHandler);
      try {
        System.out.println("Use pmempool to delete device:" + device);
        Runtime.getRuntime().exec("pmempool rm " + device);
      } catch (Exception e) {
        System.err.println("delete " + device + "failed: " + e.getMessage());
      }
    }
}
