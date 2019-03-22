package org.apache.spark.storage.pmof;

public class PmemBuffer {
    static {
        System.load("/usr/local/lib/libjnipmdk.so");
    }
    private native long nativeNewPmemBuffer();
    private native int nativeLoadPmemBuffer(long pmBuffer, long addr, int len);
    private native int nativeReadPmemBuffer(long pmBuffer, byte[] bytes, int len);
    private native long nativeDeletePmemBuffer(long pmBuffer);

    long pmBuffer;
    PmemBuffer() {
      pmBuffer = nativeNewPmemBuffer();
    }

    void load(long addr, int len) {
      nativeLoadPmemBuffer(pmBuffer, addr, len);
    }

    int get(byte[] bytes, int len) {
      int read_len = nativeReadPmemBuffer(pmBuffer, bytes, len);
      return read_len;
    }

    int get() {
      byte[] bytes = new byte[1];
      nativeReadPmemBuffer(pmBuffer, bytes, 1);
      return (bytes[0] & 0xFF);
    }

    void close() {
      nativeDeletePmemBuffer(pmBuffer);
    }
}
