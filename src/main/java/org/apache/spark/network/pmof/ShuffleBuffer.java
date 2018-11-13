package org.apache.spark.network.pmof;

import com.intel.hpnl.core.EqService;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.UnsafeMemoryAllocator;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

public class ShuffleBuffer extends ManagedBuffer {
    private final MemoryBlock memoryBlock;
    private final UnsafeMemoryAllocator unsafeAlloc = new UnsafeMemoryAllocator();
    private final int length;
    private final long address;
    private final EqService service;
    private int rdmaBufferId;
    private long rkey;
    private ByteBuffer byteBuffer;

    public ShuffleBuffer(int length, EqService service) throws IOException {
        this.length = length;
        memoryBlock = unsafeAlloc.allocate(this.length);
        this.address = memoryBlock.getBaseOffset();
        this.service = service;
        this.byteBuffer = convertToByteBuffer();
        this.byteBuffer.limit(length);
    }

    public int getLength() {
        return this.length;
    }

    public long size() {
        return this.length;
    }

    public ByteBuffer nioByteBuffer() {
        return byteBuffer;
    }

    public long getAddress() {
        return this.address;
    }

    public void setRdmaBufferId(int rdmaBufferId) {
        this.rdmaBufferId = rdmaBufferId;
    }

    public int getRdmaBufferId() {
        return this.rdmaBufferId;
    }

    public void setRkey(long rkey) {
        this.rkey = rkey;
    }

    public long getRkey() {
        return this.rkey;
    }

    public ManagedBuffer release() {
        service.unregRmaBuffer(this.rdmaBufferId);
        unsafeAlloc.free(memoryBlock);
        return this;
    }

    public Object convertToNetty() {
        return null;
    }

    public InputStream createInputStream() {
        return new ShuffleBufferInputStream(this);
    }

    public ManagedBuffer retain() {
        return null;
    }

    private ByteBuffer convertToByteBuffer() throws IOException {
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
            byteBuffer = (ByteBuffer)constructor.newInstance(address, length);
        } catch (Exception e) {
            throw new IOException("java.nio.DirectByteBuffer exception: " + e.toString());
        }

        return byteBuffer;
    }
}
