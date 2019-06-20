package org.apache.spark.storage.pmof

import java.io.OutputStream
import java.nio.ByteBuffer

import io.netty.buffer.{ByteBuf, PooledByteBufAllocator}
import org.apache.spark.internal.Logging

class PmemOutputStream(
  persistentMemoryWriter: PersistentMemoryHandler,
  numPartitions: Int,
  blockId: String,
  numMaps: Int
  ) extends OutputStream with Logging {
  var set_clean = true
  var is_closed = false

  val length: Int = 1024*1024*6
  var total: Int = 0
  val buf: ByteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(length, length)
  val byteBuffer: ByteBuffer = buf.nioBuffer(0, length)

  logDebug(blockId)

  override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
    logDebug("write")
    byteBuffer.put(bytes, off, len)
    total += len
  }

  override def write(byte: Int): Unit = {
    logDebug("write")
    byteBuffer.putInt(byte)
    total += 4
  }

  override def flush(): Unit = {
    if (size() > 0) {
      logDebug("flush")
      persistentMemoryWriter.setPartition(numPartitions, blockId, byteBuffer, size(), set_clean, numMaps)
    }
    if (set_clean) {
      set_clean = false
    }
  }

  def size(): Int = {
    total
  }

  def reset(): Unit = {
    total = 0
    byteBuffer.clear()
  }

  override def close(): Unit = {
    if (!is_closed) {
      flush()
      reset()
      buf.release()
      is_closed = true
    }
  }
}
