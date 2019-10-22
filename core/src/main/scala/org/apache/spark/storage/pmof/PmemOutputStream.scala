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
  var flushedSize: Int = 0
  var remainingSize: Int = 0
  val buf: ByteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(length, length)
  val byteBuffer: ByteBuffer = buf.nioBuffer(0, length)

  logDebug(blockId)

  override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
    byteBuffer.put(bytes, off, len)
    remainingSize += len
  }

  override def write(byte: Int): Unit = {
    byteBuffer.putInt(byte)
    remainingSize += 4
  }

  override def flush(): Unit = {
    if (remainingSize > 0) {
      persistentMemoryWriter.setPartition(numPartitions, blockId, byteBuffer, remainingSize, set_clean, numMaps)
      flushedSize += remainingSize
      remainingSize = 0
    }
    if (set_clean) {
      set_clean = false
    }
  }

  def size(): Int = {
    flushedSize
  }

  def reset(): Unit = {
    remainingSize = 0
    flushedSize = 0
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
