package org.apache.spark.storage.pmof

import java.io.OutputStream
import java.nio.ByteBuffer

import io.netty.buffer.{ByteBuf, PooledByteBufAllocator}
import org.apache.spark.internal.Logging
import java.io.IOException

class PmemOutputStream(
    persistentMemoryWriter: PersistentMemoryHandler,
    remotePersistentMemoryPool: RemotePersistentMemoryPool,
    numPartitions: Int,
    blockId: String,
    numMaps: Int,
    bufferSize: Int)
    extends OutputStream
    with Logging {
  var set_clean = true
  var is_closed = false
  var key_id = 0

  val length: Int = bufferSize
  var bufferFlushedSize: Int = 0
  var bufferRemainingSize: Int = 0
  val buf: ByteBuf = NettyByteBufferPool.allocateNewBuffer(length)
  val byteBuffer: ByteBuffer = buf.nioBuffer(0, length)
  var flushed_block_id: String = _
  var cur_block_id: String = _

  override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
    byteBuffer.put(bytes, off, len)
    bufferRemainingSize += len
  }

  override def write(byte: Int): Unit = {
    byteBuffer.putInt(byte)
    bufferRemainingSize += 4
  }

  override def flush(): Unit = {
    if (bufferRemainingSize > 0) {
      if (remotePersistentMemoryPool != null) {
        logWarning(s" [PUT Started]${cur_block_id}-${bufferRemainingSize}")
        if (remotePersistentMemoryPool.put(cur_block_id, byteBuffer, bufferRemainingSize) == -1) {
          throw new IOException(
            s"${cur_block_id}-${bufferRemainingSize} RPMem put failed due to time out.")
        }
        logWarning(s" [PUT Completed]${cur_block_id}-${bufferRemainingSize}")
        key_id += 1
        flushed_block_id = cur_block_id
        cur_block_id = s"${blockId}_${key_id}"
      } else {
        persistentMemoryWriter.setPartition(
          numPartitions,
          blockId,
          byteBuffer,
          bufferRemainingSize,
          set_clean)
      }
      bufferFlushedSize += bufferRemainingSize
      bufferRemainingSize = 0
    }
    if (set_clean) {
      set_clean = false
    }
  }

  def flushedSize(): Int = {
    bufferFlushedSize
  }

  def remainingSize(): Int = {
    bufferRemainingSize
  }

  def reset(): Unit = {
    bufferRemainingSize = 0
    bufferFlushedSize = 0
    byteBuffer.clear()
  }

  override def close(): Unit = synchronized {
    if (!is_closed) {
      flush()
      reset()
      NettyByteBufferPool.releaseBuffer(buf)
      is_closed = true
    }
  }
}
