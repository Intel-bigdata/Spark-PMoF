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

  val length: Int = bufferSize
  var bufferFlushedSize: Int = 0
  var bufferRemainingSize: Int = 0
  val buf: ByteBuf = NettyByteBufferPool.allocateNewBuffer(length)
  val byteBuffer: ByteBuffer = buf.nioBuffer(0, length)

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
      if (persistentMemoryWriter != null) {
        persistentMemoryWriter.setPartition(
          numPartitions,
          blockId,
          byteBuffer,
          bufferRemainingSize,
          set_clean)
      } else {
        logDebug(
          s"[put Remote Block] target is ${RemotePersistentMemoryPool.getHost}:${RemotePersistentMemoryPool.getPort}, "
            + s"${blockId} ${bufferRemainingSize}")
        if (remotePersistentMemoryPool.put(blockId, byteBuffer, bufferRemainingSize) == -1) {
          throw new IOException("RPMem put failed with time out.")
        }
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
