package org.apache.spark.storage.pmof

import java.io.OutputStream
import org.apache.spark.storage.pmof.PmemBuffer
import org.apache.spark.internal.Logging

class PmemOutputStream(
  persistentMemoryWriter: PersistentMemoryHandler,
  numPartitions: Int,
  blockId: String,
  numMaps: Int
  ) extends OutputStream with Logging {
  val buf = new PmemBuffer()
  var set_clean = true
  var is_closed = false
  logDebug(blockId)

  override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
    buf.put(bytes, off, len)
  }

  override def write(byte: Int): Unit = {
    var bytes: Array[Byte] = Array(byte.toByte)
    buf.put(bytes, 0, 1)
  }

  override def flush(): Unit = {
    persistentMemoryWriter.setPartition(numPartitions, blockId, buf, set_clean, numMaps)
    if (set_clean == true) {
      set_clean = false
    }
  }

  def size(): Int = {
    buf.size()
  }

  def reset(): Unit = {
    buf.clean()
  }

  override def close(): Unit = {
    if (!is_closed) {
      buf.close()
      is_closed = true
    }
  }
}
