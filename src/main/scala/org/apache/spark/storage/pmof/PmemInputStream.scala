package org.apache.spark.storage.pmof

import java.io.InputStream
import org.apache.spark.storage.pmof.PmemBuffer
import org.apache.spark.internal.Logging
import scala.util.control.Breaks._

class PmemInputStream(
  persistentMemoryWriter: PersistentMemoryHandler,
  blockId: String
  ) extends InputStream with Logging {
  var buf = new PmemBuffer()
  var index: Int = 0
  var remaining: Int = 0
  var available_bytes: Int = persistentMemoryWriter.getPartitionSize(blockId).toInt
  var blockInfo: Array[(Long, Int)] = persistentMemoryWriter.getPartitionBlockInfo(blockId)

  def loadNextStream(): Int = {
    if (index >= blockInfo.length)
      return 0
    val data_length = blockInfo(index)._2
    val data_addr = blockInfo(index)._1

    buf.load(data_addr, data_length)

    index += 1
    remaining += data_length
    data_length
  }

  override def read(): Int = {
    if (remaining == 0) {
      if (loadNextStream() == 0) {
        return -1
      }
    }
    remaining -= 1
    available_bytes -= 1
    buf.get()
  }

  override def read(bytes: Array[Byte], off: Int, len: Int): Int = {
    breakable { while ((remaining > 0 && remaining < len) || remaining == 0) {
      if (loadNextStream() == 0) {
        break
      }
    } }
    if (remaining == 0) {
      return -1
    }

    val real_len = Math.min(len, remaining)
    buf.get(bytes, off, real_len)
    remaining -= real_len
    available_bytes -= real_len
    real_len
  }

  override def available(): Int = {
    available_bytes
  }

  override def close(): Unit = {
    buf.close()
  }
}
