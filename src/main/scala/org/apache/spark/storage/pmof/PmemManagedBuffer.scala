package org.apache.spark.storage.pmof

import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer
import sun.misc.Cleaner
import io.netty.buffer.Unpooled
import io.netty.buffer.ByteBuf
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.pmof.PersistentMemoryPool
//import java.nio.channels.Channels
//import java.nio.channels.ReadableByteChannel
//import org.apache.spark.network.util.JavaUtils 

class PmemManagedBuffer(pmpool: PersistentMemoryPool, shuffleId: Int, mapId: Int, reduceId: Int) extends ManagedBuffer with Logging {
  var inputStream: InputStream = _
  var total_size: Long = -1
  var byteBuffer: ByteBuffer = _
  private val refCount = new AtomicInteger(1)

  override def size(): Long = {
    if (total_size == -1) {
      total_size = pmpool.getMapPartitionSize(shuffleId, mapId, reduceId)
    }
    total_size
  }

  override def nioByteBuffer(): ByteBuffer = {
    var data_length = size().toInt
    var in = createInputStream()
    byteBuffer = ByteBuffer.allocateDirect(data_length);
    var data = Array.ofDim[Byte](data_length)
    in.read(data)
    byteBuffer.put(data)
    byteBuffer.flip()
    byteBuffer
  }

  override def createInputStream(): InputStream = {
    if (inputStream == null) {
      inputStream = new PmemInputStream()
    }
    inputStream
  }

  override def retain(): ManagedBuffer = {
    refCount.incrementAndGet()
    this
  }

  override def release(): ManagedBuffer = {
    if (refCount.decrementAndGet() == 0) {
      if (byteBuffer != null) {
        var cleanerField: java.lang.reflect.Field = byteBuffer.getClass.getDeclaredField("cleaner");
        cleanerField.setAccessible(true);
        var cleaner: Cleaner = cleanerField.get(byteBuffer).asInstanceOf[Cleaner]
        cleaner.clean();
      }
      if (inputStream != null) {
        inputStream.close()
      }
    }
    this
  }

  override def convertToNetty(): Object = {
    // we should return a io.netty.buffer.ByteBuf
    var byteBuffer = nioByteBuffer()
    if (byteBuffer == null) {
      logInfo("shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + " convertToNetty return null")
      null
    } else {
      Unpooled.wrappedBuffer(byteBuffer)
    }
  }

  class PmemInputStream() extends InputStream with Logging{
    var buf = new PmemBuffer()
    var index: Int = 0
    var remaining: Int = 0
    var available_bytes: Int = size().toInt
    var blockInfo: ArrayBuffer[(Long, Int)] = _ 
  
    def loadBlockInfo(): Unit = {
      var res_array: Array[Long] = pmpool.getMapPartitionBlockInfo(shuffleId, mapId, reduceId)
      blockInfo = new ArrayBuffer[(Long, Int)]()
      var i = 0
      while ((i + 1) < res_array.length) {
        var info = (res_array(i), res_array(i+1).toInt)
        blockInfo += info
        i = i + 2
      }
    }
  
    def loadNextStream(): Int = {
      if (blockInfo == null) {
        loadBlockInfo()
      }
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
      buf.get(bytes, real_len)
      remaining -= real_len
      available_bytes -= real_len
      real_len
    }
  
    override def available(): Int = {
      available_bytes
    }
  
    override def close(): Unit = {
      //logInfo("close " + buf)
      buf.close()
    }

  }
}
