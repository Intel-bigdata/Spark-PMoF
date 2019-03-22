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
import org.apache.spark.storage.pmof.{PersistentMemoryPool, PmemInputStream}

class PmemManagedBuffer(pmHandler: PersistentMemoryHandler, blockId: String) extends ManagedBuffer with Logging {
  var inputStream: InputStream = _
  var total_size: Long = -1
  var byteBuffer: ByteBuffer = _
  private val refCount = new AtomicInteger(1)

  override def size(): Long = {
    if (total_size == -1) {
      total_size = pmHandler.getPartitionSize(blockId)
    }
    total_size
  }

  override def nioByteBuffer(): ByteBuffer = {
    // TODO: This function should be Deprecated by spark in near future.
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
      inputStream = new PmemInputStream(pmHandler, blockId)
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
    var data_length = size().toInt
    var in = createInputStream()
    in.asInstanceOf[PmemInputStream].load(data_length)
    Unpooled.wrappedBuffer(in.asInstanceOf[PmemInputStream].getByteBufferDirectAddr(), data_length, false)
  }
}
