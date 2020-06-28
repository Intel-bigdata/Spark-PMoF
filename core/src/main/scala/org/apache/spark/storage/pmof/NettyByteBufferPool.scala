package org.apache.spark.storage.pmof

import java.util.concurrent.atomic.AtomicLong
import io.netty.buffer.{ByteBuf, PooledByteBufAllocator, UnpooledByteBufAllocator}
import scala.collection.mutable.Stack
import java.lang.RuntimeException
import org.apache.spark.internal.Logging

object NettyByteBufferPool extends Logging {
  private val allocatedBufRenCnt: AtomicLong = new AtomicLong(0)
  private val allocatedBytes: AtomicLong = new AtomicLong(0) 
  private val peakAllocatedBytes: AtomicLong = new AtomicLong(0) 
  private val unpooledAllocatedBytes: AtomicLong = new AtomicLong(0) 
  private var fixedBufferSize: Long = 0
  private val allocatedBufferPool: Stack[ByteBuf] = Stack[ByteBuf]() 
  private var reachRead = false
  private val allocator = UnpooledByteBufAllocator.DEFAULT
  private var bufferMap: Map[ByteBuf, Long] = Map()

  def allocateNewBuffer(bufSize: Int): ByteBuf = synchronized {
    allocatedBufRenCnt.getAndIncrement()
    allocatedBytes.getAndAdd(bufSize)
    if (allocatedBytes.get > peakAllocatedBytes.get) {
      peakAllocatedBytes.set(allocatedBytes.get)
    }
    try {
      /*if (allocatedBufferPool.isEmpty == false) {
        allocatedBufferPool.pop
      } else {
        allocator.directBuffer(bufSize, bufSize)
      }*/

      val byteBuf = allocator.directBuffer(bufSize, bufSize)
      bufferMap += (byteBuf -> bufSize)
      byteBuf

    } catch {
      case e : Throwable =>
        logError(s"allocateNewBuffer size is ${bufSize}")
        throw e
    }
  }

  def allocateFlexibleNewBuffer(bufSize: Int): ByteBuf = synchronized {
    val initialCapacity = 65536
    val maxCapacity = bufSize * 2
    val byteBuf = allocator.directBuffer(initialCapacity, maxCapacity)
    bufferMap += (byteBuf -> bufSize)
    byteBuf
  }

  def releaseBuffer(buf: ByteBuf): Unit = synchronized {
    allocatedBufRenCnt.getAndDecrement()
    try {
      val bufSize = bufferMap(buf)
      allocatedBytes.getAndAdd(bufSize)

    } catch {
      case e: NoSuchElementException => {}
    }
    buf.clear()
    //allocatedBufferPool.push(buf)
    buf.release(buf.refCnt())
  }

  def unpooledInc(bufSize: Int): Unit = synchronized {
    if (reachRead == false) {
      reachRead = true
      peakAllocatedBytes.set(0)
    }
    unpooledAllocatedBytes.getAndAdd(bufSize)
  }

  def unpooledDec(bufSize: Int): Unit = synchronized {
    unpooledAllocatedBytes.getAndAdd(0 - bufSize)
  }

  def unpooledInc(bufSize: Long): Unit = synchronized {
    if (reachRead == false) {
      reachRead = true
      peakAllocatedBytes.set(0)
    }
    unpooledAllocatedBytes.getAndAdd(bufSize)
  }

  def unpooledDec(bufSize: Long): Unit = synchronized {
    unpooledAllocatedBytes.getAndAdd(0 - bufSize)
  }

  override def toString(): String = synchronized {
    return s"NettyBufferPool [refCnt|allocatedBytes|Peak|Native] is [${allocatedBufRenCnt.get}|${allocatedBytes.get}|${peakAllocatedBytes.get}|${unpooledAllocatedBytes.get}]"
  }
}
