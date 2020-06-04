package org.apache.spark.storage.pmof

import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufInputStream
import io.netty.buffer.Unpooled
import org.apache.commons.lang3.builder.ToStringBuilder
import org.apache.commons.lang3.builder.ToStringStyle
import org.apache.spark.network.buffer.ManagedBuffer

/**
 * A {@link ManagedBuffer} backed by a Netty {@link ByteBuf}.
 */
class NioManagedBuffer(bufSize: Int) extends ManagedBuffer {
  private val buf: ByteBuf = NettyByteBufferPool.allocateNewBuffer(bufSize)
  private val byteBuffer: ByteBuffer = buf.nioBuffer(0, bufSize)
  private val refCount = new AtomicInteger(1)
  private var in: InputStream = _
  private var nettyObj: ByteBuf = _

  def getByteBuf: ByteBuf = buf

  def resize(size: Int): Unit = {
    byteBuffer.limit(size)
  }

  override def size: Long = {
    byteBuffer.remaining()
  }

  override def nioByteBuffer: ByteBuffer = {
    byteBuffer
  }

  override def createInputStream: InputStream = {
    nettyObj = Unpooled.wrappedBuffer(byteBuffer)
    in = new ByteBufInputStream(nettyObj)
    in
  }

  override def retain: ManagedBuffer = {
    refCount.incrementAndGet()
    return this
  }

  override def release: ManagedBuffer = {
    if (refCount.decrementAndGet() == 0) {
      if (in != null) {
        in.close()
      }
      if (nettyObj != null) {
        nettyObj.release()
      }
      NettyByteBufferPool.releaseBuffer(buf)
    }
    return this
  }

  override def convertToNetty: Object = {
    if (nettyObj == null) {
      nettyObj = Unpooled.wrappedBuffer(byteBuffer)
    }
    nettyObj
  }

  override def toString: String = {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("buf", buf)
      .toString();
  }
}
