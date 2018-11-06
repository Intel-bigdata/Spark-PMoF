package org.apache.spark.network.pmof

import java.io.{File, IOException, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.LinkedBlockingDeque

import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.shuffle.protocol.{BlockTransferMessage, OpenBlocks}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.BlockId
import com.intel.hpnl.core._
import org.apache.spark.network.buffer.FileSegmentManagedBuffer

class RdmaServer(address: String, var port: Int) {
  if (port == 0) {
    port = Utils.getPort
  }
  val eqService = new EqService(address, port.toString, true)
  val cqService = new CqService(eqService, 1, eqService.getNativeHandle)

  final val SINGLE_BUFFER_SIZE = RdmaTransferService.CHUNKSIZE
  final val BUFFER_NUM = 1024

  def init(): Unit = {
    for (i <- 0 until BUFFER_NUM) {
      val sendBuffer = ByteBuffer.allocateDirect(SINGLE_BUFFER_SIZE)
      eqService.setSendBuffer(sendBuffer, SINGLE_BUFFER_SIZE, i)
    }
    for (i <- 0 until BUFFER_NUM*2) {
      val recvBuffer = ByteBuffer.allocateDirect(SINGLE_BUFFER_SIZE)
      eqService.setRecvBuffer(recvBuffer, SINGLE_BUFFER_SIZE, i)
    }
  }

  def start(): Unit = {
    cqService.start()
    eqService.start(1)
  }

  def stop(): Unit = {
    cqService.shutdown()
  }

  def waitToStop(): Unit = {
    cqService.join()
    eqService.shutdown()
    eqService.join()
  }

  def setRecvHandler(handler: Handler): Unit = {
    eqService.setRecvCallback(handler)
    cqService.addExternalEvent(new ExternalHandler {
      override def handle(): Unit = {
        handler.asInstanceOf[ServerRecvHandler] handleDeferredReq()
      }
    })
  }
}

class ServerRecvHandler(server: RdmaServer, appid: String, serializer: Serializer,
                        blockManager: BlockDataManager) extends Handler {

  private val deferredBufferList = new LinkedBlockingDeque[ServerDeferredReq]()
  private val byteBuffersQueue = new LinkedBlockingDeque[Array[ByteBuffer]]()

  def sendMetadata(con: Connection, byteBuffers: Array[ByteBuffer], seq: Int, isDeferred: Boolean): Unit = {
    val sendBuffer = con.getSendBuffer(false)
    if (sendBuffer == null) {
      if (isDeferred) {
        deferredBufferList.addFirst(new ServerDeferredReq(con, byteBuffers, seq))
      } else {
        deferredBufferList.addLast(new ServerDeferredReq(con, byteBuffers, seq))
      }
      return
    }
    val blocksNum = byteBuffers.length
    val byteBufferTmp = ByteBuffer.allocate(20*blocksNum+4)
    byteBufferTmp.putInt(blocksNum)
    for (i <- 0 until blocksNum) {
      byteBufferTmp.putInt(byteBuffers(i).capacity())
      byteBufferTmp.putLong(server.eqService.getBufferAddress(byteBuffers(i)))
      byteBufferTmp.putLong(server.eqService.regRmaBuffer(byteBuffers(i), byteBuffers(i).capacity()))
    }
    byteBufferTmp.flip()
    sendBuffer.put(byteBufferTmp, 0.toByte, 0, seq)
    con.send(sendBuffer.remaining(), sendBuffer.getRdmaBufferId)
  }

  def handleDeferredReq(): Unit = {
    val deferredReq = deferredBufferList.pollFirst
    if (deferredReq == null) return
    val con = deferredReq.con
    val seq = deferredReq.seq
    sendMetadata(con, deferredReq.byteBuffers, seq, isDeferred = true)
  }

  def toByteBuffer(file: File, offset: Long, length: Long): ByteBuffer = {
    val channel: FileChannel = new RandomAccessFile(file, "r").getChannel
    //channel.map(FileChannel.MapMode.READ_WRITE, offset, length)
    val buf = ByteBuffer.allocateDirect(length.toInt)
    channel.position(offset)
    while (buf.remaining != 0) {
      if (channel.read(buf) == -1)
        throw new IOException()
    }
    buf.flip
    buf
  }

  override def handle(con: Connection, rdmaBufferId: Int, blockBufferSize: Int): Unit = {
    val buffer: Buffer = con.getRecvBuffer(rdmaBufferId)
    val rpcMessage: ByteBuffer = buffer.get(blockBufferSize)
    val message = BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    val openBlocks = message.asInstanceOf[OpenBlocks]

    val blocksNum = openBlocks.blockIds.length
    val byteBuffers = new Array[ByteBuffer](blocksNum)
    for (i <- (0 until blocksNum).view) {
      val managedBuffer = blockManager.getBlockData(BlockId.apply(openBlocks.blockIds(i))).asInstanceOf[FileSegmentManagedBuffer]
      byteBuffers(i) = toByteBuffer(managedBuffer.getFile, managedBuffer.getOffset, managedBuffer.getLength)
    }
    byteBuffersQueue.add(byteBuffers)
    val seq = buffer.getSeq
    sendMetadata(con, byteBuffers, seq, isDeferred = false)
  }
}

class ServerDeferredReq(var con: Connection, var byteBuffers: Array[ByteBuffer], var seq: Int) {}
