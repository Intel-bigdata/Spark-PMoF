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

  def getEqService: EqService = {
    eqService
  }
}

class ServerRecvHandler(server: RdmaServer, appid: String, serializer: Serializer,
                        blockManager: BlockDataManager) extends Handler {

  private val deferredBufferList = new LinkedBlockingDeque[ServerDeferredReq]()
  //private val byteBuffersQueue = new LinkedBlockingDeque[Array[ByteBuffer]]()

  def sendMetadata(con: Connection, shuffleBuffers: Array[ShuffleBuffer], seq: Int, isDeferred: Boolean): Unit = {
    val sendBuffer = con.getSendBuffer(false)
    if (sendBuffer == null) {
      if (isDeferred) {
        deferredBufferList.addFirst(new ServerDeferredReq(con, shuffleBuffers, seq))
      } else {
        deferredBufferList.addLast(new ServerDeferredReq(con, shuffleBuffers, seq))
      }
      return
    }
    val blocksNum = shuffleBuffers.length
    val byteBufferTmp = ByteBuffer.allocate(20*blocksNum+4)

    byteBufferTmp.putInt(blocksNum)
    for (i <- 0 until blocksNum) {
      byteBufferTmp.putInt(shuffleBuffers(i).size.toInt)
      byteBufferTmp.putLong(shuffleBuffers(i).getAddress)
      byteBufferTmp.putLong(shuffleBuffers(i).getRkey)
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
    sendMetadata(con, deferredReq.shuffleBuffers, seq, isDeferred = true)
  }

  def toShuffleBuffer(file: File, offset: Long, length: Long): ShuffleBuffer = {
    val channel: FileChannel = new RandomAccessFile(file, "rw").getChannel
    val shuffleBuffer = new ShuffleBuffer(offset, length, channel, server.getEqService)
    val rdmaBuffer = server.getEqService.regRmaBufferByAddress(shuffleBuffer.nioByteBuffer(), shuffleBuffer.getAddress, length.toInt)
    shuffleBuffer.setRdmaBufferId(rdmaBuffer.getRdmaBufferId)
    shuffleBuffer.setRkey(rdmaBuffer.getRKey)
    shuffleBuffer
  }

  override def handle(con: Connection, rdmaBufferId: Int, blockBufferSize: Int): Unit = {

    val buffer: RdmaBuffer = con.getRecvBuffer(rdmaBufferId)
    val rpcMessage: ByteBuffer = buffer.get(blockBufferSize)
    val message = BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    val openBlocks = message.asInstanceOf[OpenBlocks]

    val blocksNum = openBlocks.blockIds.length
    val shuffleBuffers = new Array[ShuffleBuffer](blocksNum)
    for (i <- (0 until blocksNum).view) {
      val managedBuffer = blockManager.getBlockData(BlockId.apply(openBlocks.blockIds(i))).asInstanceOf[FileSegmentManagedBuffer]
      shuffleBuffers(i) = toShuffleBuffer(managedBuffer.getFile, managedBuffer.getOffset, managedBuffer.getLength)
    }
    val seq = buffer.getSeq
    sendMetadata(con, shuffleBuffers, seq, isDeferred = false)
  }
}

class ServerDeferredReq(var con: Connection, var shuffleBuffers: Array[ShuffleBuffer], var seq: Int) {}
