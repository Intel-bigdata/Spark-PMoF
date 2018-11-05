package org.apache.spark.network.pmof

import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingDeque

import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.shuffle.protocol.{BlockTransferMessage, OpenBlocks}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.BlockId
import com.intel.hpnl.core._

class RdmaServer(address: String, var port: Int) {
  if (port == 0) {
    port = Utils.getPort
  }
  val eqService = new EqService(address, port.toString, true)
  val cqService = new CqService(eqService, 5, eqService.getNativeHandle)

  final val SINGLE_BUFFER_SIZE = RdmaTransferService.CHUNKSIZE
  final val BUFFER_NUM = 4096

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

  def sendBuffer(con: Connection, byteBuffer: ByteBuffer, chunkIndex: Int, seq: Int, chunkNums: Int, isDeferred: Boolean): Unit = {
    for (i <- chunkIndex until chunkNums) {
      val sendBuffer = con.getSendBuffer(false)
      if (sendBuffer == null) {
        if (isDeferred) {
          deferredBufferList.addFirst(new ServerDeferredReq(con, byteBuffer, 1, chunkNums - i, seq, chunkNums))
        } else {
          deferredBufferList.addLast(new ServerDeferredReq(con, byteBuffer, 1, chunkNums - i, seq, chunkNums))
        }
        return
      }
      byteBuffer.position(i * (RdmaTransferService.CHUNKSIZE - 9))
      if (i == chunkNums - 1) {
        byteBuffer.limit(byteBuffer.capacity())
      } else {
        byteBuffer.limit((i + 1) * (RdmaTransferService.CHUNKSIZE - 9))
      }
      sendBuffer.put(byteBuffer.slice(), 1, i, seq)
      con.send(sendBuffer.remaining(), sendBuffer.getRdmaBufferId)
    }
  }

  def sendBufferSize(con: Connection, byteBuffer: ByteBuffer, seq: Int, isDeferred: Boolean): Unit = {
    val sendBuffer = con.getSendBuffer(false)
    if (sendBuffer == null) {
      if (isDeferred) {
        deferredBufferList.addFirst(new ServerDeferredReq(con, byteBuffer, 0, 0, seq, 0))
      } else {
        deferredBufferList.addLast(new ServerDeferredReq(con, byteBuffer, 0, 0, seq, 0))
      }
      return
    }
    sendBuffer.putInt(byteBuffer.capacity(), 0, 0, seq)
    con.send(sendBuffer.remaining(), sendBuffer.getRdmaBufferId)
  }

  def handleDeferredReq(): Unit = {
    val deferredReq = deferredBufferList.pollFirst
    if (deferredReq == null) return
    val con = deferredReq.con
    val chunkNums = deferredReq.chunkNums
    val chunkIndex = chunkNums-deferredReq.remainingChunks
    val seq = deferredReq.seq
    val msgType = deferredReq.msgType
    if (msgType == 1.toByte) {
      sendBuffer(con, deferredReq.byteBuffer, chunkIndex, seq, chunkNums, isDeferred = true)
    } else {
      sendBufferSize(con, deferredReq.byteBuffer, seq, isDeferred = true)
    }
  }

  override def handle(con: Connection, rdmaBufferId: Int, blockBufferSize: Int): Unit = {
    val buffer: Buffer = con.getRecvBuffer(rdmaBufferId)
    val rpcMessage: ByteBuffer = buffer.get(blockBufferSize)
    val message = BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    val openBlocks = message.asInstanceOf[OpenBlocks]
    assert(openBlocks.blockIds.length == 1)
    val seq = buffer.getSeq
    val msgType: Byte = buffer.getType
    val byteBuffer: ByteBuffer = blockManager.getBlockData(BlockId.apply(openBlocks.blockIds(0))).nioByteBuffer()
    if (msgType == 0.toByte) {
      sendBufferSize(con, byteBuffer, seq, isDeferred = false)
    } else {
      var chunkNums = byteBuffer.capacity()/(RdmaTransferService.CHUNKSIZE-9)
      if (byteBuffer.capacity()%(RdmaTransferService.CHUNKSIZE-9) != 0) {
        chunkNums += 1
      }
      sendBuffer(con, byteBuffer, 0, seq, chunkNums, isDeferred = false)
    }
  }
}

class ServerDeferredReq(var con: Connection, var byteBuffer: ByteBuffer, var msgType: Byte, var remainingChunks: Int,
                        var seq: Int, var chunkNums: Int) {}
