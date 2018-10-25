package org.apache.spark.network.pmof


import java.nio.ByteBuffer
import java.util.LinkedList

import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.shuffle.protocol.{BlockTransferMessage, OpenBlocks}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.BlockId
import com.intel.hpnl.core._

class RDMAServer(address: String, var port: Int) {
  if (port == 0) {
    port = Utils.getPort
  }
  val eqService = new EqService(address, port.toString, true)
  val cqService = new CqService(eqService, 1, eqService.getNativeHandle)

  final val SINGLE_BUFFER_SIZE = RDMATransferService.CHUNKSIZE
  final val BUFFER_NUM = 512

  def init(): Unit = {
    for (i <- 0 until BUFFER_NUM) {
      val recvBuffer = ByteBuffer.allocateDirect(SINGLE_BUFFER_SIZE)
      val sendBuffer = ByteBuffer.allocateDirect(SINGLE_BUFFER_SIZE)
      eqService.setRecvBuffer(recvBuffer, SINGLE_BUFFER_SIZE, i)
      eqService.setSendBuffer(sendBuffer, SINGLE_BUFFER_SIZE, i)
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
        handler.asInstanceOf[ServerRecvHandler].handleDeferredRequest()
      }
    })
  }
}

class ServerRecvHandler(server: RDMAServer, appid: String, serializer: Serializer,
                        blockManager: BlockDataManager) extends Handler {

  private val deferredBufferList = new LinkedList[DeferredBuffer]()

  def sendBuffer(con: Connection, byteBuffer: ByteBuffer, chunkIndex: Int, seq: Int, chunkNums: Int, isDeferred: Boolean): Unit = {
    for (i <- chunkIndex until chunkNums) {
      val sendBuffer = con.getSendBuffer(false)
      if (sendBuffer == null) {
        if (isDeferred) {
          deferredBufferList.addFirst(new DeferredBuffer(con, byteBuffer, chunkNums - i, seq, chunkNums))
        } else {
          deferredBufferList.addLast(new DeferredBuffer(con, byteBuffer, chunkNums - i, seq, chunkNums))
        }
        return
      }
      byteBuffer.position(i * (RDMATransferService.CHUNKSIZE - 9))
      if (i == chunkNums - 1) {
        byteBuffer.limit(byteBuffer.capacity())
      } else {
        byteBuffer.limit((i + 1) * (RDMATransferService.CHUNKSIZE - 9))
      }
      sendBuffer.put(byteBuffer.slice(), 1, i, seq)
      con.send(sendBuffer.remaining(), sendBuffer.getRdmaBufferId)
    }
  }

  def handleDeferredRequest(): Unit = {
    if (deferredBufferList.size() > 0) {
      val deferredBuffer = deferredBufferList.pollFirst
      val con = deferredBuffer.con
      val chunkNums = deferredBuffer.chunkNums
      val chunkIndex = chunkNums-deferredBuffer.remainingChunks
      val seq = deferredBuffer.seq
      sendBuffer(con, deferredBuffer.byteBuffer, chunkIndex, seq, chunkNums, true)
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
      val sendBuffer = con.getSendBuffer(false)
      sendBuffer.putInt(byteBuffer.capacity(), 0, 0, seq)
      con.send(sendBuffer.remaining(), sendBuffer.getRdmaBufferId)
    } else {
      var chunkNums = byteBuffer.capacity()/(RDMATransferService.CHUNKSIZE-9)
      if (byteBuffer.capacity()%(RDMATransferService.CHUNKSIZE-9) != 0) {
        chunkNums += 1
      }
      sendBuffer(con, byteBuffer, 0, seq, chunkNums, false)
    }
  }
}

class DeferredBuffer(var con: Connection, var byteBuffer: ByteBuffer, var remainingChunks: Int, var seq: Int, var chunkNums: Int) {

}
