package org.apache.spark.network

import java.nio.ByteBuffer

import org.apache.spark.network.shuffle.protocol.{BlockTransferMessage, OpenBlocks}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.BlockId

import com.intel.hpnl.core.{Connection, CqService, EqService, Handler, Buffer}

class RDMAServer(address: String, port: Int) {
  val eqService = new EqService(address, port.toString, true)
  val cqService = new CqService(eqService, 1, eqService.getNativeHandle)

  final val SINGLE_BUFFER_SIZE = 65536
  final val BUFFER_NUM = 32

  def init(): Unit = {
    for (i <- 0 to BUFFER_NUM) {
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
    eqService.shutdown()
  }

  def waitToStop(): Unit = {
    eqService.waitToStop()
    eqService.join()
    cqService.shutdown()
    cqService.join()
  }

  def setRecvHandler(handler: Handler): Unit = {
    eqService.setRecvCallback(handler)
  }
}

class ServerRecvHandler(server: RDMAServer, appid: String, serializer: Serializer, blockManager: BlockDataManager) extends Handler {
  override def handle(con: Connection, rdmaBufferId: Int, blockBufferSize: Int): Unit = {
    val buffer: Buffer = con.getRecvBuffer(rdmaBufferId)
    val rpcMessage: ByteBuffer = buffer.get(blockBufferSize)
    val message = BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    val openBlocks = message.asInstanceOf[OpenBlocks]
    val blocksNum = openBlocks.blockIds.length
    val seq = buffer.getSeq
    for (i <- (0 until blocksNum).view) {
      val nioBuffer: ByteBuffer = blockManager.getBlockData(BlockId.apply(openBlocks.blockIds(i))).nioByteBuffer()
      val sendBuffer = con.getSendBuffer
      sendBuffer.put(nioBuffer, i, seq)
      con.send(sendBuffer.getByteBuffer.remaining(), sendBuffer.getRdmaBufferId)
    }
  }
}
