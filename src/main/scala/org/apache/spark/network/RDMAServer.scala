package org.apache.spark.network

import java.nio.ByteBuffer

import com.intel.hpnl.core.{Connection, CqService, EqService, Handler}
import org.apache.spark.network.shuffle.protocol.{BlockTransferMessage, OpenBlocks}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.BlockId

class RDMAServer(address: String, port: String) {
  val eqService = new EqService(address, port, true)
  val cqService = new CqService(eqService, 1, eqService.getNativeHandle)

  final val SINGLE_BUFFER_SIZE = 65536
  final val TOTAl_BUFFER_SIZE = SINGLE_BUFFER_SIZE*16
  private var recvBuffer: ByteBuffer = _
  private var sendBuffer: ByteBuffer = _

  def init(): Unit = {
    recvBuffer = ByteBuffer.allocateDirect(TOTAl_BUFFER_SIZE)
    sendBuffer = ByteBuffer.allocateDirect(TOTAl_BUFFER_SIZE)
  }

  def start(): Unit = {
    eqService.setConnectedCallback(null)
    eqService.setSendCallback(null)
    eqService.setShutdownCallback(null)

    eqService.set_recv_buffer(recvBuffer, TOTAl_BUFFER_SIZE)
    eqService.set_send_buffer(sendBuffer, TOTAl_BUFFER_SIZE)

    cqService.start()
    eqService.start(1)

    eqService.join()
    cqService.shutdown()
    cqService.join()
  }

  def setRecvHandler(handler: Handler): Unit = {
    eqService.setRecvCallback(handler)
  }

  def getRdmaBuffer(index: Int): ByteBuffer = {
    val start = index * SINGLE_BUFFER_SIZE
    val end = (index + 1) * SINGLE_BUFFER_SIZE
    recvBuffer.position(start)
    recvBuffer.limit(end)
    recvBuffer.slice
  }

  def stop(): Unit = {

  }
}

class ServerRecvHandler(server: RDMAServer, appid: String, serializer: Serializer, blockManager: BlockDataManager) extends Handler {
  override def handle(con: Connection, rdmaBufferId: Int, bufferSize: Int, blockId: Int, seq: Long): Unit = {
    val buffer: ByteBuffer = server.getRdmaBuffer(rdmaBufferId)
    buffer.position(0)
    buffer.limit(bufferSize)
    val rpcMessage: ByteBuffer = buffer.slice()
    val message = BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    val openBlocks = message.asInstanceOf[OpenBlocks]
    val blocksNum = openBlocks.blockIds.length
    for (i <- (0 until blocksNum).view) {
      val nioBuffer: ByteBuffer = blockManager.getBlockData(BlockId.apply(openBlocks.blockIds(i))).nioByteBuffer()
      // TODO: need to specify the index of rdma buffer
      /**
        * con.write(buffer, bufferSize, rdmaBufferId, blockId)
        */
      con.send(nioBuffer, nioBuffer.remaining(), 1, i, seq)
    }
  }
}
