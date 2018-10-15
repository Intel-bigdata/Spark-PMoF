package org.apache.spark.network.pmof

import java.nio.ByteBuffer

import org.apache.spark.network.BlockDataManager

import com.intel.hpnl.core.EqService
import com.intel.hpnl.core.CqService
import com.intel.hpnl.core.Buffer
import com.intel.hpnl.core.Connection
import com.intel.hpnl.core.Handler
import com.intel.hpnl.core.Utils

class RdmaServer(address: String, var port: Int) {
  if (port == 0) {
    port = Utils.getPort
  }
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
    cqService.shutdown()
  }

  def waitToStop(): Unit = {
    cqService.join()
    eqService.shutdown()
    eqService.join()
  }

  def setRecvHandler(handler: Handler): Unit = {
    eqService.setRecvCallback(handler)
  }
}

class ServerRecvHandler(server: RdmaServer, appid: String,
                        blockManager: BlockDataManager, blockTracker: RdmaBlockTracker,
                        isDriver: Boolean) extends Handler {
  override def handle(con: Connection, rdmaBufferId: Int, blockBufferSize: Int): Unit = {
    val buffer: Buffer = con.getRecvBuffer(rdmaBufferId)
    val rpcMessage: ByteBuffer = buffer.get(blockBufferSize)

    val msgType = buffer.getType
    val seq = buffer.getSeq
    if (msgType == MessageType.REGISTER_BLOCK.id()) {
      blockTracker.asInstanceOf[RdmaBlockTrackerDriver].registerBlockStatus(rpcMessage)
    } else if (msgType == MessageType.FETCH_BLOCK_STATUS.id()) {
      val byteBuffer = blockTracker.asInstanceOf[RdmaBlockTrackerDriver].getBlockStatus
      val sendBuffer = con.getSendBuffer
      sendBuffer.put(byteBuffer, msgType, 0, seq)
      con.send(sendBuffer.remaining(), sendBuffer.getRdmaBufferId)
    } else {
    }
  }
}
