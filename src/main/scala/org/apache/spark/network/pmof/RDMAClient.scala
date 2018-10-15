package org.apache.spark.network.pmof

import java.nio.ByteBuffer
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap

import com.intel.hpnl.core._

class RdmaClient(address: String, port: Int, blockTracker: RdmaBlockTracker, isDriver: Boolean) {
  val eqService = new EqService(address, port.toString, false)
  val cqService = new CqService(eqService, 1, eqService.getNativeHandle)
  val connectHandler = new ClientConnectHandler(this)
  val recvHandler = new ClientRecvHandler(this, blockTracker, isDriver)

  val outstandingFetches:ConcurrentHashMap[Int, BlockTrackerCallback] = new ConcurrentHashMap[Int, BlockTrackerCallback]()

  final val SINGLE_BUFFER_SIZE: Int = 65536
  final val BUFFER_NUM: Int = 32
  private var con: Connection = _

  def init(): Unit = {
    for (i <- 0 to BUFFER_NUM) {
      val recvBuffer = ByteBuffer.allocateDirect(SINGLE_BUFFER_SIZE)
      val sendBuffer = ByteBuffer.allocateDirect(SINGLE_BUFFER_SIZE)
      eqService.setRecvBuffer(recvBuffer, SINGLE_BUFFER_SIZE, i)
      eqService.setSendBuffer(sendBuffer, SINGLE_BUFFER_SIZE, i)
    }
  }

  def start(): Unit = {
    eqService.setConnectedCallback(connectHandler)
    eqService.setRecvCallback(recvHandler)

    cqService.start()
    eqService.start(1)
    eqService.waitToConnected()
  }

  def stop(): Unit = {
    cqService.shutdown()
  }

  def waitToStop(): Unit = {
    cqService.join()
    eqService.shutdown()
    eqService.join()
  }

  def setCon(con: Connection): Unit = {
    this.con = con
  }

  def getCon: Connection = {
    assert(this.con != null)
    this.con
  }

  def send(byteBuffer: ByteBuffer, messageType: MessageType, seq: Int, callback: BlockTrackerCallback): Unit = {
    assert(con != null)
    outstandingFetches.put(seq, callback)
    val sendBuffer = this.con.getSendBuffer
    sendBuffer.put(byteBuffer, messageType.id(),  0, seq)
    con.send(sendBuffer.remaining(), sendBuffer.getRdmaBufferId)
  }

  def getOutStandingFetches(seq: Int): BlockTrackerCallback = {
    outstandingFetches.get(seq)
  }
}

class ClientConnectHandler(client: RdmaClient) extends Handler {
  override def handle(connection: Connection, rdmaBufferId: Int, bufferBufferSize: Int): Unit = {
    client.setCon(connection)
  }
}

class ClientRecvHandler(client: RdmaClient, blockTracker: RdmaBlockTracker, isDriver: Boolean) extends Handler {
  override def handle(con: Connection, rdmaBufferId: Int, blockBufferSize: Int): Unit = {
    val buffer: Buffer = con.getRecvBuffer(rdmaBufferId)
    val rpcMessage = buffer.get(blockBufferSize)
    val msgType = buffer.getType
    val seq = buffer.getSeq
    val callback = client.getOutStandingFetches(seq)
    if (msgType == MessageType.FETCH_BLOCK_STATUS.id()) {
      callback.onSuccess(0, rpcMessage)
    } else {
      callback.onFailure(0, new IOException())
    }
  }
}
