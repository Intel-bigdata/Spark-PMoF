package org.apache.spark.network.pmof

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import com.intel.hpnl.core._
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.client.ChunkReceivedCallback

class RDMAClient(address: String, port: Int) {
  val eqService = new EqService(address, port.toString, false)
  val cqService = new CqService(eqService, 1, eqService.getNativeHandle)
  val connectHandler = new ClientConnectHandler(this)
  val recvHandler = new ClientRecvHandler(this)

  val outstandingFetches:ConcurrentHashMap[Int, ChunkReceivedCallback] = new ConcurrentHashMap[Int, ChunkReceivedCallback]()

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

  def send(byteBuffer: ByteBuffer, seq: Int, callback: ChunkReceivedCallback): Unit = {
    assert(con != null)
    outstandingFetches.put(seq, callback)
    val sendBuffer = this.con.getSendBuffer
    sendBuffer.put(byteBuffer, 0, seq)
    con.send(sendBuffer.getByteBuffer.remaining(), sendBuffer.getRdmaBufferId)
  }

  def getOutStandingFetches(seq: Int): ChunkReceivedCallback = {
    outstandingFetches.get(seq)
  }
}

class ClientConnectHandler(client: RDMAClient) extends Handler {
  override def handle(connection: Connection, rdmaBufferId: Int, bufferBufferSize: Int): Unit = {
    client.setCon(connection)
  }
}

class ClientRecvHandler(client: RDMAClient) extends Handler {
  override def handle(con: Connection, rdmaBufferId: Int, blockBufferSize: Int): Unit = {
    val buffer: Buffer = con.getRecvBuffer(rdmaBufferId)
    val rpcMessage: ByteBuffer = buffer.get(blockBufferSize)
    val seq = buffer.getSeq
    val callback = client.getOutStandingFetches(seq)
    val managedBuffer = new NioManagedBuffer(rpcMessage)
    callback.onSuccess(buffer.getBlockBufferId, managedBuffer)
  }
}
