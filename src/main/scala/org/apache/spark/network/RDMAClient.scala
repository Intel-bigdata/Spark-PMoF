package org.apache.spark.network

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import com.intel.hpnl.core.{Connection, CqService, EqService, Handler}
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.client.ChunkReceivedCallback

class RDMAClient(address: String, port: String) {
  val eqService = new EqService(address, port, true)
  val cqService = new CqService(eqService, 1, eqService.getNativeHandle)
  val connectHandler = new ClientConnectHandler(this)
  val recvHandler = new ClientRecvHandler(this)

  val outstandingFetches:ConcurrentHashMap[Int, ChunkReceivedCallback] = new ConcurrentHashMap[Int, ChunkReceivedCallback]()

  final val SINGLE_BUFFER_SIZE: Int = 65536
  final val TOTAl_BUFFER_SIZE = SINGLE_BUFFER_SIZE*16
  private var recvBuffer: ByteBuffer = _
  private var sendBuffer: ByteBuffer = _
  private var con: Connection = _

  def init(): Unit = {
    recvBuffer = ByteBuffer.allocateDirect(TOTAl_BUFFER_SIZE)
    sendBuffer = ByteBuffer.allocateDirect(TOTAl_BUFFER_SIZE)
  }

  def start(): Unit = {
    eqService.setConnectedCallback(connectHandler)
    eqService.setRecvCallback(recvHandler)
    eqService.setSendCallback(null)
    eqService.setShutdownCallback(null)

    eqService.set_recv_buffer(recvBuffer, TOTAl_BUFFER_SIZE)
    eqService.set_send_buffer(sendBuffer, TOTAl_BUFFER_SIZE)

    cqService.start()
    eqService.start(1)
    eqService.waitToConnected()

    eqService.waitToStop()
    con.shutdown()
    eqService.join()
    cqService.shutdown()
    cqService.join()
  }

  def setCon(con: Connection): Unit = {
    this.con = con
  }

  def send(buffer: ByteBuffer, blockBufferSize: Int, rdmaBufferId: Int, blockBufferId: Int, seq: Long, callback: ChunkReceivedCallback): Unit = {
    assert(con != null)
    outstandingFetches.put(blockBufferId, callback)
    con.send(buffer, blockBufferSize, rdmaBufferId, blockBufferId, seq)
  }

  def getRdmaBuffer(index: Int): ByteBuffer = {
    val start = index * SINGLE_BUFFER_SIZE
    val end = (index + 1) * SINGLE_BUFFER_SIZE
    recvBuffer.position(start)
    recvBuffer.limit(end)
    recvBuffer.slice
  }

  def getOutStandingFetches(rdmaBufferId: Int): ChunkReceivedCallback = outstandingFetches.get(rdmaBufferId)
}

class ClientConnectHandler(client: RDMAClient) extends Handler {
  override def handle(connection: Connection, rdmaBufferId: Int, bufferSize: Int, blockId: Int, seq: Long): Unit = {
    client.setCon(connection)
  }
}

class ClientRecvHandler(client: RDMAClient) extends Handler {
  override def handle(connection: Connection, rdmaBufferId: Int, blockBufferSize: Int, blockBufferId: Int, seq: Long): Unit = {
    val buffer: ByteBuffer = client.getRdmaBuffer(rdmaBufferId)
    buffer.position(0)
    buffer.limit(blockBufferSize)
    val rpcMessage: ByteBuffer = buffer.slice()

    val callback = client.getOutStandingFetches(rdmaBufferId)
    val managedBuffer = new NioManagedBuffer(rpcMessage)
    callback.onSuccess(blockBufferId, managedBuffer)
  }
}
