package org.apache.spark.network.pmof

import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingDeque}

import com.intel.hpnl.core._

class RdmaClient(address: String, port: Int) {
  val eqService = new EqService(address, port.toString, false)
  val cqService = new CqService(eqService, 1, eqService.getNativeHandle)
  val connectHandler = new ClientConnectHandler(this)
  val recvHandler = new ClientRecvHandler(this)

  val outstandingFetches: ConcurrentHashMap[Int, ReceivedCallback] = new ConcurrentHashMap[Int, ReceivedCallback]()
  val seqToBlockIndex: ConcurrentHashMap[Int, Int] = new ConcurrentHashMap[Int, Int]()

  final val SINGLE_BUFFER_SIZE: Int = RdmaTransferService.CHUNKSIZE
  final val BUFFER_NUM: Int = 16
  private var con: Connection = _

  private val deferredReqList = new LinkedBlockingDeque[ClientDeferredReq]()

  def init(): Unit = {
    for (i <- 0 until BUFFER_NUM) {
      val sendBuffer = ByteBuffer.allocateDirect(SINGLE_BUFFER_SIZE)
      eqService.setSendBuffer(sendBuffer, SINGLE_BUFFER_SIZE, i)
    }
    for (i <- 0 until BUFFER_NUM*2) {
      val recvBuffer = ByteBuffer.allocateDirect(SINGLE_BUFFER_SIZE)
      eqService.setRecvBuffer(recvBuffer, SINGLE_BUFFER_SIZE, i)
    }
    cqService.addExternalEvent(new ExternalHandler {
      override def handle(): Unit = {
        handleDeferredReq()
      }
    })
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

  def handleDeferredReq(): Unit = {
    if (!deferredReqList.isEmpty) {
      val deferredReq = deferredReqList.pollFirst()
      val byteBuffer = deferredReq.byteBuffer
      val seq = deferredReq.seq
      val blockIndex = deferredReq.blockIndex
      val msgType = deferredReq.msgType
      val callback = deferredReq.callback
      send(byteBuffer, seq, blockIndex, msgType, callback, isDeferred = true)
    }
  }

  def send(byteBuffer: ByteBuffer, seq: Int, blockIndex: Int, msgType: Byte,
           callback: ReceivedCallback, isDeferred: Boolean): Unit = {
    assert(con != null)
    outstandingFetches.putIfAbsent(seq, callback)
    seqToBlockIndex.putIfAbsent(seq, blockIndex)
    val sendBuffer = this.con.getSendBuffer(false)
    if (sendBuffer == null) {
      if (isDeferred) {
        deferredReqList.addFirst(new ClientDeferredReq(byteBuffer, seq, blockIndex, msgType, callback))
      } else {
        deferredReqList.addLast(new ClientDeferredReq(byteBuffer, seq, blockIndex, msgType, callback))
      }
      return
    }
    sendBuffer.put(byteBuffer, msgType, 0, seq)
    con.send(sendBuffer.remaining(), sendBuffer.getRdmaBufferId)
  }

  def getOutStandingFetches(seq: Int): ReceivedCallback = {
    outstandingFetches.get(seq)
  }

  def getSeqToBlockIndex(seq: Int): Int = {
    seqToBlockIndex.get(seq)
  }
}

class ClientConnectHandler(client: RdmaClient) extends Handler {
  override def handle(connection: Connection, rdmaBufferId: Int, bufferBufferSize: Int): Unit = {
    client.setCon(connection)
  }
}

class ClientRecvHandler(client: RdmaClient) extends Handler {
  override def handle(con: Connection, rdmaBufferId: Int, blockBufferSize: Int): Unit = {
    val buffer: Buffer = con.getRecvBuffer(rdmaBufferId)
    val rpcMessage: ByteBuffer = buffer.get(blockBufferSize)
    val seq = buffer.getSeq
    val msgType = buffer.getType
    val chunkIndex = buffer.getBlockBufferId
    val callback = client.getOutStandingFetches(seq)
    callback.onSuccess(client.getSeqToBlockIndex(seq), chunkIndex, msgType, rpcMessage)
  }
}

class ClientDeferredReq(var byteBuffer: ByteBuffer, var seq: Int, var blockIndex: Int,
                        var msgType: Byte, var callback: ReceivedCallback) {}
