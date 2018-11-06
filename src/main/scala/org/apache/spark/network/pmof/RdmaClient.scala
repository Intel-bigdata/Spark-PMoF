package org.apache.spark.network.pmof

import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingDeque}

import com.intel.hpnl.core._

class RdmaClient(address: String, port: Int) {
  val eqService = new EqService(address, port.toString, false)
  val cqService = new CqService(eqService, 1, eqService.getNativeHandle)
  val connectHandler = new ClientConnectHandler(this)
  val recvHandler = new ClientRecvHandler(this)
  val readHandler = new ClientReadHandler(this)

  val outstandingFetches: ConcurrentHashMap[Int, ReceivedCallback] = new ConcurrentHashMap[Int, ReceivedCallback]()
  val seqToBlockIndex: ConcurrentHashMap[Int, Int] = new ConcurrentHashMap[Int, Int]()

  final val SINGLE_BUFFER_SIZE: Int = RdmaTransferService.CHUNKSIZE
  final val BUFFER_NUM: Int = 16
  private var con: Connection = _

  private val deferredReqList = new LinkedBlockingDeque[ClientDeferredReq]()
  private val deferredReadList = new LinkedBlockingDeque[ClientDeferredRead]()

  def init(): Unit = {
    for (i <- 0 until BUFFER_NUM) {
      val sendBuffer = ByteBuffer.allocateDirect(SINGLE_BUFFER_SIZE)
      eqService.setSendBuffer(sendBuffer, SINGLE_BUFFER_SIZE, i)
    }
    for (i <- 0 until BUFFER_NUM * 2) {
      val recvBuffer = ByteBuffer.allocateDirect(SINGLE_BUFFER_SIZE)
      eqService.setRecvBuffer(recvBuffer, SINGLE_BUFFER_SIZE, i)
    }
    cqService.addExternalEvent(new ExternalHandler {
      override def handle(): Unit = {
        handleDeferredReq()
        handleDeferredRead()
      }
    })
  }

  def start(): Unit = {
    eqService.setConnectedCallback(connectHandler)
    eqService.setRecvCallback(recvHandler)
    eqService.setReadCallback(readHandler)

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
      val callback = deferredReq.callback
      send(byteBuffer, seq, blockIndex, callback, isDeferred = true)
    }
  }

  def handleDeferredRead(): Unit = {
    if (!deferredReadList.isEmpty) {
      val deferredRead = deferredReadList.pollFirst()
      read(deferredRead.rmaBuffer, deferredRead.blockIndex, deferredRead.seq, deferredRead.reqBufSize, deferredRead.rmaAddress, deferredRead.rmaRkey, null, true)
    }
  }

  def read(rmaBuffer: Buffer, blockIndex: Int,
           seq: Int, reqBufSize: Int,
           rmaAddress: Long, rmaRkey: Long,
           callback: ReceivedCallback, isDeferred: Boolean = false): Unit = {
    if (!isDeferred) {
      outstandingFetches.putIfAbsent(seq, callback)
      rmaBuffer.getRawBuffer.putInt(blockIndex)
      rmaBuffer.getRawBuffer.putInt(seq)
      rmaBuffer.getRawBuffer.flip()
    }
    val ret = con.read(rmaBuffer.getRdmaBufferId, 8, reqBufSize, rmaAddress, rmaRkey)
    if (ret == -11) {
      if (isDeferred)
        deferredReadList.addFirst(new ClientDeferredRead(rmaBuffer, blockIndex, seq, reqBufSize, rmaAddress, rmaRkey))
      else
        deferredReadList.addLast(new ClientDeferredRead(rmaBuffer, blockIndex, seq, reqBufSize, rmaAddress, rmaRkey))
    }
  }

  def send(byteBuffer: ByteBuffer, seq: Int, blockIndex: Int,
           callback: ReceivedCallback, isDeferred: Boolean): Unit = {
    assert(con != null)
    outstandingFetches.putIfAbsent(seq, callback)
    val sendBuffer = this.con.getSendBuffer(false)
    if (sendBuffer == null) {
      if (isDeferred) {
        deferredReqList.addFirst(new ClientDeferredReq(byteBuffer, seq, blockIndex, callback))
      } else {
        deferredReqList.addLast(new ClientDeferredReq(byteBuffer, seq, blockIndex, callback))
      }
      return
    }
    sendBuffer.put(byteBuffer, 0, 0, seq)
    con.send(sendBuffer.remaining(), sendBuffer.getRdmaBufferId)
  }

  def getRmaBuffer(bufferSize: Int): Buffer = {
    eqService.getRmaBuffer(bufferSize)
  }



  def getOutStandingFetches(seq: Int): ReceivedCallback = {
    outstandingFetches.get(seq)
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
    val callback = client.getOutStandingFetches(seq)
    callback.onSuccess(0, 0, rpcMessage)
  }
}

class ClientReadHandler(client: RdmaClient) extends Handler {
  override def handle(con: Connection, rdmaBufferId: Int, blockBufferSize: Int): Unit = {
    val byteBuffer = con.getRmaBuffer(rdmaBufferId)
    val blockIndex = byteBuffer.getInt()
    val seq = byteBuffer.getInt()
    val callback = client.getOutStandingFetches(seq)
    callback.onSuccess(blockIndex, 1, byteBuffer)
  }
}

class ClientDeferredReq(var byteBuffer: ByteBuffer, var seq: Int, var blockIndex: Int,
                        var callback: ReceivedCallback) {}

class ClientDeferredRead(val rmaBuffer: Buffer, val blockIndex: Int, val seq: Int, val reqBufSize: Int,
                         val rmaAddress: Long, val rmaRkey: Long) {}

