package org.apache.spark.network.pmof

import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingDeque}

import com.intel.hpnl.core._
import org.apache.spark.SparkConf

class RdmaClient(conf: SparkConf, address: String, port: Int) {
  final val eqService = new EqService(address, port.toString, false)
  final val cqService = new CqService(eqService, 1, eqService.getNativeHandle)
  final val connectHandler = new ClientConnectHandler(this)
  final val recvHandler = new ClientRecvHandler(this)
  final val readHandler = new ClientReadHandler(this)

  val outstandingReceiveFetches: ConcurrentHashMap[Int, ReceivedCallback] = new ConcurrentHashMap[Int, ReceivedCallback]()
  val outstandingReadFetches: ConcurrentHashMap[Int, (Int, ReadCallback, Int)] = new ConcurrentHashMap[Int, (Int, ReadCallback, Int)]()

  val shuffleBufferMap: ConcurrentHashMap[Int, ShuffleBuffer] = new ConcurrentHashMap[Int, ShuffleBuffer]()
  val seqToAddress: ConcurrentHashMap[Int, Long] = new ConcurrentHashMap[Int, Long]()

  final val SINGLE_BUFFER_SIZE: Int = RdmaTransferService.CHUNKSIZE
  final val BUFFER_NUM: Int = conf.getInt("spark.shuffle.pmof.client_buffer_nums", 16)
  private var con: Connection = _

  private val deferredReqList = new LinkedBlockingDeque[ClientDeferredReq]()
  private val deferredReadList = new LinkedBlockingDeque[ClientDeferredRead]()
  private val deferredCloseList = new LinkedBlockingDeque[ClientDeferredCloseReq]()

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
      read(deferredRead.shuffleBuffer, deferredRead.blockIndex, deferredRead.seq, deferredRead.reqSize, deferredRead.rmaAddress, deferredRead.rmaRkey, null, isDeferred = true)
    }
  }

  def handleDeferredCloseReq(): Unit = {
    if (!deferredCloseList.isEmpty) {
      val deferredCloseReq = deferredCloseList.pollFirst()
      close(deferredCloseReq.rmaAddress)
    }
  }

  def read(shuffleBuffer: ShuffleBuffer, blockIndex: Int,
           seq: Int, reqSize: Int, rmaAddress: Long, rmaRkey: Long,
           callback: ReadCallback, isDeferred: Boolean = false): Unit = {
    if (!isDeferred) {
      outstandingReadFetches.putIfAbsent(shuffleBuffer.getRdmaBufferId, (blockIndex, callback, seq))
      shuffleBufferMap.putIfAbsent(shuffleBuffer.getRdmaBufferId, shuffleBuffer)
    }
    val ret = con.read(shuffleBuffer.getRdmaBufferId, 0, reqSize, rmaAddress, rmaRkey)
    if (ret == -11) {
      if (isDeferred)
        deferredReadList.addFirst(new ClientDeferredRead(shuffleBuffer, blockIndex, seq, reqSize, rmaAddress, rmaRkey))
      else
        deferredReadList.addLast(new ClientDeferredRead(shuffleBuffer, blockIndex, seq, reqSize, rmaAddress, rmaRkey))
    } else {
      seqToAddress.put(seq, rmaAddress)
    }
  }

  def send(byteBuffer: ByteBuffer, seq: Int, blockIndex: Int,
           callback: ReceivedCallback, isDeferred: Boolean): Unit = {
    assert(con != null)
    outstandingReceiveFetches.putIfAbsent(seq, callback)
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

  def close(rmaAddress: Long): Unit = {
    val sendBuffer = this.con.getSendBuffer(false)
    if (sendBuffer == null) {
      deferredCloseList.addLast(new ClientDeferredCloseReq(rmaAddress))
      return
    }
    val byteBufferTmp = ByteBuffer.allocate(4096)
    byteBufferTmp.putLong(rmaAddress)
    byteBufferTmp.flip()
    sendBuffer.put(byteBufferTmp, 1, 0, 0)
    con.send(sendBuffer.remaining(), sendBuffer.getRdmaBufferId)
  }

  def getEqService: EqService = eqService
}

class ClientConnectHandler(client: RdmaClient) extends Handler {
  override def handle(connection: Connection, rdmaBufferId: Int, bufferBufferSize: Int): Unit = {
    client.setCon(connection)
  }
}

class ClientRecvHandler(client: RdmaClient) extends Handler {
  override def handle(con: Connection, rdmaBufferId: Int, blockBufferSize: Int): Unit = {
    val buffer: RdmaBuffer = con.getRecvBuffer(rdmaBufferId)
    val rpcMessage: ByteBuffer = buffer.get(blockBufferSize)
    val seq = buffer.getSeq
    val callback = client.outstandingReceiveFetches.get(seq)
    callback.onSuccess(0, rpcMessage)
    client.outstandingReceiveFetches.remove(seq)
  }
}

class ClientReadHandler(client: RdmaClient) extends Handler {
  override def handle(con: Connection, rdmaBufferId: Int, blockBufferSize: Int): Unit = {
    val blockIndex = client.outstandingReadFetches.get(rdmaBufferId)._1
    val callback = client.outstandingReadFetches.get(rdmaBufferId)._2
    val seq = client.outstandingReadFetches.get(rdmaBufferId)._3
    val shuffleBuffer = client.shuffleBufferMap.get(rdmaBufferId)
    callback.onSuccess(blockIndex, shuffleBuffer)
    client.shuffleBufferMap.remove(rdmaBufferId)
    client.outstandingReadFetches.remove(rdmaBufferId)
    client.close(client.seqToAddress.get(seq))
  }
}

class ClientDeferredReq(val byteBuffer: ByteBuffer, val seq: Int, val blockIndex: Int,
                        val callback: ReceivedCallback) {}

class ClientDeferredRead(val shuffleBuffer: ShuffleBuffer, val blockIndex: Int, val seq: Int, val reqSize: Int, val rmaAddress: Long, val rmaRkey: Long) {}

class ClientDeferredCloseReq(val rmaAddress: Long) {}
