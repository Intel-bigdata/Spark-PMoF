package org.apache.spark.network.pmof

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingDeque}

import com.intel.hpnl.core._
import org.apache.spark.SparkConf
import org.apache.spark.shuffle.pmof.PmofShuffleManager

class RdmaClient(conf: SparkConf, val shuffleManager: PmofShuffleManager, address: String, port: Int, supportRma: Boolean) {
  var SINGLE_BUFFER_SIZE: Int = _
  var BUFFER_NUM: Int = _
  if (supportRma) {
    SINGLE_BUFFER_SIZE = 0
    BUFFER_NUM = 0
  } else {
    SINGLE_BUFFER_SIZE = RdmaTransferService.CHUNKSIZE
    BUFFER_NUM = conf.getInt("spark.shuffle.pmof.client_buffer_nums", 16)
  }
  final val eqService = new EqService(address, port.toString, 1, BUFFER_NUM, false).init()
  final val cqService = new CqService(eqService, eqService.getNativeHandle).init()

  final val connectHandler = new ClientConnectHandler(this)
  final val recvHandler = new ClientRecvHandler(this)
  final val readHandler = new ClientReadHandler(this)
  final val started: AtomicBoolean = new AtomicBoolean(false)

  val outstandingReceiveFetches: ConcurrentHashMap[Long, ReceivedCallback] =
    new ConcurrentHashMap[Long, ReceivedCallback]()
  val outstandingReadFetches: ConcurrentHashMap[Int, ReadCallback] =
    new ConcurrentHashMap[Int, ReadCallback]()

  val shuffleBufferMap: ConcurrentHashMap[Int, ShuffleBuffer] = new ConcurrentHashMap[Int, ShuffleBuffer]()

  private var con: Connection = _

  private val deferredReqList = new LinkedBlockingDeque[ClientDeferredReq]()
  private val deferredReadList = new LinkedBlockingDeque[ClientDeferredRead]()

  def init(): Unit = {
    eqService.initBufferPool(BUFFER_NUM, SINGLE_BUFFER_SIZE, BUFFER_NUM*2)

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

    eqService.start()
    cqService.start()
    eqService.waitToConnected()

    started.set(true)
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
      val msgType = deferredReq.msgType
      val callback = deferredReq.callback
      send(byteBuffer, seq, msgType, callback, isDeferred = true)
    }
  }

  def handleDeferredRead(): Unit = {
    if (!deferredReadList.isEmpty) {
      val deferredRead = deferredReadList.pollFirst()
      read(deferredRead.shuffleBuffer, deferredRead.reqSize, deferredRead.rmaAddress, deferredRead.rmaRkey, deferredRead.localAddress, null, isDeferred = true)
    }
  }

  def read(shuffleBuffer: ShuffleBuffer, reqSize: Int,
           rmaAddress: Long, rmaRkey: Long, localAddress: Int,
           callback: ReadCallback, isDeferred: Boolean = false): Unit = {
    if (!isDeferred) {
      outstandingReadFetches.putIfAbsent(shuffleBuffer.getRdmaBufferId, callback)
      shuffleBufferMap.putIfAbsent(shuffleBuffer.getRdmaBufferId, shuffleBuffer)
    }
    val ret = con.read(shuffleBuffer.getRdmaBufferId, localAddress, reqSize, rmaAddress, rmaRkey)
    if (ret == -11) {
      if (isDeferred) {
        deferredReadList.addFirst(new ClientDeferredRead(shuffleBuffer, reqSize, rmaAddress, rmaRkey, localAddress))
      } else {
        deferredReadList.addLast(new ClientDeferredRead(shuffleBuffer, reqSize, rmaAddress, rmaRkey, localAddress))
      }
    }
  }

  def send(byteBuffer: ByteBuffer, seq: Long, msgType: Byte,
           callback: ReceivedCallback, isDeferred: Boolean): Unit = {
    assert(con != null)
    if (callback != null) {
      outstandingReceiveFetches.putIfAbsent(seq, callback)
    }
    val sendBuffer = this.con.takeSendBuffer(false)
    if (sendBuffer == null) {
      if (isDeferred) {
        deferredReqList.addFirst(new ClientDeferredReq(byteBuffer, seq, msgType, callback))
      } else {
        deferredReqList.addLast(new ClientDeferredReq(byteBuffer, seq, msgType, callback))
      }
      return
    }
    sendBuffer.put(byteBuffer, msgType, seq)
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
    val msgType = buffer.getType
    val callback = client.outstandingReceiveFetches.get(seq)
    if (msgType == 0.toByte) {
      callback.onSuccess(null)
    } else {
      val metadataResolver = client.shuffleManager.metadataResolver
      val blockInfoArray = metadataResolver.deserializeShuffleBlockInfo(rpcMessage)
      callback.onSuccess(blockInfoArray)
    }
  }
}

class ClientReadHandler(client: RdmaClient) extends Handler {
  def fun(v1: Int): Unit = {
    client.shuffleBufferMap.remove(v1)
    client.outstandingReadFetches.remove(v1)
  }
  override def handle(con: Connection, rdmaBufferId: Int, blockBufferSize: Int): Unit = {
    val callback = client.outstandingReadFetches.get(rdmaBufferId)
    val shuffleBuffer = client.shuffleBufferMap.get(rdmaBufferId)
    callback.onSuccess(shuffleBuffer, fun)
  }
}

class ClientDeferredReq(val byteBuffer: ByteBuffer, val seq: Long, val msgType: Byte,
                        val callback: ReceivedCallback) {}

class ClientDeferredRead(val shuffleBuffer: ShuffleBuffer, val reqSize: Int, val rmaAddress: Long, val rmaRkey: Long, val localAddress: Int) {}
