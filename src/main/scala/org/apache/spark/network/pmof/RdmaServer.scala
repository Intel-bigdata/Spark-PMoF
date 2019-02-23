package org.apache.spark.network.pmof

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.LinkedBlockingDeque

import org.apache.spark.serializer.Serializer
import com.intel.hpnl.core._
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.pmof.PmofShuffleManager

class RdmaServer(conf: SparkConf, val shuffleManager: PmofShuffleManager, address: String, var port: Int, supportRma: Boolean) {
  if (port == 0) {
    port = Utils.getPort
  }
  var SINGLE_BUFFER_SIZE: Int = _
  var BUFFER_NUM: Int = _

  if (supportRma) {
    SINGLE_BUFFER_SIZE = 0
    BUFFER_NUM = 0
  } else {
    SINGLE_BUFFER_SIZE = RdmaTransferService.CHUNKSIZE
    BUFFER_NUM = conf.getInt("spark.shuffle.pmof.server_buffer_nums", 256)
  }

  final val workers = conf.getInt("spark.shuffle.pmof.server_pool_size", 1)

  final val eqService = new EqService(address, port.toString, workers, BUFFER_NUM, true).init()
  final val cqService = new CqService(eqService, eqService.getNativeHandle).init()

  val conList = new util.ArrayList[Connection]()

  def init(): Unit = {
    eqService.initBufferPool(BUFFER_NUM, SINGLE_BUFFER_SIZE, BUFFER_NUM*2)
  }

  def start(): Unit = {
    eqService.start()
    cqService.start()
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
    cqService.addExternalEvent(new ExternalHandler {
      override def handle(): Unit = {
        handler.asInstanceOf[ServerRecvHandler] handleDeferredReq()
      }
    })
  }

  def setConnectHandler(handler: Handler): Unit = {
    eqService.setConnectedCallback(handler)
  }

  def getEqService: EqService = {
    eqService
  }

  def addCon(con: Connection): Unit = synchronized {
    conList.add(con)
  }

  def getConSize: Int = synchronized {
    conList.size()
  }
}

class ServerRecvHandler(server: RdmaServer, appid: String, serializer: Serializer) extends Handler with Logging {

  private final val deferredBufferList = new LinkedBlockingDeque[ServerDeferredReq]()
  private final val byteBufferTmp = ByteBuffer.allocate(4)

  def sendMetadata(con: Connection, byteBuffer: ByteBuffer, msgType: Byte, seq: Long, isDeferred: Boolean): Unit = {
    val sendBuffer = con.takeSendBuffer(false)
    if (sendBuffer == null) {
      if (isDeferred) {
        deferredBufferList.addFirst(new ServerDeferredReq(con, byteBuffer, msgType, seq))
      } else {
        deferredBufferList.addLast(new ServerDeferredReq(con, byteBuffer, msgType, seq))
      }
      return
    }
    sendBuffer.put(byteBuffer, msgType, seq)
    con.send(sendBuffer.remaining(), sendBuffer.getRdmaBufferId)
  }

  def handleDeferredReq(): Unit = {
    val deferredReq = deferredBufferList.pollFirst
    if (deferredReq == null) return
    val con = deferredReq.con
    val byteBuffer = deferredReq.byteBuffer
    val msgType = deferredReq.msgType
    val seq = deferredReq.seq
    sendMetadata(con, byteBuffer, msgType, seq, isDeferred = true)
  }

  override def handle(con: Connection, rdmaBufferId: Int, blockBufferSize: Int): Unit = synchronized {
    val buffer: RdmaBuffer = con.getRecvBuffer(rdmaBufferId)
    val message: ByteBuffer = buffer.get(blockBufferSize)
    val seq = buffer.getSeq
    val msgType = buffer.getType
    val metadataResolver = server.shuffleManager.metadataResolver
    if (msgType == 0.toByte) {
      metadataResolver.addShuffleBlockInfo(message)
      sendMetadata(con, byteBufferTmp, 0.toByte, seq, isDeferred = false)
    } else {
      val outputBuffer = metadataResolver.serializeShuffleBlockInfo(message)
      sendMetadata(con, outputBuffer, 1.toByte, seq, isDeferred = false)
    }
  }
}

class ServerConnectHandler(server: RdmaServer) extends Handler {
  override def handle(con: Connection, rdmaBufferId: Int, bufferBufferSize: Int): Unit = {
    server.addCon(con)
  }
}

class ServerDeferredReq(val con: Connection, val byteBuffer: ByteBuffer, val msgType: Byte, val seq: Long) {}
