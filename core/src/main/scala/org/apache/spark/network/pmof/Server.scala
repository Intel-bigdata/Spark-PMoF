package org.apache.spark.network.pmof

import java.nio.ByteBuffer
import java.util

import com.intel.hpnl.core._
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.pmof.PmofShuffleManager

class Server(conf: SparkConf, val shuffleManager: PmofShuffleManager, address: String, var port: Int) {
  if (port == 0) {
    port = Utils.getPort
  }
  final val SINGLE_BUFFER_SIZE: Int = PmofTransferService.CHUNKSIZE
  final val BUFFER_NUM: Int = conf.getInt("spark.shuffle.pmof.server_buffer_nums", 256)
  final val workers = conf.getInt("spark.shuffle.pmof.server_pool_size", 1)

  final val eqService = new EqService(workers, BUFFER_NUM, true).init()
  final val cqService = new CqService(eqService).init()

  val conList = new util.ArrayList[Connection]()

  def init(): Unit = {
    eqService.initBufferPool(BUFFER_NUM, SINGLE_BUFFER_SIZE, BUFFER_NUM * 2)
    val recvHandler = new ServerRecvHandler(this)
    val connectedHandler = new ServerConnectedHandler(this)
    eqService.setConnectedCallback(connectedHandler)
    eqService.setRecvCallback(recvHandler)
  }

  def start(): Unit = {
    cqService.start()
    eqService.listen(address, port.toString)
  }

  def stop(): Unit = {
    cqService.shutdown()
  }

  def waitToStop(): Unit = {
    cqService.join()
    eqService.shutdown()
    eqService.join()
  }

  def getEqService: EqService = {
    eqService
  }

  def addCon(con: Connection): Unit = synchronized {
    conList.add(con)
  }
}

class ServerRecvHandler(server: Server) extends Handler with Logging {

  private final val byteBufferTmp = ByteBuffer.allocate(4)

  def sendMetadata(con: Connection, byteBuffer: ByteBuffer, msgType: Byte, seq: Long, isDeferred: Boolean): Unit = {
    con.send(byteBuffer, msgType, seq)
  }

  override def handle(con: Connection, bufferId: Int, blockBufferSize: Int): Unit = synchronized {
    val buffer: HpnlBuffer = con.getRecvBuffer(bufferId)
    val message: ByteBuffer = buffer.get(blockBufferSize)
    val seq = buffer.getSeq
    val msgType = buffer.getType
    val metadataResolver = server.shuffleManager.metadataResolver
    if (msgType == 0.toByte) {
      metadataResolver.addShuffleBlockInfo(message)
      sendMetadata(con, byteBufferTmp, 0.toByte, seq, isDeferred = false)
    } else {
      val bufferArray = metadataResolver.serializeShuffleBlockInfo(message)
      for (buffer <- bufferArray) {
        sendMetadata(con, buffer, 1.toByte, seq, isDeferred = false)
      }
    }
  }
}

class ServerConnectedHandler(server: Server) extends Handler {
  override def handle(con: Connection, rdmaBufferId: Int, bufferBufferSize: Int): Unit = {
    server.addCon(con)
  }
}
