package org.apache.spark.network.pmof

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import com.intel.hpnl.core._
import org.apache.spark.SparkConf
import org.apache.spark.shuffle.pmof.PmofShuffleManager

import scala.collection.mutable.ArrayBuffer

class ClientFactory(conf: SparkConf) {
  final val SINGLE_BUFFER_SIZE: Int = PmofTransferService.CHUNKSIZE
  final val BUFFER_NUM: Int = conf.getInt("spark.shuffle.pmof.client_buffer_nums", 16)
  final val workers = conf.getInt("spark.shuffle.pmof.server_pool_size", 1)

  final val eqService = new EqService(workers, BUFFER_NUM, false).init()
  final val cqService = new CqService(eqService).init()

  final val conArray: ArrayBuffer[Connection] = ArrayBuffer()
  final val clientMap = new ConcurrentHashMap[InetSocketAddress, Client]()
  final val conMap = new ConcurrentHashMap[Connection, Client]()

  def init(): Unit = {
    eqService.initBufferPool(BUFFER_NUM, SINGLE_BUFFER_SIZE, BUFFER_NUM * 2)
    val clientRecvHandler = new ClientRecvHandler
    val clientReadHandler = new ClientReadHandler
    eqService.setRecvCallback(clientRecvHandler)
    eqService.setReadCallback(clientReadHandler)
    cqService.start()
  }

  def createClient(shuffleManager: PmofShuffleManager, address: String, port: Int): Client = {
    val socketAddress: InetSocketAddress = InetSocketAddress.createUnresolved(address, port)
    var client = clientMap.get(socketAddress)
    if (client == null) {
      ClientFactory.this.synchronized {
        client = clientMap.get(socketAddress)
        if (client == null) {
          val con = eqService.connect(address, port.toString, 0)
          client = new Client(this, shuffleManager, con)
          clientMap.put(socketAddress, client)
          conMap.put(con, client)
        }
      }
    }
    client
  }

  def stop(): Unit = {
    cqService.shutdown()
  }

  def waitToStop(): Unit = {
    cqService.join()
    eqService.shutdown()
    eqService.join()
  }

  def getEqService: EqService = eqService

  class ClientRecvHandler() extends Handler {
    override def handle(con: Connection, rdmaBufferId: Int, blockBufferSize: Int): Unit = {
      val buffer: HpnlBuffer = con.getRecvBuffer(rdmaBufferId)
      val rpcMessage: ByteBuffer = buffer.get(blockBufferSize)
      val seq = buffer.getSeq
      val msgType = buffer.getType
      val callback = conMap.get(con).outstandingReceiveFetches.get(seq)
      if (msgType == 0.toByte) {
        callback.onSuccess(null)
      } else {
        val metadataResolver = conMap.get(con).shuffleManager.metadataResolver
        val blockInfoArray = metadataResolver.deserializeShuffleBlockInfo(rpcMessage)
        callback.onSuccess(blockInfoArray)
      }
    }
  }

  class ClientReadHandler() extends Handler {
    override def handle(con: Connection, rdmaBufferId: Int, blockBufferSize: Int): Unit = {
      def fun(v1: Int): Unit = {
        conMap.get(con).shuffleBufferMap.remove(v1)
        conMap.get(con).outstandingReadFetches.remove(v1)
      }

      val callback = conMap.get(con).outstandingReadFetches.get(rdmaBufferId)
      val shuffleBuffer = conMap.get(con).shuffleBufferMap.get(rdmaBufferId)
      callback.onSuccess(shuffleBuffer, fun)
    }
  }
}
