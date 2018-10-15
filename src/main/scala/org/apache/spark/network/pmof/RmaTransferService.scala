package org.apache.spark.network.pmof

import java.nio.ByteBuffer
import java.util.Random
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.shuffle.protocol.OpenBlocks
import org.apache.spark.network.shuffle.{BlockFetchingListener, TempFileManager}
import org.apache.spark.SparkConf

class RmaTransferService(conf: SparkConf, val hostname: String, var port: Int, blockTracker: RdmaBlockTracker, isDriver: Boolean) extends TransferService {

  private var server: RdmaServer = _
  private var recvHandler: ServerRecvHandler = _
  private var clientFactory: RdmaClientFactory = _
  private var appId: String = _
  private var nextReqId: AtomicInteger = _

  override def fetchBlocks(host: String,
                           port: Int,
                           executId: String,
                           blockIds: Array[String],
                           blockFetchingListener: BlockFetchingListener,
                           tempFileManager: TempFileManager): Unit = {}

  def fetchBlocks(reqHost: String,
                  reqPort: Int,
                  execId: String,
                  blockIds: Array[String],
                  callback: BlockTrackerCallback,
                  tempFileManager: TempFileManager): Unit = {
    val client = clientFactory.createClient(reqHost, reqPort)
  }

  def registerBlockStatus(reqHost: String,
                     reqPort: Int,
                     byteBuffer: ByteBuffer): Unit = {
    val client = clientFactory.createClient(reqHost, reqPort)
    client.send(byteBuffer, MessageType.REGISTER_BLOCK, nextReqId.getAndIncrement(), null)
  }

  def fetchBlockStatus(reqHost: String,
                       reqPort: Int,
                       callback: BlockTrackerCallback): Unit = {
    val client = clientFactory.createClient(reqHost, reqPort)
    client.send(null, MessageType.FETCH_BLOCK_STATUS, nextReqId.getAndIncrement(), callback)
  }

  override def close(): Unit = {
    if (clientFactory != null) {
      clientFactory.stop()
      clientFactory.waitToStop()
    }
    if (server != null) {
      server.stop()
      server.waitToStop()
    }
  }

  def init(blockManager: BlockDataManager): Unit = {
    this.server = new RdmaServer(hostname, port)
    this.appId = conf.getAppId

    this.recvHandler = new ServerRecvHandler(server, appId, blockManager, blockTracker, isDriver)
    this.server.setRecvHandler(recvHandler)

    this.clientFactory = new RdmaClientFactory(blockTracker, isDriver)

    this.server.init()
    this.server.start()
    this.port = server.port
    val random = new Random().nextInt(Integer.MAX_VALUE)
    this.nextReqId = new AtomicInteger(random)
  }
}
