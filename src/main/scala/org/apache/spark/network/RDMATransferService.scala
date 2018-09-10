package org.apache.spark.network

import java.util.concurrent.atomic.AtomicInteger
import java.util.Random

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.config.DRIVER_BIND_ADDRESS
import org.apache.spark.network.client.ChunkReceivedCallback
import org.apache.spark.network.shuffle.protocol.OpenBlocks
import org.apache.spark.network.shuffle.{BlockFetchingListener, TempFileManager}
import org.apache.spark.serializer.JavaSerializer

class RDMATransferService(conf: SparkConf, val hostname: String, override val port: Int) extends TransferService {

  private var server: RDMAServer = _
  private var recvHandler: ServerRecvHandler = _
  private var clientFactory: RDMAClientFactory = _
  private var appId: String = _
  private var blockDataManager: BlockDataManager = _
  private var nextReqId: AtomicInteger = _
  private var initialized = 0

  private val serializer = new JavaSerializer(conf)

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
                  callback: ChunkReceivedCallback,
                  tempFileManager: TempFileManager): Unit = {
    val client = clientFactory.createClient(reqHost, reqPort)
    val openBlocks: OpenBlocks = new OpenBlocks(appId, execId, blockIds)
    client.send(openBlocks.toByteBuffer, nextReqId.getAndIncrement(), callback)
  }

  override def close(): Unit = {
    if (clientFactory != null) {
      clientFactory.stop()
    }
    if (server != null) {
      server.stop()
    }
  }

  override def init(blockDataManager: BlockDataManager): Unit = {
    if (initialized == 1) {
      return
    } else {
      initialized = 1
    }
    this.server = new RDMAServer(hostname, port)
    this.appId = conf.getAppId
    this.blockDataManager = blockDataManager
    this.recvHandler = new ServerRecvHandler(server, appId, serializer, blockDataManager)
    this.server.setRecvHandler(recvHandler)
    this.clientFactory = new RDMAClientFactory()
    this.server.init()
    this.server.start()
    val random = new Random().nextInt(Integer.MAX_VALUE)
    this.nextReqId = new AtomicInteger(random)
  }
}

object RDMATransferService {
  val env: SparkEnv = SparkEnv.get
  val conf: SparkConf = env.conf
  val bindAddress: String = conf.get(DRIVER_BIND_ADDRESS)
  val port: Int = conf.get("spark.driver.port").toInt
  private val transferService = new RDMATransferService(conf, bindAddress, port)
  def getTransferServiceInstance(blockDataManager: BlockDataManager): RDMATransferService = {
    transferService.init(blockDataManager)
    transferService
  }
}

