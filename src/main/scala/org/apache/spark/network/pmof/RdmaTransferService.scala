package org.apache.spark.network.pmof

import java.util.Random
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.shuffle.protocol.OpenBlocks
import org.apache.spark.network.shuffle.{BlockFetchingListener, TempFileManager}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.BlockManager
import org.apache.spark.{SparkConf, SparkEnv}
import com.intel.hpnl.core.Buffer

class RdmaTransferService(conf: SparkConf, val hostname: String, var port: Int) extends TransferService {

  private var server: RdmaServer = _
  private var recvHandler: ServerRecvHandler = _
  private var clientFactory: RdmaClientFactory = _
  private var appId: String = _
  private var nextReqId: AtomicInteger = _

  private val serializer = new JavaSerializer(conf)

  override def fetchBlocks(host: String,
                           port: Int,
                           executId: String,
                           blockIds: Array[String],
                           blockFetchingListener: BlockFetchingListener,
                           tempFileManager: TempFileManager): Unit = {}

  def fetchMetadata(reqHost: String,
                          reqPort: Int,
                          execId: String,
                          blockIds: Array[String],
                          callback: ReceivedCallback,
                          client: RdmaClient = null): Unit = {
    val openBlocks: OpenBlocks = new OpenBlocks(appId, execId, blockIds)
    if (client == null) {
      clientFactory.createClient(reqHost, reqPort).send(openBlocks.toByteBuffer, nextReqId.getAndIncrement(), 0, callback, isDeferred = false)
    } else {
      client.send(openBlocks.toByteBuffer, nextReqId.getAndIncrement(), 0, callback, isDeferred = false)
    }
  }

  def fetchBlock(reqHost: String,
                 reqPort: Int,
                 execId: String,
                 blockId: String,
                 blockIndex: Int,
                 rmaBuffer: Buffer,
                 reqBufSize: Int,
                 rmaAddress: Long,
                 rmaRkey: Long,
                 callback: ReceivedCallback,
                 client: RdmaClient = null): Unit = {
    val seq = nextReqId.getAndIncrement()
    if (client == null) {
      clientFactory.createClient(reqHost, reqPort).read(rmaBuffer, blockIndex, seq, reqBufSize, rmaAddress, rmaRkey, callback)
    } else {
      client.read(rmaBuffer, blockIndex, seq, reqBufSize, rmaAddress, rmaRkey, callback)
    }
  }

  def getRdmaClient(reqHost: String, reqPort: Int): RdmaClient = {
    clientFactory.createClient(reqHost, reqPort)
  }

  def getRmaBuffer(reqHost: String, reqPort: Int, bufferSize: Int, client: RdmaClient = null): Buffer = {
    if (client == null) {
      clientFactory.createClient(reqHost, reqPort).getRmaBuffer(bufferSize)
    } else {
      client.getRmaBuffer(bufferSize)
    }
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

  override def init(blockManager: BlockDataManager): Unit = {
    this.server = new RdmaServer(hostname, port)
    this.appId = conf.getAppId
    this.recvHandler = new ServerRecvHandler(server, appId, serializer, blockManager)
    this.server.setRecvHandler(recvHandler)
    this.clientFactory = new RdmaClientFactory()
    this.server.init()
    this.server.start()
    this.port = server.port
    val random = new Random().nextInt(Integer.MAX_VALUE)
    this.nextReqId = new AtomicInteger(random)
  }
}

object RdmaTransferService {
  val env: SparkEnv = SparkEnv.get
  val conf: SparkConf = env.conf
  val CHUNKSIZE: Int = 65536
  private var initialized = 0
  private var transferService: RdmaTransferService = _
  def getTransferServiceInstance(blockManager: BlockManager): RdmaTransferService = synchronized {
    if (initialized == 0) {
      transferService = new RdmaTransferService(conf, blockManager.shuffleServerId.host, 0)
      transferService.init(blockManager)
      initialized = 1
      transferService
    } else {
      transferService
    }
  }
}
