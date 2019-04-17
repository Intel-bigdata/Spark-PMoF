package org.apache.spark.network.pmof

import java.nio.ByteBuffer
import java.util.Random
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.shuffle.{BlockFetchingListener, TempFileManager}
import org.apache.spark.shuffle.pmof.{MetadataResolver, PmofShuffleManager}
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleBlockId}
import org.apache.spark.{SparkConf, SparkEnv}

import scala.collection.mutable

class PmofTransferService(conf: SparkConf, val shuffleManager: PmofShuffleManager,
                          val hostname: String, var port: Int) extends TransferService {
  final var server: Server = _
  final private var clientFactory: ClientFactory = _
  private var nextReqId: AtomicLong = _
  final val metadataResolver: MetadataResolver = this.shuffleManager.metadataResolver

  override def fetchBlocks(host: String,
                           port: Int,
                           executId: String,
                           blockIds: Array[String],
                           blockFetchingListener: BlockFetchingListener,
                           tempFileManager: TempFileManager): Unit = {}

  def fetchBlock(reqHost: String, reqPort: Int, rmaAddress: Long, rmaLength: Int,
                 rmaRkey: Long, localAddress: Int, shuffleBuffer: ShuffleBuffer,
                 client: Client, callback: ReadCallback): Unit = {
    client.read(shuffleBuffer, rmaLength, rmaAddress, rmaRkey, localAddress, callback)
  }

  def fetchBlockInfo(blockIds: Array[BlockId], receivedCallback: ReceivedCallback): Unit = {
    val shuffleBlockIds = blockIds.map(blockId=>blockId.asInstanceOf[ShuffleBlockId])
    metadataResolver.fetchBlockInfo(shuffleBlockIds, receivedCallback)
  }

  def syncBlocksInfo(host: String, port: Int, byteBuffer: ByteBuffer, msgType: Byte,
                     callback: ReceivedCallback): Unit = {
    clientFactory.createClient(shuffleManager, host, port).
      send(byteBuffer, nextReqId.getAndIncrement(), msgType, callback, isDeferred = false)
  }

  def getClient(reqHost: String, reqPort: Int): Client = {
    clientFactory.createClient(shuffleManager, reqHost, reqPort)
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

  def init(): Unit = {
    this.server = new Server(conf, shuffleManager, hostname, port)
    this.clientFactory = new ClientFactory(conf)
    this.server.init()
    this.server.start()
    this.clientFactory.init()
    this.port = server.port
    val random = new Random().nextInt(Integer.MAX_VALUE)
    this.nextReqId = new AtomicLong(random)
  }

  override def init(blockDataManager: BlockDataManager): Unit = {}
}

object PmofTransferService {
  final val env: SparkEnv = SparkEnv.get
  final val conf: SparkConf = env.conf
  final val CHUNKSIZE: Int = conf.getInt("spark.shuffle.pmof.chunk_size", 4096*3)
  final val driverHost: String = conf.get("spark.driver.rhost", defaultValue = "172.168.0.43")
  final val driverPort: Int = conf.getInt("spark.driver.rport", defaultValue = 61000)
  final val shuffleNodes: Array[Array[String]] =
    conf.get("spark.shuffle.pmof.node", defaultValue = "").split(",").map(_.split("-"))
  final val shuffleNodesMap: mutable.Map[String, String] = new mutable.HashMap[String, String]()
  for (array <- shuffleNodes) {
    shuffleNodesMap.put(array(0), array(1))
  }
  private val initialized = new AtomicBoolean(false)
  private var transferService: PmofTransferService = _
  def getTransferServiceInstance(blockManager: BlockManager, shuffleManager: PmofShuffleManager = null,
                                 isDriver: Boolean = false): PmofTransferService = {
    if (!initialized.get()) {
      PmofTransferService.this.synchronized {
        if (initialized.get()) return transferService
        if (isDriver) {
          transferService =
            new PmofTransferService(conf, shuffleManager, driverHost, driverPort)
        } else {
          transferService =
            new PmofTransferService(conf, shuffleManager, shuffleNodesMap(blockManager.shuffleServerId.host), 0)
        }
        transferService.init()
        initialized.set(true)
        transferService
      }
    } else {
      transferService
    }
  }
}
