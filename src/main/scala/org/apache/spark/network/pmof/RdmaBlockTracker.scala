package org.apache.spark.network.pmof

import java.nio.ByteBuffer
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.internal.Logging

import org.apache.spark.network.shuffle.TempFileManager
import org.apache.spark.storage.BlockManager
import org.apache.spark.{SparkConf, SparkEnv}

private[spark] abstract class RdmaBlockTracker {
  var host: String = _
  var port: Int = _
  var enable_rdma: Boolean = true
  protected val env: SparkEnv = SparkEnv.get
  protected var conf: SparkConf = env.conf
  protected var blockManager: BlockManager = env.blockManager
  protected var transferService: RmaTransferService = _
  protected var blockStatusMsg: BlockStatusMessage = new BlockStatusMessage
  protected var blockStatusSerializer: BlockStatusSerializer = new BlockStatusSerializer

  def initialize(): Unit = {}
}

private[spark] class RdmaBlockTrackerDriver extends RdmaBlockTracker {
  transferService =
    new RmaTransferService(conf, blockManager.shuffleServerId.host, 0, this, true)

  host = transferService.hostname
  port = transferService.port

  initialize()

  override def initialize(): Unit = {
    transferService.init(blockManager)
  }

  def registerBlockStatus(byteBuffer: ByteBuffer): Unit = {
    val executorBlockStatusMsg = blockStatusSerializer.deserialize(byteBuffer)
    blockStatusMsg.enqueue(executorBlockStatusMsg)
  }

  def getBlockStatus: ByteBuffer = {
    val byteBuffer = blockStatusSerializer.serialize(blockStatusMsg)
    byteBuffer
  }
}

private[spark] class RdmaBlockTrackerExecutor extends RdmaBlockTracker {
  transferService =
    new RmaTransferService(conf, blockManager.shuffleServerId.host, 0, this, false)

  host = transferService.hostname
  port = transferService.port

  val globalStatusMsg: BlockStatusMessage = new BlockStatusMessage()

  initialize()

  override def initialize(): Unit = {
    transferService.init(blockManager)
  }

  def registerBlockStatus(blockStatusMsg: BlockStatusMessage): Unit = {
    assert(blockStatusMsg != null)
    val byteBuffer = blockStatusSerializer.serialize(blockStatusMsg)
    transferService.registerBlockStatus("", 2, byteBuffer)
  }

  def fetchBlockStatus(): Unit = {
    val blockStatusReceivedCallback = new BlockTrackerCallback {
      override def onSuccess(chunkIndex: Int, buffer: ManagedBuffer): Unit = {
        val blockStatusMessage = blockStatusSerializer.deserialize(buffer.nioByteBuffer())
        globalStatusMsg.enqueue(blockStatusMessage)
      }

      override def onFailure(chunkIndex: Int, e: Throwable): Unit = {
      }
    }
    transferService.fetchBlockStatus("", 2, blockStatusReceivedCallback)
  }

  def fetchBlocks(reqHost: String,
                  reqPort: Int,
                  execId: String,
                  blockIds: Array[String],
                  callback: BlockTrackerCallback,
                  tempFileManager: TempFileManager): Unit = {
    transferService.fetchBlocks(reqHost, reqPort, execId, blockIds, callback, tempFileManager)
  }
}

private[spark] class RdmaBlockTrackerDisable extends RdmaBlockTracker {
  host = ""
  port = 0
  enable_rdma = false
  def registerBlockStatus(byteBuffer: ByteBuffer): Unit = {}
}

object RdmaBlockTracker extends Logging {
  def getBlockTracker(isDriver: Boolean, enable_rdma: Boolean): RdmaBlockTracker = synchronized {
    if (!enable_rdma) {
      logInfo("Using RdmaBlockTrackerDisable");
      new RdmaBlockTrackerDisable
    } else {
      if (isDriver) {
        logInfo("Using RdmaBlockTrackerDriver");
        new RdmaBlockTrackerDriver
      } else {
        logInfo("Using RdmaBlockTrackerExecutor");
        new RdmaBlockTrackerDriver
      }
    }
  }
}
