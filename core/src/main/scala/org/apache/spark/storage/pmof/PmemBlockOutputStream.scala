package org.apache.spark.storage.pmof

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer._
import org.apache.spark.storage._
import org.apache.spark.util.Utils
import org.apache.spark.util.configuration.pmof.PmofConf

import scala.collection.mutable.ArrayBuffer

class PmemBlockId(stageId: Int, tmpId: Int) extends ShuffleBlockId(stageId, 0, tmpId) {
  override def name: String = "reduce_spill_" + stageId + "_" + tmpId

  override def isShuffle: Boolean = false
}

object PmemBlockId {
  private var tempId: Int = 0

  def getTempBlockId(stageId: Int): PmemBlockId = synchronized {
    val cur_tempId = tempId
    tempId += 1
    new PmemBlockId(stageId, cur_tempId)
  }
}

private[spark] class PmemBlockOutputStream(
    taskMetrics: TaskMetrics,
    blockId: BlockId,
    serializerManager: SerializerManager,
    serializer: Serializer,
    conf: SparkConf,
    pmofConf: PmofConf,
    numMaps: Int = 0,
    numPartitions: Int = 1)
    extends DiskBlockObjectWriter(
      new File(Utils.getConfiguredLocalDirs(conf).toList(0) + "/null"),
      null,
      null,
      0,
      true,
      null,
      null)
    with Logging {

  var size: Int = 0
  var records: Int = 0
  var recordsPerBlock: Int = 0
  val recordsArray: ArrayBuffer[Int] = ArrayBuffer()
  var spilled: Boolean = false
  var partitionMeta: Array[(Long, Int, Int)] = _
  val root_dir = Utils.getConfiguredLocalDirs(conf).toList(0)
  var persistentMemoryWriter: PersistentMemoryHandler = _
  var remotePersistentMemoryPool: RemotePersistentMemoryPool = _

  if (!pmofConf.enableRemotePmem) {
    persistentMemoryWriter = PersistentMemoryHandler.getPersistentMemoryHandler(
      pmofConf,
      root_dir,
      pmofConf.path_list,
      blockId.name,
      pmofConf.maxPoolSize)
  } else {
    remotePersistentMemoryPool =
      RemotePersistentMemoryPool.getInstance(pmofConf.rpmpHost, pmofConf.rpmpPort)
  }

  //disable metadata updating by default
  //persistentMemoryWriter.updateShuffleMeta(blockId.name)

  val pmemOutputStream: PmemOutputStream = new PmemOutputStream(
    persistentMemoryWriter,
    remotePersistentMemoryPool,
    numPartitions,
    blockId.name,
    numMaps,
    (pmofConf.spill_throttle.toInt + 1024))
  val serInstance = serializer.newInstance()
  val bs = serializerManager.wrapStream(blockId, pmemOutputStream)
  var objStream: SerializationStream = serInstance.serializeStream(bs)

  override def write(key: Any, value: Any): Unit = {
    objStream.writeKey(key)
    objStream.writeValue(value)
    records += 1
    recordsPerBlock += 1
    if (blockId.isShuffle == true) {
      taskMetrics.shuffleWriteMetrics.incRecordsWritten(1)
    }
    maybeSpill()
  }

  override def close() {
    if (objStream != null) {
      objStream.close()
      objStream = null
    }
    pmemOutputStream.close()
  }

  override def flush() {
    objStream.flush()
    bs.flush()
  }

  def maybeSpill(force: Boolean = false): Unit = {
    if (force == true) {
      flush()
    }
    if ((pmofConf.spill_throttle != -1 && pmemOutputStream.remainingSize >= pmofConf.spill_throttle) || force == true) {
      val start = System.nanoTime()
      pmemOutputStream.flush()
      val bufSize = pmemOutputStream.flushedSize
      if (bufSize > 0) {
        recordsArray += recordsPerBlock
        recordsPerBlock = 0
        size = bufSize

        if (blockId.isShuffle == true) {
          val writeMetrics = taskMetrics.shuffleWriteMetrics
          writeMetrics.incWriteTime(System.nanoTime() - start)
          writeMetrics.incBytesWritten(bufSize)
        } else {
          taskMetrics.incDiskBytesSpilled(bufSize)
        }
        pmemOutputStream.reset()
        spilled = true
      }
    }
  }

  def ifSpilled(): Boolean = {
    spilled
  }

  def getPartitionBlockInfo(res_array: Array[Long]): Array[(Long, Int, Int)] = {
    var i = -3
    var blockInfo = Array.ofDim[(Long, Int)]((res_array.length) / 3)
    blockInfo.map { x =>
      i += 3;
      (res_array(i), res_array(i + 1).toInt, res_array(i + 2).toInt)
    }
  }

  def getPartitionMeta(): Array[(Long, Int, Int)] = {
    if (partitionMeta == null) {
      var i = -1
      partitionMeta = if (!pmofConf.enableRemotePmem) {
        persistentMemoryWriter
          .getPartitionBlockInfo(blockId.name)
          .map(x => {
            i += 1
            (x._1, x._2, getRkey().toInt)
          })
      } else {
        getPartitionBlockInfo(remotePersistentMemoryPool.getMeta(blockId.name)).map(x => {
          i += 1
          (x._1, x._2, x._3)
        })
      }
    }
    partitionMeta
  }

  def getBlockId(): BlockId = {
    blockId
  }

  def getRkey(): Long = {
    persistentMemoryWriter.rkey
  }

  def getTotalRecords(): Long = {
    records
  }

  def getSize(): Long = {
    size
  }

  def getPersistentMemoryHandler: PersistentMemoryHandler = {
    persistentMemoryWriter
  }
}
