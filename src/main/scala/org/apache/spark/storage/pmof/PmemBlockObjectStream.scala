package org.apache.spark.storage.pmof

import org.apache.spark.storage._
import org.apache.spark.storage.pmof.{PmemInputStream, PmemOutputStream}
import org.apache.spark.serializer._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import java.io.{ByteArrayOutputStream, OutputStream}

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils
import java.io.File
import java.io.{InputStream, OutputStream}

import scala.collection.mutable.ArrayBuffer

class PmemBlockId (stageId: Int, tmpId: Int) extends ShuffleBlockId(stageId, 0, tmpId) {
  override def name: String = "reduce_spill_" + stageId + "_" + tmpId
  override def isShuffle: Boolean = false
}

object PmemBlockId {
  private var tempId: Int = 0
  def getTempBlockId(stageId: Int): PmemBlockId = synchronized {
    var cur_tempId = tempId
    tempId += 1
    new PmemBlockId (stageId, cur_tempId)
  }
}

private[spark] class PmemBlockObjectStream(
    serializerManager: SerializerManager,
    serializerInstance: SerializerInstance,
    taskMetrics: TaskMetrics,
    blockId: BlockId,
    conf: SparkConf,
    numMaps: Int = 0,
    numPartitions: Int = 0
) extends DiskBlockObjectWriter(new File(Utils.getConfiguredLocalDirs(conf).toList(0) + "/null"), null, null, 0, true, null, null) with Logging {
  var initialized = false

  var size: Int = 0
  var records: Int = 0

  var recordsPerBlock: Int = 0
  val recordsArray: ArrayBuffer[Int] = ArrayBuffer()
  var spilled: Boolean = false
  var partitionMeta: Array[(Long, Int, Int)] = _

  val root_dir = Utils.getConfiguredLocalDirs(conf).toList(0)
  val path_list = conf.get("spark.shuffle.pmof.pmem_list").split(",").map(_.trim).toList
  val maxPoolSize: Long = conf.getLong("spark.shuffle.pmof.pmpool_size", defaultValue = 1073741824)
  val maxStages: Int = conf.getInt("spark.shuffle.pmof.max_stage_num", defaultValue = 1000)
  val enable_rdma: Boolean = conf.getBoolean("spark.shuffle.pmof.enable_rdma", defaultValue = true)
  val persistentMemoryWriter: PersistentMemoryHandler = PersistentMemoryHandler.getPersistentMemoryHandler(root_dir, path_list, blockId.name, maxPoolSize, maxStages, numMaps, enable_rdma)
  var spill_throttle = 4194304
  persistentMemoryWriter.updateShuffleMeta(blockId.name)
  logDebug(blockId.name)

  var objStream: SerializationStream = _
  var wrappedStream: OutputStream = _
  var bytesStream: OutputStream = new PmemOutputStream(
    persistentMemoryWriter, numPartitions, blockId.name)
  var inputStream: InputStream = _

  override def write(key: Any, value: Any): Unit = {
    if (!initialized) {
      wrappedStream = serializerManager.wrapStream(blockId, bytesStream)
      objStream = serializerInstance.serializeStream(wrappedStream)
      initialized = true
    }
    objStream.writeObject(key)
    objStream.writeObject(value)
    records += 1
    recordsPerBlock += 1
    maybeSpill()
  }

  override def close() {
    if (initialized) {
      logDebug("PersistentMemoryHandlerPartition: stream closed.")
      bytesStream.close()
    }
  }

  def noAutoSpill(): Unit = {
    spill_throttle = -1
  }

  def maybeSpill(force: Boolean = false): Unit = {
    if ((spill_throttle != -1 && bytesStream.asInstanceOf[PmemOutputStream].size >= spill_throttle) || force == true) {
      val start = System.nanoTime()
      objStream.flush()
      bytesStream.flush()

      recordsArray += recordsPerBlock
      recordsPerBlock = 0
      size += bytesStream.asInstanceOf[PmemOutputStream].size

			if (blockId.isShuffle == true) {
        val writeMetrics = taskMetrics.shuffleWriteMetrics
        writeMetrics.incWriteTime(System.nanoTime() - start)
        writeMetrics.incBytesWritten(bytesStream.asInstanceOf[PmemOutputStream].size)
			} else {
        taskMetrics.incDiskBytesSpilled(bytesStream.asInstanceOf[PmemOutputStream].size)
			}
      bytesStream.asInstanceOf[PmemOutputStream].reset()
      spilled = true
    }
  }

  def ifSpilled(): Boolean = {
    spilled
  }

  def getPartitionMeta(): Array[(Long, Int, Int)] = {
    if (partitionMeta == null) {
      var blockInfo: Array[(Long, Int)] = persistentMemoryWriter.getPartitionBlockInfo(blockId.name)
      var i = -1
      partitionMeta = blockInfo.map{ x=> i+=1; (x._1, x._2, recordsArray(i))}
    }
    partitionMeta
  }

  def getBlockId(): BlockId = {
    blockId
  }

  def getRkey(): Long = {
    persistentMemoryWriter.rkey
  }

  /*def getAllBytes(): Array[Byte] = {
    persistentMemoryWriter.getPartition(blockId.name)
  }*/

  def getTotalRecords(): Long = {
    records    
  }

  def getSize(): Long = {
    size
  }

  def getInputStream(): InputStream = {
    if (inputStream == null) {
      inputStream = new PmemInputStream(persistentMemoryWriter, blockId.name)
    }
    inputStream
  }
  
}
