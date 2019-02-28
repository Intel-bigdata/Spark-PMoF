package org.apache.spark.storage.pmof

import org.apache.spark.storage._
import org.apache.spark.serializer._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import java.io.{ByteArrayOutputStream, OutputStream}

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils
import java.io.File
import sun.misc.Unsafe
import java.io.InputStream
import java.nio.ByteBuffer

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
  var set_clean = true

  var objStream: SerializationStream = _
  var wrappedStream: OutputStream = _
  var bytesStream: ByteArrayOutputStream = _
  var inputStream: InputStream = _

  var size: Int = 0
  var records: Int = 0
  var recordsPerBlock: Int = 0
  var partitionMeta: ArrayBuffer[(Long, Int, Int)] = ArrayBuffer()

  val root_dir = Utils.getConfiguredLocalDirs(conf).toList(0)
  val path_list = conf.get("spark.shuffle.pmof.pmem_list").split(",").map(_.trim).toList
  val maxPoolSize: Long = conf.getLong("spark.shuffle.pmof.pmpool_size", defaultValue = 1073741824)
  val maxStages: Int = conf.getInt("spark.shuffle.pmof.max_stage_num", defaultValue = 1000)
  val enable_rdma: Boolean = conf.getBoolean("spark.shuffle.pmof.enable_rdma", defaultValue = true)
  val persistentMemoryWriter: PersistentMemoryHandler = PersistentMemoryHandler.getPersistentMemoryHandler(root_dir, path_list, blockId.name, maxPoolSize, maxStages, numMaps, enable_rdma)
  var spill_throttle = 4194304
  persistentMemoryWriter.updateShuffleMeta(blockId.name)
  logDebug(blockId.name)

  override def write(key: Any, value: Any): Unit = {
    if (!initialized) {
      bytesStream = new ByteArrayOutputStream()
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

  private def get() : (Array[Byte], Int) = {
    if (initialized) {
      objStream.flush()
      wrappedStream.flush()
      bytesStream.flush()

      val data = bytesStream.toByteArray
      val recordsPerBlock_r = recordsPerBlock

      // update in class variables
      size += data.size
      bytesStream.reset()
      recordsPerBlock = 0
      (data, recordsPerBlock_r)
    } else {
      (Array[Byte](), 0)
    }
  }
  
  override def close() {
    if (initialized) {
      logDebug("PersistentMemoryHandlerPartition: stream closed.")
      objStream.close()
      wrappedStream.close()
      bytesStream.close()
    }
  }

  def noAutoSpill(): Unit = {
    spill_throttle = -1
  }

  def maybeSpill(force: Boolean = false): Unit = {
    if ((spill_throttle != -1 && bytesStream.size >= spill_throttle) || force == true) {
      val (tmp_data, recordsPerBlock_r) = get()
      val start = System.nanoTime()
      var partitionInfo = (persistentMemoryWriter.setPartition(numPartitions, blockId.name, tmp_data, set_clean), tmp_data.length, recordsPerBlock_r)
      if (set_clean == true) {
        // after first written to this partition, set_clean to false for later on appending
        set_clean = false
      }
      partitionMeta += partitionInfo
			if (blockId.isShuffle == true) {
        val writeMetrics = taskMetrics.shuffleWriteMetrics
        writeMetrics.incWriteTime(System.nanoTime() - start)
        writeMetrics.incBytesWritten(tmp_data.length)
			} else {
        //taskMetrics.incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        taskMetrics.incDiskBytesSpilled(tmp_data.length)
        //taskMetrics.incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
			}
    }
  }

  def ifSpilled(): Boolean = {
    !partitionMeta.isEmpty
  }

  def getPartitionMeta(): ArrayBuffer[(Long, Int, Int)] = {
    partitionMeta
  }

  def getBlockId(): BlockId = {
    blockId
  }

  def getRkey(): Long = {
    persistentMemoryWriter.rkey
  }

  def getAllBytes(): Array[Byte] = {
    persistentMemoryWriter.getPartition(blockId.name)
  }

  def getTotalRecords(): Long = {
    records    
  }

  def getInputStream(): InputStream = {
    if (inputStream == null) {
      inputStream = new PmemBlockObjectInputStream()
    }
    inputStream
  }

  class PmemBlockObjectInputStream extends InputStream {
    var UNSAFE: Unsafe = _
    val f = classOf[Unsafe].getDeclaredField("theUnsafe")
    f.setAccessible(true)
    UNSAFE = f.get(null).asInstanceOf[Unsafe]
  
    var bufferCapacity = partitionMeta(0)._2
    var buf: ByteBuffer = ByteBuffer.allocate(8192 * 1024)
    var index: Int = 0
    var remaining: Int = 0
  
    def loadNextStream(): Unit = {
      if (index >= partitionMeta.length)
        return
      val data_length = partitionMeta(index)._2
      val data_addr = partitionMeta(index)._1
      val records = partitionMeta(index)._3
      logDebug("PmemBlockObjectInputStream.loadNextStream() for " + blockId.name + ", addr: " + data_addr + ", length: " + data_length + ", records: " + records + ", remaining: " + remaining)

      var data = Array.ofDim[Byte](data_length)
      UNSAFE.copyMemory(null, data_addr, data, Unsafe.ARRAY_BYTE_BASE_OFFSET, data_length)

      if (remaining > 0) {
        // move remaining bytes to buf front
        var swapBytes = Array.ofDim[Byte](remaining)
        buf.get(swapBytes)
        buf.position(0)
        buf.put(swapBytes)
      } else {
        buf.position(0)
      }
      buf.put(data)
      buf.position(0)
      index += 1
      remaining += data_length
    }
  
    override def read(): Int = {
      if (remaining == 0) {
        loadNextStream()
        if (remaining == 0) {
          return -1
        }
      }
      buf.get()
    }

    override def read(bytes: Array[Byte], off: Int, len: Int): Int = {
      if ((remaining > 0 && remaining < len) || remaining == 0) {
        loadNextStream()
        if (remaining == 0) {
          return -1
        }
      }
  
      val real_len = Math.min(len, remaining)
      buf.get(bytes, off, real_len)
      remaining -= real_len
      real_len
    }
  
    override def available(): Int = {
      remaining
    }

    override def close(): Unit = {
      //delete this spill block here
      logInfo("persistentMemoryWriter.deletePartition(" + blockId.name + ")")
    }
  }
}
