package org.apache.spark.storage.pmof

import java.io.InputStream
import com.esotericsoftware.kryo.KryoException
import org.apache.spark.internal.Logging
import org.apache.spark.SparkEnv
import org.apache.spark.serializer.{
  DeserializationStream,
  Serializer,
  SerializerInstance,
  SerializerManager
}
import org.apache.spark.storage.BlockId
import org.apache.spark.util.configuration.pmof.PmofConf

trait PmemBlockInputStream[K, C] {
  def readNextItem(): (K, C)
}

class LocalPmemBlockInputStream[K, C](
    blockId: BlockId,
    total_records: Long,
    serializer: Serializer)
    extends PmemBlockInputStream[K, C] {
  val serializerManager: SerializerManager = SparkEnv.get.serializerManager
  val serInstance: SerializerInstance = serializer.newInstance()
  val persistentMemoryWriter: PersistentMemoryHandler =
    PersistentMemoryHandler.getPersistentMemoryHandler
  var pmemInputStream: PmemInputStream = new PmemInputStream(persistentMemoryWriter, blockId.name)
  val wrappedStream = serializerManager.wrapStream(blockId, pmemInputStream)
  var inObjStream: DeserializationStream = serInstance.deserializeStream(wrappedStream)

  var indexInBatch: Int = 0
  var closing: Boolean = false

  def readNextItem(): (K, C) = {
    if (closing == true) {
      close()
      return null
    }
    try {
      val k = inObjStream.readObject().asInstanceOf[K]
      val c = inObjStream.readObject().asInstanceOf[C]
      indexInBatch += 1
      if (indexInBatch >= total_records) {
        closing = true
      }
      (k, c)
    } catch {
      case ex: KryoException => {}
        sys.exit(0)
    }
  }

  def close(): Unit = {
    inObjStream.close
    pmemInputStream.close
    inObjStream = null
  }
}

class RemotePmemBlockInputStream[K, C](
    blockId: BlockId,
    mapStatus: Seq[(String, Long, Int)],
    serializer: Serializer,
    pmofConf: PmofConf)
    extends PmemBlockInputStream[K, C]
    with Logging {
  val serializerManager: SerializerManager = SparkEnv.get.serializerManager
  val serInstance: SerializerInstance = serializer.newInstance()
  val remotePersistentMemoryPool =
    RemotePersistentMemoryPool.getInstance(pmofConf.rpmpHost, pmofConf.rpmpPort)

  var map_index: Int = 0
  var num_items: Int = 0
  var cur_num_items: Int = 0
  var inObjStream: DeserializationStream = _
  var buf: NioManagedBuffer = _
  var input: InputStream = _

  def loadStream(): Unit = {
    if (buf != null) {
      inObjStream.close()
      input.close()
      buf.release()
    }
    if (map_index == mapStatus.size) {
      inObjStream = null
    } else {
      num_items = mapStatus(map_index)._3
      buf = new NioManagedBuffer(mapStatus(map_index)._2.toInt)
      logWarning(s"[GET started] ${mapStatus(map_index)._1}-${mapStatus(map_index)._2}")
      val readed_len = remotePersistentMemoryPool.get(
        mapStatus(map_index)._1,
        mapStatus(map_index)._2,
        buf.nioByteBuffer)
      logWarning(s"[GET Completed] ${mapStatus(map_index)._1}-${mapStatus(map_index)._2}")
      val in = buf.createInputStream()
      input = serializerManager.wrapStream(blockId, in)
      inObjStream = serInstance.deserializeStream(input)
      map_index += 1
    }
  }

  def readNextItem(): (K, C) = {
    try {
      if (buf == null || cur_num_items >= num_items) {
        loadStream()
        cur_num_items = 0
      }
      if (inObjStream == null) {
        return null
      }
      val k = inObjStream.readObject().asInstanceOf[K]
      val c = inObjStream.readObject().asInstanceOf[C]
      cur_num_items += 1
      (k, c)
    } catch {
      case ex: KryoException => {}
        sys.exit(0)
    }
  }

}
