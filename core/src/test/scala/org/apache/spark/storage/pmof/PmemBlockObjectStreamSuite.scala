package org.apache.spark.storage.pmof

import java.io.File

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.executor.{TaskMetrics, ShuffleWriteMetrics}
import org.apache.spark.serializer._
import org.apache.spark.util.Utils
import org.apache.spark.storage._
import org.apache.spark.storage.pmof._

class PmemBlockObjectStreamSuite extends SparkFunSuite with BeforeAndAfterEach {

  val conf = new SparkConf()
  conf.set("spark.shuffle.pmof.enable_rdma", "false")
  conf.set("spark.shuffle.pmof.enable_pmem", "true")
  //val serializer = new JavaSerializer(conf)
  val serializer = new KryoSerializer(conf)
  val serializerManager = new SerializerManager(serializer, conf)
  val taskMetrics = new TaskMetrics()

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    super.afterEach()
  }

  private def createWriter(blockId: ShuffleBlockId): (PmemBlockObjectStream, ShuffleWriteMetrics) = {
    val writeMetrics = taskMetrics.shuffleWriteMetrics
    val writer = new PmemBlockObjectStream(
      serializerManager, serializer.newInstance(), taskMetrics, blockId, conf, 100, 100)
    (writer, writeMetrics)
  }

  test("verify ShuffleWrite of Shuffle_0_0_0, then check read") {
    val blockId = ShuffleBlockId(0, 0, 0)
    val (writer, writeMetrics) = createWriter(blockId)
    val key: String = "key"
    val value: String = "value"
    writer.write(key, value)
    // Record metrics update on every write
    assert(writeMetrics.recordsWritten === 1)
    // Metrics don't update on every write
    assert(writeMetrics.bytesWritten == 0)
    // write then flush, metrics should update
    writer.write(key, value)
    writer.flush()
    assert(writeMetrics.recordsWritten === 2)
    writer.close()

    val inStream = writer.getInputStream()
    val wrappedStream = serializerManager.wrapStream(blockId, inStream)
    val inObjStream = serializer.newInstance().deserializeStream(wrappedStream)
    val k = inObjStream.readObject().asInstanceOf[String]
    val v = inObjStream.readObject().asInstanceOf[String]
    assert(k.equals(key))
    assert(v.equals(value))
    inObjStream.close()
  }

  test("verify ShuffleRead of Shuffle_0_0_0") {
    val blockId = ShuffleBlockId(0, 0, 0)
    val persistentMemoryHandler = PersistentMemoryHandler.getPersistentMemoryHandler
    val buf = persistentMemoryHandler.getPartitionManagedBuffer(blockId.name)
    val inStream = buf.createInputStream()
    val wrappedStream = serializerManager.wrapStream(blockId, inStream)
    val inObjStream = serializer.newInstance().deserializeStream(wrappedStream)
    val k = inObjStream.readObject().asInstanceOf[String]
    val v = inObjStream.readObject().asInstanceOf[String]
    assert(k.equals("key"))
    assert(v.equals("value"))
    inObjStream.close()
  }

  test("verify ShuffleRead of none exists Shuffle_0_0_1") {
    val blockId = ShuffleBlockId(0, 0, 1)
    val persistentMemoryHandler = PersistentMemoryHandler.getPersistentMemoryHandler
    val buf = persistentMemoryHandler.getPartitionManagedBuffer(blockId.name)

    val inStream = buf.createInputStream()
    val wrappedStream = serializerManager.wrapStream(blockId, inStream)
    val inObjStream = serializer.newInstance().deserializeStream(wrappedStream)
    try{
      val k = inObjStream.readObject().asInstanceOf[String]
      val v = inObjStream.readObject().asInstanceOf[String]
    } catch {
      case ex: java.io.EOFException =>
        logInfo(s"Expected Error: $ex")
    }
    inObjStream.close()
  }
}
