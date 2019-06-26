/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.shuffle.pmof

import scala.collection.mutable.ArrayBuffer

import org.mockito.Mockito._
import org.mockito.MockitoAnnotations
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterEach

import org.apache.spark._
import org.apache.spark.executor.{TaskMetrics, ShuffleWriteMetrics}
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.serializer._
import org.apache.spark.util.Utils
import org.apache.spark.storage._
import org.apache.spark.storage.pmof._
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.pmof.PmemExternalSorter

class PmemShuffleWriterSuite extends SparkFunSuite with SharedSparkContext with Matchers {

  private val shuffleId = 0
  private val numMaps = 5
  val blockId = new ShuffleBlockId(shuffleId, 2, 0)

  private var shuffleBlockResolver: PmemShuffleBlockResolver = _
  private var serializer: KryoSerializer = _
  private var serializerManager: SerializerManager = _
  private var taskMetrics: TaskMetrics = _
  private var partitioner: Partitioner = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    MockitoAnnotations.initMocks(this)
    conf.set("spark.shuffle.pmof.enable_rdma", "false")
    conf.set("spark.shuffle.pmof.enable_pmem", "true")
    conf.set("spark.shuffle.pmof.pmem_list", "/dev/dax0.0,/dev/dax1.0")
    shuffleBlockResolver = new PmemShuffleBlockResolver(conf)
    serializer = new KryoSerializer(conf)
    serializerManager = new SerializerManager(serializer, conf)
    taskMetrics = new TaskMetrics()
    partitioner = new Partitioner() {
      def numPartitions = 1
      def getPartition(key: Any) = Utils.nonNegativeMod(key.hashCode, numPartitions)
    }
  }

  override def afterEach(): Unit = {
    try {
      shuffleBlockResolver.stop()
    } finally {
      super.afterAll()
    }
  }

  def verify(buf: PmemManagedBuffer, expected: List[(Int, Int)]): Unit = {
    val inStream = buf.createInputStream()
    val wrappedStream = serializerManager.wrapStream(blockId, inStream)
    val inObjStream = serializer.newInstance().deserializeStream(wrappedStream)
    for (kv <- expected) {
      val k = inObjStream.readObject().asInstanceOf[Int]
      val v = inObjStream.readObject().asInstanceOf[Int]
      logDebug(s"$k->$v")
      assert(k.equals(kv._1))
      assert(v.equals(kv._2))
    }
    inObjStream.close()
  }

  test("write with some records") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val records = List[(Int, Int)]((6, 5), (2, 3), (4, 4), (1, 2))
    val shuffleHandle: BaseShuffleHandle[Int, Int, Int] = {
      val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
      when(dependency.partitioner).thenReturn(partitioner)
      when(dependency.serializer).thenReturn(serializer)
      when(dependency.aggregator).thenReturn(None)
      when(dependency.keyOrdering).thenReturn(None)

      new BaseShuffleHandle(shuffleId, numMaps = numMaps, dependency)
    }
    val writer = new PmemShuffleWriter[Int, Int, Int](
      shuffleBlockResolver,
      null,
      shuffleHandle,
      mapId = 2,
      context,
      conf)
    writer.write(records.toIterator)
    writer.stop(success = true)
    val buf = shuffleBlockResolver.getBlockData(blockId).asInstanceOf[PmemManagedBuffer]
    verify(buf, records)
  }
}
