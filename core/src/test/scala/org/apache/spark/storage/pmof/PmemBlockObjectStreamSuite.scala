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
package org.apache.spark.storage.pmof

import java.io.File

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.executor.{TaskMetrics, ShuffleWriteMetrics}
import org.apache.spark.serializer._
import org.apache.spark.util.Utils
import org.apache.spark.storage._

class PmemBlockObjectStreamSuite extends SparkFunSuite with BeforeAndAfterEach {

  var tempDir: File = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempDir = Utils.createTempDir()
  }

  override def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterEach()
    }
  }

  private def createWriter(): (PmemBlockObjectStream, ShuffleWriteMetrics) = {
    val conf = new SparkConf()
    conf.set("spark.shuffle.pmof.enable_rdma", "false")
    conf.set("spark.shuffle.pmof.enable_pmem", "true")
    //val serializer = new JavaSerializer(conf)
    val serializer = new KryoSerializer(conf)
    val serializerManager = new SerializerManager(serializer, conf)
    val taskMetrics = new TaskMetrics()
    val writeMetrics = taskMetrics.shuffleWriteMetrics
    val writer = new PmemBlockObjectStream(
      serializerManager, serializer.newInstance(), taskMetrics, ShuffleBlockId(0, 0, 0), conf, 100, 100)
    (writer, writeMetrics)
  }

  test("verify write metrics") {
    val (writer, writeMetrics) = createWriter()
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
    //assert(writeMetrics.bytesWritten > 0)
    //assert(writer.size == writeMetrics.bytesWritten)
  }
}
