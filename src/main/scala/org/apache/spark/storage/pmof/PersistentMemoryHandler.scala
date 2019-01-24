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

import org.apache.spark._
import org.apache.spark.internal.Logging

import java.nio.ByteBuffer
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.storage.pmof.PersistentMemoryPool

private[spark] class PersistentMemoryHandler(
    val pathId: String,
    val maxStages: Int = 1000,
    val maxShuffles: Int = 1000,
    val poolSize: Long = -1,
    val core_s: Int = 0,
    val core_e: Int = 16) extends Logging {
  val pmpool = new PersistentMemoryPool(pathId, maxStages, maxShuffles, poolSize, core_s, core_e)
  log("Open PersistentMemoryPool: " + pathId + " ,binds to core " + core_s + "-" + core_e)

  def setPartition(numPartitions: Int, stageId: Int, shuffleId: Int, partitionId: Int, data: Array[Byte]): Long = {
    if (data.size > 0) {
      pmpool.setPartition(numPartitions, stageId, shuffleId, partitionId, data.size, data)
    } else {
      -1
    }
  }

  def getPartition(stageId: Int, shuffleId: Int, partitionId: Int): ManagedBuffer = {
    var data = pmpool.getPartition(stageId, shuffleId, partitionId)
    new NioManagedBuffer(ByteBuffer.wrap(data))
  }

  def close() = synchronized {
    pmpool.close() 
  } 

  def log(printout: String) {
    logInfo(printout)
  }
}

object PersistentMemoryHandler {
  private var persistentMemoryHandler: PersistentMemoryHandler = _
  var path: String = _
  var stopped = false
  def getPersistentMemoryHandler(path_arg: String, pmPoolSize: Long, maxStages: Int, maxMaps: Int, core_s: Int, core_e: Int) = synchronized {
    if (stopped == false) {
      if (persistentMemoryHandler == null) {
        path = path_arg
        persistentMemoryHandler = new PersistentMemoryHandler(path, maxStages, maxMaps, pmPoolSize, core_s, core_e)
      }
      //persistentMemoryHandler.log("Using persistentMemoryHandler for " + path)
    }
    persistentMemoryHandler
  }

  def getPersistentMemoryHandler() = synchronized {
    if (stopped == false) {
      if (persistentMemoryHandler == null) {
        throw new NullPointerException("persistentMemoryHandler")
      }
      //persistentMemoryHandler.log("Using persistentMemoryHandler for " + path)
    }
    persistentMemoryHandler
  }

  def stop() = synchronized {
    if (stopped == false && persistentMemoryHandler != null) {
      persistentMemoryHandler.close()
      stopped = true
    }
  }
}
