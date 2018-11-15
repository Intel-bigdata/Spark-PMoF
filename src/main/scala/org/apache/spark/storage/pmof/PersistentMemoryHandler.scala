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
import org.apache.spark.storage.pmof.PersistentMemoryPool
import org.apache.spark.executor.ShuffleWriteMetrics


private[spark] class PersistentMemoryHandler(
    val pathId: String,
    val maxStages: Int = 1000,
    val maxShuffles: Int = 1000,
    val poolSize: Long = -1) extends Logging {
  val pmpool = new PersistentMemoryPool(pathId, maxStages, maxShuffles, poolSize)

  def initializeShuffle(stageId: Int, shuffleId: Int, numPartitions: Int) = synchronized {
    pmpool.openShuffleBlock(stageId, shuffleId, numPartitions)
  }

  def write(stageId: Int, shuffleId: Int, partitionId: Int, data: Array[Byte]) = synchronized {
    if (data.size > 0) {
      pmpool.addPartition(stageId, shuffleId, partitionId, data.size);
      pmpool.set(stageId, shuffleId, partitionId, data)
    }
  }

  def getPartitionBlock(stageId: Int, shuffleId: Int, partitionId: Int): ManagedBuffer = synchronized {
    var data = pmpool.get(stageId, shuffleId, partitionId)
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
  def getPersistentMemoryHandler(path_arg: String, pmPoolSize: Long, maxStages: Int, maxMaps: Int) = synchronized {
    if (persistentMemoryHandler == null) {
      path = path_arg
      persistentMemoryHandler = new PersistentMemoryHandler(path, maxStages, maxMaps, pmPoolSize)
    }
    persistentMemoryHandler.log("Using persistentMemoryHandler for " + path)
    persistentMemoryHandler
  }

  def getPersistentMemoryHandler() = synchronized {
    if (persistentMemoryHandler == null) {
      throw new NullPointerException("persistentMemoryHandler")
    }
    persistentMemoryHandler.log("Using persistentMemoryHandler for " + path)
    persistentMemoryHandler
  }
}
