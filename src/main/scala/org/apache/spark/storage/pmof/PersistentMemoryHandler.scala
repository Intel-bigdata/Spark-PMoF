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

import org.apache.spark.internal.Logging
import java.nio.ByteBuffer

import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.pmof.RdmaTransferService
import org.apache.spark.storage.pmof.PersistentMemoryMetaHandler
import org.apache.spark.SparkEnv
import scala.collection.JavaConverters._

private[spark] class PersistentMemoryHandler(
    val root_dir: String,
    val path_list: List[String],
    val shuffleId: String,
    val maxStages: Int = 1000,
    val maxShuffles: Int = 1000,
    val poolSize: Long = -1) extends Logging {
  // need to use a locked file to get which pmem device should be used.
  val pmMetaHandler: PersistentMemoryMetaHandler = new PersistentMemoryMetaHandler(root_dir); 
  var device: String = pmMetaHandler.getShuffleDevice(shuffleId);
  if(device == "") {
    //this shuffleId haven't been written before, choose a new device
    logInfo("This a new shuffleBlock, find an unused device for this task.")
    val path_array_list = new java.util.ArrayList[String](path_list.asJava)
    device = pmMetaHandler.getUnusedDevice(path_array_list);
  }
  
  logInfo("Open PersistentMemoryPool: " + device)
  val pmpool = new PersistentMemoryPool(device, maxStages, maxShuffles, poolSize)
  var rkey: Long = 0

  def getDevice(): String = {
    device
  }

  def updateShuffleMeta(shuffleId: String): Unit = synchronized {
    pmMetaHandler.insertRecord(shuffleId, device);
  }

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

  def close(): Unit = synchronized {
    pmpool.close() 
    pmMetaHandler.remove()
  }

  def getRootAddr(): Long = {
    pmpool.getRootAddr();
  }

  def log(printout: String) {
    logInfo(printout)
  }
}

object PersistentMemoryHandler {
  private var persistentMemoryHandler: PersistentMemoryHandler = _
  var stopped: Boolean = false
  def getPersistentMemoryHandler(root_dir: String, path_arg: List[String], shuffleBlockId: String, pmPoolSize: Long, maxStages: Int, maxMaps: Int, enable_rdma: Boolean): PersistentMemoryHandler = synchronized {
    if (!stopped) {
      if (persistentMemoryHandler == null) {
        persistentMemoryHandler = new PersistentMemoryHandler(root_dir, path_arg, shuffleBlockId, maxStages, maxMaps, pmPoolSize)
        persistentMemoryHandler.log("Use persistentMemoryHandler Object: " + this)
        if (enable_rdma) {
          val blockManager = SparkEnv.get.blockManager
          val eqService = RdmaTransferService.getTransferServiceInstance(blockManager).server.getEqService
          val size: Long = 264239054848L
          val offset: Long = persistentMemoryHandler.getRootAddr
          val rdmaBuffer = eqService.regRmaBufferByAddress(null, offset, size)
          persistentMemoryHandler.rkey = rdmaBuffer.getRKey()
        }
      }
    }
    persistentMemoryHandler
  }

  def getPersistentMemoryHandler: PersistentMemoryHandler = synchronized {
    if (!stopped) {
      if (persistentMemoryHandler == null) {
        throw new NullPointerException("persistentMemoryHandler")
      }
    }
    persistentMemoryHandler
  }

  def stop(): Unit = synchronized {
    if (!stopped && persistentMemoryHandler != null) {
      persistentMemoryHandler.close()
      stopped = true
    }
  }
}
