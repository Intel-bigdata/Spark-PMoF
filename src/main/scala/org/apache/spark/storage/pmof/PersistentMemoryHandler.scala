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

import org.apache.spark.network.pmof.RdmaTransferService
import org.apache.spark.SparkEnv

import scala.collection.JavaConverters._
import java.nio.file.{Files, Paths}
import java.util.UUID
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.pmof.PmemManagedBuffer
import org.apache.spark.storage.pmof.PmemBuffer

private[spark] class PersistentMemoryHandler(
    val root_dir: String,
    val path_list: List[String],
    val shuffleId: String,
    var poolSize: Long = -1) extends Logging {
  // need to use a locked file to get which pmem device should be used.
  val pmMetaHandler: PersistentMemoryMetaHandler = new PersistentMemoryMetaHandler(root_dir)
  var device: String = pmMetaHandler.getShuffleDevice(shuffleId)
  if(device == "") {
    //this shuffleId haven't been written before, choose a new device
    val path_array_list = new java.util.ArrayList[String](path_list.asJava)
    device = pmMetaHandler.getUnusedDevice(path_array_list)

    val dev = Paths.get(device)
    if (Files.isDirectory(dev)) {
      // this is fsdax, add a subfile
      device += "/shuffle_block_" + UUID.randomUUID().toString()
      logInfo("This is a fsdax, filename:" + device)
    } else {
      poolSize = 0
    }
  }
  
  val pmpool = new PersistentMemoryPool(device, poolSize)
  var rkey: Long = 0

  def getDevice(): String = {
    device
  }

  def updateShuffleMeta(shuffleId: String): Unit = synchronized {
    pmMetaHandler.insertRecord(shuffleId, device);
  }

  def getPartitionBlockInfo(blockId: String): Array[(Long, Int)] = {
    var res_array: Array[Long] = pmpool.getPartitionBlockInfo(blockId)
    var i = -2
    var blockInfo = Array.ofDim[(Long, Int)]((res_array.length)/2)
    blockInfo.map{ x => i += 2; (res_array(i), res_array(i+1).toInt)}
  }

  def getPartitionSize(blockId: String): Long = {
    pmpool.getPartitionSize(blockId)
  }
  
  def setPartition(numPartitions: Int, blockId: String, buf: PmemBuffer, clean: Boolean): Long = {
    pmpool.setPartition(blockId, buf, clean)
  }

  def getPartition(blockId: String): Array[Byte] = {
    pmpool.getPartition(blockId)
  }

  def deletePartition(blockId: String): Unit = {
    pmpool.deletePartition(blockId)
  }

  def getPartitionManagedBuffer(blockId: String): ManagedBuffer = {
    new PmemManagedBuffer(this, blockId)
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
  def getPersistentMemoryHandler(root_dir: String, path_arg: List[String], shuffleBlockId: String, pmPoolSize: Long, enable_rdma: Boolean): PersistentMemoryHandler = synchronized {
    if (!stopped) {
      if (persistentMemoryHandler == null) {
        persistentMemoryHandler = new PersistentMemoryHandler(root_dir, path_arg, shuffleBlockId, pmPoolSize)
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
