package org.apache.spark.shuffle.pmof

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.storage.{BlockManager, ShuffleBlockId}
import org.apache.spark.storage.pmof.PersistentMemoryHandler

private[spark] class PersistentMemoryShuffleBlockResolver(
    conf: SparkConf,
    _blockManager: BlockManager = null)
  extends IndexShuffleBlockResolver(conf, _blockManager) with Logging {
  // create ShuffleHandler here, so multiple executors can share

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    // return BlockId corresponding ManagedBuffer
    val persistentMemoryHandler = PersistentMemoryHandler.getPersistentMemoryHandler()
    persistentMemoryHandler.initializeShuffle(blockId.shuffleId, blockId.mapId, -1)
    persistentMemoryHandler.getPartitionBlock(blockId.shuffleId, blockId.mapId, blockId.reduceId)
  }

  override def stop() {
    PersistentMemoryHandler.stop()
    super.stop()
  }
}
