package org.apache.spark.shuffle.pmof

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging
import org.apache.spark.network.pmof.{RdmaBlockTracker, RdmaBlockTrackerDriver, RdmaBlockTrackerExecutor}
import org.apache.spark.shuffle._
import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId, ShuffleDataBlockId}
import org.apache.spark.storage.pmof.PersistentMemoryHandler

private[spark] class RdmaShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {
  logInfo("Initialize RdmaShuffleManager")
  if (!conf.getBoolean("spark.shuffle.spill", defaultValue = true)) logWarning("spark.shuffle.spill was set to false")
  val enable_rdma: Boolean = conf.getBoolean("spark.shuffle.pmof.enable_rdma", defaultValue = true)
  val path = "/mnt/mem/spark_shuffle_pmpool"
  val pmPoolSize: Long = conf.getLong("spark.shuffle.pmof.pmpool_size", defaultValue = 1073741824)
  val maxStages: Int = conf.getInt("spark.shuffle.pmof.max_stage_num", defaultValue = 1000)
  val maxMaps: Int = conf.getInt("spark.shuffle.pmof.max_task_num", defaultValue = 1000)

  private[this] val numMapsForShuffle = new ConcurrentHashMap[Int, Int]()
  private[this] var blockTracker: RdmaBlockTracker = _

  if (enable_rdma) {
    logInfo("spark pmof rdma support enabled")
  }

  override def registerShuffle[K, V, C](shuffleId: Int, numMaps: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    blockTracker = RdmaBlockTracker.getBlockTracker(true, enable_rdma)
    // TODO: need to handle other handle
    new BaseShuffleHandle(shuffleId, numMaps, dependency)
  }

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V] = {
    blockTracker = RdmaBlockTracker.getBlockTracker(false, enable_rdma)
    numMapsForShuffle.putIfAbsent(handle.shuffleId, handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps)

    new RdmaShuffleWriter(shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
      handle.asInstanceOf[BaseShuffleHandle[K, V, _]], mapId, context, blockTracker, path, pmPoolSize, maxStages, maxMaps)
  }

  override def getReader[K, C](handle: _root_.org.apache.spark.shuffle.ShuffleHandle, startPartition: Int, endPartition: Int, context: _root_.org.apache.spark.TaskContext): _root_.org.apache.spark.shuffle.ShuffleReader[K, C] = {
    blockTracker = RdmaBlockTracker.getBlockTracker(false, enable_rdma)
    if (enable_rdma) {
      new RdmaShuffleReader(handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
        startPartition, endPartition, context, blockTracker)
    } else {
      new BlockStoreShuffleReader(
        handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
    }
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(numMapsForShuffle.remove(shuffleId)).foreach { numMaps =>
      (0 until numMaps).foreach { mapId =>
        shuffleBlockResolver.removeDataByMap(shuffleId, mapId)
      }
    }
    true
  }

  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }

  override val shuffleBlockResolver: PersistentMemoryShuffleBlockResolver = new PersistentMemoryShuffleBlockResolver(conf)
}
