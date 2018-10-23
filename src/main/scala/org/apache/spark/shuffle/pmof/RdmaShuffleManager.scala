package org.apache.spark.shuffle.pmof

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}

private[spark] class RdmaShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {
  if (!conf.getBoolean("spark.shuffle.spill", defaultValue = true)) logWarning("spark.shuffle.spill was set to false")
  val enable_rdma: Boolean = conf.getBoolean("spark.shuffle.pmof.enable_rdma", defaultValue = true)

  private[this] val numMapsForShuffle = new ConcurrentHashMap[Int, Int]()
  if (enable_rdma) {
    logInfo("spark pmof rdma support enabled")
  }

  override def registerShuffle[K, V, C](shuffleId: Int, numMaps: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    logInfo("")
    // TODO: need to handle other handle
    new BaseShuffleHandle(shuffleId, numMaps, dependency)
  }

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V] = {
    logInfo("Using spark pmof RDMAShuffleWriter")
    numMapsForShuffle.putIfAbsent(handle.shuffleId, handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps)
    // TODO: need to handle unsafe writer
    new RdmaShuffleWriter(shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver], handle.asInstanceOf[BaseShuffleHandle[K, V, _]], mapId, context)
  }

  override def getReader[K, C](handle: _root_.org.apache.spark.shuffle.ShuffleHandle, startPartition: Int, endPartition: Int, context: _root_.org.apache.spark.TaskContext): _root_.org.apache.spark.shuffle.ShuffleReader[K, C] = {
    if (enable_rdma) {
      new RdmaShuffleReader(handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
        startPartition, endPartition, context)
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

  override val shuffleBlockResolver: IndexShuffleBlockResolver = new IndexShuffleBlockResolver(conf)
}
