package org.apache.spark.shuffle.pmof

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging
import org.apache.spark.network.pmof.RdmaTransferService
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.{SerializedShuffleHandle, SerializedShuffleWriter, SortShuffleManager}
import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}

private[spark] class PmofShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {
  logInfo("Initialize RdmaShuffleManager")
  if (!conf.getBoolean("spark.shuffle.spill", defaultValue = true)) logWarning("spark.shuffle.spill was set to false")

  val enable_rdma: Boolean = conf.getBoolean("spark.shuffle.pmof.enable_rdma", defaultValue = true)
  val enable_pmem: Boolean = conf.getBoolean("spark.shuffle.pmof.enable_pmem", defaultValue = true)

  val metadataResolver: MetadataResolver = new MetadataResolver(conf)

  private[this] val numMapsForShuffle = new ConcurrentHashMap[Int, Int]()
  if (enable_rdma) {
    logInfo("spark pmof rdma support enabled")
  }

  override def registerShuffle[K, V, C](shuffleId: Int, numMaps: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    val env: SparkEnv = SparkEnv.get
    RdmaTransferService.getTransferServiceInstance(env.blockManager, this, isDriver = true)
    if (enable_pmem) {
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    } else if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
      // Otherwise, try to buffer map outputs in a serialized form, since this is more efficient:
      new SerializedShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      // Otherwise, buffer map outputs in a deserialized form:
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }
  }

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V] = {
    logInfo("Using spark PmofShuffleWriter")
    val env: SparkEnv = SparkEnv.get
    numMapsForShuffle.putIfAbsent(handle.shuffleId, handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps)
    RdmaTransferService.getTransferServiceInstance(env.blockManager, this)
    handle match {
      case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
        new SerializedShuffleWriter(
          env.blockManager,
          shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
          metadataResolver,
          context.taskMemoryManager(),
          unsafeShuffleHandle,
          mapId,
          context,
          env.conf,
          enable_rdma)
      case other: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
        if (enable_pmem) {
          new PmemShuffleWriter(shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver], metadataResolver,
      handle.asInstanceOf[BaseShuffleHandle[K, V, _]], mapId, context, env.conf)
        } else {
          new BaseShuffleWriter(shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver], metadataResolver, other, mapId, context, enable_rdma)
        }
    }
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
        shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver].removeDataByMap(shuffleId, mapId)
      }
    }
    true
  }

  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }

  override val shuffleBlockResolver: ShuffleBlockResolver = {
    if (enable_pmem)
      new PersistentMemoryShuffleBlockResolver(conf)
    else
      new IndexShuffleBlockResolver(conf)
  }
}
