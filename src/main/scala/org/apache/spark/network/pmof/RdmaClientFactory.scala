package org.apache.spark.network.pmof

import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkConf
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.shuffle.pmof.PmofShuffleManager

import scala.collection.JavaConverters._

class RdmaClientFactory(conf: SparkConf) {
  final val CON_NUM: Int = conf.getInt("spark.shuffle.pmof.client_pool_size", 2)
  val nextReqId: AtomicInteger = new AtomicInteger(0)
  val conPools: ConcurrentHashMap[SocketAddress, RdmaClientPool] =
    new ConcurrentHashMap[SocketAddress, RdmaClientPool]()

  def createClient(shuffleManager: PmofShuffleManager, address: String, port: Int, supportRma: Boolean): RdmaClient = {
    val socketAddress: InetSocketAddress = InetSocketAddress.createUnresolved(address, port)
    var conPool: RdmaClientPool = conPools.get(socketAddress)
    if (conPool == null) {
      RdmaClientFactory.this.synchronized {
        conPool = conPools.get(socketAddress)
        if (conPool == null) {
          conPool = new RdmaClientPool(conf, shuffleManager, CON_NUM, address, port)
          conPools.put(socketAddress, conPool)
        }
      }
    }
    if (!supportRma) {
      conPool.get(0)
    } else {
      val reqId = nextReqId.getAndIncrement()
      conPool.get(reqId%(CON_NUM-1)+1)
    }
  }

  def stop(): Unit = {
    for ((_, v) <- conPools.asScala) {
      v.stop()
    }
  }

  def waitToStop(): Unit = {
    for ((_, v) <- conPools.asScala) {
      v.waitToStop()
    }
  }
}
