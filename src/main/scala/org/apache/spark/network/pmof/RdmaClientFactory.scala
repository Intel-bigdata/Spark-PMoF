package org.apache.spark.network.pmof

import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkConf

import scala.collection.JavaConverters._

class RdmaClientFactory(conf: SparkConf) {
  final val CON_NUM: Int = conf.getInt("spark.shuffle.pmof.client_pool_size", 1)
  val nextReqId: AtomicInteger = new AtomicInteger(0)
  val conPools: ConcurrentHashMap[SocketAddress, RdmaClientPool] =
    new ConcurrentHashMap[SocketAddress, RdmaClientPool]()

  def createClient(address: String, port: Int): RdmaClient = {
    val socketAddress: InetSocketAddress = InetSocketAddress.createUnresolved(address, port)
    var conPool: RdmaClientPool = conPools.get(socketAddress)
    if (conPool == null) {
      conPool = new RdmaClientPool(conf, CON_NUM, address, port)
      conPools.put(socketAddress, conPool)
    }
    val con = conPool.get(nextReqId.get() % CON_NUM)
    nextReqId.getAndIncrement()
    con
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
