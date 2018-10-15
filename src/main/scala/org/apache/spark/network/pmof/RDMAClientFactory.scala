package org.apache.spark.network.pmof

import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.concurrent

class RdmaClientFactory(blockTracker: RdmaBlockTracker, isDriver: Boolean) {
  val conPool: concurrent.Map[SocketAddress, RdmaClient] = new ConcurrentHashMap[SocketAddress, RdmaClient]().asScala

  def createClient(address: String, port: Int): RdmaClient = {
    val socketAddress: InetSocketAddress = InetSocketAddress.createUnresolved(address, port)
    val client: RdmaClient = conPool.getOrElse(socketAddress, {
      val clientTmp: RdmaClient = new RdmaClient(address, port, blockTracker, isDriver)
      clientTmp.init()
      clientTmp.start()
      conPool.put(socketAddress, clientTmp)
      clientTmp
    })
    client
  }

  def getClient(address: String, port: Int): RdmaClient = {
    val socketAddress: InetSocketAddress = InetSocketAddress.createUnresolved(address, port)
    conPool.getOrElse(socketAddress, null)
  }

  def stop(): Unit = {
    conPool.foreach(_._2.stop())
  }

  def waitToStop(): Unit = {
    conPool.foreach(_._2.waitToStop())
  }
}
