package org.apache.spark.network

import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.concurrent

class RDMAClientFactory {
  val conPool: concurrent.Map[SocketAddress, RDMAClient] = new ConcurrentHashMap[SocketAddress, RDMAClient]().asScala

  def createClient(address: String, port: Int): RDMAClient = {
    val socketAddress: InetSocketAddress = InetSocketAddress.createUnresolved(address, port)
    val client: RDMAClient = conPool.getOrElse(socketAddress, {
      val clientTmp: RDMAClient = new RDMAClient(address, port)
      clientTmp.init()
      clientTmp.start()
      conPool.put(socketAddress, clientTmp)
      clientTmp
    })
    client
  }

  def getClient(address: String, port: Int): RDMAClient = {
    val socketAddress: InetSocketAddress = InetSocketAddress.createUnresolved(address, port)
    return conPool.get(socketAddress).getOrElse(null)
  }

  def stop(): Unit = {
    conPool.foreach(_._2.stop())
  }

  def waitToStop(): Unit = {
    conPool.foreach(_._2.waitToStop())
  }
}
