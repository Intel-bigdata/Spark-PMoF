package org.apache.spark.network

import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.ConcurrentHashMap

class RDMAClientFactory {
  val conPool = new ConcurrentHashMap[SocketAddress, RDMAClient]()

  def createClient(address: String, port: Int): RDMAClient = {
    if (conPool.containsKey(address)) {
      val client: RDMAClient = conPool.get(address)
      client
    } else {
      val socketAddress: InetSocketAddress = InetSocketAddress.createUnresolved(address, port)
      val client: RDMAClient = new RDMAClient(address, port.toString)
      client.init()
      client.start()
      conPool.put(socketAddress, client)
      client
    }
  }
}
