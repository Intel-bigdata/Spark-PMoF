package org.apache.spark.network.pmof

import org.apache.spark.SparkConf
import org.apache.spark.shuffle.pmof.PmofShuffleManager

class RdmaClientPool(conf: SparkConf, shuffleManager: PmofShuffleManager, poolSize: Int, address: String, port: Int) {
  val RdmaClients = new Array[RdmaClient](poolSize)

  def get(index: Int): RdmaClient = {
    if (RdmaClients(index) == null || !RdmaClients(index).started.get()) {
      RdmaClientPool.this.synchronized {
        if (RdmaClients(index) == null || !RdmaClients(index).started.get()) {
          if (index == 0) {
            RdmaClients(0) = new RdmaClient(conf, shuffleManager, address, port, false)
            RdmaClients(0).init()
            RdmaClients(0).start()
          } else {
            RdmaClients(index) = new RdmaClient(conf, shuffleManager, address, port, true)
            RdmaClients(index).init()
            RdmaClients(index).start()
          }
        }
      }
    }
    RdmaClients(index)
  }

  def stop(): Unit = {
    RdmaClients.foreach(_.stop())
  }

  def waitToStop(): Unit = {
    RdmaClients.foreach(_.waitToStop())
  }
}
