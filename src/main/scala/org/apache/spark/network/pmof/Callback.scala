package org.apache.spark.network.pmof

import scala.collection.mutable.ArrayBuffer

trait ReadCallback {
  def onSuccess(blockIndex: Int, buffer: ShuffleBuffer, f: (Int) => Unit): Unit
  def onFailure(blockIndex: Int, e: Throwable): Unit
}

trait ReceivedCallback {
  def onSuccess(obj: ArrayBuffer[ShuffleBlockInfo]): Unit
  def onFailure(e: Throwable): Unit
}
