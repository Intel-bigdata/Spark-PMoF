package org.apache.spark.shuffle.pmof

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}
import java.util.zip.{Deflater, DeflaterOutputStream, Inflater, InflaterInputStream}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferInputStream, ByteBufferOutputStream, Input, Output}
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.network.pmof._
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage.{ShuffleBlockId, ShuffleDataBlockId}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class MetadataResolver(conf: SparkConf) {
  private lazy val blockManager = SparkEnv.get.blockManager
  private lazy val blockMap: ConcurrentHashMap[String, ShuffleBuffer] = new ConcurrentHashMap[String, ShuffleBuffer]()
  val driverHost: String = conf.get("spark.driver.rhost", defaultValue = "172.168.0.43")
  val driverPort: Int = conf.getInt("spark.driver.rport", defaultValue = 61000)

  var map_serializer_buffer_size = 0L
  if (conf == null) {
    map_serializer_buffer_size = 16 * 1024L
  }
  else {
    map_serializer_buffer_size = conf.getLong("spark.shuffle.pmof.map_serializer_buffer_size", 16 * 1024)
  }

  var reduce_serializer_buffer_size = 0L
  if (conf == null) {
    reduce_serializer_buffer_size = 16 * 1024L
  }
  else {
    reduce_serializer_buffer_size = conf.getLong("spark.shuffle.pmof.reduce_serializer_buffer_size", 16 * 1024)
  }

  val metadataCompress: Boolean = conf.getBoolean("spark.shuffle.pmof.metadata_compress", defaultValue = false)

  val shuffleBlockSize: Int = conf.getInt("spark.shuffle.pmof.shuffle_block_size", defaultValue = 2048)

  val info_serialize_stream = new Kryo()
  val shuffleBlockInfoSerializer = new ShuffleBlockInfoSerializer
  info_serialize_stream.register(classOf[ShuffleBlockInfo], shuffleBlockInfoSerializer)

  val shuffleBlockMap = new ConcurrentHashMap[String, ArrayBuffer[ShuffleBlockInfo]]()

  def commitPmemBlockInfo(shuffleId: Int, mapId: Int, dataAddressMap: Array[Array[(Long, Int)]], rkey: Long): Unit = {
    val byteBuffer = ByteBuffer.allocate(map_serializer_buffer_size.toInt)
    val bos = new ByteBufferOutputStream(byteBuffer)
    var output: Output = null
    if (metadataCompress) {
      val dos = new DeflaterOutputStream(bos, new Deflater(9, true))
      output = new Output(dos)
    } else {
      output = new Output(bos)
    }
    val partitionNums = dataAddressMap.length
    MetadataResolver.this.synchronized {
      for (i <- 0 until partitionNums) {
        for ((address, length) <- dataAddressMap(i)) {
          val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, i).name
          info_serialize_stream.writeObject(output, new ShuffleBlockInfo(shuffleBlockId, address, length.toInt, rkey))
        }
      }
    }

    output.flush()
    output.close()
    byteBuffer.flip()

    val latch = new CountDownLatch(1)

    val receivedCallback = new ReceivedCallback {
      override def onSuccess(obj: ArrayBuffer[ShuffleBlockInfo]): Unit = {
        latch.countDown()
      }

      override def onFailure(e: Throwable): Unit = {

      }
    }

    PmofTransferService.getTransferServiceInstance(null, null).
        syncBlocksInfo(driverHost, driverPort, byteBuffer, 0.toByte, receivedCallback)

    latch.await()
  }

  def commitBlockInfo(shuffleId: Int, mapId: Int, partitionLengths: Array[Long]): Unit = {
    var offset: Long = 0L
    val file = blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
    val channel: FileChannel = new RandomAccessFile(file, "rw").getChannel

    var totalLength = 0L
    for (executorId <- partitionLengths.indices) {
      val currentLength: Long = partitionLengths(executorId)
      totalLength = totalLength + currentLength
    }

    val eqService = PmofTransferService.getTransferServiceInstance(blockManager).server.getEqService
    val shuffleBuffer = new ShuffleBuffer(0, totalLength, channel, eqService)
    val startedAddress = shuffleBuffer.getAddress
    val rdmaBuffer = eqService.regRmaBufferByAddress(shuffleBuffer.nioByteBuffer(), startedAddress, totalLength.toInt)
    shuffleBuffer.setRdmaBufferId(rdmaBuffer.getBufferId)
    shuffleBuffer.setRkey(rdmaBuffer.getRKey)
    val blockId = ShuffleBlockId(shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
    blockMap.put(blockId.name, shuffleBuffer)

    val byteBuffer = ByteBuffer.allocate(map_serializer_buffer_size.toInt)
    val bos = new ByteBufferOutputStream(byteBuffer)
    var output: Output = null
    if (metadataCompress) {
      val dos = new DeflaterOutputStream(bos, new Deflater(9, true))
      output = new Output(dos)
    } else {
      output = new Output(bos)
    }
    MetadataResolver.this.synchronized {
      for (executorId <- partitionLengths.indices) {
        val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, executorId)
        val currentLength: Int = partitionLengths(executorId).toInt
        val blockNums = currentLength / shuffleBlockSize + (if (currentLength % shuffleBlockSize == 0) 0 else 1)
        for (i <- 0 until blockNums) {
          if (i != blockNums - 1) {
            info_serialize_stream.writeObject(output, new ShuffleBlockInfo(shuffleBlockId.name, startedAddress + offset, shuffleBlockSize, rdmaBuffer.getRKey))
            offset += shuffleBlockSize
          } else {
            info_serialize_stream.writeObject(output, new ShuffleBlockInfo(shuffleBlockId.name, startedAddress + offset, currentLength - (i * shuffleBlockSize), rdmaBuffer.getRKey))
            offset += (currentLength - (i * shuffleBlockSize))
          }
        }
      }
    }

    output.flush()
    output.close()
    byteBuffer.flip()

    val latch = new CountDownLatch(1)

    val receivedCallback = new ReceivedCallback {
      override def onSuccess(obj: ArrayBuffer[ShuffleBlockInfo]): Unit = {
        latch.countDown()
      }

      override def onFailure(e: Throwable): Unit = {

      }
    }

    PmofTransferService.getTransferServiceInstance(null, null).
        syncBlocksInfo(driverHost, driverPort, byteBuffer, 0.toByte, receivedCallback)
    latch.await()
  }

  def closeBlocks(): Unit = {
    for ((_, v) <- blockMap.asScala) {
      v.close()
    }
  }

  def fetchBlockInfo(blockIds: Array[ShuffleBlockId], receivedCallback: ReceivedCallback): Unit = {
    val nums = blockIds.length
    val byteBufferTmp = ByteBuffer.allocate(4+12*nums)
    byteBufferTmp.putInt(nums)
    for (i <- 0 until nums) {
      byteBufferTmp.putInt(blockIds(i).shuffleId)
      byteBufferTmp.putInt(blockIds(i).mapId)
      byteBufferTmp.putInt(blockIds(i).reduceId)
    }
    byteBufferTmp.flip()
    PmofTransferService.getTransferServiceInstance(null, null).
      syncBlocksInfo(driverHost, driverPort, byteBufferTmp, 1.toByte, receivedCallback)
  }

  def addShuffleBlockInfo(byteBuffer: ByteBuffer): Unit = {
    val bis = new ByteBufferInputStream(byteBuffer)
    var input: Input = null
    if (metadataCompress) {
      val iis = new InflaterInputStream(bis, new Inflater(true))
      input = new Input(iis)
    } else {
      input = new Input(bis)
    }
    do {
      if (input.available() > 0) {
        val shuffleBlockInfo = info_serialize_stream.readObject(input, classOf[ShuffleBlockInfo])
        if (shuffleBlockMap.containsKey(shuffleBlockInfo.getShuffleBlockId)) {
          shuffleBlockMap.get(shuffleBlockInfo.getShuffleBlockId)
            .append(shuffleBlockInfo)
        } else {
          val blockInfoArray = new ArrayBuffer[ShuffleBlockInfo]()
          blockInfoArray.append(shuffleBlockInfo)
          shuffleBlockMap.put(shuffleBlockInfo.getShuffleBlockId, blockInfoArray)
        }
      } else {
        input.close()
        return
      }
    } while (true)
  }

  def serializeShuffleBlockInfo(byteBuffer: ByteBuffer): ByteBuffer = {
    val outputBuffer = ByteBuffer.allocate(reduce_serializer_buffer_size.toInt)
    val baos = new ByteBufferOutputStream(outputBuffer)
    val output = new Output(baos)
    val nums = byteBuffer.getInt()
    for (_ <- 0 until nums) {
      val shuffleId = byteBuffer.getInt()
      val mapId = byteBuffer.getInt()
      val reducerId = byteBuffer.getInt()
      val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, reducerId).name
      val blockInfoArray = shuffleBlockMap.get(shuffleBlockId)
      val partitionNums = blockInfoArray.size
      MetadataResolver.this.synchronized {
        for (i <- 0 until partitionNums) {
          info_serialize_stream.writeObject(output, blockInfoArray(i))
        }
      }
    }
    output.flush()
    output.close()
    outputBuffer.flip
    outputBuffer
  }

  def deserializeShuffleBlockInfo(byteBuffer: ByteBuffer): ArrayBuffer[ShuffleBlockInfo] = {
    val blockInfoArray: ArrayBuffer[ShuffleBlockInfo] = ArrayBuffer[ShuffleBlockInfo]()
    val bais = new ByteBufferInputStream(byteBuffer)
    val input = new Input(bais)
    do {
      if (input.available() > 0) {
        val shuffleBlockInfo = info_serialize_stream.readObject(input, classOf[ShuffleBlockInfo])
        blockInfoArray += shuffleBlockInfo
      } else {
        input.close()
        return blockInfoArray
      }
    } while (true)
    null
  }
}
