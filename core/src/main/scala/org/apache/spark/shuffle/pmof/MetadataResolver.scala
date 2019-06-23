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
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.{Breaks, ControlThrowable}

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

  def commitPmemBlockInfo(shuffleId: Int, mapId: Int, dataAddressMap: mutable.HashMap[Int, Array[(Long, Int)]], rkey: Long): Unit = {
    val buffer: Array[Byte] = new Array[Byte](reduce_serializer_buffer_size.toInt)
    var output = new Output(buffer)
    val bufferArray = new ArrayBuffer[ByteBuffer]()

    MetadataResolver.this.synchronized {
      for (iterator <- dataAddressMap) {
        for ((address, length) <- iterator._2) {
          val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, iterator._1).name
          info_serialize_stream.writeObject(output, new ShuffleBlockInfo(shuffleBlockId, address, length.toInt, rkey))
          output.flush()
          if (output.position() >= map_serializer_buffer_size * 0.9) {
            val blockBuffer = ByteBuffer.wrap(output.getBuffer)
            blockBuffer.position(output.position())
            blockBuffer.flip()
            bufferArray += blockBuffer
            output.close()
            val new_buffer = new Array[Byte](reduce_serializer_buffer_size.toInt)
            output = new Output(new_buffer)
          }
        }
      }
    }
    if (output.position() != 0) {
      val blockBuffer = ByteBuffer.wrap(output.getBuffer)
      blockBuffer.position(output.position())
      blockBuffer.flip()
      bufferArray += blockBuffer
      output.close()
    }
    val latch = new CountDownLatch(bufferArray.size)
    val receivedCallback = new ReceivedCallback {
      override def onSuccess(obj: ArrayBuffer[ShuffleBlockInfo]): Unit = {
        latch.countDown()
      }

      override def onFailure(e: Throwable): Unit = {

      }
    }
    for (buffer <- bufferArray) {
      PmofTransferService.getTransferServiceInstance(null, null).
        syncBlocksInfo(driverHost, driverPort, buffer, 0.toByte, receivedCallback)
    }
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
    MetadataResolver.this.synchronized {
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
  }

  def serializeShuffleBlockInfo(byteBuffer: ByteBuffer): ArrayBuffer[ByteBuffer] = {
    val buffer: Array[Byte] = new Array[Byte](reduce_serializer_buffer_size.toInt)
    var output = new Output(buffer)

    val bufferArray = new ArrayBuffer[ByteBuffer]()
    val totalBlock = byteBuffer.getInt()
    var cur = 0
    var pre = -1
    var psbi: String = null
    var csbi: String = null
    MetadataResolver.this.synchronized {
      try {
        while (cur < totalBlock) {
          if (cur == pre) {
            csbi = psbi
          } else {
            csbi = ShuffleBlockId(byteBuffer.getInt(), byteBuffer.getInt(), byteBuffer.getInt()).name
            psbi = csbi
            pre = cur
          }
          if (shuffleBlockMap.containsKey(csbi)) {
            val blockInfoArray = shuffleBlockMap.get(csbi)
            val startPos = output.position()
            val loop = Breaks
            loop.breakable {
              for (i <- blockInfoArray.indices) {
                info_serialize_stream.writeObject(output, blockInfoArray(i))
                if (output.position() >= reduce_serializer_buffer_size * 0.9) {
                  output.setPosition(startPos)
                  val blockBuffer = ByteBuffer.wrap(output.getBuffer)
                  blockBuffer.position(output.position())
                  blockBuffer.flip()
                  bufferArray += blockBuffer
                  output.close()
                  val new_buffer = new Array[Byte](reduce_serializer_buffer_size.toInt)
                  output = new Output(new_buffer)
                  cur -= 1
                  loop.break()
                }
              }
            }
          }
          cur += 1
        }
      } catch {
        case c: ControlThrowable => throw c
        case t: Throwable => t.printStackTrace()
      }
      if (output.position() != 0) {
        val blockBuffer = ByteBuffer.wrap(output.getBuffer)
        blockBuffer.position(output.position())
        blockBuffer.flip()
        bufferArray += blockBuffer
        output.close()
      }
    }
    bufferArray
  }

  def deserializeShuffleBlockInfo(byteBuffer: ByteBuffer): ArrayBuffer[ShuffleBlockInfo] = {
    val blockInfoArray: ArrayBuffer[ShuffleBlockInfo] = ArrayBuffer[ShuffleBlockInfo]()
    val bais = new ByteBufferInputStream(byteBuffer)
    val input = new Input(bais)
    MetadataResolver.this.synchronized {
      do {
        if (input.available() > 0) {
          val shuffleBlockInfo = info_serialize_stream.readObject(input, classOf[ShuffleBlockInfo])
          blockInfoArray += shuffleBlockInfo
        } else {
          input.close()
          return blockInfoArray
        }
      } while (true)
    }
    null
  }
}
