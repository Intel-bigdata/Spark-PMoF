package org.apache.spark.network.pmof

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingDeque}

import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.shuffle.protocol.{BlockTransferMessage, OpenBlocks}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.BlockId
import com.intel.hpnl.core._
import org.apache.spark.SparkConf
import org.apache.spark.network.buffer.FileSegmentManagedBuffer

class RdmaServer(conf: SparkConf, address: String, var port: Int) {
  if (port == 0) {
    port = Utils.getPort
  }
  final val workers = conf.getInt("spark.shuffle.pmof.server_pool_size", 1)
  final val eqService = new EqService(address, port.toString, true)
  final val cqService = new CqService(eqService, workers, eqService.getNativeHandle)

  final val SINGLE_BUFFER_SIZE = RdmaTransferService.CHUNKSIZE
  final val BUFFER_NUM = conf.getInt("spark.shuffle.pmof.server_buffer_nums", 1024)

  val shuffleBufferMap: ConcurrentHashMap[Long, ShuffleBuffer] = new ConcurrentHashMap[Long, ShuffleBuffer]()

  def init(): Unit = {
    for (i <- 0 until BUFFER_NUM) {
      val sendBuffer = ByteBuffer.allocateDirect(SINGLE_BUFFER_SIZE)
      eqService.setSendBuffer(sendBuffer, SINGLE_BUFFER_SIZE, i)
    }
    for (i <- 0 until BUFFER_NUM*2) {
      val recvBuffer = ByteBuffer.allocateDirect(SINGLE_BUFFER_SIZE)
      eqService.setRecvBuffer(recvBuffer, SINGLE_BUFFER_SIZE, i)
    }
  }

  def start(): Unit = {
    cqService.start()
    eqService.start(1)
  }

  def stop(): Unit = {
    cqService.shutdown()
  }

  def waitToStop(): Unit = {
    cqService.join()
    eqService.shutdown()
    eqService.join()
  }

  def setRecvHandler(handler: Handler): Unit = {
    eqService.setRecvCallback(handler)
    cqService.addExternalEvent(new ExternalHandler {
      override def handle(): Unit = {
        handler.asInstanceOf[ServerRecvHandler] handleDeferredReq()
      }
    })
  }

  def getEqService: EqService = {
    eqService
  }
}

class ServerRecvHandler(server: RdmaServer, appid: String, serializer: Serializer,
                        blockManager: BlockDataManager) extends Handler {

  private val deferredBufferList = new LinkedBlockingDeque[ServerDeferredReq]()

  def sendMetadata(con: Connection, shuffleBuffers: Array[ShuffleBuffer], seq: Int, isDeferred: Boolean): Unit = {
    val sendBuffer = con.getSendBuffer(false)
    if (sendBuffer == null) {
      if (isDeferred) {
        deferredBufferList.addFirst(new ServerDeferredReq(con, shuffleBuffers, seq))
      } else {
        deferredBufferList.addLast(new ServerDeferredReq(con, shuffleBuffers, seq))
      }
      return
    }
    val blocksNum = shuffleBuffers.length
    val byteBufferTmp = ByteBuffer.allocate(20*blocksNum+4)

    byteBufferTmp.putInt(blocksNum)
    for (i <- 0 until blocksNum) {
      byteBufferTmp.putInt(shuffleBuffers(i).size.toInt)
      byteBufferTmp.putLong(shuffleBuffers(i).getAddress)
      byteBufferTmp.putLong(shuffleBuffers(i).getRkey)
      server.shuffleBufferMap.put(shuffleBuffers(i).getAddress, shuffleBuffers(i))
    }
    byteBufferTmp.flip()
    sendBuffer.put(byteBufferTmp, 0.toByte, 0, seq)
    con.send(sendBuffer.remaining(), sendBuffer.getRdmaBufferId)
  }

  def handleDeferredReq(): Unit = {
    val deferredReq = deferredBufferList.pollFirst
    if (deferredReq == null) return
    val con = deferredReq.con
    val seq = deferredReq.seq
    sendMetadata(con, deferredReq.shuffleBuffers, seq, isDeferred = true)
  }

  def convertToShuffleBuffer(file: File, offset: Long, length: Long): ShuffleBuffer = {
    var shuffleBuffer: ShuffleBuffer = null
    val channel: FileChannel = new RandomAccessFile(file, "rw").getChannel
    if (false) {
      shuffleBuffer = new ShuffleBuffer(offset, length, channel, server.getEqService)
    } else {
      shuffleBuffer = new ShuffleBuffer(length, server.getEqService)
      channel.position(offset)
      while (shuffleBuffer.nioByteBuffer().remaining() != 0) {
        if (channel.read(shuffleBuffer.nioByteBuffer()) == -1) {
          shuffleBuffer.nioByteBuffer().flip()
        }
      }
      channel.close()
    }
    val rdmaBuffer = server.getEqService.regRmaBufferByAddress(shuffleBuffer.nioByteBuffer(), shuffleBuffer.getAddress, length.toInt)
    shuffleBuffer.setRdmaBufferId(rdmaBuffer.getRdmaBufferId)
    shuffleBuffer.setRkey(rdmaBuffer.getRKey)
    shuffleBuffer
  }

  override def handle(con: Connection, rdmaBufferId: Int, blockBufferSize: Int): Unit = {

    val buffer: RdmaBuffer = con.getRecvBuffer(rdmaBufferId)
    val serializedMessage: ByteBuffer = buffer.get(blockBufferSize)
    val seq = buffer.getSeq
    val msgType = buffer.getType
    if (msgType == 0.toByte) {
      val message = BlockTransferMessage.Decoder.fromByteBuffer(serializedMessage)
      val openBlocks = message.asInstanceOf[OpenBlocks]

      val blocksNum = openBlocks.blockIds.length
      var shuffleBuffers = new Array[ShuffleBuffer](blocksNum)
      for (i <- (0 until blocksNum).view) {
        val managedBuffer = blockManager.getBlockData(BlockId.apply(openBlocks.blockIds(i))).asInstanceOf[FileSegmentManagedBuffer]
        shuffleBuffers(i) = convertToShuffleBuffer(managedBuffer.getFile, managedBuffer.getOffset, managedBuffer.getLength)
      }
      sendMetadata(con, shuffleBuffers, seq, isDeferred = false)
      shuffleBuffers = null
    } else {
      val rmaAddress = serializedMessage.getLong()
      val shuffleBuffer = server.shuffleBufferMap.get(rmaAddress)
      shuffleBuffer.close
      server.shuffleBufferMap.remove(rmaAddress)
    }
  }
}

class ServerDeferredReq(var con: Connection, var shuffleBuffers: Array[ShuffleBuffer], var seq: Int) {}
