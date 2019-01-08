package org.apache.spark.storage.pmof

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.{File, IOException, InputStream}
import java.util.concurrent.LinkedBlockingQueue

import javax.annotation.concurrent.GuardedBy
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle.{ShuffleClient, TempFileManager}
import org.apache.spark.network.pmof._
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage._
import org.apache.spark.{SparkException, TaskContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * An iterator that fetches multiple blocks. For local blocks, it fetches from the local block
  * manager. For remote blocks, it fetches them using the provided BlockTransferService.
  *
  * This creates an iterator of (BlockID, InputStream) tuples so the caller can handle blocks
  * in a pipelined fashion as they are received.
  *
  * The implementation throttles the remote fetches so they don't exceed maxBytesInFlight to avoid
  * using too much memory.
  *
  * @param context [[TaskContext]], used for metrics update
  * @param shuffleClient [[ShuffleClient]] for fetching remote blocks
  * @param blockManager [[BlockManager]] for reading local blocks
  * @param blocksByAddress list of blocks to fetch grouped by the [[BlockManagerId]].
  *                        For each block we also require the size (in bytes as a long field) in
  *                        order to throttle the memory usage.
  * @param streamWrapper A function to wrap the returned input stream.
  * @param maxBytesInFlight max size (in bytes) of remote blocks to fetch at any given point.
  * @param maxReqsInFlight max number of remote requests to fetch blocks at any given point.
  * @param maxBlocksInFlightPerAddress max number of shuffle blocks being fetched at any given point
  *                                    for a given remote host:port.
  * @param maxReqSizeShuffleToMem max size (in bytes) of a request that can be shuffled to memory.
  * @param detectCorrupt whether to detect any corruption in fetched blocks.
  */
private[spark]
final class RdmaShuffleBlockFetcherIterator(
                                         context: TaskContext,
                                         shuffleClient: ShuffleClient,
                                         blockManager: BlockManager,
                                         blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])],
                                         streamWrapper: (BlockId, InputStream) => InputStream,
                                         maxBytesInFlight: Long,
                                         maxReqsInFlight: Int,
                                         maxBlocksInFlightPerAddress: Int,
                                         maxReqSizeShuffleToMem: Long,
                                         detectCorrupt: Boolean)
  extends Iterator[(BlockId, InputStream)] with TempFileManager with Logging {

  import RdmaShuffleBlockFetcherIterator._

  /**
    * Total number of blocks to fetch. This can be smaller than the total number of blocks
    * in [[blocksByAddress]] because we filter out zero-sized blocks in [[initialize]].
    *
    * This should equal localBlocks.size + remoteBlocks.size.
    */
  private[this] var numBlocksToFetch = 0

  /**
    * The number of blocks processed by the caller. The iterator is exhausted when
    * [[numBlocksProcessed]] == [[numBlocksToFetch]].
    */
  private[this] var numBlocksProcessed = 0

  /** Local blocks to fetch, excluding zero-sized blocks. */
  private[this] val localBlocks = new ArrayBuffer[BlockId]()

  /**
    * A queue to hold our results. This turns the asynchronous model provided by
    * [[org.apache.spark.network.BlockTransferService]] into a synchronous model (iterator).
    */
  private[this] val results = new LinkedBlockingQueue[FetchResult]

  /**
    * Current [[FetchResult]] being processed. We track this so we can release the current buffer
    * in case of a runtime exception when processing the current buffer.
    */
  @volatile private[this] var currentResult: SuccessFetchResult = _

  /** Current bytes in flight from our requests */
  private[this] var bytesInFlight = 0L

  /** Current number of requests in flight */
  private[this] var reqsInFlight = 0

  /** Current number of blocks in flight per host:port */
  private[this] val numBlocksInFlightPerAddress = new mutable.HashMap[BlockManagerId, Int]()

  private[this] val shuffleMetrics = context.taskMetrics().createTempShuffleReadMetrics()

  /**
    * Whether the iterator is still active. If isZombie is true, the callback interface will no
    * longer place fetched blocks into [[results]].
    */
  @GuardedBy("this")
  private[this] var isZombie = false

  /**
    * A set to store the files used for shuffling remote huge blocks. Files in this set will be
    * deleted when cleanup. This is a layer of defensiveness against disk file leaks.
    */
  @GuardedBy("this")
  private[this] val shuffleFilesSet = mutable.HashSet[File]()

  private[this] val remoteRdmaRequestQueue = new LinkedBlockingQueue[RdmaRequest]()

  initialize()

  def startFetchMetadata(blockManagerId: BlockManagerId, blockIds: Array[BlockId]): Unit = {
    val receivedCallback = new ReceivedCallback {
      override def onSuccess(blockInfoArray: ArrayBuffer[ShuffleBlockInfo]): Unit = {
        val num = blockInfoArray.size
        assert(num >= 1)
        var last: ShuffleBlockInfo = blockInfoArray(0)
        var startIndex = 0
        var reqSize = 0
        for (i <- 0 until num) {
          val current = blockInfoArray(i)
          if (current.getShuffleBlockId != last.getShuffleBlockId) {
            remoteRdmaRequestQueue.put(new RdmaRequest(blockManagerId, last.getShuffleBlockId, blockInfoArray.slice(startIndex, i), reqSize))
            startIndex = i
            reqSize = 0
            Future { fetchRemoteBlocks() }
          }
          last = current
          reqSize += current.getLength
        }
        remoteRdmaRequestQueue.put(new RdmaRequest(blockManagerId, last.getShuffleBlockId, blockInfoArray.slice(startIndex, num), reqSize))
        Future { fetchRemoteBlocks() }
      }

      override def onFailure(e: Throwable): Unit = {

      }
    }

    numBlocksToFetch += blockIds.length

    val rdmaTransferService = shuffleClient.asInstanceOf[RdmaTransferService]
    rdmaTransferService.fetchBlockInfo(blockIds, receivedCallback)
  }

  def sendRequest(rdmaRequest: RdmaRequest): Unit = {
    val shuffleBlockInfos = rdmaRequest.shuffleBlockInfos
    var partitionNums = shuffleBlockInfos.size
    bytesInFlight += rdmaRequest.reqSize
    reqsInFlight += 1
    val blockManagerId = rdmaRequest.blockManagerId
    val shuffleBlockIdName = rdmaRequest.shuffleBlockIdName

    numBlocksInFlightPerAddress(blockManagerId) =
        numBlocksInFlightPerAddress.getOrElse(blockManagerId, 0) + 1

    val rdmaTransferService = shuffleClient.asInstanceOf[RdmaTransferService]

    val blockFetchingReadCallback = new ReadCallback {
      def onSuccess(blockIndex: Int, shuffleBuffer: ShuffleBuffer, f: Int => Unit): Unit = {
        if (!isZombie) {
          RdmaShuffleBlockFetcherIterator.this.synchronized {
            partitionNums -= 1
            if (partitionNums == 0) {
              results.put(SuccessFetchResult(BlockId(shuffleBlockIdName), blockManagerId, rdmaRequest.reqSize, shuffleBuffer, isNetworkReqDone = true))
              f(shuffleBuffer.getRdmaBufferId)
            }
          }
        }
      }

      override def onFailure(blockIndex: Int, e: Throwable): Unit = {
        results.put(FailureFetchResult(BlockId(shuffleBlockIdName), blockManagerId, e))
      }
    }

    val rdmaClient = rdmaTransferService.getClient(blockManagerId.host, blockManagerId.port)
    val shuffleBuffer = new ShuffleBuffer(rdmaRequest.reqSize, rdmaClient.getEqService, true)
    val rdmaBuffer = rdmaClient.getEqService.regRmaBufferByAddress(shuffleBuffer.nioByteBuffer(), shuffleBuffer.getAddress, shuffleBuffer.getLength.toInt)
    shuffleBuffer.setRdmaBufferId(rdmaBuffer.getRdmaBufferId)

    var offset = 0
    for (i <- 0 until partitionNums) {
      rdmaTransferService.fetchBlock(blockManagerId.host, blockManagerId.port, shuffleBlockInfos(i).getAddress, shuffleBlockInfos(i).getLength, shuffleBlockInfos(i).getRkey, offset, shuffleBuffer, rdmaClient, blockFetchingReadCallback)
      offset += shuffleBlockInfos(i).getLength
    }
  }

  def isRemoteBlockFetchable(rdmaRequest: RdmaRequest): Boolean = {
    reqsInFlight + 1 <= maxReqsInFlight && bytesInFlight + rdmaRequest.reqSize <= maxBytesInFlight
  }

  def fetchRemoteBlocks(): Unit = {
    val rdmaRequest = remoteRdmaRequestQueue.poll()
    if (rdmaRequest == null) {
      return
    }
    if (!isRemoteBlockFetchable(rdmaRequest)) {
      remoteRdmaRequestQueue.put(rdmaRequest)
    } else {
      sendRequest(rdmaRequest)
    }
  }

  def startFetch(remoteBlocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])]): Unit = {
    for ((blockManagerId, blockInfos) <- remoteBlocksByAddress) {
      startFetchMetadata(blockManagerId, blockInfos.map(_._1).toArray)
    }
    fetchLocalBlocks()
  }

  def initialize(): Unit = {
    context.addTaskCompletionListener(_ => cleanup())

    val remoteBlocksByAddress = blocksByAddress.filter(_._1.executorId != blockManager.blockManagerId.executorId)
    for ((address, blockInfos) <- blocksByAddress) {
      if (address.executorId == blockManager.blockManagerId.executorId) {
        localBlocks ++= blockInfos.filter(_._2 != 0).map(_._1)
        numBlocksToFetch += localBlocks.size
      }
    }

    startFetch(remoteBlocksByAddress)
  }

  // Decrements the buffer reference count.
  // The currentResult is set to null to prevent releasing the buffer again on cleanup()
  private[storage] def releaseCurrentResultBuffer(): Unit = {
    // Release the current buffer if necessary
    if (currentResult != null) {
      currentResult.buf.release()
    }
    currentResult = null
  }

  override def createTempFile(): File = {
    blockManager.diskBlockManager.createTempLocalBlock()._2
  }

  override def registerTempFileToClean(file: File): Boolean = synchronized {
    if (isZombie) {
      false
    } else {
      shuffleFilesSet += file
      true
    }
  }

  /**
    * Mark the iterator as zombie, and release all buffers that haven't been deserialized yet.
    */
  private[this] def cleanup() {
    synchronized {
      isZombie = true
    }
    releaseCurrentResultBuffer()
    // Release buffers in the results queue
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(_, address, _, buf, _) =>
          if (address != blockManager.blockManagerId) {
            shuffleMetrics.incRemoteBytesRead(buf.size)
            if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
              shuffleMetrics.incRemoteBytesReadToDisk(buf.size)
            }
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          buf.release()
        case _ =>
      }
    }
    shuffleFilesSet.foreach { file =>
      if (!file.delete()) {
        logWarning("Failed to cleanup shuffle fetch temp file " + file.getAbsolutePath)
      }
    }
  }

  /**
    * Fetch the local blocks while we are fetching remote blocks. This is ok because
    * `ManagedBuffer`'s memory is allocated lazily when we create the input stream, so all we
    * track in-memory are the ManagedBuffer references themselves.
    */
  private[this] def fetchLocalBlocks() {
    val iter = localBlocks.iterator
    while (iter.hasNext) {
      val blockId = iter.next()
      try {
        val buf = blockManager.getBlockData(blockId)
        shuffleMetrics.incLocalBlocksFetched(1)
        shuffleMetrics.incLocalBytesRead(buf.size)
        buf.retain()
        results.put(SuccessFetchResult(blockId, blockManager.blockManagerId, 0, buf, isNetworkReqDone = false))
      } catch {
        case e: Exception =>
          // If we see an exception, stop immediately.
          logError(s"Error occurred while fetching local blocks", e)
          results.put(FailureFetchResult(blockId, blockManager.blockManagerId, e))
          return
      }
    }
  }

  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch

  /**
    * Fetches the next (BlockId, InputStream). If a task fails, the ManagedBuffers
    * underlying each InputStream will be freed by the cleanup() method registered with the
    * TaskCompletionListener. However, callers should close() these InputStreams
    * as soon as they are no longer needed, in order to release memory as early as possible.
    *
    * Throws a FetchFailedException if the next block could not be fetched.
    */
  override def next(): (BlockId, InputStream) = {
    if (!hasNext) {
      throw new NoSuchElementException
    }

    numBlocksProcessed += 1

    var result: FetchResult = null
    var input: InputStream = null
    // Take the next fetched result and try to decompress it to detect data corruption,
    // then fetch it one more time if it's corrupt, throw FailureFetchResult if the second fetch
    // is also corrupt, so the previous stage could be retried.
    // For local shuffle block, throw FailureFetchResult for the first IOException.
    while (result == null) {
      val startFetchWait = System.currentTimeMillis()
      result = results.take()
      val stopFetchWait = System.currentTimeMillis()
      shuffleMetrics.incFetchWaitTime(stopFetchWait - startFetchWait)

      result match {
        case SuccessFetchResult(blockId, address, size, buf, isNetworkReqDone) =>
          if (address != blockManager.blockManagerId) {
            if (numBlocksInFlightPerAddress.contains(address))
              numBlocksInFlightPerAddress(address) = numBlocksInFlightPerAddress(address) - 1
            shuffleMetrics.incRemoteBytesRead(buf.size)
            if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
              shuffleMetrics.incRemoteBytesReadToDisk(buf.size)
            }
            shuffleMetrics.incRemoteBlocksFetched(1)
            logDebug("take remote block.")
          }
          bytesInFlight -= size
          if (isNetworkReqDone) {
            reqsInFlight -= 1
          }

          val in = try {
            buf.createInputStream()
          } catch {
            // The exception could only be throwed by local shuffle block
            case e: IOException =>
              assert(buf.isInstanceOf[FileSegmentManagedBuffer])
              logError("Failed to create input stream from local block", e)
              buf.release()
              throwFetchFailedException(blockId, address, e)
          }

          input = streamWrapper(blockId, in)
          // Only copy the stream if it's wrapped by compression or encryption, also the size of
          // block is small (the decompressed block is smaller than maxBytesInFlight)
        case FailureFetchResult(blockId, address, e) =>
          throwFetchFailedException(blockId, address, e)
      }

      // Send fetch requests up to maxBytesInFlight
      Future { fetchRemoteBlocks() }
    }

    currentResult = result.asInstanceOf[SuccessFetchResult]
    (currentResult.blockId, new RDMABufferReleasingInputStream(input, this))
  }

  def isRemoteAddressMaxedOut(remoteAddress: BlockManagerId, request: FetchRequest): Boolean = {
    numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size >
      maxBlocksInFlightPerAddress
  }

  private def throwFetchFailedException(blockId: BlockId, address: BlockManagerId, e: Throwable) = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, e)
      case _ =>
        throw new SparkException(
          "Failed to get block " + blockId + ", which is not a shuffle block", e)
    }
  }
}

private class RdmaRequest(val blockManagerId: BlockManagerId, val shuffleBlockIdName: String, val shuffleBlockInfos: ArrayBuffer[ShuffleBlockInfo], val reqSize: Int) {}

/**
  * Helper class that ensures a ManagedBuffer is released upon InputStream.close()
  */
private class RDMABufferReleasingInputStream(
                                          private val delegate: InputStream,
                                          private val iterator: RdmaShuffleBlockFetcherIterator)
  extends InputStream {
  private[this] var closed = false

  override def read(): Int = delegate.read()

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      iterator.releaseCurrentResultBuffer()
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long = delegate.skip(n)

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = delegate.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = delegate.read(b, off, len)

  override def reset(): Unit = delegate.reset()
}

private[storage]
object RdmaShuffleBlockFetcherIterator {

  /**
    * A request to fetch blocks from a remote BlockManager.
    * @param address remote BlockManager to fetch from.
    * @param blocks Sequence of tuple, where the first element is the block id,
    *               and the second element is the estimated size, used to calculate bytesInFlight.
    */
  case class FetchRequest(address: BlockManagerId, blocks: Seq[(BlockId, Long)]) {
    val size: Long = blocks.map(_._2).sum
  }

  /**
    * Result of a fetch from a remote block.
    */
  private[storage] sealed trait FetchResult {
    val blockId: BlockId
    val address: BlockManagerId
  }

  /**
    * Result of a fetch from a remote block successfully.
    * @param blockId block id
    * @param address BlockManager that the block was fetched from.
    * @param size estimated size of the block, used to calculate bytesInFlight.
    *             Note that this is NOT the exact bytes.
    * @param buf `ManagedBuffer` for the content.
    * @param isNetworkReqDone Is this the last network request for this host in this fetch request.
    */
  private[storage] case class SuccessFetchResult(
                                                  blockId: BlockId,
                                                  address: BlockManagerId,
                                                  size: Long,
                                                  buf: ManagedBuffer,
                                                  isNetworkReqDone: Boolean) extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }

  /**
    * Result of a fetch from a remote block unsuccessfully.
    * @param blockId block id
    * @param address BlockManager that the block was attempted to be fetched from
    * @param e the failure exception
    */
  private[storage] case class FailureFetchResult(
                                                  blockId: BlockId,
                                                  address: BlockManagerId,
                                                  e: Throwable)
    extends FetchResult
}
