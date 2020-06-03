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
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import javax.annotation.concurrent.GuardedBy
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.pmof._
import org.apache.spark.network.shuffle.{ShuffleClient, TempFileManager}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage._
import org.apache.spark.util.configuration.pmof.PmofConf
import org.apache.spark.{SparkException, TaskContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBufferOutputStream
import java.nio.ByteBuffer
import io.netty.buffer.{ByteBuf, ByteBufInputStream, ByteBufOutputStream}
import io.netty.buffer.Unpooled

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
 * @param context                     [[TaskContext]], used for metrics update
 * @param blockManager                [[BlockManager]] for reading local blocks
 * @param blocksByAddress             list of blocks to fetch grouped by the [[BlockManagerId]].
 *                                    For each block we also require the size (in bytes as a long field) in
 *                                    order to throttle the memory usage.
 * @param streamWrapper               A function to wrap the returned input stream.
 * @param maxBytesInFlight            max size (in bytes) of remote blocks to fetch at any given point.
 * @param maxReqsInFlight             max number of remote requests to fetch blocks at any given point.
 * @param maxBlocksInFlightPerAddress max number of shuffle blocks being fetched at any given point
 *                                    for a given remote host:port.
 * @param maxReqSizeShuffleToMem      max size (in bytes) of a request that can be shuffled to memory.
 * @param detectCorrupt               whether to detect any corruption in fetched blocks.
 */
private[spark] final class RpmpShuffleBlockFetcherIterator(
    context: TaskContext,
    blockManager: BlockManager,
    blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])],
    streamWrapper: (BlockId, InputStream) => InputStream,
    maxBytesInFlight: Long,
    maxReqsInFlight: Int,
    maxBlocksInFlightPerAddress: Int,
    maxReqSizeShuffleToMem: Long,
    detectCorrupt: Boolean,
    pmofConf: PmofConf)
    extends Iterator[(BlockId, InputStream)]
    with TempFileManager
    with Logging {

  import RpmpShuffleBlockFetcherIterator._

  /** Local blocks to fetch, excluding zero-sized blocks. */
  private[this] val localBlocks = new ArrayBuffer[BlockId]()

  /**
   * A queue to hold our results. This turns the asynchronous model provided by
   * [[org.apache.spark.network.BlockTransferService]] into a synchronous model (iterator).
   */
  private[this] val results = new LinkedBlockingQueue[FetchResult]
  private[this] val shuffleMetrics =
    context.taskMetrics().createTempShuffleReadMetrics()

  /**
   * A set to store the files used for shuffling remote huge blocks. Files in this set will be
   * deleted when cleanup. This is a layer of defensiveness against disk file leaks.
   */
  @GuardedBy("this")
  private[this] val shuffleFilesSet = mutable.HashSet[File]()

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

  private[this] val numRemoteBlockToFetch = new AtomicInteger(0)
  private[this] val numRemoteBlockProcessing = new AtomicInteger(0)
  private[this] val numRemoteBlockProcessed = new AtomicInteger(0)

  /**
   * Current [[FetchResult]] being processed. We track this so we can release the current buffer
   * in case of a runtime exception when processing the current buffer.
   */
  @volatile private[this] var currentResult: SuccessFetchResult = _

  /** Current bytes in flight from our requests */
  private[this] val bytesInFlight = new AtomicLong(0)

  /** Current number of requests in flight */
  private[this] val reqsInFlight = new AtomicInteger(0)

  /**
   * Whether the iterator is still active. If isZombie is true, the callback interface will no
   * longer place fetched blocks into [[results]].
   */
  @GuardedBy("this")
  private[this] var isZombie = false

  private[this] var blocksByAddressSize = 0
  private[this] var blocksByAddressCurrentId = 0
  private[this] var address: BlockManagerId = _
  private[this] var blockInfos: Seq[(BlockId, Long)] = _
  private[this] var iterator: Iterator[(BlockId, Long)] = _

  initialize()

  def initialize(): Unit = {
    context.addTaskCompletionListener(_ => cleanup())
    blocksByAddressSize = blocksByAddress.size
    if (blocksByAddressCurrentId < blocksByAddressSize) {
      val res = blocksByAddress(blocksByAddressCurrentId)
      address = res._1
      blockInfos = res._2
      iterator = blockInfos.iterator
      blocksByAddressCurrentId += 1
    }
  }

  val remotePersistentMemoryPool =
    RemotePersistentMemoryPool.getInstance(pmofConf.rpmpHost, pmofConf.rpmpPort)

  /**
   * Fetches the next (BlockId, InputStream). If a task fails, the ManagedBuffers
   * underlying each InputStream will be freed by the cleanup() method registered with the
   * TaskCompletionListener. However, callers should close() these InputStreams
   * as soon as they are no longer needed, in order to release memory as early as possible.
   *
   * Throws a FetchFailedException if the next block could not be fetched.
   */
  private[this] var next_called: Boolean = true
  private[this] var has_next: Boolean = false
  override def next(): (BlockId, InputStream) = {
    next_called = true
    var input: InputStream = null
    val (blockId, size) = iterator.next()
    var buf = new NioManagedBuffer(size.toInt)
    val startFetchWait = System.currentTimeMillis()
    val readed_len = remotePersistentMemoryPool.get(blockId.name, size, buf.nioByteBuffer)
    if (readed_len != -1) {
      val stopFetchWait = System.currentTimeMillis()
      shuffleMetrics.incFetchWaitTime(stopFetchWait - startFetchWait)
      shuffleMetrics.incRemoteBytesRead(buf.size)
      shuffleMetrics.incRemoteBlocksFetched(1)

      val in = buf.createInputStream()
      input = streamWrapper(blockId, in)
      if (detectCorrupt && !input.eq(in)) {
        val originalInput = input
        val out = new ChunkedByteBufferOutputStream(64 * 1024, ByteBuffer.allocate)
        try {
          Utils.copyStream(input, out)
          out.close()
          input = out.toChunkedByteBuffer.toInputStream(dispose = true)
          logDebug(
            s"[GET] buf ${blockId}-${size} decompress succeeded, input is ${input}, ${NettyByteBufferPool
              .dump(buf.nioByteBuffer)}")
        } catch {
          case e: IOException =>
            logWarning(
              s" buf ${blockId}-${size} decompress corrupted, input is ${input}, ${NettyByteBufferPool
                .dump(buf.nioByteBuffer)}")
            throw e

        } finally {
          originalInput.close()
          in.close()
          buf.release()
        }
      }
    } else {
      throw new IOException(
        s"remotePersistentMemoryPool.get(${blockId}, ${size}) failed due to timeout.");
    }
    (blockId, new RpmpBufferReleasingInputStream(input, null))
  }

  override def hasNext: Boolean = {
    if (!next_called) {
      return has_next
    }
    next_called = false
    if (iterator.hasNext) {
      has_next = true
    } else {
      if (blocksByAddressCurrentId >= blocksByAddressSize) {
        has_next = false
        return has_next
      }
      val res = blocksByAddress(blocksByAddressCurrentId)
      address = res._1
      blockInfos = res._2
      iterator = blockInfos.iterator
      blocksByAddressCurrentId += 1
      has_next = true
    }
    return has_next
  }

  /**
   * Mark the iterator as zombie, and release all buffers that haven't been deserialized yet.
   */
  private[this] def cleanup() {
    synchronized {
      isZombie = true
    }
  }

  override def registerTempFileToClean(file: File): Boolean = synchronized {
    if (isZombie) {
      false
    } else {
      shuffleFilesSet += file
      true
    }
  }

  private def throwFetchFailedException(
      blockId: BlockId,
      address: BlockManagerId,
      e: Throwable) = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, e)
      case _ =>
        throw new SparkException(
          "Failed to get block " + blockId + ", which is not a shuffle block",
          e)
    }
  }
  override def createTempFile(): File = {
    blockManager.diskBlockManager.createTempLocalBlock()._2
  }

}

/**
 * Helper class that ensures a ManagedBuffer is released upon InputStream.close()
 */
private class RpmpBufferReleasingInputStream(
    private val delegate: InputStream,
    private val parent: NioManagedBuffer)
    extends InputStream {
  private[this] var closed = false

  override def read(): Int = delegate.read()

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      if (parent != null) {
        parent.release()
      }
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long = delegate.skip(n)

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = delegate.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int =
    delegate.read(b, off, len)

  override def reset(): Unit = delegate.reset()
}

private[storage] object RpmpShuffleBlockFetcherIterator {

  /**
   * Result of a fetch from a remote block.
   */
  private[storage] sealed trait FetchResult {
    val blockId: BlockId
    val address: BlockManagerId
  }

  /**
   * A request to fetch blocks from a remote BlockManager.
   *
   * @param address remote BlockManager to fetch from.
   * @param blocks  Sequence of tuple, where the first element is the block id,
   *                and the second element is the estimated size, used to calculate bytesInFlight.
   */
  case class FetchRequest(address: BlockManagerId, blocks: Seq[(BlockId, Long)]) {
    val size: Long = blocks.map(_._2).sum
  }

  /**
   * Result of a fetch from a remote block successfully.
   *
   * @param blockId          block id
   * @param address          BlockManager that the block was fetched from.
   * @param size             estimated size of the block, used to calculate bytesInFlight.
   *                         Note that this is NOT the exact bytes.
   * @param buf              `ManagedBuffer` for the content.
   * @param isNetworkReqDone Is this the last network request for this host in this fetch request.
   */
  private[storage] case class SuccessFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      size: Long,
      buf: ManagedBuffer,
      isNetworkReqDone: Boolean)
      extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }

  /**
   * Result of a fetch from a remote block unsuccessfully.
   *
   * @param blockId block id
   * @param address BlockManager that the block was attempted to be fetched from
   * @param e       the failure exception
   */
  private[storage] case class FailureFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      e: Throwable)
      extends FetchResult

}
