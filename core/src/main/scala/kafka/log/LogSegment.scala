/**
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
package kafka.log

import java.io.{File, IOException}
import java.nio.file.{Files, NoSuchFileException}
import java.nio.file.attribute.FileTime
import java.util.concurrent.TimeUnit

import kafka.common.LogSegmentOffsetOverflowException
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.server.epoch.LeaderEpochFileCache
import kafka.server.{FetchDataInfo, LogOffsetMetadata}
import kafka.utils._
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.record.FileRecords.{LogOffsetPosition, TimestampAndOffset}
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Time

import scala.jdk.CollectionConverters._
import scala.math._

/**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileRecords containing
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
 * any previous segment.
 *
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file.
 *
 * 每个日志段由两个核心组件构成：日志和索引。当然，这里的索引泛指广义的索引文件。
 * 另外，这段注释还给出了一个重要的事实：每个日志段都有一个起始位移值（Base Offset），
 * 而该位移值是此日志段所有消息中最小的位移值，同时，该值却又比前面任何日志段中消息的位移值都大。
 *
 * @param log The file records containing log entries
 *            消息日志文件
 * @param lazyOffsetIndex The offset index
 *                        位移索引文件
 * @param lazyTimeIndex The timestamp index
 *                      时间戳索引文件
 * @param txnIndex The transaction index
 *                 已中止事务索引文件
 * @param baseOffset A lower bound on the offsets in this segment
 *                   起始位移 每个 LogSegment 对象实例一旦被创建，它的起始位移就是固定的了，不能再被更改。
 * @param indexIntervalBytes The approximate number of bytes between entries in the index
 *                           它控制了日志段对象新增索引项的频率 日志段至少新写入 4KB 的消息数据才会新增一条索引项
 * @param rollJitterMs The maximum random jitter subtracted from the scheduled segment roll time
 *                     日志段对象新增倒计时的“扰动值” 可以缓解物理磁盘的 I/O 负载瓶颈
 * @param time The time instance
 *             用于统计计时的一个实现类
 */
@nonthreadsafe
class LogSegment private[log] (val log: FileRecords,
                               val lazyOffsetIndex: LazyIndex[OffsetIndex],
                               val lazyTimeIndex: LazyIndex[TimeIndex],
                               val txnIndex: TransactionIndex,
                               val baseOffset: Long,
                               val indexIntervalBytes: Int,
                               val rollJitterMs: Long,
                               val time: Time) extends Logging {

  def offsetIndex: OffsetIndex = lazyOffsetIndex.get

  def timeIndex: TimeIndex = lazyTimeIndex.get

  // 扰动值状态
  def shouldRoll(rollParams: RollParams): Boolean = {
    val reachedRollMs = timeWaitedForRoll(rollParams.now, rollParams.maxTimestampInMessages) > rollParams.maxSegmentMs - rollJitterMs
    size > rollParams.maxSegmentBytes - rollParams.messagesSize ||
      (size > 0 && reachedRollMs) ||
      offsetIndex.isFull || timeIndex.isFull || !canConvertToRelativeOffset(rollParams.maxOffsetInMessages)
  }

  def resizeIndexes(size: Int): Unit = {
    offsetIndex.resize(size)
    timeIndex.resize(size)
  }

  def sanityCheck(timeIndexFileNewlyCreated: Boolean): Unit = {
    // 位移索引文件是否存在
    if (lazyOffsetIndex.file.exists) {
      // Resize the time index file to 0 if it is newly created.
      // .timeindex 如果时间索引文件是新创建的，则将其大小调整为0。
      if (timeIndexFileNewlyCreated)
        timeIndex.resize(0)
      // Sanity checks for time index and offset index are skipped because
      // we will recover the segments above the recovery point in recoverLog()
      // in any case so sanity checking them here is redundant.
      //忽略时间索引和偏移量索引的完整性检查，因为
      //我们将在recoverLog()中恢复恢复点以上的日志段
      //在任何情况下，完整性检查在这里是多余的。
      txnIndex.sanityCheck()
    }
    else throw new NoSuchFileException(s"Offset index file ${lazyOffsetIndex.file.getAbsolutePath} does not exist")
  }

  private var created = time.milliseconds

  /* the number of bytes since we last added an entry in the offset index */
  // 自上次在偏移索引中添加条目以来的字节数
  private var bytesSinceLastIndexEntry = 0

  // The timestamp we used for time based log rolling and for ensuring max compaction delay
  // 用于基于时间的日志滚动和确保最大压缩延迟的时间戳
  // volatile for LogCleaner to see the update
  // LogCleaner用于查看更新的volatile
  @volatile private var rollingBasedTimestamp: Option[Long] = None

  /* The maximum timestamp we see so far */
  // 到目前为止我们看到的最大时间戳
  @volatile private var _maxTimestampSoFar: Option[Long] = None
  def maxTimestampSoFar_=(timestamp: Long): Unit = _maxTimestampSoFar = Some(timestamp)
  def maxTimestampSoFar: Long = {
    if (_maxTimestampSoFar.isEmpty)
      _maxTimestampSoFar = Some(timeIndex.lastEntry.timestamp)
    _maxTimestampSoFar.get
  }

  // 所属消息位移
  @volatile private var _offsetOfMaxTimestampSoFar: Option[Long] = None
  def offsetOfMaxTimestampSoFar_=(offset: Long): Unit = _offsetOfMaxTimestampSoFar = Some(offset)
  def offsetOfMaxTimestampSoFar: Long = {
    if (_offsetOfMaxTimestampSoFar.isEmpty)
      _offsetOfMaxTimestampSoFar = Some(timeIndex.lastEntry.offset)
    _offsetOfMaxTimestampSoFar.get
  }

  /* Return the size in bytes of this log segment */
  def size: Int = log.sizeInBytes()

  /**
   * checks that the argument offset can be represented as an integer offset relative to the baseOffset.
   * 检查参数偏移量是否可以表示为相对于基偏移量的整数偏移量。
   */
  def canConvertToRelativeOffset(offset: Long): Boolean = {
    offsetIndex.canAppendOffset(offset)
  }

  /**
   * Append the given messages starting with the given offset. Add
   * an entry to the index if needed.
   *
   * 以给定的偏移量开始附加给定的消息。如果需要，向索引添加一个条目。
   *
   * It is assumed this method is being called from within a lock.
   *
   * 假定这个方法是从锁中调用的。
   *
   * @param largestOffset The last offset in the message set
   *                      最大位移值
   * @param largestTimestamp The largest timestamp in the message set.
   *                         最大时间戳
   * @param shallowOffsetOfMaxTimestamp The offset of the message that has the largest timestamp in the messages to append.
   *                                    最大时间戳对应消息的位移
   * @param records The log entries to append.
   *                真正要写入的消息集合
   * @return the physical position in the file of the appended records
   *         附加记录在文件中的物理位置
   * @throws LogSegmentOffsetOverflowException if the largest offset causes index offset overflow
   *                                           如果最大的偏移量导致索引偏移量溢出
   */
  @nonthreadsafe
  def append(largestOffset: Long,
             largestTimestamp: Long,
             shallowOffsetOfMaxTimestamp: Long,
             records: MemoryRecords): Unit = {
    if (records.sizeInBytes > 0) {
      trace(s"Inserting ${records.sizeInBytes} bytes at end offset $largestOffset at position ${log.sizeInBytes} " +
            s"with largest timestamp $largestTimestamp at shallow offset $shallowOffsetOfMaxTimestamp")
      // 消息日志文件,这些记录的大小，以字节为单位。
      // 首先调用 log.sizeInBytes 方法判断该日志段是否为空，如果是空的话，
      // Kafka 需要记录要写入消息集合的最大时间戳，并将其作为后面新增日志段倒计时的依据。
      val physicalPosition = log.sizeInBytes()
      // 判断日志是否为空
      if (physicalPosition == 0)
        // 更新用户日志段切分的时间戳
        rollingBasedTimestamp = Some(largestTimestamp)

      // 确保输入参数最大位移值是合法的
      ensureOffsetInRange(largestOffset)

      // append the messages
      // todo: append 方法调用 FileRecords 的 append 方法执行真正的写入
      val appendedBytes = log.append(records)
      trace(s"Appended $appendedBytes to ${log.file} at end offset $largestOffset")
      // Update the in memory max timestamp and corresponding offset.
      // 更新日志段的最大时间戳以及最大时间戳所属消息的位移值属性
      if (largestTimestamp > maxTimestampSoFar) {
        maxTimestampSoFar = largestTimestamp
        offsetOfMaxTimestampSoFar = shallowOffsetOfMaxTimestamp
      }
      // append an entry to the index (if needed)
      // 是否需要新增索引项
      if (bytesSinceLastIndexEntry > indexIntervalBytes) {
        // todo: 写入相对位移值和物理文件位置
        offsetIndex.append(largestOffset, physicalPosition)
        timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar)
        // 是： 新增索引项
        bytesSinceLastIndexEntry = 0
      }
      // 否：更新已写入字节数
      bytesSinceLastIndexEntry += records.sizeInBytes
    }
  }

  private def ensureOffsetInRange(offset: Long): Unit = {
    // 检查参数偏移量是否可以表示为相对于基偏移量的整数偏移量。
    if (!canConvertToRelativeOffset(offset))
      // 能走到这,说明 offset - baseOffset < 0 返回None
      throw new LogSegmentOffsetOverflowException(this, offset)
  }

  private def appendChunkFromFile(records: FileRecords, position: Int, bufferSupplier: BufferSupplier): Int = {
    var bytesToAppend = 0
    var maxTimestamp = Long.MinValue
    var offsetOfMaxTimestamp = Long.MinValue
    var maxOffset = Long.MinValue
    var readBuffer = bufferSupplier.get(1024 * 1024)

    def canAppend(batch: RecordBatch) =
      canConvertToRelativeOffset(batch.lastOffset) &&
        (bytesToAppend == 0 || bytesToAppend + batch.sizeInBytes < readBuffer.capacity)

    // find all batches that are valid to be appended to the current log segment and
    // determine the maximum offset and timestamp
    val nextBatches = records.batchesFrom(position).asScala.iterator
    for (batch <- nextBatches.takeWhile(canAppend)) {
      if (batch.maxTimestamp > maxTimestamp) {
        maxTimestamp = batch.maxTimestamp
        offsetOfMaxTimestamp = batch.lastOffset
      }
      maxOffset = batch.lastOffset
      bytesToAppend += batch.sizeInBytes
    }

    if (bytesToAppend > 0) {
      // Grow buffer if needed to ensure we copy at least one batch
      if (readBuffer.capacity < bytesToAppend)
        readBuffer = bufferSupplier.get(bytesToAppend)

      readBuffer.limit(bytesToAppend)
      records.readInto(readBuffer, position)

      append(maxOffset, maxTimestamp, offsetOfMaxTimestamp, MemoryRecords.readableRecords(readBuffer))
    }

    bufferSupplier.release(readBuffer)
    bytesToAppend
  }

  /**
   * Append records from a file beginning at the given position until either the end of the file
   * is reached or an offset is found which is too large to convert to a relative offset for the indexes.
   *
   * @return the number of bytes appended to the log (may be less than the size of the input if an
   *         offset is encountered which would overflow this segment)
   */
  def appendFromFile(records: FileRecords, start: Int): Int = {
    var position = start
    val bufferSupplier: BufferSupplier = new BufferSupplier.GrowableBufferSupplier
    while (position < start + records.sizeInBytes) {
      val bytesAppended = appendChunkFromFile(records, position, bufferSupplier)
      if (bytesAppended == 0)
        return position - start
      position += bytesAppended
    }
    position - start
  }

  @nonthreadsafe
  def updateTxnIndex(completedTxn: CompletedTxn, lastStableOffset: Long): Unit = {
    if (completedTxn.isAborted) {
      trace(s"Writing aborted transaction $completedTxn to transaction index, last stable offset is $lastStableOffset")
      txnIndex.append(new AbortedTxn(completedTxn, lastStableOffset))
    }
  }

  private def updateProducerState(producerStateManager: ProducerStateManager, batch: RecordBatch): Unit = {
    if (batch.hasProducerId) {
      val producerId = batch.producerId
      val appendInfo = producerStateManager.prepareUpdate(producerId, origin = AppendOrigin.Replication)
      val maybeCompletedTxn = appendInfo.append(batch, firstOffsetMetadataOpt = None)
      producerStateManager.update(appendInfo)
      maybeCompletedTxn.foreach { completedTxn =>
        val lastStableOffset = producerStateManager.lastStableOffset(completedTxn)
        updateTxnIndex(completedTxn, lastStableOffset)
        producerStateManager.completeTxn(completedTxn)
      }
    }
    producerStateManager.updateMapEndOffset(batch.lastOffset + 1)
  }

  /**
   * Find the physical file position for the first message with offset >= the requested offset.
   *
   * The startingFilePosition argument is an optimization that can be used if we already know a valid starting position
   * in the file higher than the greatest-lower-bound from the index.
   *
   * @param offset The offset we want to translate
   * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and
   * when omitted, the search will begin at the position in the offset index.
   * @return The position in the log storing the message with the least offset >= the requested offset and the size of the
    *        message or null if no message meets this criteria.
   */
  @threadsafe
  private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): LogOffsetPosition = {
    val mapping = offsetIndex.lookup(offset)
    log.searchForOffsetWithSize(offset, max(mapping.position, startingFilePosition))
  }

  /**
   * Read a message set from this segment beginning with the first offset >= startOffset. The message set will include
   * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
   *
   * 从这个段从第一个 offset>= startOffset开始读取消息集。
   * 消息集将不包含超过maxSize字节，并且如果指定了maxOffset，消息集将在maxOffset之前结束。
   *
   * @param startOffset A lower bound on the first offset to include in the message set we read
   *                    要读取的第一条消息的位移；
   * @param maxSize The maximum number of bytes to include in the message set we read
   *                能读取的最大字节数；
   * @param maxPosition The maximum position in the log segment that should be exposed for read
   *                    能读到的最大文件位置；
   * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxSize` (if one exists)
   *                      是否允许在消息体过大时至少返回第一条消息。
   *
   * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
   *         or null if the startOffset is larger than the largest offset in this log
   */
  @threadsafe
  def read(startOffset: Long,
           maxSize: Int,
           maxPosition: Long = size,
           minOneMessage: Boolean = false): FetchDataInfo = {
    if (maxSize < 0)
      throw new IllegalArgumentException(s"Invalid max size $maxSize for log read from segment $log")

    // 调用 translateOffset 方法定位要读取的起始文件位置 （startPosition）。
    val startOffsetAndSize = translateOffset(startOffset)

    // if the start position is already off the end of the log, return null
    if (startOffsetAndSize == null)
      return null

    // Kafka 需要根据索引信息找到对应的物理文件位置才能开始读取消息。
    val startPosition = startOffsetAndSize.position
    val offsetMetadata = LogOffsetMetadata(startOffset, this.baseOffset, startPosition)

    val adjustedMaxSize =
      if (minOneMessage) math.max(maxSize, startOffsetAndSize.size)
      else maxSize

    // return a log segment but with zero size in the case below
    if (adjustedMaxSize == 0)
      return FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY)

    // calculate the length of the message set to read based on whether or not they gave us a maxOffset
    // 举个例子，假设 maxSize=100，maxPosition=300，startPosition=250，
    // 那么 read 方法只能读取 50 字节，因为 maxPosition - startPosition = 50。
    val fetchSize: Int = min((maxPosition - startPosition).toInt, adjustedMaxSize)

    // 最后一步是调用 FileRecords 的 slice 方法，从指定位置读取指定大小的消息集合。
    FetchDataInfo(offsetMetadata, log.slice(startPosition, fetchSize),
      firstEntryIncomplete = adjustedMaxSize < startOffsetAndSize.size)
  }

   def fetchUpperBoundOffset(startOffsetPosition: OffsetPosition, fetchSize: Int): Option[Long] =
     offsetIndex.fetchUpperBoundOffset(startOffsetPosition, fetchSize).map(_.offset)

  /**
   * Run recovery on the given segment. This will rebuild the index from the log file and lop off any invalid bytes
   * from the end of the log and index.
   * 恢复日志段
   * @param producerStateManager Producer state corresponding to the segment's base offset. This is needed to recover
   *                             the transaction index.
   * @param leaderEpochCache Optionally a cache for updating the leader epoch during recovery.
   * @return The number of bytes truncated from the log
   * @throws LogSegmentOffsetOverflowException if the log segment contains an offset that causes the index offset to overflow
   */
  @nonthreadsafe
  def recover(producerStateManager: ProducerStateManager, leaderEpochCache: Option[LeaderEpochFileCache] = None): Int = {
    // 调用索引对象的 reset 方法清空所有的索引文件
    offsetIndex.reset()
    timeIndex.reset()
    txnIndex.reset()
    var validBytes = 0
    var lastIndexEntry = 0
    maxTimestampSoFar = RecordBatch.NO_TIMESTAMP
    try {
      // 之后会开始遍历日志段中的所有消息集合或消息批次（RecordBatch）
      for (batch <- log.batches.asScala) {
        batch.ensureValid()
        // 该集合中最后一条消息的位移值不能越界，即它与日志段起始位移的差值必须是一个正整数值。
        ensureOffsetInRange(batch.lastOffset)

        // The max timestamp is exposed at the batch level, so no need to iterate the records
        // 更新遍历过程中观测到的最大时间戳以及所属消息的位移值
        if (batch.maxTimestamp > maxTimestampSoFar) {
          maxTimestampSoFar = batch.maxTimestamp
          offsetOfMaxTimestampSoFar = batch.lastOffset
        }

        // Build offset index
        // 不断累加当前已读取的消息字节数
        if (validBytes - lastIndexEntry > indexIntervalBytes) {
          offsetIndex.append(batch.lastOffset, validBytes)
          timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar)
          lastIndexEntry = validBytes
        }
        validBytes += batch.sizeInBytes()

        if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
          leaderEpochCache.foreach { cache =>
            if (batch.partitionLeaderEpoch > 0 && cache.latestEpoch.forall(batch.partitionLeaderEpoch > _))
              cache.assign(batch.partitionLeaderEpoch, batch.baseOffset)
          }
          updateProducerState(producerStateManager, batch)
        }
      }
    } catch {
      case e@ (_: CorruptRecordException | _: InvalidRecordException) =>
        warn("Found invalid messages in log segment %s at byte offset %d: %s. %s"
          .format(log.file.getAbsolutePath, validBytes, e.getMessage, e.getCause))
    }
    // Kafka 会将日志段当前总字节数和刚刚累加的已读取字节数进行比较，如果发现前者比后者大，
    val truncated = log.sizeInBytes - validBytes
    if (truncated > 0)
      debug(s"Truncated $truncated invalid bytes at the end of segment ${log.file.getAbsoluteFile} during recovery")
    // 说明日志段写入了一些非法消息，需要执行截断操作，将日志段大小调整回合法的数值
    log.truncateTo(validBytes)
    // Kafka 还必须相应地调整索引文件的大小
    offsetIndex.trimToValidSize()
    // A normally closed segment always appends the biggest timestamp ever seen into log segment, we do this as well.
    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar, skipFullCheck = true)
    timeIndex.trimToValidSize()
    truncated
  }

  private def loadLargestTimestamp(): Unit = {
    // Get the last time index entry. If the time index is empty, it will return (-1, baseOffset)
    val lastTimeIndexEntry = timeIndex.lastEntry
    maxTimestampSoFar = lastTimeIndexEntry.timestamp
    offsetOfMaxTimestampSoFar = lastTimeIndexEntry.offset

    val offsetPosition = offsetIndex.lookup(lastTimeIndexEntry.offset)
    // Scan the rest of the messages to see if there is a larger timestamp after the last time index entry.
    val maxTimestampOffsetAfterLastEntry = log.largestTimestampAfter(offsetPosition.position)
    if (maxTimestampOffsetAfterLastEntry.timestamp > lastTimeIndexEntry.timestamp) {
      maxTimestampSoFar = maxTimestampOffsetAfterLastEntry.timestamp
      offsetOfMaxTimestampSoFar = maxTimestampOffsetAfterLastEntry.offset
    }
  }

  /**
   * Check whether the last offset of the last batch in this segment overflows the indexes.
   */
  def hasOverflow: Boolean = {
    val nextOffset = readNextOffset
    nextOffset > baseOffset && !canConvertToRelativeOffset(nextOffset - 1)
  }

  def collectAbortedTxns(fetchOffset: Long, upperBoundOffset: Long): TxnIndexSearchResult =
    txnIndex.collectAbortedTxns(fetchOffset, upperBoundOffset)

  override def toString: String = "LogSegment(baseOffset=" + baseOffset +
    ", size=" + size +
    ", lastModifiedTime=" + lastModified +
    ", largestTime=" + largestTimestamp +
    ")"

  /**
   * Truncate off all index and log entries with offsets >= the given offset.
   * If the given offset is larger than the largest message in this segment, do nothing.
   *
   * @param offset The offset to truncate to
   * @return The number of log bytes truncated
   */
  @nonthreadsafe
  def truncateTo(offset: Long): Int = {
    // Do offset translation before truncating the index to avoid needless scanning
    // in case we truncate the full index
    val mapping = translateOffset(offset)
    offsetIndex.truncateTo(offset)
    timeIndex.truncateTo(offset)
    txnIndex.truncateTo(offset)

    // After truncation, reset and allocate more space for the (new currently active) index
    offsetIndex.resize(offsetIndex.maxIndexSize)
    timeIndex.resize(timeIndex.maxIndexSize)

    val bytesTruncated = if (mapping == null) 0 else log.truncateTo(mapping.position)
    if (log.sizeInBytes == 0) {
      created = time.milliseconds
      rollingBasedTimestamp = None
    }

    bytesSinceLastIndexEntry = 0
    if (maxTimestampSoFar >= 0)
      loadLargestTimestamp()
    bytesTruncated
  }

  /**
   * Calculate the offset that would be used for the next message to be append to this segment.
   * Note that this is expensive.
   */
  @threadsafe
  def readNextOffset: Long = {
    val fetchData = read(offsetIndex.lastOffset, log.sizeInBytes)
    if (fetchData == null)
      baseOffset
    else
      fetchData.records.batches.asScala.lastOption
        .map(_.nextOffset)
        .getOrElse(baseOffset)
  }

  /**
   * Flush this log segment to disk
   *
   * 将此日志段刷新到磁盘
   */
  @threadsafe
  def flush(): Unit = {
    LogFlushStats.logFlushTimer.time {
      log.flush()
      offsetIndex.flush()
      timeIndex.flush()
      txnIndex.flush()
    }
  }

  /**
   * Update the directory reference for the log and indices in this segment. This would typically be called after a
   * directory is renamed.
   */
  def updateParentDir(dir: File): Unit = {
    log.updateParentDir(dir)
    lazyOffsetIndex.updateParentDir(dir)
    lazyTimeIndex.updateParentDir(dir)
    txnIndex.updateParentDir(dir)
  }

  /**
   * Change the suffix for the index and log files for this log segment
   * IOException from this method should be handled by the caller
   */
  def changeFileSuffixes(oldSuffix: String, newSuffix: String): Unit = {
    log.renameTo(new File(CoreUtils.replaceSuffix(log.file.getPath, oldSuffix, newSuffix)))
    lazyOffsetIndex.renameTo(new File(CoreUtils.replaceSuffix(lazyOffsetIndex.file.getPath, oldSuffix, newSuffix)))
    lazyTimeIndex.renameTo(new File(CoreUtils.replaceSuffix(lazyTimeIndex.file.getPath, oldSuffix, newSuffix)))
    txnIndex.renameTo(new File(CoreUtils.replaceSuffix(txnIndex.file.getPath, oldSuffix, newSuffix)))
  }

  /**
   * Append the largest time index entry to the time index and trim the log and indexes.
   *
   * The time index entry appended will be used to decide when to delete the segment.
   */
  def onBecomeInactiveSegment(): Unit = {
    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar, skipFullCheck = true)
    offsetIndex.trimToValidSize()
    timeIndex.trimToValidSize()
    log.trim()
  }

  /**
    * If not previously loaded,
    * load the timestamp of the first message into memory.
    * 如果以前没有加载，则将第一条消息的时间戳加载到内存中。
    */
  private def loadFirstBatchTimestamp(): Unit = {
    if (rollingBasedTimestamp.isEmpty) {
      val iter = log.batches.iterator()
      if (iter.hasNext)
        rollingBasedTimestamp = Some(iter.next().maxTimestamp)
    }
  }

  /**
   * The time this segment has waited to be rolled.
   * If the first message batch has a timestamp we use its timestamp to determine when to roll a segment. A segment
   * is rolled if the difference between the new batch's timestamp and the first batch's timestamp exceeds the
   * segment rolling time.
   * If the first batch does not have a timestamp, we use the wall clock time to determine when to roll a segment. A
   * segment is rolled if the difference between the current wall clock time and the segment create time exceeds the
   * segment rolling time.
   *
   * segment等待滚动的时间。
   * 如果第一个消息批处理有时间戳，我们使用它的时间戳来确定何时滚动一个段。
   * 如果新批的时间戳和第一批的时间戳之间的差异超过了段滚动时间，则对一个段进行滚动。
   * 如果第一批没有时间戳，我们使用挂钟时间来确定何时滚动一个段。
   * 如果当前挂钟时间与创建时间的段之间的时间差超过了该段的滚动时间，则对该段进行滚动。
   */
  def timeWaitedForRoll(now: Long, messageTimestamp: Long) : Long = {
    // Load the timestamp of the first message into memory
    loadFirstBatchTimestamp()
    rollingBasedTimestamp match {
      case Some(t) if t >= 0 => messageTimestamp - t
      case _ => now - created
    }
  }

  /**
    * @return the first batch timestamp if the timestamp is available. Otherwise return Long.MaxValue
    */
  def getFirstBatchTimestamp() : Long = {
    loadFirstBatchTimestamp()
    rollingBasedTimestamp match {
      case Some(t) if t >= 0 => t
      case _ => Long.MaxValue
    }
  }

  /**
   * Search the message offset based on timestamp and offset.
   *
   * This method returns an option of TimestampOffset. The returned value is determined using the following ordered list of rules:
   *
   * - If all the messages in the segment have smaller offsets, return None
   * - If all the messages in the segment have smaller timestamps, return None
   * - If all the messages in the segment have larger timestamps, or no message in the segment has a timestamp
   *   the returned the offset will be max(the base offset of the segment, startingOffset) and the timestamp will be Message.NoTimestamp.
   * - Otherwise, return an option of TimestampOffset. The offset is the offset of the first message whose timestamp
   *   is greater than or equals to the target timestamp and whose offset is greater than or equals to the startingOffset.
   *
   * This methods only returns None when 1) all messages' offset < startOffing or 2) the log is not empty but we did not
   * see any message when scanning the log from the indexed position. The latter could happen if the log is truncated
   * after we get the indexed position but before we scan the log from there. In this case we simply return None and the
   * caller will need to check on the truncated log and maybe retry or even do the search on another log segment.
   *
   * @param timestamp The timestamp to search for.
   * @param startingOffset The starting offset to search.
   * @return the timestamp and offset of the first message that meets the requirements. None will be returned if there is no such message.
   */
  def findOffsetByTimestamp(timestamp: Long, startingOffset: Long = baseOffset): Option[TimestampAndOffset] = {
    // Get the index entry with a timestamp less than or equal to the target timestamp
    val timestampOffset = timeIndex.lookup(timestamp)
    val position = offsetIndex.lookup(math.max(timestampOffset.offset, startingOffset)).position

    // Search the timestamp
    Option(log.searchForTimestamp(timestamp, position, startingOffset))
  }

  /**
   * Close this log segment
   */
  def close(): Unit = {
    if (_maxTimestampSoFar.nonEmpty || _offsetOfMaxTimestampSoFar.nonEmpty)
      CoreUtils.swallow(timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar,
        skipFullCheck = true), this)
    CoreUtils.swallow(lazyOffsetIndex.close(), this)
    CoreUtils.swallow(lazyTimeIndex.close(), this)
    CoreUtils.swallow(log.close(), this)
    CoreUtils.swallow(txnIndex.close(), this)
  }

  /**
    * Close file handlers used by the log segment but don't write to disk. This is used when the disk may have failed
    */
  def closeHandlers(): Unit = {
    CoreUtils.swallow(lazyOffsetIndex.closeHandler(), this)
    CoreUtils.swallow(lazyTimeIndex.closeHandler(), this)
    CoreUtils.swallow(log.closeHandlers(), this)
    CoreUtils.swallow(txnIndex.close(), this)
  }

  /**
   * Delete this log segment from the filesystem.
   */
  def deleteIfExists(): Unit = {
    def delete(delete: () => Boolean, fileType: String, file: File, logIfMissing: Boolean): Unit = {
      try {
        if (delete())
          info(s"Deleted $fileType ${file.getAbsolutePath}.")
        else if (logIfMissing)
          info(s"Failed to delete $fileType ${file.getAbsolutePath} because it does not exist.")
      }
      catch {
        case e: IOException => throw new IOException(s"Delete of $fileType ${file.getAbsolutePath} failed.", e)
      }
    }

    CoreUtils.tryAll(Seq(
      () => delete(log.deleteIfExists _, "log", log.file, logIfMissing = true),
      () => delete(lazyOffsetIndex.deleteIfExists _, "offset index", lazyOffsetIndex.file, logIfMissing = true),
      () => delete(lazyTimeIndex.deleteIfExists _, "time index", lazyTimeIndex.file, logIfMissing = true),
      () => delete(txnIndex.deleteIfExists _, "transaction index", txnIndex.file, logIfMissing = false)
    ))
  }

  /**
   * The last modified time of this log segment as a unix time stamp
   */
  def lastModified = log.file.lastModified

  /**
   * The largest timestamp this segment contains, if maxTimestampSoFar >= 0, otherwise None.
   *
   * 如果maxTimestampSoFar >= 0，则此段包含的最大时间戳，否则为None。
   */
  def largestRecordTimestamp: Option[Long] = if (maxTimestampSoFar >= 0) Some(maxTimestampSoFar) else None

  /**
   * The largest timestamp this segment contains.
   */
  def largestTimestamp = if (maxTimestampSoFar >= 0) maxTimestampSoFar else lastModified

  /**
   * Change the last modified time for this log segment
   */
  def lastModified_=(ms: Long) = {
    val fileTime = FileTime.fromMillis(ms)
    Files.setLastModifiedTime(log.file.toPath, fileTime)
    Files.setLastModifiedTime(lazyOffsetIndex.file.toPath, fileTime)
    Files.setLastModifiedTime(lazyTimeIndex.file.toPath, fileTime)
  }

}

object LogSegment {

  // dir：就是这个日志所在的文件夹路径，也就是主题分区的路径
  // baseOffset : 获取文件偏移量
  def open(dir: File, baseOffset: Long, config: LogConfig, time: Time, fileAlreadyExists: Boolean = false,
           initFileSize: Int = 0, preallocate: Boolean = false, fileSuffix: String = ""): LogSegment = {
    // segment.index.bytes
    val maxIndexSize = config.maxIndexSize
    new LogSegment(
      FileRecords.open(Log.logFile(dir, baseOffset, fileSuffix), fileAlreadyExists, initFileSize, preallocate),
      LazyIndex.forOffset(Log.offsetIndexFile(dir, baseOffset, fileSuffix), baseOffset = baseOffset, maxIndexSize = maxIndexSize),
      LazyIndex.forTime(Log.timeIndexFile(dir, baseOffset, fileSuffix), baseOffset = baseOffset, maxIndexSize = maxIndexSize),
      new TransactionIndex(baseOffset, Log.transactionIndexFile(dir, baseOffset, fileSuffix)),
      baseOffset,
      indexIntervalBytes = config.indexInterval,
      rollJitterMs = config.randomSegmentJitter,
      time)
  }

  def deleteIfExists(dir: File, baseOffset: Long, fileSuffix: String = ""): Unit = {
    Log.deleteFileIfExists(Log.offsetIndexFile(dir, baseOffset, fileSuffix))
    Log.deleteFileIfExists(Log.timeIndexFile(dir, baseOffset, fileSuffix))
    Log.deleteFileIfExists(Log.transactionIndexFile(dir, baseOffset, fileSuffix))
    Log.deleteFileIfExists(Log.logFile(dir, baseOffset, fileSuffix))
  }
}

// 结尾有个 Stats，它是做统计用的，主要负责为日志落盘进行计时。
object LogFlushStats extends KafkaMetricsGroup {
  val logFlushTimer = new KafkaTimer(newTimer("LogFlushRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
}
