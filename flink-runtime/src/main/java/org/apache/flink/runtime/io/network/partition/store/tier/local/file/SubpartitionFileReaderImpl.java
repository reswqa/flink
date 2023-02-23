/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.store.tier.local.file;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.store.common.BufferIndexOrError;
import org.apache.flink.runtime.io.network.partition.store.common.ConsumerId;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Deque;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.positionToNextBuffer;
import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.readFromByteChannel;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Default implementation of {@link SubpartitionFileReader}.
 *
 * <p>Note: This class is not thread safe.
 */
public class SubpartitionFileReaderImpl implements SubpartitionFileReader {

    private static final Logger LOG = LoggerFactory.getLogger(SubpartitionFileReaderImpl.class);

    private final ByteBuffer headerBuf;

    private final int subpartitionId;

    private final ConsumerId consumerId;

    private final FileChannel dataFileChannel;

    private final SubpartitionConsumerInternalOperations operations;

    private final CachedRegionManager cachedRegionManager;

    private final BufferIndexManager bufferIndexManager;

    private final Deque<BufferIndexOrError> loadedBuffers = new LinkedBlockingDeque<>();

    private final Consumer<SubpartitionFileReader> fileReaderReleaser;

    private final BiFunction<Integer, Integer, Boolean> isLastRecordInSegmentDecider;

    private volatile boolean isFailed;

    public SubpartitionFileReaderImpl(
            int subpartitionId,
            ConsumerId consumerId,
            FileChannel dataFileChannel,
            SubpartitionConsumerInternalOperations operations,
            RegionBufferIndexTracker dataIndex,
            int maxBufferReadAhead,
            Consumer<SubpartitionFileReader> fileReaderReleaser,
            BiFunction<Integer, Integer, Boolean> isLastRecordInSegmentDecider,
            ByteBuffer headerBuf) {
        this.subpartitionId = subpartitionId;
        this.consumerId = consumerId;
        this.dataFileChannel = dataFileChannel;
        this.operations = operations;
        this.headerBuf = headerBuf;
        this.bufferIndexManager = new BufferIndexManager(maxBufferReadAhead);
        this.cachedRegionManager = new CachedRegionManager(subpartitionId, dataIndex);
        this.fileReaderReleaser = fileReaderReleaser;
        this.isLastRecordInSegmentDecider = isLastRecordInSegmentDecider;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubpartitionFileReaderImpl that = (SubpartitionFileReaderImpl) o;
        return subpartitionId == that.subpartitionId && Objects.equals(consumerId, that.consumerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subpartitionId, consumerId);
    }

    /**
     * Read subpartition data into buffers.
     *
     * <p>This transfers the ownership of used buffers to this class. It's this class'
     * responsibility to release the buffers using the recycler when no longer needed.
     *
     * <p>Calling this method does not always use up all the provided buffers. It's this class'
     * decision when to stop reading. Currently, it stops reading when: 1) buffers are used up, or
     * 2) reaches the end of the subpartition data within the region, or 3) enough data have been
     * read ahead the downstream consuming offset.
     */
    @Override
    public synchronized void readBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler)
            throws IOException {
        if (isFailed) {
            throw new IOException("subpartition reader has already failed.");
        }
        int firstBufferToLoad = bufferIndexManager.getNextToLoad();
        if (firstBufferToLoad < 0) {
            return;
        }

        // If lookup result is empty, it means that one the following things have happened:
        // 1) The target buffer has not been spilled into disk.
        // 2) The target buffer has not been released from memory.
        // So, just skip this round reading.
        int numRemainingBuffer = cachedRegionManager.getRemainingBuffersInRegion(firstBufferToLoad);
        if (numRemainingBuffer == 0) {
            return;
        }
        moveFileOffsetToBuffer(firstBufferToLoad);

        int indexToLoad;
        int numLoaded = 0;
        while (!buffers.isEmpty()
                && (indexToLoad = bufferIndexManager.getNextToLoad()) >= 0
                && numRemainingBuffer-- > 0) {
            MemorySegment segment = buffers.poll();
            Buffer buffer;
            try {
                if ((buffer = readFromByteChannel(dataFileChannel, headerBuf, segment, recycler))
                        == null) {
                    buffers.add(segment);
                    break;
                }
            } catch (Throwable throwable) {
                buffers.add(segment);
                throw throwable;
            }
            loadedBuffers.add(BufferIndexOrError.newBuffer(buffer, indexToLoad));
            bufferIndexManager.updateLastLoaded(indexToLoad);
            cachedRegionManager.advance(
                    buffer.readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH);
            ++numLoaded;
        }

        if (loadedBuffers.size() <= numLoaded) {
            operations.notifyDataAvailable();
        }
    }

    @Override
    public synchronized void fail(Throwable failureCause) {
        if (isFailed) {
            return;
        }
        isFailed = true;
        BufferIndexOrError bufferIndexOrError;
        // empty from tail, in-case subpartition view consumes concurrently and gets the wrong order
        while ((bufferIndexOrError = loadedBuffers.pollLast()) != null) {
            if (bufferIndexOrError.getBuffer().isPresent()) {
                checkNotNull(bufferIndexOrError.getBuffer().get()).recycleBuffer();
            }
        }

        loadedBuffers.add(BufferIndexOrError.newError(failureCause));
        operations.notifyDataAvailable();
    }

    @Override
    public void release() {
        BufferIndexOrError bufferIndexOrError;
        while ((bufferIndexOrError = loadedBuffers.pollLast()) != null) {
            if (bufferIndexOrError.getBuffer().isPresent()) {
                checkNotNull(bufferIndexOrError.getBuffer().get()).recycleBuffer();
            }
        }
        operations.notifyDataAvailable();
    }

    /** Refresh downstream consumption progress for another round scheduling of reading. */
    @Override
    public void prepareForScheduling() {
        // Access the consuming offset with lock, to prevent loading any buffer released from the
        // memory data manager that is already consumed.
        int consumingOffset = operations.getConsumingOffset(true);
        bufferIndexManager.updateLastConsumed(consumingOffset);
        cachedRegionManager.updateConsumingOffset(consumingOffset);
    }

    /** Provides priority calculation logic for io scheduler. */
    @Override
    public int compareTo(SubpartitionFileReader that) {
        checkArgument(that instanceof SubpartitionFileReaderImpl);
        return Long.compare(
                getNextOffsetToLoad(), ((SubpartitionFileReaderImpl) that).getNextOffsetToLoad());
    }

    public Deque<BufferIndexOrError> getLoadedBuffers() {
        return loadedBuffers;
    }

    @Override
    public Optional<ResultSubpartition.BufferAndBacklog> consumeBuffer(int nextBufferToConsume, Queue<Buffer> errorBuffers)
            throws Throwable {
        if (!checkAndGetFirstBufferIndexOrError(nextBufferToConsume, errorBuffers).isPresent()) {
            return Optional.empty();
        }

        // already ensure that peek element is not null and not throwable.
        BufferIndexOrError current = checkNotNull(loadedBuffers.poll());

        BufferIndexOrError next = loadedBuffers.peek();

        Buffer.DataType nextDataType = next == null ? Buffer.DataType.NONE : next.getDataType();
        int backlog = loadedBuffers.size();
        int bufferIndex = current.getIndex();
        Buffer buffer =
                current.getBuffer()
                        .orElseThrow(
                                () ->
                                        new NullPointerException(
                                                "Get a non-throwable and non-buffer bufferIndexOrError, which is not allowed"));

        //Boolean apply = isLastRecordInSegmentDecider.apply(subpartitionId, nextBufferToConsume);
        //if(buffer.getDataType() == Buffer.DataType.SEGMENT_EVENT){
        //    System.out.println();
        //}

        return Optional.of(
                ResultSubpartition.BufferAndBacklog.fromBufferAndLookahead(
                        buffer,
                        nextDataType,
                        backlog,
                        bufferIndex,
                        buffer.getDataType() == Buffer.DataType.SEGMENT_EVENT
                        ));
    }

    @Override
    public Buffer.DataType peekNextToConsumeDataType(int nextBufferToConsume, Queue<Buffer> errorBuffers) {
        Buffer.DataType dataType = Buffer.DataType.NONE;
        try {
            dataType =
                    checkAndGetFirstBufferIndexOrError(nextBufferToConsume, errorBuffers)
                            .map(BufferIndexOrError::getDataType)
                            .orElse(Buffer.DataType.NONE);
        } catch (Throwable throwable) {
            ExceptionUtils.rethrow(throwable);
        }
        return dataType;
    }

    @Override
    public void releaseDataView() {
        fileReaderReleaser.accept(this);
    }

    @Override
    public int getBacklog() {
        return loadedBuffers.size();
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private Optional<BufferIndexOrError> checkAndGetFirstBufferIndexOrError(int expectedBufferIndex, Queue<Buffer> errorBuffers)
            throws Throwable {
        if (loadedBuffers.isEmpty()) {
            return Optional.empty();
        }

        BufferIndexOrError peek = loadedBuffers.peek();
        while (!peek.getThrowable().isPresent() && peek.getIndex() < expectedBufferIndex) {
            pollAndRecycleBuffer(loadedBuffers, errorBuffers);
            peek = loadedBuffers.peek();
            if (peek == null) {
                return Optional.empty();
            }
        }

        if (peek.getThrowable().isPresent()) {
            throw peek.getThrowable().get();
        } else if (peek.getIndex() != expectedBufferIndex) {
            return Optional.empty();
        }

        return Optional.of(peek);
    }

    private void pollAndRecycleBuffer(Deque<BufferIndexOrError> loadedBuffers, Queue<Buffer> errorBuffers){
        BufferIndexOrError peek = loadedBuffers.peek();
        checkNotNull(peek);
        errorBuffers.add(checkNotNull(peek.getBuffer().get()));
        loadedBuffers.poll();
    }

    private void moveFileOffsetToBuffer(int bufferIndex) throws IOException {
        Tuple2<Integer, Long> indexAndOffset =
                cachedRegionManager.getNumSkipAndFileOffset(bufferIndex);
        dataFileChannel.position(indexAndOffset.f1);
        for (int i = 0; i < indexAndOffset.f0; ++i) {
            positionToNextBuffer(dataFileChannel, headerBuf);
        }
        cachedRegionManager.skipAll(dataFileChannel.position());
    }

    /** Returns Long.MAX_VALUE if it shouldn't load. */
    private long getNextOffsetToLoad() {
        int bufferIndex = bufferIndexManager.getNextToLoad();
        if (bufferIndex < 0) {
            return Long.MAX_VALUE;
        } else {
            return cachedRegionManager.getFileOffset(bufferIndex);
        }
    }

    /** Take care of buffer index consumed by the file reader. */
    static class BufferIndexManager {
        private final int maxBuffersReadAhead;

        /** Index of the last buffer that has ever been loaded from file. */
        private int lastLoaded = -1;
        /** Index of the last buffer that has been consumed by downstream, to the best knowledge. */
        private int lastConsumed = -1;

        BufferIndexManager(int maxBuffersReadAhead) {
            this.maxBuffersReadAhead = maxBuffersReadAhead;
        }

        private void updateLastLoaded(int lastLoaded) {
            checkState(this.lastLoaded <= lastLoaded);
            this.lastLoaded = lastLoaded;
        }

        private void updateLastConsumed(int lastConsumed) {
            this.lastConsumed = lastConsumed;
        }

        /** Returns a negative value if shouldn't load. */
        private int getNextToLoad() {
            int nextToLoad = Math.max(lastLoaded, lastConsumed) + 1;
            int maxToLoad = lastConsumed + maxBuffersReadAhead;
            return nextToLoad <= maxToLoad ? nextToLoad : -1;
        }
    }

    /**
     * Maintains a set of cursors on the last fetched readable region.
     *
     * <p>The semantics are:
     *
     * <ol>
     *   <li>The offset of the buffer with {@code currentBufferIndex} in file can be derived by
     *       starting from {@code offset} and skipping {@code numSkip} buffers.
     *   <li>The {@code numReadable} continuous buffers starting from the offset of the buffer with
     *       {@code currentBufferIndex} belongs to the same readable region.
     * </ol>
     */
    private static class CachedRegionManager {
        private final int subpartitionId;
        private final RegionBufferIndexTracker dataIndex;

        private int consumingOffset = -1;

        private int currentBufferIndex;
        private int numSkip;
        private int numReadable;
        private long offset;

        private CachedRegionManager(int subpartitionId, RegionBufferIndexTracker dataIndex) {
            this.subpartitionId = subpartitionId;
            this.dataIndex = dataIndex;
        }

        // ------------------------------------------------------------------------
        //  Called by HsSubpartitionFileReader
        // ------------------------------------------------------------------------

        public void updateConsumingOffset(int consumingOffset) {
            this.consumingOffset = consumingOffset;
        }

        /** Return Long.MAX_VALUE if region does not exist to giving the lowest priority. */
        private long getFileOffset(int bufferIndex) {
            updateCachedRegionIfNeeded(bufferIndex);
            return currentBufferIndex == -1 ? Long.MAX_VALUE : offset;
        }

        private int getRemainingBuffersInRegion(int bufferIndex) {
            updateCachedRegionIfNeeded(bufferIndex);

            return numReadable;
        }

        private void skipAll(long newOffset) {
            this.offset = newOffset;
            this.numSkip = 0;
        }

        /**
         * Maps the given buffer index to the offset in file.
         *
         * @return a tuple of {@code <numSkip,offset>}. The offset of the given buffer index can be
         *         derived by starting from the {@code offset} and skipping {@code numSkip} buffers.
         */
        private Tuple2<Integer, Long> getNumSkipAndFileOffset(int bufferIndex) {
            updateCachedRegionIfNeeded(bufferIndex);

            checkState(numSkip >= 0, "num skip must be greater than or equal to 0");
            // Assumption: buffer index is always requested / updated increasingly
            checkState(currentBufferIndex <= bufferIndex);
            return new Tuple2<>(numSkip, offset);
        }

        private void advance(long bufferSize) {
            if (isInCachedRegion(currentBufferIndex + 1)) {
                currentBufferIndex++;
                numReadable--;
                offset += bufferSize;
            }
        }

        // ------------------------------------------------------------------------
        //  Internal Methods
        // ------------------------------------------------------------------------

        /** Points the cursors to the given buffer index, if possible. */
        private void updateCachedRegionIfNeeded(int bufferIndex) {
            if (isInCachedRegion(bufferIndex)) {
                int numAdvance = bufferIndex - currentBufferIndex;
                numSkip += numAdvance;
                numReadable -= numAdvance;
                currentBufferIndex = bufferIndex;
                return;
            }

            Optional<RegionBufferIndexTracker.ReadableRegion> lookupResultOpt =
                    dataIndex.getReadableRegion(subpartitionId, bufferIndex, consumingOffset);
            if (!lookupResultOpt.isPresent()) {
                currentBufferIndex = -1;
                numReadable = 0;
                numSkip = 0;
                offset = -1L;
            } else {
                RegionBufferIndexTracker.ReadableRegion cachedRegion = lookupResultOpt.get();
                currentBufferIndex = bufferIndex;
                numSkip = cachedRegion.numSkip;
                numReadable = cachedRegion.numReadable;
                offset = cachedRegion.offset;
            }
        }

        private boolean isInCachedRegion(int bufferIndex) {
            return bufferIndex < currentBufferIndex + numReadable
                    && bufferIndex >= currentBufferIndex;
        }
    }

    /** Factory of {@link SubpartitionFileReader}. */
    public static class Factory implements SubpartitionFileReader.Factory {
        public static final Factory INSTANCE = new Factory();

        private Factory() {}

        @Override
        public SubpartitionFileReader createFileReader(
                int subpartitionId,
                ConsumerId consumerId,
                FileChannel dataFileChannel,
                SubpartitionConsumerInternalOperations operation,
                RegionBufferIndexTracker dataIndex,
                int maxBuffersReadAhead,
                Consumer<SubpartitionFileReader> fileReaderReleaser,
                BiFunction<Integer, Integer, Boolean> isLastRecordInSegmentDecider,
                ByteBuffer headerBuffer) {
            return new SubpartitionFileReaderImpl(
                    subpartitionId,
                    consumerId,
                    dataFileChannel,
                    operation,
                    dataIndex,
                    maxBuffersReadAhead,
                    fileReaderReleaser,
                    isLastRecordInSegmentDecider,
                    headerBuffer);
        }
    }
}
