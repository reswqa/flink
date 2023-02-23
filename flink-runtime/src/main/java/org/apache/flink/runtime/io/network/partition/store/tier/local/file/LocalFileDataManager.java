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

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultSubpartition;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.store.TieredStoreConfiguration;
import org.apache.flink.runtime.io.network.partition.store.common.BufferConsumeView;
import org.apache.flink.runtime.io.network.partition.store.common.BufferPoolHelper;
import org.apache.flink.runtime.io.network.partition.store.common.ConsumerId;
import org.apache.flink.runtime.io.network.partition.store.common.EndOfSegmentEventBuilder;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierDataGate;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierReader;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierWriter;
import org.apache.flink.runtime.io.network.partition.store.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.metrics.TimerGauge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.SEGMENT_EVENT;
import static org.apache.flink.runtime.io.network.partition.store.TieredStoreMode.SpillingType.SELECTIVE;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The DataManager of LOCAL file. */
public class LocalFileDataManager implements SingleTierWriter, SingleTierDataGate {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileDataManager.class);

    public static final int BROADCAST_CHANNEL = 0;

    public static final String DATA_FILE_SUFFIX = ".store.data";

    private final int numSubpartitions;

    private final int networkBufferSize;

    private final ResultPartitionID resultPartitionID;

    private final BufferPoolHelper bufferPoolHelper;

    private final Path dataFilePath;

    private final boolean isBroadcastOnly;

    private final RegionBufferIndexTracker regionBufferIndexTracker;

    private final BufferCompressor bufferCompressor;

    private final TieredStoreConfiguration storeConfiguration;

    /** Record the last assigned consumerId for each subpartition. */
    private final ConsumerId[] lastConsumerIds;

    private final LocalDiskDataManager localDiskDataManager;

    private CacheDataManager cacheDataManager;

    private final SubpartitionSegmentIndexTracker segmentIndexTracker;

    // TODO, Make this configurable.
    private int numBytesInASegment = 4 * 1024 * 1024; // 4 M

    private volatile boolean isReleased;

    private volatile boolean isClosed;

    public LocalFileDataManager(
            int numSubpartitions,
            int networkBufferSize,
            ResultPartitionID resultPartitionID,
            BufferPoolHelper bufferPoolHelper,
            String dataFileBasePath,
            boolean isBroadcastOnly,
            @Nullable BufferCompressor bufferCompressor,
            BatchShuffleReadBufferPool readBufferPool,
            ScheduledExecutorService readIOExecutor,
            TieredStoreConfiguration storeConfiguration) {
        this.numSubpartitions = numSubpartitions;
        this.networkBufferSize = networkBufferSize;
        this.resultPartitionID = resultPartitionID;
        this.dataFilePath = new File(dataFileBasePath + DATA_FILE_SUFFIX).toPath();
        this.isBroadcastOnly = isBroadcastOnly;
        this.bufferPoolHelper = bufferPoolHelper;
        this.bufferCompressor = bufferCompressor;
        checkNotNull(bufferCompressor);
        this.storeConfiguration = storeConfiguration;
        this.regionBufferIndexTracker =
                new RegionBufferIndexTrackerImpl(isBroadcastOnly ? 1 : numSubpartitions);
        this.lastConsumerIds = new ConsumerId[numSubpartitions];
        this.localDiskDataManager =
                new LocalDiskDataManager(
                        readBufferPool,
                        readIOExecutor,
                        regionBufferIndexTracker,
                        dataFilePath,
                        SubpartitionFileReaderImpl.Factory.INSTANCE,
                        storeConfiguration,
                        this::isLastRecordInSegment);
        this.segmentIndexTracker =
                new SubpartitionSegmentIndexTracker(numSubpartitions, isBroadcastOnly);
    }

    @Override
    public void setup() throws IOException {
        this.cacheDataManager =
                new CacheDataManager(
                        isBroadcastOnly ? 1 : numSubpartitions,
                        networkBufferSize,
                        bufferPoolHelper,
                        getSpillingStrategy(storeConfiguration),
                        regionBufferIndexTracker,
                        dataFilePath,
                        bufferCompressor);
    }

    boolean isLastRecordInSegment(int subpartitionId, int bufferIndex) {
        return cacheDataManager.isLastBufferInSegment(subpartitionId, bufferIndex);
    }

    private TsSpillingStrategy getSpillingStrategy(TieredStoreConfiguration storeConfiguration) {
        return new FullSpillingStrategy(storeConfiguration);
    }

    @Override
    public void emit(
            ByteBuffer record,
            int targetSubpartition,
            Buffer.DataType dataType,
            boolean isBroadcast,
            boolean isLastRecordInSegment,
            boolean isEndOfPartition,
            long segmentIndex)
            throws IOException {
        segmentIndexTracker.addSubpartitionSegmentIndex(targetSubpartition, segmentIndex);
        if (isLastRecordInSegment) {
            emit(record, targetSubpartition, dataType, false);
            // Send the EndOfSegmentEvent
            ByteBuffer endOfSegment =
                    EndOfSegmentEventBuilder.buildEndOfSegmentEvent(
                            segmentIndex + 1L, isBroadcastOnly);
            emit(endOfSegment, targetSubpartition, SEGMENT_EVENT, true);
        }else {
            emit(record, targetSubpartition, dataType, isLastRecordInSegment);
        }
    }

    private void emit(
            ByteBuffer record,
            int targetSubpartition,
            Buffer.DataType dataType,
            boolean isLastRecordInSegment)
            throws IOException {
        cacheDataManager.append(record, targetSubpartition, dataType, isLastRecordInSegment);
    }

    /**
     * In the local tier, only one memory data manager and only one file disk data manager are used,
     * and the subpartitionId is not used. So return directly.
     */
    @Override
    public SingleTierWriter createPartitionTierWriter() {
        return this;
    }

    @Override
    public SingleTierReader createSubpartitionTierReader(
            int subpartitionId, BufferAvailabilityListener availabilityListener)
            throws IOException {
        // If data file is not readable, throw PartitionNotFoundException to mark this result
        // partition failed. Otherwise, the partition data is not regenerated, so failover can not
        // recover the job.
        if (!Files.isReadable(dataFilePath)) {
            throw new PartitionNotFoundException(resultPartitionID);
        }
        // if broadcastOptimize is enabled, map every subpartitionId to the special broadcast
        // channel.
        subpartitionId = isBroadcastOnly ? BROADCAST_CHANNEL : subpartitionId;

        SubpartitionConsumer subpartitionConsumer = new SubpartitionConsumer(availabilityListener);
        ConsumerId lastConsumerId = lastConsumerIds[subpartitionId];
        checkMultipleConsumerIsAllowed(lastConsumerId, storeConfiguration);
        // assign a unique id for each consumer, now it is guaranteed by the value that is one
        // higher than the last consumerId's id field.
        ConsumerId consumerId = ConsumerId.newId(lastConsumerId);
        lastConsumerIds[subpartitionId] = consumerId;
        BufferConsumeView diskDataView =
                localDiskDataManager.registerNewConsumer(
                        subpartitionId, consumerId, subpartitionConsumer);
        subpartitionConsumer.setDiskDataView(diskDataView);
        return subpartitionConsumer;
    }

    @Override
    public boolean canStoreNextSegment(int subpartitionId) {
        return new Random().nextBoolean();
    }

    @Override
    public int getNewSegmentSize() {
        return numBytesInASegment;
    }

    @Override
    public void setNumBytesInASegment(int numBytesInASegment) {
        this.numBytesInASegment = numBytesInASegment;
    }

    @Override
    public boolean hasCurrentSegment(int subpartitionId, long segmentIndex) {
        return segmentIndexTracker.hasCurrentSegment(subpartitionId, segmentIndex);
    }

    @Override
    public void close() {
        if (!isClosed) {
            // close is called when task is finished or failed.
            checkNotNull(cacheDataManager).close();
            isClosed = true;
        }
    }

    @Override
    public void release() {
        // release is called when release by scheduler, later than close.
        // mainly work :
        // 1. release read scheduler.
        // 2. delete shuffle file.
        // 3. release all data in memory.
        if (!isReleased) {
            localDiskDataManager.release();
            checkNotNull(cacheDataManager).release();
            segmentIndexTracker.release();
            isReleased = true;
        }
    }

    @Override
    public void setOutputMetrics(OutputMetrics tieredStoreOutputMetrics) {
        checkNotNull(cacheDataManager).setOutputMetrics(tieredStoreOutputMetrics);
    }

    @Override
    public void setTimerGauge(TimerGauge timerGauge) {
        // nothing to do
    }

    private static void checkMultipleConsumerIsAllowed(
            ConsumerId lastConsumerId, TieredStoreConfiguration storeConfiguration) {
        if (Objects.equals(storeConfiguration.getTieredStoreSpillingType(), SELECTIVE.toString())) {
            checkState(
                    lastConsumerId == null,
                    "Multiple consumer is not allowed for %s spilling strategy mode",
                    storeConfiguration.getTieredStoreSpillingType());
        }
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        // Nothing to do
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        // Nothing to do
    }

    @Override
    public void flushAll() {
        // Nothing to do
    }

    @Override
    public void flush(int subpartitionIndex) {
        // Nothing to do
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        // Batch shuffle does not need to provide QueuedBuffers information
        return Integer.MIN_VALUE;
    }

    @Override
    public long getSizeOfQueuedBuffersUnsafe() {
        // Batch shuffle does not need to provide QueuedBuffers information
        return Integer.MIN_VALUE;
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        // Batch shuffle does not need to provide QueuedBuffers information
        return Integer.MIN_VALUE;
    }

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        // Batch shuffle doesn't support to set channel state writer
    }

    @Override
    public CheckpointedResultSubpartition getCheckpointedSubpartition(int subpartitionIndex) {
        // Batch shuffle doesn't support checkpoint
        return null;
    }

    @Override
    public void finishReadRecoveredState(boolean notifyAndBlockOnCompletion) throws IOException {
        // Batch shuffle doesn't support state
    }

    @Override
    public void onConsumedSubpartition(int subpartitionIndex) {
        // Batch shuffle doesn't support onConsumedSubpartition
    }

    @Override
    public CompletableFuture<Void> getAllDataProcessedFuture() {
        // Batch shuffle doesn't support getAllDataProcessedFuture
        return null;
    }

    @Override
    public void onSubpartitionAllDataProcessed(int subpartition) {
        // Batch shuffle doesn't support onSubpartitionAllDataProcessed
    }
}
