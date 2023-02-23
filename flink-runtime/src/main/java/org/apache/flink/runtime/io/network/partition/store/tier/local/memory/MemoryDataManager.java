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

package org.apache.flink.runtime.io.network.partition.store.tier.local.memory;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultSubpartition;
import org.apache.flink.runtime.io.network.partition.store.TieredStoreConfiguration;
import org.apache.flink.runtime.io.network.partition.store.TieredStoreMode;
import org.apache.flink.runtime.io.network.partition.store.common.BufferConsumeView;
import org.apache.flink.runtime.io.network.partition.store.common.BufferPoolHelper;
import org.apache.flink.runtime.io.network.partition.store.common.ConsumerId;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierDataGate;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierReader;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierWriter;
import org.apache.flink.runtime.io.network.partition.store.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.OutputMetrics;
import org.apache.flink.runtime.metrics.TimerGauge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.io.network.partition.store.TieredStoreMode.SpillingType.SELECTIVE;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The DataManager of LOCAL file. */
public class MemoryDataManager implements SingleTierDataGate {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryDataManager.class);

    public static final int BROADCAST_CHANNEL = 0;

    private final int numSubpartitions;

    private final int networkBufferSize;

    private final BufferPoolHelper bufferPoolHelper;

    private final boolean isBroadcastOnly;

    private final BufferCompressor bufferCompressor;

    private final TieredStoreConfiguration storeConfiguration;

    /** Record the last assigned consumerId for each subpartition. */
    private final ConsumerId[] lastConsumerIds;

    private MemoryDataWriter memoryDataWriter;

    private final SubpartitionSegmentIndexTracker segmentIndexTracker;

    private int numBytesInASegment = 1;

    private final int bufferNumberInSegment = numBytesInASegment / 32 / 1024;

    private volatile boolean isReleased;

    private volatile boolean isClosed;

    public MemoryDataManager(
            int numSubpartitions,
            int networkBufferSize,
            BufferPoolHelper bufferPoolHelper,
            boolean isBroadcastOnly,
            @Nullable BufferCompressor bufferCompressor,
            TieredStoreConfiguration storeConfiguration) {
        this.numSubpartitions = numSubpartitions;
        this.networkBufferSize = networkBufferSize;
        this.isBroadcastOnly = isBroadcastOnly;
        this.bufferPoolHelper = bufferPoolHelper;
        this.bufferCompressor = bufferCompressor;
        checkNotNull(bufferCompressor);
        this.storeConfiguration = storeConfiguration;
        this.lastConsumerIds = new ConsumerId[numSubpartitions];
        this.segmentIndexTracker =
                new SubpartitionSegmentIndexTracker(numSubpartitions, isBroadcastOnly);
    }

    @Override
    public void setup() throws IOException {
        this.memoryDataWriter =
                new MemoryDataWriter(
                        isBroadcastOnly ? 1 : numSubpartitions,
                        networkBufferSize,
                        bufferPoolHelper,
                        bufferCompressor,
                        segmentIndexTracker,
                        isBroadcastOnly,
                        numSubpartitions);
        this.memoryDataWriter.setup();
    }


    /**
     * In the local tier, only one memory data manager and only one file disk data manager are used,
     * and the subpartitionId is not used. So return directly.
     */
    @Override
    public SingleTierWriter createPartitionTierWriter() {
        return memoryDataWriter;
    }

    @Override
    public SingleTierReader createSubpartitionTierReader(
            int subpartitionId, BufferAvailabilityListener availabilityListener)
            throws IOException {
        // if broadcastOptimize is enabled, map every subpartitionId to the special broadcast
        // channel.
        subpartitionId = isBroadcastOnly ? BROADCAST_CHANNEL : subpartitionId;

        MemoryReader memoryReader = new MemoryReader(availabilityListener);
        ConsumerId lastConsumerId = lastConsumerIds[subpartitionId];
        checkMultipleConsumerIsAllowed(lastConsumerId, storeConfiguration);
        // assign a unique id for each consumer, now it is guaranteed by the value that is one
        // higher than the last consumerId's id field.
        ConsumerId consumerId = ConsumerId.newId(lastConsumerId);
        lastConsumerIds[subpartitionId] = consumerId;

        BufferConsumeView memoryDataView =
                checkNotNull(memoryDataWriter)
                        .registerNewConsumer(subpartitionId, consumerId, memoryReader);

        memoryReader.setMemoryDataView(memoryDataView);
        return memoryReader;
    }

    int flag = 0;

    @Override
    public boolean canStoreNextSegment(int subpartitionId) {
        //flag++;
        //return flag % 2 == 1;
        //return true;
        return bufferPoolHelper.canStoreNextSegmentForMemoryTier(bufferNumberInSegment)
                && memoryDataWriter.isConsumerRegistered(subpartitionId);
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
            segmentIndexTracker.release();
            isReleased = true;
        }
    }

    @Override
    public void setOutputMetrics(OutputMetrics tieredStoreOutputMetrics) {
        checkNotNull(memoryDataWriter).setOutputMetrics(tieredStoreOutputMetrics);
    }

    @Override
    public void setTimerGauge(TimerGauge timerGauge) {
        // nothing to do
    }

    private static void checkMultipleConsumerIsAllowed(
            ConsumerId lastConsumerId, TieredStoreConfiguration storeConfiguration) {
        if (TieredStoreMode.SpillingType.valueOf(storeConfiguration.getTieredStoreSpillingType())
                == SELECTIVE) {
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
