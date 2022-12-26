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

package org.apache.flink.runtime.io.network.partition.store.tier.local;

import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.store.TieredStoreConfiguration;
import org.apache.flink.runtime.io.network.partition.store.common.BufferPoolHelper;
import org.apache.flink.runtime.io.network.partition.store.common.ConsumerId;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierDataGate;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierReader;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierWriter;
import org.apache.flink.runtime.io.network.partition.store.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.CacheDataManager;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.FullSpillingStrategy;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.LocalDiskDataManager;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.OutputMetrics;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.RegionBufferIndexTracker;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.RegionBufferIndexTrackerImpl;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.SelectiveSpillingStrategy;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.SubpartitionConsumer;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.SubpartitionFileReaderImpl;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.TsSpillingStrategy;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The DataManager of LOCAL. */
public class LocalDataManager implements SingleTierWriter, SingleTierDataGate {

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

    private volatile boolean isReleased;

    private volatile boolean isClosed;

    public LocalDataManager(
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
        this.segmentIndexTracker = new SubpartitionSegmentIndexTracker(numSubpartitions, isBroadcastOnly);
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
        switch (storeConfiguration.getSpillingStrategyType()) {
            case FULL:
                return new FullSpillingStrategy(storeConfiguration);
            case SELECTIVE:
                return new SelectiveSpillingStrategy(storeConfiguration);
            default:
                throw new IllegalConfigurationException("Illegal spilling strategy.");
        }
    }

    @Override
    public void emit(
            ByteBuffer record,
            int targetSubpartition,
            Buffer.DataType dataType,
            boolean isBroadcast,
            boolean isLastRecordInSegment,
            boolean isEndOfPartition,
            int segmentIndex)
            throws IOException {
        segmentIndexTracker.addSubpartitionSegmentIndex(
                targetSubpartition, segmentIndex);
        emit(record, targetSubpartition, dataType, isLastRecordInSegment);
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

        BufferConsumeView memoryDataView =
                checkNotNull(cacheDataManager)
                        .registerNewConsumer(subpartitionId, consumerId, subpartitionConsumer);

        subpartitionConsumer.setDiskDataView(diskDataView);
        subpartitionConsumer.setMemoryDataView(memoryDataView);
        return subpartitionConsumer;
    }

    @Override
    public boolean canStoreNextSegment() {
        return new Random().nextBoolean();
        //return true;
    }

    @Override
    public boolean hasCurrentSegment(int subpartitionId, int segmentIndex) {
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
    public org.apache.flink.core.fs.Path getBaseSubpartitionPath(int subpartitionId) {
        return null;
    }

    @Override
    public void setOutputMetrics(OutputMetrics tieredStoreOutputMetrics) {
        checkNotNull(cacheDataManager).setOutputMetrics(tieredStoreOutputMetrics);
    }

    private static void checkMultipleConsumerIsAllowed(
            ConsumerId lastConsumerId, TieredStoreConfiguration storeConfiguration) {
        if (storeConfiguration.getSpillingStrategyType()
                == TieredStoreConfiguration.SpillingStrategyType.SELECTIVE) {
            checkState(
                    lastConsumerId == null,
                    "Multiple consumer is not allowed for %s spilling strategy mode",
                    storeConfiguration.getSpillingStrategyType());
        }
    }
}
