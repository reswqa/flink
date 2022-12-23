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

package org.apache.flink.runtime.io.network.partition.store.tier.dfs;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.store.common.BufferPoolHelper;
import org.apache.flink.runtime.io.network.partition.store.common.ConsumerId;
import org.apache.flink.runtime.io.network.partition.store.common.DataManagerOperation;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierDataGate;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierReader;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierWriter;
import org.apache.flink.runtime.io.network.partition.store.common.SubpartitionSegmentIndexTracker;
import org.apache.flink.runtime.io.network.partition.store.tier.local.BufferConsumeView;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.OutputMetrics;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.partition.store.tier.dfs.DfsFileWriter.BROADCAST_CHANNEL;

/** The DataManager of DFS. */
public class DfsDataManager implements SingleTierDataGate, DataManagerOperation {

    private final int numSubpartitions;

    private final boolean isBroadcastOnly;

    /** Record the last assigned consumerId for each subpartition. */
    private final ConsumerId[] lastConsumerIds;

    private final SubpartitionSegmentIndexTracker segmentIndexTracker;

    private final DfsCacheDataManager dfsCacheDataManager;

    public DfsDataManager(
            JobID jobID,
            int numSubpartitions,
            int networkBufferSize,
            ResultPartitionID resultPartitionID,
            BufferPoolHelper bufferPoolHelper,
            boolean isBroadcastOnly,
            String baseDfsPath,
            @Nullable BufferCompressor bufferCompressor)
            throws IOException {
        this.numSubpartitions = numSubpartitions;
        this.isBroadcastOnly = isBroadcastOnly;
        this.lastConsumerIds = new ConsumerId[numSubpartitions];
        this.segmentIndexTracker = new SubpartitionSegmentIndexTracker(numSubpartitions, isBroadcastOnly);
        this.dfsCacheDataManager =
                new DfsCacheDataManager(
                        jobID,
                        resultPartitionID,
                        isBroadcastOnly ? 1 : numSubpartitions,
                        networkBufferSize,
                        isBroadcastOnly,
                        baseDfsPath,
                        bufferPoolHelper,
                        bufferCompressor);
    }

    @Override
    public void setup() throws IOException {}

    @Override
    public SingleTierWriter createPartitionTierWriter() throws IOException {
        return new DfsFileWriter(
                numSubpartitions, isBroadcastOnly, segmentIndexTracker, dfsCacheDataManager);
    }

    @Override
    public SingleTierReader createSubpartitionTierReader(
            int subpartitionId, BufferAvailabilityListener availabilityListener)
            throws IOException {
        // if broadcastOptimize is enabled, map every subpartitionId to the special broadcast
        // channel.
        subpartitionId = isBroadcastOnly ? BROADCAST_CHANNEL : subpartitionId;

        DfsFileReader dfsFileReader = new DfsFileReader(availabilityListener);
        ConsumerId lastConsumerId = lastConsumerIds[subpartitionId];
        // assign a unique id for each consumer, now it is guaranteed by the value that is one
        // higher than the last consumerId's id field.
        ConsumerId consumerId = ConsumerId.newId(lastConsumerId);
        lastConsumerIds[subpartitionId] = consumerId;
        BufferConsumeView dfsDataView =
                dfsCacheDataManager.registerNewConsumer(subpartitionId, consumerId, dfsFileReader);

        dfsFileReader.setDfsDataView(dfsDataView);
        return dfsFileReader;
    }

    @Override
    public boolean canStoreNextSegment() {
        return true;
    }

    @Override
    public boolean hasCurrentSegment(int subpartitionId, int segmentIndex) {
        return segmentIndexTracker.hasCurrentSegment(subpartitionId, segmentIndex);
    }

    @Override
    public void onDataAvailable(int subpartitionId) {}

    @Override
    public void setOutputMetrics(OutputMetrics tieredStoreOutputMetrics) {
        dfsCacheDataManager.setOutputMetrics(tieredStoreOutputMetrics);
    }

    @Override
    public void close() {}

    @Override
    public void release() {
        segmentIndexTracker.release();
    }

    @VisibleForTesting
    @Override
    public Path getBaseSubpartitionPath(int subpartitionId) {
        return dfsCacheDataManager.getBaseSubpartitionPath(subpartitionId);
    }
}
