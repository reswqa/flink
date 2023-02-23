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

package org.apache.flink.runtime.io.network.partition.consumer.tier;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.deployment.SubpartitionIndexRange;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.tier.fetcher.LocalDataFetcherClient;
import org.apache.flink.runtime.io.network.partition.consumer.tier.fetcher.TieredStoreDataFetcherClient;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.throughput.BufferDebloater;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * An tiered store input gate consumes one or more partitions of a single produced intermediate
 * result. It can also consume data from DFS, or REMOTE.
 */
public class TieredStoreSingleInputGate extends SingleInputGate {

    private final JobID jobID;

    private final List<ResultPartitionID> resultPartitionIDs;

    private final int subpartitionIndex;

    private final String baseDfsPath;

    private TieredStoreDataFetcherClient dataFetcherClient;

    public TieredStoreSingleInputGate(
            String owningTaskName,
            int gateIndex,
            int subpartitionIndex,
            IntermediateDataSetID consumedResultId,
            ResultPartitionType consumedPartitionType,
            SubpartitionIndexRange subpartitionIndexRange,
            int numberOfInputChannels,
            PartitionProducerStateProvider partitionProducerStateProvider,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory,
            @Nullable BufferDecompressor bufferDecompressor,
            MemorySegmentProvider memorySegmentProvider,
            int segmentSize,
            ThroughputCalculator throughputCalculator,
            @Nullable BufferDebloater bufferDebloater,
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            String baseDfsPath) {
        super(
                owningTaskName,
                gateIndex,
                consumedResultId,
                consumedPartitionType,
                subpartitionIndexRange,
                numberOfInputChannels,
                partitionProducerStateProvider,
                bufferPoolFactory,
                bufferDecompressor,
                memorySegmentProvider,
                segmentSize,
                throughputCalculator,
                bufferDebloater);
        this.jobID = jobID;
        this.resultPartitionIDs = resultPartitionIDs;
        this.subpartitionIndex = subpartitionIndex;
        this.baseDfsPath = baseDfsPath;
    }

    @Override
    public void setup() throws IOException {
        super.setup();
        this.dataFetcherClient = new LocalDataFetcherClient(inputChannelsWithData, this);
    }

    @Override
    public Optional<InputWithData<InputChannel, InputChannel.BufferAndAvailability>>
            waitAndGetNextData(boolean blocking) throws IOException, InterruptedException {
        return dataFetcherClient.waitAndGetNextData(blocking);
    }

    @Override
    public void close() throws IOException {
        super.close();
        dataFetcherClient.close();
    }
}
