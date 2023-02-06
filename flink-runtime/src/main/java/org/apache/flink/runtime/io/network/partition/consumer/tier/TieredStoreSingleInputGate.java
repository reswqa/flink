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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.tier.fetcher.DataFetcherState;
import org.apache.flink.runtime.io.network.partition.consumer.tier.fetcher.DfsDataFetcher;
import org.apache.flink.runtime.io.network.partition.consumer.tier.fetcher.TieredStoreDataFetcher;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.throughput.BufferDebloater;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An tiered store input gate consumes one or more partitions of a single produced intermediate
 * result. It can also consume data from DFS, or REMOTE.
 */
public class TieredStoreSingleInputGate extends SingleInputGate {

    private static final Logger LOG = LoggerFactory.getLogger(TieredStoreSingleInputGate.class);

    private final TieredStoreDataFetcher tieredStoreDataFetcher = new DfsDataFetcher();

    private final JobID jobID;

    private final List<ResultPartitionID> resultPartitionIDs;

    private final int subpartitionIndex;

    private final String baseDfsPath;

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
        if(this.baseDfsPath != null){
            try {
                tieredStoreDataFetcher.setup(
                        this.jobID,
                        this.resultPartitionIDs,
                        getNewLocalBufferPool(),
                        subpartitionIndex,
                        baseDfsPath);
            } catch (InterruptedException e) {
                throw new RuntimeException("TieredStoreDataFetcher failed to setup.");
            }
        }
    }

    private BufferPool getNewLocalBufferPool() throws IOException {
        NetworkBufferPool networkBufferPool = (NetworkBufferPool) getMemorySegmentProvider();
        return networkBufferPool.createBufferPool(10, 100);
    }

    @Override
    protected Optional<InputWithData<InputChannel, InputChannel.BufferAndAvailability>>
            waitAndGetNextData(boolean blocking) throws IOException, InterruptedException {
        while (true) {
            synchronized (inputChannelsWithData) {
                Optional<InputChannel.BufferAndAvailability> bufferAndAvailabilityOpt;
                InputChannel inputChannel;
                LOG.debug("### TRYING GET 1");
                if (tieredStoreDataFetcher.getState() == DataFetcherState.RUNNING) {
                    LOG.debug("### TRYING GET 4");
                    bufferAndAvailabilityOpt = tieredStoreDataFetcher.getNextBuffer(true);
                    LOG.debug("### TRYING GET 5");
                    bufferAndAvailabilityOpt.ifPresent(
                            bufferAndAvailability ->
                                    LOG.debug(
                                            "### Getting the requested bufferAndAvailabilityOpt: "
                                                    + bufferAndAvailability.buffer().getSize()));
                    if (!bufferAndAvailabilityOpt.isPresent()) {
                        LOG.debug(
                                "### TRYING GET, but result is Empty!, {}",
                                tieredStoreDataFetcher.getState());
                    }
                    inputChannel =
                            checkNotNull(tieredStoreDataFetcher.getCurrentChannel().orElse(null));
                    if (!bufferAndAvailabilityOpt.isPresent()) {
                        tieredStoreDataFetcher.setState(DataFetcherState.WAITING);
                        continue;
                    }
                } else {
                    LOG.debug("### TRYING GET 1.1, is blocking {}", blocking);
                    Optional<InputChannel> inputChannelOpt = getChannel(blocking);
                    if (!inputChannelOpt.isPresent()) {
                        return Optional.empty();
                    }
                    inputChannel = inputChannelOpt.get();
                    bufferAndAvailabilityOpt = inputChannel.getNextBuffer();
                    LOG.debug("### TRYING GET 1.2");
                    if (!bufferAndAvailabilityOpt.isPresent()) {
                        LOG.debug("### TRYING GET 1.3");
                        checkUnavailability();
                        continue;
                    }
                }
                LOG.debug(
                        "### TRYING GET 2, {}, {}",
                        bufferAndAvailabilityOpt.get().buffer().getDataType(),
                        bufferAndAvailabilityOpt.get().buffer().getSize());
                final InputChannel.BufferAndAvailability bufferAndAvailability =
                        bufferAndAvailabilityOpt.get();
                if (bufferAndAvailability.moreAvailable()) {
                    LOG.debug("### TRYING GET 2.1");
                    // enqueue the inputChannel at the end to avoid starvation
                    queueChannelUnsafe(inputChannel, bufferAndAvailability.morePriorityEvents());
                }

                final boolean morePriorityEvents =
                        inputChannelsWithData.getNumPriorityElements() > 0;
                if (bufferAndAvailability.hasPriority()) {
                    lastPrioritySequenceNumber[inputChannel.getChannelIndex()] =
                            bufferAndAvailability.getSequenceNumber();
                    if (!morePriorityEvents) {
                        priorityAvailabilityHelper.resetUnavailable();
                    }
                }
                checkUnavailability();
                if (bufferAndAvailability.buffer().getDataType()
                        == Buffer.DataType.SEGMENT_INFO_BUFFER) {
                    LOG.debug("### TRYING GET 3");
                    if (tieredStoreDataFetcher.getState() != DataFetcherState.WAITING) {
                        throw new IOException(
                                "Trying to set segment info when data fetcher is still running.");
                    }
                    tieredStoreDataFetcher.setSegmentInfo(
                            inputChannel, bufferAndAvailability.buffer());
                    continue;
                }
                LOG.debug("### TRYING GET NON DFS DATA");
                return Optional.of(
                        new InputWithData<>(
                                inputChannel,
                                bufferAndAvailability,
                                !inputChannelsWithData.isEmpty(),
                                morePriorityEvents));
            }
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        tieredStoreDataFetcher.close();
    }
}
