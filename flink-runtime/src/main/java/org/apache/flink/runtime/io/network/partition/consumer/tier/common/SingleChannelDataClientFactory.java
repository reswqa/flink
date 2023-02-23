package org.apache.flink.runtime.io.network.partition.consumer.tier.common;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.network.partition.PrioritizedDeque;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.tier.SingleChannelDfsDataClient;
import org.apache.flink.runtime.io.network.partition.consumer.tier.SingleChannelLocalDataClient;

import java.util.List;

/** The factory of {@link SingleChannelDataClient}. */
public class SingleChannelDataClientFactory {

    private final JobID jobID;

    private final List<ResultPartitionID> resultPartitionIDs;

    private final MemorySegmentProvider memorySegmentProvider;

    private final int subpartitionIndex;

    private final String baseDfsPath;

    private final SingleInputGate singleInputGate;

    private final boolean hasDfsClient;

    public SingleChannelDataClientFactory(
            SingleInputGate singleInputGate,
            int numInputChannels,
            PrioritizedDeque<InputChannel> inputChannelsWithData,
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            MemorySegmentProvider memorySegmentProvider,
            int subpartitionIndex,
            String baseDfsPath) {
        this.singleInputGate = singleInputGate;
        this.jobID = jobID;
        this.resultPartitionIDs = resultPartitionIDs;
        this.memorySegmentProvider = memorySegmentProvider;
        this.subpartitionIndex = subpartitionIndex;
        this.baseDfsPath = baseDfsPath;
        this.hasDfsClient = baseDfsPath != null;
    }

    public SingleChannelDataClient createLocalSingleChannelDataClient() {
        return new SingleChannelLocalDataClient();
    }

    public SingleChannelDataClient createDfsSingleChannelDataClient() {
        if (!hasDfsClient) {
            return null;
        }
        return new SingleChannelDfsDataClient();
    }

    public boolean hasDfsClient() {
        return hasDfsClient;
    }
}
