package org.apache.flink.runtime.io.network.partition.consumer.tier;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.SubpartitionIndexRange;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateFactory;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.runtime.throughput.BufferDebloater;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Factory for {@link TieredStoreSingleInputGate} to use in {@link NettyShuffleEnvironment}. */
public class TieredStoreSingleInputGateFactory extends SingleInputGateFactory {

    private final String baseDfsPath;

    public TieredStoreSingleInputGateFactory(
            @Nonnull ResourceID taskExecutorResourceId,
            @Nonnull NettyShuffleEnvironmentConfiguration networkConfig,
            @Nonnull ConnectionManager connectionManager,
            @Nonnull ResultPartitionManager partitionManager,
            @Nonnull TaskEventPublisher taskEventPublisher,
            @Nonnull NetworkBufferPool networkBufferPool,
            @Nullable String baseDfsPath) {
        super(
                taskExecutorResourceId,
                networkConfig,
                connectionManager,
                partitionManager,
                taskEventPublisher,
                networkBufferPool);
        this.baseDfsPath = baseDfsPath;
    }

    private TieredStoreSingleInputGate createInputGate(
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
            List<ResultPartitionID> resultPartitionIDs) {
        return new TieredStoreSingleInputGate(
                owningTaskName,
                gateIndex,
                subpartitionIndex,
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
                bufferDebloater,
                jobID,
                resultPartitionIDs,
                baseDfsPath);
    }

    @Override
    public TieredStoreSingleInputGate create(
            @Nonnull ShuffleIOOwnerContext owner,
            int gateIndex,
            @Nonnull InputGateDeploymentDescriptor igdd,
            @Nonnull PartitionProducerStateProvider partitionProducerStateProvider) {
        SupplierWithException<BufferPool, IOException> bufferPoolFactory =
                createBufferPoolFactory(networkBufferPool, floatingNetworkBuffersPerGate);

        BufferDecompressor bufferDecompressor = null;
        if (igdd.getConsumedPartitionType().supportCompression()
                && batchShuffleCompressionEnabled) {
            bufferDecompressor = new BufferDecompressor(networkBufferSize, compressionCodec);
        }

        final String owningTaskName = owner.getOwnerName();
        final MetricGroup networkInputGroup = owner.getInputGroup();

        SubpartitionIndexRange subpartitionIndexRange = igdd.getConsumedSubpartitionIndexRange();
        List<ResultPartitionID> resultPartitionIDs = new ArrayList<>();
        for (ShuffleDescriptor shuffleDescriptor : igdd.getShuffleDescriptors()) {
            resultPartitionIDs.add(shuffleDescriptor.getResultPartitionID());
        }
        TieredStoreSingleInputGate inputGate =
                createInputGate(
                        owningTaskName,
                        gateIndex,
                        owner.getExecutionAttemptID().getSubtaskIndex(),
                        igdd.getConsumedResultId(),
                        igdd.getConsumedPartitionType(),
                        subpartitionIndexRange,
                        calculateNumChannels(
                                igdd.getShuffleDescriptors().length, subpartitionIndexRange),
                        partitionProducerStateProvider,
                        bufferPoolFactory,
                        bufferDecompressor,
                        networkBufferPool,
                        networkBufferSize,
                        new ThroughputCalculator(SystemClock.getInstance()),
                        maybeCreateBufferDebloater(
                                owningTaskName, gateIndex, networkInputGroup.addGroup(gateIndex)),
                        owner.getJobID(),
                        resultPartitionIDs);

        InputChannelMetrics metrics =
                new InputChannelMetrics(networkInputGroup, owner.getParentGroup());
        createInputChannels(owningTaskName, igdd, inputGate, subpartitionIndexRange, metrics);
        return inputGate;
    }
}
