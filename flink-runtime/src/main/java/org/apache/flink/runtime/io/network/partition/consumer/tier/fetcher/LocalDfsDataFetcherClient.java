package org.apache.flink.runtime.io.network.partition.consumer.tier.fetcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.PrioritizedDeque;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.tier.TieredStoreSingleInputGate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The data fetcher client for Local and Dfs Tier. */
public class LocalDfsDataFetcherClient implements TieredStoreDataFetcherClient {

    private static final Logger LOG = LoggerFactory.getLogger(TieredStoreSingleInputGate.class);

    private final PrioritizedDeque<InputChannel> inputChannelsWithData;

    private final TieredStoreDataFetcher tieredStoreDataFetcher = new DfsDataFetcher();

    private final JobID jobID;

    private final List<ResultPartitionID> resultPartitionIDs;

    private final MemorySegmentProvider memorySegmentProvider;

    private final int subpartitionIndex;

    private final String baseDfsPath;

    private final SingleInputGate singleInputGate;

    public LocalDfsDataFetcherClient(
            SingleInputGate singleInputGate,
            PrioritizedDeque<InputChannel> inputChannelsWithData,
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            MemorySegmentProvider memorySegmentProvider,
            int subpartitionIndex,
            String baseDfsPath) {
        this.singleInputGate = singleInputGate;
        this.inputChannelsWithData = inputChannelsWithData;
        this.jobID = jobID;
        this.resultPartitionIDs = resultPartitionIDs;
        this.memorySegmentProvider = memorySegmentProvider;
        this.subpartitionIndex = subpartitionIndex;
        this.baseDfsPath = baseDfsPath;
    }

    @Override
    public void setup() throws Exception {
        BufferPool bufferPool =
                ((NetworkBufferPool) memorySegmentProvider).createBufferPool(10, 100);
        tieredStoreDataFetcher.setup(
                this.jobID, this.resultPartitionIDs, bufferPool, subpartitionIndex, baseDfsPath);
    }

    @Override
    public Optional<InputGate.InputWithData<InputChannel, InputChannel.BufferAndAvailability>>
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
                    Optional<InputChannel> inputChannelOpt = singleInputGate.getChannel(blocking);
                    if (!inputChannelOpt.isPresent()) {
                        return Optional.empty();
                    }
                    inputChannel = inputChannelOpt.get();
                    bufferAndAvailabilityOpt = inputChannel.getNextBuffer();
                    LOG.debug("### TRYING GET 1.2");
                    if (!bufferAndAvailabilityOpt.isPresent()) {
                        LOG.debug("### TRYING GET 1.3");
                        singleInputGate.checkUnavailability();
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
                    singleInputGate.queueChannelUnsafe(inputChannel, bufferAndAvailability.morePriorityEvents());
                }
                singleInputGate.checkUnavailability();
                if (bufferAndAvailability.buffer().getDataType()
                        == Buffer.DataType.SEGMENT_EVENT) {
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
                        new InputGate.InputWithData<>(
                                inputChannel,
                                bufferAndAvailability,
                                !inputChannelsWithData.isEmpty(),
                                false));
            }
        }
    }

    @Override
    public void close() throws IOException {
        tieredStoreDataFetcher.close();
    }
}
