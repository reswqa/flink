package org.apache.flink.runtime.io.network.partition.consumer.tier.fetcher;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.PrioritizedDeque;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;

import java.io.IOException;
import java.util.Optional;

/** The data fetcher client for Local Tier. */
public class LocalDataFetcherClient implements TieredStoreDataFetcherClient {

    private final PrioritizedDeque<InputChannel> inputChannelsWithData;

    private final SingleInputGate singleInputGate;

    public LocalDataFetcherClient(
            PrioritizedDeque<InputChannel> inputChannelsWithData, SingleInputGate singleInputGate) {
        this.inputChannelsWithData = inputChannelsWithData;
        this.singleInputGate = singleInputGate;
    }

    @Override
    public void setup() {
        // nothing to do
    }

    public Optional<InputGate.InputWithData<InputChannel, InputChannel.BufferAndAvailability>>
            waitAndGetNextData(boolean blocking) throws IOException, InterruptedException {
        while (true) {
            synchronized (inputChannelsWithData) {
                Optional<InputChannel.BufferAndAvailability> bufferAndAvailabilityOpt;
                InputChannel inputChannel;
                Optional<InputChannel> inputChannelOpt = singleInputGate.getChannel(blocking);
                if (!inputChannelOpt.isPresent()) {
                    return Optional.empty();
                }
                inputChannel = inputChannelOpt.get();
                bufferAndAvailabilityOpt = inputChannel.getNextBuffer();
                if (!bufferAndAvailabilityOpt.isPresent()) {
                    singleInputGate.checkUnavailability();
                    continue;
                }
                final InputChannel.BufferAndAvailability bufferAndAvailability =
                        bufferAndAvailabilityOpt.get();
                if (bufferAndAvailability.moreAvailable()) {
                    // enqueue the inputChannel at the end to avoid starvation
                    singleInputGate.queueChannelUnsafe(inputChannel, bufferAndAvailability.morePriorityEvents());
                }

                final boolean morePriorityEvents =
                        inputChannelsWithData.getNumPriorityElements() > 0;
                if (bufferAndAvailability.hasPriority()) {
                    singleInputGate.lastPrioritySequenceNumber[inputChannel.getChannelIndex()] =
                            bufferAndAvailability.getSequenceNumber();
                    if (!morePriorityEvents) {
                        singleInputGate.priorityAvailabilityHelper.resetUnavailable();
                    }
                }
                singleInputGate.checkUnavailability();
                if (bufferAndAvailability.buffer().getDataType()
                        == Buffer.DataType.SEGMENT_EVENT) {
                    //LOG.debug("### TRYING GET 3");
                    //if (tieredStoreDataFetcher.getState() != DataFetcherState.WAITING) {
                    //    throw new IOException(
                    //            "Trying to set segment info when data fetcher is still running.");
                    //}
                    //tieredStoreDataFetcher.setSegmentInfo(
                    //        inputChannel, bufferAndAvailability.buffer());
                    continue;
                }
                return Optional.of(
                        new InputGate.InputWithData<>(
                                inputChannel,
                                bufferAndAvailability,
                                !inputChannelsWithData.isEmpty(),
                                morePriorityEvents));
            }
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}
