package org.apache.flink.runtime.io.network.partition.consumer.tier;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.tier.common.SingleChannelDataClient;

import java.io.IOException;
import java.util.Optional;

/** The data client is used to fetch data from Dfs tier. */
public class SingleChannelDfsDataClient implements SingleChannelDataClient {
    @Override
    public boolean hasSegmentId(InputChannel inputChannel, long segmentId) {
        return false;
    }

    @Override
    public Optional<InputChannel.BufferAndAvailability> getNextBuffer(
            InputChannel inputChannel, long segmentId) {
        return Optional.empty();
    }

    @Override
    public void close() throws IOException {}
}
