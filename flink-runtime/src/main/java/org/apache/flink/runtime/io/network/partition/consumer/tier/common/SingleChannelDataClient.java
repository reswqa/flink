package org.apache.flink.runtime.io.network.partition.consumer.tier.common;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;

import java.io.IOException;
import java.util.Optional;

/** The interface of {@link SingleChannelDataClient} in Tiered Store. */
public interface SingleChannelDataClient {

    boolean hasSegmentId(InputChannel inputChannel, long segmentId);

    Optional<InputChannel.BufferAndAvailability> getNextBuffer(
            InputChannel inputChannel, long segmentId) throws IOException, InterruptedException;

    void close() throws IOException;
}
