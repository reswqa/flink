package org.apache.flink.runtime.io.network.partition.consumer.tier.common;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;

import java.io.IOException;
import java.util.Optional;

/** The interface of {@link DataFetcher} in Tiered Store. */
public interface DataFetcher {

    void setup() throws IOException;

    Optional<BufferAndAvailability> getNextBuffer(
            InputChannel inputChannel) throws IOException, InterruptedException;

    void close() throws IOException;
}
