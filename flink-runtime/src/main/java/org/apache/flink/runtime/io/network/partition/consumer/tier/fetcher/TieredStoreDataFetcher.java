package org.apache.flink.runtime.io.network.partition.consumer.tier.fetcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** The data fetcher interface for Tiered Store, which can fetch shuffle data on DFS, or REMOTE. */
public interface TieredStoreDataFetcher {

    /** Set up the TieredStoreDataFetcher. */
    void setup(
            JobID jobID,
            List<ResultPartitionID> resultPartitionIDs,
            BufferPool bufferPool,
            int subpartitionIndex,
            String baseDfsPath)
            throws InterruptedException, IOException;

    /** Close the TieredStoreDataFetcher. */
    void close() throws IOException;

    /** Initialize the TieredStoreDataFetcher with segment information. */
    void setSegmentInfo(InputChannel inputChannel, Buffer segmentInfo)
            throws InterruptedException, IOException;

    /** Get the buffer in the blocking mode or non-blocking mode. */
    Optional<BufferAndAvailability> getNextBuffer(Boolean isBlocking) throws InterruptedException;

    /** Get the current state of TieredStoreDataFetcher. */
    DataFetcherState getState();

    /** Set the current state of TieredStoreDataFetcher. */
    void setState(DataFetcherState state);

    /** Get the current reading channel. */
    Optional<InputChannel> getCurrentChannel();
}
