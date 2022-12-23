package org.apache.flink.runtime.io.network.partition.consumer.tier.fetcher;

/** The state of {@link TieredStoreDataFetcher}. */
public enum DataFetcherState {
    CLOSED,
    WAITING,
    RUNNING,
    FINISHED;
}
