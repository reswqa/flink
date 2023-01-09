package org.apache.flink.runtime.io.network.partition.store.tier.local.memory;

/** The interface including operations in {@link LocalMemoryDataManager}. */
public interface LocalMemoryDataManagerOperations {

    boolean isLastRecordInSegment(int subpartitionId, int bufferIndex);
}
