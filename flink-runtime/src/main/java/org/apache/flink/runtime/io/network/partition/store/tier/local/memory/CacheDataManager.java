/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.store.tier.local.memory;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.store.TieredStoreMode;
import org.apache.flink.runtime.io.network.partition.store.common.BufferConsumeView;
import org.apache.flink.runtime.io.network.partition.store.common.BufferPoolHelper;
import org.apache.flink.runtime.io.network.partition.store.common.ConsumerId;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.OutputMetrics;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** This class is responsible for managing cached buffers data before flush to local files. */
public class CacheDataManager implements CacheDataManagerOperation {

    private static final Logger LOG = LoggerFactory.getLogger(CacheDataManager.class);

    private final int numSubpartitions;

    private final SubpartitionCacheDataManager[] subpartitionCacheDataManagers;

    private final BufferPoolHelper bufferPoolHelper;

    /**
     * Each element of the list is all views of the subpartition corresponding to its index, which
     * are stored in the form of a map that maps consumer id to its subpartition view.
     */
    private final List<Map<ConsumerId, SubpartitionConsumerInternalOperations>>
            subpartitionViewOperationsMap;

    public CacheDataManager(
            int numSubpartitions,
            int bufferSize,
            BufferPoolHelper bufferPoolHelper,
            BufferCompressor bufferCompressor)
            throws IOException {
        this.numSubpartitions = numSubpartitions;
        this.bufferPoolHelper = bufferPoolHelper;
        this.subpartitionCacheDataManagers = new SubpartitionCacheDataManager[numSubpartitions];

        this.subpartitionViewOperationsMap = new ArrayList<>(numSubpartitions);
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionCacheDataManagers[subpartitionId] =
                    new SubpartitionCacheDataManager(
                            subpartitionId,
                            bufferSize,
                            bufferCompressor,
                            this);
            subpartitionViewOperationsMap.add(new ConcurrentHashMap<>());
        }
    }

    // ------------------------------------
    //          For ResultPartition
    // ------------------------------------

    /**
     * Append record to {@link CacheDataManager}, It will be managed by {@link
     * SubpartitionConsumerCacheDataManager} witch it belongs to.
     *
     * @param record to be managed by this class.
     * @param targetChannel target subpartition of this record.
     * @param dataType the type of this record. In other words, is it data or event.
     * @param isLastRecordInSegment whether this record is the last record in a segment.
     */
    public void append(
            ByteBuffer record,
            int targetChannel,
            Buffer.DataType dataType,
            boolean isLastRecordInSegment)
            throws IOException {
        try {
            getSubpartitionMemoryDataManager(targetChannel)
                    .append(record, dataType, isLastRecordInSegment);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    /**
     * Register {@link
     * org.apache.flink.runtime.io.network.partition.store.tier.local.memory.SubpartitionConsumerInternalOperations}
     * to {@link #subpartitionViewOperationsMap}. It is used to obtain the consumption progress of
     * the subpartition.
     */
    public BufferConsumeView registerNewConsumer(
            int subpartitionId,
            ConsumerId consumerId,
            SubpartitionConsumerInternalOperations viewOperations) {
        SubpartitionConsumerInternalOperations oldView =
                subpartitionViewOperationsMap.get(subpartitionId).put(consumerId, viewOperations);
        Preconditions.checkState(
                oldView == null, "Each subpartition view should have unique consumerId.");
        return getSubpartitionMemoryDataManager(subpartitionId).registerNewConsumer(consumerId);
    }

    /** Close this {@link CacheDataManager}, it means no data will be appended to memory. */
    public void close() {}

    /**
     * Release this {@link CacheDataManager}, it means all memory taken by this class will recycle.
     */
    public void release() {
        for (int i = 0; i < numSubpartitions; i++) {
            getSubpartitionMemoryDataManager(i).release();
        }
    }

    public void setOutputMetrics(OutputMetrics metrics) {
        // HsOutputMetrics is not thread-safe. It can be shared by all the subpartitions because it
        // is expected always updated from the producer task's mailbox thread.
        for (int i = 0; i < numSubpartitions; i++) {
            getSubpartitionMemoryDataManager(i).setOutputMetrics(metrics);
        }
    }

    // ------------------------------------
    //      Callback for subpartition
    // ------------------------------------

    @Override
    public BufferBuilder requestBufferFromPool() throws InterruptedException {
        MemorySegment segment =
                bufferPoolHelper.requestMemorySegmentBlocking(
                        TieredStoreMode.TieredType.IN_MEM, true);
        return new BufferBuilder(segment, this::recycleBuffer);
    }

    @Override
    public void onDataAvailable(int subpartitionId, Collection<ConsumerId> consumerIds) {
        Map<ConsumerId, SubpartitionConsumerInternalOperations> consumerViewMap =
                subpartitionViewOperationsMap.get(subpartitionId);
        consumerIds.forEach(
                consumerId -> {
                    SubpartitionConsumerInternalOperations consumerView =
                            consumerViewMap.get(consumerId);
                    if (consumerView != null) {
                        consumerView.notifyDataAvailable();
                    }
                });
    }

    @Override
    public void onConsumerReleased(int subpartitionId, ConsumerId consumerId) {
        subpartitionViewOperationsMap.get(subpartitionId).remove(consumerId);
        getSubpartitionMemoryDataManager(subpartitionId).releaseConsumer(consumerId);
    }

    @Override
    public boolean isLastBufferInSegment(int subpartitionId, int bufferIndex) {
        return getSubpartitionMemoryDataManager(subpartitionId)
                .getLastBufferIndexOfSegments()
                .contains(bufferIndex);
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    private SubpartitionCacheDataManager getSubpartitionMemoryDataManager(int targetChannel) {
        return subpartitionCacheDataManagers[targetChannel];
    }

    private void recycleBuffer(MemorySegment buffer) {
        bufferPoolHelper.recycleBuffer(buffer, TieredStoreMode.TieredType.IN_MEM, true);
    }
}
