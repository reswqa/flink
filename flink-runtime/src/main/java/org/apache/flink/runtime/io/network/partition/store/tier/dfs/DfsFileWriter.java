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

package org.apache.flink.runtime.io.network.partition.store.tier.dfs;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierWriter;
import org.apache.flink.runtime.io.network.partition.store.common.SubpartitionSegmentIndexTracker;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Through the {@link DfsFileWriter}, records from {@link DfsDataManager} is writen to cached
 * buffers.
 */
public class DfsFileWriter implements SingleTierWriter {

    public static final int BROADCAST_CHANNEL = 0;

    private final int numSubpartitions;

    private final boolean isBroadcastOnly;

    private final SubpartitionSegmentIndexTracker segmentIndexTracker;

    private final DfsCacheDataManager cacheDataManager;

    public DfsFileWriter(
            int numSubpartitions,
            boolean isBroadcastOnly,
            SubpartitionSegmentIndexTracker segmentIndexTracker,
            DfsCacheDataManager dfsCacheDataManager) {
        this.numSubpartitions = numSubpartitions;
        this.isBroadcastOnly = isBroadcastOnly;
        this.segmentIndexTracker = segmentIndexTracker;
        this.cacheDataManager = dfsCacheDataManager;
    }

    @Override
    public void setup() throws IOException {}

    @Override
    public void emit(
            ByteBuffer record,
            int targetSubpartition,
            Buffer.DataType dataType,
            boolean isBroadcast,
            boolean isLastRecordInSegment,
            boolean isEndOfPartition,
            int segmentIndex)
            throws IOException {
        if (segmentIndexTracker.addSubpartitionSegmentIndex(
                targetSubpartition, segmentIndex)) {
            cacheDataManager.startSegment(targetSubpartition, segmentIndex);
        }
        emit(record, targetSubpartition, dataType, isLastRecordInSegment);
        if (isLastRecordInSegment || isEndOfPartition) {
            cacheDataManager.finishSegment(targetSubpartition, segmentIndex);
        }
    }

    private void emit(
            ByteBuffer record,
            int targetSubpartition,
            Buffer.DataType dataType,
            boolean isLastRecordInSegment)
            throws IOException {
        cacheDataManager.append(record, targetSubpartition, dataType, isLastRecordInSegment);
    }

    @Override
    public void release() {
        cacheDataManager.release();
    }

    @Override
    public void close() {
        cacheDataManager.close();
    }
}
