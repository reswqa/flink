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

package org.apache.flink.runtime.io.network.partition.store.writer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierDataGate;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierWriter;
import org.apache.flink.runtime.io.network.partition.store.common.TieredStoreProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This is a common entrypoint of the emitted records. These records will be transferred to the
 * appropriate {@link SingleTierWriter}.
 */
public class TieredStoreProducerImpl implements TieredStoreProducer {

    private static final Logger LOG = LoggerFactory.getLogger(TieredStoreProducerImpl.class);

    private int numBytesInASegment = 64 * 1024; // 64M default;

    private final SingleTierDataGate[] tierDataGates;

    private final SingleTierWriter[] singleTierWriters;

    // Record the newest segment index belonged to each sub partition.
    private final int[] subpartitionSegmentIndexes;

    // Record the byte number currently written to each sub partition.
    private final int[] numSubpartitionEmitBytes;

    // Record the index of writer currently used by each sub partition.
    private final int[] subpartitionWriterIndex;

    private final boolean isBroadcastOnly;

    private final int numSubpartitions;

    public TieredStoreProducerImpl(
            SingleTierDataGate[] tierDataGates, int numSubpartitions, boolean isBroadcastOnly)
            throws IOException {
        this.tierDataGates = tierDataGates;
        this.subpartitionSegmentIndexes = new int[numSubpartitions];
        this.numSubpartitionEmitBytes = new int[numSubpartitions];
        this.subpartitionWriterIndex = new int[numSubpartitions];
        this.singleTierWriters = new SingleTierWriter[tierDataGates.length];
        this.isBroadcastOnly = isBroadcastOnly;
        this.numSubpartitions = numSubpartitions;

        Arrays.fill(subpartitionSegmentIndexes, 0);
        Arrays.fill(numSubpartitionEmitBytes, 0);
        Arrays.fill(subpartitionWriterIndex, 0);
        for (int i = 0; i < tierDataGates.length; i++) {
            singleTierWriters[i] = tierDataGates[i].createPartitionTierWriter();
        }
    }

    @VisibleForTesting
    @Override
    public void setNumBytesInASegment(int numBytesInASegment) {
        this.numBytesInASegment = numBytesInASegment;
    }

    @Override
    public void emit(
            ByteBuffer record,
            int targetSubpartition,
            Buffer.DataType dataType,
            boolean isBroadcast,
            boolean isEndOfPartition)
            throws IOException {
        List<WriterAndSegmentIndex> writerAndSegmentIndexes =
                selectTieredWriterAndGetRegionIndexes(
                        record.remaining(), targetSubpartition, isBroadcast);
        for (WriterAndSegmentIndex writerAndSegmentIndex : writerAndSegmentIndexes) {
            int segmentIndex = writerAndSegmentIndex.getSegmentIndex();
            boolean isLastRecord = writerAndSegmentIndex.isLastRecordInSegment();
            int subpartitionId = writerAndSegmentIndex.getSubpartitionId();
            int writerIndex = writerAndSegmentIndex.getWriterIndex();
            LOG.debug(" ### TieredStoreProducerImpl is trying to write subpartition {}, writer index {}, segmentIndex is {}", subpartitionId, singleTierWriters[writerIndex], segmentIndex);
            singleTierWriters[writerIndex].emit(
                    record.duplicate(),
                    subpartitionId,
                    dataType,
                    isBroadcast,
                    isLastRecord,
                    isEndOfPartition,
                    segmentIndex);
        }
    }

    // Choose right tiered writers.
    public List<WriterAndSegmentIndex> selectTieredWriterAndGetRegionIndexes(
            int numRecordBytes, int targetSubpartition, boolean isBroadcast) throws IOException {
        List<WriterAndSegmentIndex> writerAndSegmentIndexes = new ArrayList<>();
        if (isBroadcast && !isBroadcastOnly) {
            for (int i = 0; i < numSubpartitions; ++i) {
                writerAndSegmentIndexes.add(getTieredWriterAndGetRegionIndex(numRecordBytes, i));
            }
        } else {
            writerAndSegmentIndexes.add(
                    getTieredWriterAndGetRegionIndex(numRecordBytes, targetSubpartition));
        }
        return writerAndSegmentIndexes;
    }

    private WriterAndSegmentIndex getTieredWriterAndGetRegionIndex(
            int numRecordBytes, int targetSubpartition) throws IOException {
        numSubpartitionEmitBytes[targetSubpartition] += numRecordBytes;
        if (numSubpartitionEmitBytes[targetSubpartition] < numBytesInASegment) {
            return new WriterAndSegmentIndex(
                    subpartitionWriterIndex[targetSubpartition],
                    false,
                    subpartitionSegmentIndexes[targetSubpartition],
                    targetSubpartition);
        }

        // Each subpartition calculates the amount of data written to a tier separately. If the
        // amount of data exceeds the threshold, the segment is switched. Different subpartitions
        // may have duplicate segment indexes, so it is necessary to distinguish different
        // subpartitions when determining whether a tier contains the segment data.
        // Start checking from the first tier each time.
        int preTierGateIndex = this.subpartitionWriterIndex[targetSubpartition];
        int tierGateIndex = 0;
        SingleTierDataGate tierDataGates = this.tierDataGates[tierGateIndex];
        while (!tierDataGates.canStoreNextSegment() && this.tierDataGates.length != 1) {
            tierGateIndex++;
            if (tierGateIndex >= this.tierDataGates.length) {
                throw new IOException("Can not select a valid tiered writer.");
            }
            tierDataGates = this.tierDataGates[tierGateIndex];
        }
        this.subpartitionWriterIndex[targetSubpartition] = tierGateIndex;
        numSubpartitionEmitBytes[targetSubpartition] = 0;
        return new WriterAndSegmentIndex(
                preTierGateIndex,
                true,
                subpartitionSegmentIndexes[targetSubpartition]++,
                targetSubpartition);
    }

    public void release() {
        Arrays.stream(singleTierWriters).forEach(SingleTierWriter::release);
        Arrays.stream(tierDataGates).forEach(SingleTierDataGate::release);
    }

    public void close() {
        Arrays.stream(singleTierWriters).forEach(SingleTierWriter::close);
        Arrays.stream(tierDataGates).forEach(SingleTierDataGate::close);
    }
}
