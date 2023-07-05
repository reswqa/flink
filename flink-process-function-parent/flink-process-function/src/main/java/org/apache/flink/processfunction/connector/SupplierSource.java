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

package org.apache.flink.processfunction.connector;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.function.SupplierFunction;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SupplierSource<T>
        implements Source<T, SourceUtils.DummySourceSplit, SourceUtils.NoOpEnumState> {

    private final SupplierFunction<T> supplierFunction;

    public SupplierSource(SupplierFunction<T> supplierFunction) {
        this.supplierFunction = supplierFunction;
    }

    public SupplierFunction<T> getSupplierFunction() {
        return supplierFunction;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<SourceUtils.DummySourceSplit, SourceUtils.NoOpEnumState>
            createEnumerator(SplitEnumeratorContext<SourceUtils.DummySourceSplit> enumContext)
                    throws Exception {
        // The supplierFunction itself implicitly represents the only split and the enumerator is
        // not utilized.
        return null;
    }

    @Override
    public SplitEnumerator<SourceUtils.DummySourceSplit, SourceUtils.NoOpEnumState>
            restoreEnumerator(
                    SplitEnumeratorContext<SourceUtils.DummySourceSplit> enumContext,
                    SourceUtils.NoOpEnumState checkpoint)
                    throws Exception {
        // This source is not fault-tolerant.
        return null;
    }

    @Override
    public SimpleVersionedSerializer<SourceUtils.DummySourceSplit> getSplitSerializer() {
        return new NoOpSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<SourceUtils.NoOpEnumState>
            getEnumeratorCheckpointSerializer() {
        // This source is not fault-tolerant.
        return null;
    }

    @Override
    public SourceReader<T, SourceUtils.DummySourceSplit> createReader(
            SourceReaderContext readerContext) throws Exception {
        return new SupplierSourceReader();
    }

    private class SupplierSourceReader implements SourceReader<T, SourceUtils.DummySourceSplit> {
        private boolean isRunning;

        @Override
        public void start() {
            isRunning = true;
        }

        @Override
        public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
            if (isRunning) {
                Thread.sleep(1000L);
                T record = supplierFunction.get();
                output.collect(record);
                return InputStatus.MORE_AVAILABLE;
            }
            return InputStatus.END_OF_INPUT;
        }

        @Override
        public List<SourceUtils.DummySourceSplit> snapshotState(long checkpointId) {
            return null;
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void addSplits(List<SourceUtils.DummySourceSplit> splits) {}

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public void close() {
            isRunning = false;
        }
    }

    /**
     * Not used - only required to avoid NullPointerException. The split is not transferred from the
     * enumerator, it is implicitly represented by the supplierFunction.
     */
    private static class NoOpSplitSerializer
            implements SimpleVersionedSerializer<SourceUtils.DummySourceSplit> {

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(SourceUtils.DummySourceSplit obj) throws IOException {
            return new byte[0];
        }

        @Override
        public SourceUtils.DummySourceSplit deserialize(int version, byte[] serialized) {
            return null;
        }
    }
}
