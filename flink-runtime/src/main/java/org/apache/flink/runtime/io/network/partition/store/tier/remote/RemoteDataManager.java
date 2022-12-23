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

package org.apache.flink.runtime.io.network.partition.store.tier.remote;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierDataGate;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierReader;
import org.apache.flink.runtime.io.network.partition.store.common.SingleTierWriter;
import org.apache.flink.runtime.io.network.partition.store.tier.local.file.OutputMetrics;

import java.io.IOException;

public class RemoteDataManager implements SingleTierDataGate {
    @Override
    public void setup() throws IOException {}

    @Override
    public SingleTierWriter createPartitionTierWriter() throws IOException {
        return null;
    }

    @Override
    public SingleTierReader createSubpartitionTierReader(
            int subpartitionId, BufferAvailabilityListener availabilityListener)
            throws IOException {
        return null;
    }

    @Override
    public boolean canStoreNextSegment() {
        return false;
    }

    @Override
    public boolean hasCurrentSegment(int subpartitionId, int segmentIndex) {
        return false;
    }

    @Override
    public void setOutputMetrics(OutputMetrics tieredStoreOutputMetrics) {}

    @Override
    public void close() {}

    @Override
    public void release() {}

    @Override
    public Path getBaseSubpartitionPath(int subpartitionId) {
        return null;
    }
}
