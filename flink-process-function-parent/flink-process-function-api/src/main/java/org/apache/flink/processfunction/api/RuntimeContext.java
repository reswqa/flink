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

package org.apache.flink.processfunction.api;

import org.apache.flink.api.common.eventtime.ProcessWatermark;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.processfunction.api.state.StateDeclaration.ListStateDeclaration;
import org.apache.flink.processfunction.api.state.StateDeclaration.MapStateDeclaration;
import org.apache.flink.processfunction.api.state.StateDeclaration.ValueStateDeclaration;
import org.apache.flink.processfunction.api.stream.KeyedPartitionStream;

import java.util.Optional;

public interface RuntimeContext {
    <T> Optional<ListState<T>> getState(ListStateDeclaration stateDeclaration) throws Exception;

    <T> Optional<ValueState<T>> getState(ValueStateDeclaration stateDeclaration) throws Exception;

    <K, V> Optional<MapState<K, V>> getState(MapStateDeclaration stateDeclaration) throws Exception;

    /**
     * Get the key of current record.
     *
     * @return The key of current processed record for {@link KeyedPartitionStream}. {@link
     *     Optional#empty()} for other non-keyed stream.
     */
    <K> Optional<K> getCurrentKey();

    ExecutionMode getExecutionMode();

    <T extends ProcessWatermark<T>> void emitWatermark(ProcessWatermark<T> watermark);

    enum ExecutionMode {
        STREAMING,
        BATCH
    }
}
