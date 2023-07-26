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

package org.apache.flink.processfunction.api.function;

import org.apache.flink.api.common.eventtime.ProcessWatermark;
import org.apache.flink.processfunction.api.RuntimeContext;

import java.util.function.Consumer;

public interface SingleStreamProcessFunction<IN, OUT> extends ProcessFunction {
    void processRecord(IN record, Consumer<OUT> output, RuntimeContext ctx) throws Exception;

    /**
     * This will be called ONLY in BATCH execution mode, allowing the ProcessFunction to emit
     * results at once rather than upon each record.
     *
     * <p>For {@link org.apache.flink.processfunction.api.stream.KeyedPartitionStream}, this will be
     * called for each keyed partition when all data from that partition have been processed. Use
     * {@link RuntimeContext#getCurrentKey()} to find out which partition this is called for.
     *
     * <p>For {@link org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream}, this will
     * be called for each non-keyed partition (i.e. each parallel processing instance) when all data
     * from that partition have been processed.
     *
     * <p>Note: This will NOT be called in STREAMING execution mode.
     */
    default void endOfPartition(Consumer<OUT> output, RuntimeContext ctx) {}

    default void onWatermark(
            ProcessWatermark<?> watermark, Consumer<OUT> output, RuntimeContext ctx) {}

    default void onProcessingTimer(long timestamp, Consumer<OUT> output, RuntimeContext ctx) {}
}
