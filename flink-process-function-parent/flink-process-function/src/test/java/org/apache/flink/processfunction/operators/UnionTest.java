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

package org.apache.flink.processfunction.operators;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.processfunction.api.ExecutionEnvironment;
import org.apache.flink.processfunction.api.builtin.BatchStreamingUnifiedFunctions;
import org.apache.flink.processfunction.api.builtin.Sinks;
import org.apache.flink.processfunction.api.builtin.Sources;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

class UnionTest {
    @Test
    void testJoin() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Integer> source1 =
                env.fromSource(
                        Sources.collection(Arrays.asList(1, 3, 5, 7)),
                        WatermarkStrategy.noWatermarks(),
                        "source1");

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Integer> source2 =
                env.fromSource(
                        Sources.collection(Arrays.asList(2, 4, 6, 8)),
                        WatermarkStrategy.noWatermarks(),
                        "source2");

        source1.process(BatchStreamingUnifiedFunctions.union(source2))
                .process(BatchStreamingUnifiedFunctions.map(x -> x))
                .sinkTo(Sinks.consumer((record) -> System.out.println(record)));

        env.execute();
    }
}
