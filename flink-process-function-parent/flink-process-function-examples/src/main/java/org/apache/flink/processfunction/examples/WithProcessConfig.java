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

package org.apache.flink.processfunction.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.processfunction.api.ExecutionEnvironment;
import org.apache.flink.processfunction.api.builtin.BatchStreamingUnifiedFunctions;
import org.apache.flink.processfunction.api.builtin.Sinks;
import org.apache.flink.processfunction.api.builtin.Sources;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class WithProcessConfig {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Integer> source =
                env.fromSource(
                        Sources.collection(Arrays.asList(1, 2, 3, 4)),
                        WatermarkStrategy.noWatermarks(),
                        "collector source");
        source.withName("With-Name-Source").withParallelism(2);

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<String> process =
                source.process(
                        BatchStreamingUnifiedFunctions.map(
                                record ->
                                        new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS")
                                                .format(new Date(record))));
        process.withName("With-Name-Process").withParallelism(2);
        process.coalesce()
                // Don't use Lambda reference as PrintStream is not serializable.
                .sinkTo(Sinks.consumer((tsStr) -> System.out.println(tsStr)));
        env.execute();
    }
}
