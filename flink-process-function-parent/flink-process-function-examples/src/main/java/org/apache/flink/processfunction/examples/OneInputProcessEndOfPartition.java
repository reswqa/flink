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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.processfunction.api.Collector;
import org.apache.flink.processfunction.api.ExecutionEnvironment;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.builtin.Sinks;
import org.apache.flink.processfunction.api.builtin.Sources;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.api.stream.KeyedPartitionStream;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream;

import java.util.Arrays;
import java.util.Optional;

public class OneInputProcessEndOfPartition {
    public static void main(String[] args) throws Exception {
        // TODO switch keyed / non-keyed & sort / un-sort by parse args.
        // nonKeyed();
        keyed(false);
    }

    private static void nonKeyed() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.tmpSetRuntimeMode(RuntimeExecutionMode.BATCH);
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Integer> source =
                env.fromSource(
                        Sources.collection(Arrays.asList(1, 2, 3, 4, 5, 6)),
                        WatermarkStrategy.noWatermarks(),
                        "number-source");
        source.process(
                        new SingleStreamProcessFunction<Integer, String>() {
                            @Override
                            public void processRecord(
                                    Integer record, Collector<String> output, RuntimeContext ctx)
                                    throws Exception {
                                output.collect("record: " + record);
                            }

                            @Override
                            public void endOfPartition(
                                    Collector<String> output, RuntimeContext ctx) {
                                output.collect("EOP");
                            }
                        })
                .sinkTo(Sinks.consumer(r -> System.out.println(r)));
        env.execute();
    }

    private static void keyed(boolean sortInputs) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();
        configuration.setString(
                "execution.sorted-inputs.enabled",
                sortInputs ? Boolean.TRUE.toString() : Boolean.FALSE.toString());
        if (!sortInputs) {
            // batch state backend depends on sorted inputs.
            configuration.setString("execution.batch-state-backend.enabled", "false");
        }
        env.tmpWithConfiguration(configuration);
        env.tmpSetRuntimeMode(RuntimeExecutionMode.BATCH);
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Integer> source =
                env.fromSource(
                        Sources.collection(Arrays.asList(1, 2, 3, 4, 5, 6)),
                        WatermarkStrategy.noWatermarks(),
                        "number-source");
        KeyedPartitionStream<Integer, Integer> keyedStream = source.keyBy(record -> record % 3);
        keyedStream
                .process(
                        new SingleStreamProcessFunction<Integer, String>() {
                            @Override
                            public void processRecord(
                                    Integer record, Collector<String> output, RuntimeContext ctx)
                                    throws Exception {
                                output.collect("record: " + record);
                            }

                            @Override
                            public void endOfPartition(
                                    Collector<String> output, RuntimeContext ctx) {
                                Optional<Integer> key = ctx.getCurrentKey();
                                output.collect("EOP : " + key.get());
                            }
                        })
                .sinkTo(Sinks.consumer(r -> System.out.println(r)));
        env.execute();
    }
}
