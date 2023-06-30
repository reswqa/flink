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
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.builtin.Sinks;
import org.apache.flink.processfunction.api.builtin.Sources;
import org.apache.flink.processfunction.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream;

import java.util.Arrays;
import java.util.function.Consumer;

/** Test two output stream. */
public class TwoOutputProcess {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        NonKeyedPartitionStream<Integer> source =
                env.fromSource(
                        Sources.collection(Arrays.asList(1, 2, 3)),
                        WatermarkStrategy.noWatermarks(),
                        "source");
        NonKeyedPartitionStream.ProcessConfigurableAndTwoNonKeyedPartitionStreams<Integer, String>
                twoOutputStreams =
                        source.process(
                                // Do not use lambda expression as type will be erased from
                                // Consumer<T>.
                                new TwoOutputStreamProcessFunction<Integer, Integer, String>() {
                                    @Override
                                    public void processRecord(
                                            Integer record,
                                            Consumer<Integer> output1,
                                            Consumer<String> output2,
                                            RuntimeContext ctx) {
                                        output1.accept(record);
                                        output2.accept("side-output: " + record);
                                    }
                                });
        twoOutputStreams.getFirst().sinkTo(Sinks.consumer(out -> System.out.println(out)));
        twoOutputStreams.getSecond().sinkTo(Sinks.consumer(out -> System.out.println(out)));
        env.execute();
    }
}
