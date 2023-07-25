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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.processfunction.api.ExecutionEnvironment;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.builtin.Sinks;
import org.apache.flink.processfunction.api.builtin.Sources;
import org.apache.flink.processfunction.api.builtin.Windows;
import org.apache.flink.processfunction.api.function.WindowProcessFunction;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream;
import org.apache.flink.processfunction.api.windowing.window.Window;

import java.util.Arrays;
import java.util.function.Consumer;

public class WindowExample {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Integer> source =
                env.fromSource(
                        Sources.collection(Arrays.asList(1, 2, 3, 4, 5, 6)),
                        WatermarkStrategy.noWatermarks(),
                        "source");
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<String> process =
                source.keyBy(x -> x % 2)
                        .process(
                                Windows.builder(
                                                // TODO It seems that this type hint is necessary.
                                                Windows.TimeWindows.<Integer>ofTumbling(
                                                        Time.seconds(5),
                                                        Windows.TimeWindows.TimeType.PROCESSING))
                                        // .withTrigger()
                                        // .withEvictor
                                        .apply(
                                                new WindowProcessFunction<
                                                        Iterable<Integer>, String, Window>() {
                                                    @Override
                                                    public void processRecord(
                                                            Iterable<Integer> record,
                                                            Consumer<String> output,
                                                            RuntimeContext ctx,
                                                            WindowContext<Window> windowContext)
                                                            throws Exception {
                                                        Window window = windowContext.window();
                                                        // handle records.
                                                    }
                                                }));
        process.sinkTo(
                Sinks.consumer(
                        (x) -> {
                            System.out.println(x);
                        }));
    }
}
