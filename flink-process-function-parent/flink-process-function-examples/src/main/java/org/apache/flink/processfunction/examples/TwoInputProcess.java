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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.processfunction.api.ExecutionEnvironment;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.builtin.Sinks;
import org.apache.flink.processfunction.api.builtin.Sources;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.processfunction.api.state.StateDeclaration;
import org.apache.flink.processfunction.api.state.StateDeclaration.ValueStateDeclaration;
import org.apache.flink.processfunction.api.state.States;
import org.apache.flink.processfunction.api.stream.KeyedPartitionStream;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

public class TwoInputProcess {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        KeyedPartitionStream<String, String> source1 =
                env.fromSource(
                                Sources.collection(Arrays.asList("A:1", "A:3", "B:9", "C:1")),
                                WatermarkStrategy.noWatermarks(),
                                "source1")
                        .keyBy(x -> x.split(":")[0]);
        KeyedPartitionStream<String, Tuple2<String, Long>> source2 =
                env.fromSource(
                                Sources.collection(
                                        Arrays.asList(
                                                Tuple2.of("A", 5L),
                                                Tuple2.of("B", 3L),
                                                Tuple2.of("C", 11L),
                                                Tuple2.of("D", 1L))),
                                WatermarkStrategy.noWatermarks(),
                                "source2")
                        .keyBy(x -> x.f0);
        source1.connectAndProcess(
                        source2,
                        new TwoInputStreamProcessFunction<String, Tuple2<String, Long>, String>() {
                            private final ValueStateDeclaration stateDeclaration =
                                    States.valueState("max-value", TypeDescriptors.LONG);

                            // TODO provide open method in process function to do something like get
                            // state.
                            @Override
                            public void processFirstInputRecord(
                                    String record, Consumer<String> output, RuntimeContext ctx)
                                    throws Exception {
                                // "key:value"
                                String key = record.split(":")[0];
                                Long value = Long.parseLong(record.split(":")[1]);
                                Optional<ValueState<Long>> stateOptional =
                                        ctx.getState(stateDeclaration);
                                Preconditions.checkState(stateOptional.isPresent());
                                ValueState<Long> state = stateOptional.get();
                                if (state.value() == null) {
                                    state.update(value);
                                    output.accept(key + " : " + value);
                                } else if (state.value() < value) {
                                    state.update(value);
                                    output.accept(key + " : " + value);
                                }
                            }

                            @Override
                            public void processSecondInputRecord(
                                    Tuple2<String, Long> record,
                                    Consumer<String> output,
                                    RuntimeContext ctx)
                                    throws Exception {
                                // (key, value)
                                Long value = record.f1;
                                Optional<ValueState<Long>> stateOptional =
                                        ctx.getState(stateDeclaration);
                                Preconditions.checkState(stateOptional.isPresent());
                                ValueState<Long> state = stateOptional.get();
                                if (state.value() == null) {
                                    state.update(value);
                                    output.accept(record.f0 + " : " + record.f1);
                                } else if (state.value() < value) {
                                    state.update(value);
                                    output.accept(record.f0 + " : " + record.f1);
                                }
                            }

                            @Override
                            public Set<StateDeclaration> usesStates() {
                                return Collections.singleton(stateDeclaration);
                            }
                        })
                .sinkTo(Sinks.consumer(out -> System.out.println(out)));
        env.execute();
    }
}
