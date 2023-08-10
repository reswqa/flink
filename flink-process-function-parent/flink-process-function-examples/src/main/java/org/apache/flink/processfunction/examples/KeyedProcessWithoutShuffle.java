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
import org.apache.flink.processfunction.api.Collector;
import org.apache.flink.processfunction.api.ExecutionEnvironment;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.builtin.BatchStreamingUnifiedFunctions;
import org.apache.flink.processfunction.api.builtin.Sinks;
import org.apache.flink.processfunction.api.builtin.Sources;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.processfunction.api.stream.KeyedPartitionStream;
import org.apache.flink.processfunction.api.stream.KeyedPartitionStream.ProcessConfigurableAndKeyedPartitionStream;

import java.util.Arrays;
import java.util.Optional;

public class KeyedProcessWithoutShuffle {
    public static void main(String[] args) throws Exception {
        // TODO switch by parse args.
        // oneInput();
        // twoInput();
        twoOutput();
    }

    private static void oneInput() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ProcessConfigurableAndKeyedPartitionStream<Integer, Integer> keyStream =
                env.fromSource(
                                Sources.collection(Arrays.asList(1, 2, 3, 4, 5, 6)),
                                WatermarkStrategy.noWatermarks(),
                                "source")
                        .keyBy(v -> v % 2)
                        .process(
                                BatchStreamingUnifiedFunctions.map(value -> value + 2), v -> v % 2);
        keyStream
                .process(new EmitRecordWithKeyProcessFunction<>())
                .sinkTo(Sinks.consumer((tsStr) -> System.out.println(tsStr)));
        // Expected:
        // KeyedProcess -> KeyedProcess -> Sink: Writer
        env.execute();
    }

    private static void twoInput() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        KeyedPartitionStream<Integer, Integer> keyed1 =
                env.fromSource(
                                Sources.collection(Arrays.asList(1, 2, 3, 4, 5, 6)),
                                WatermarkStrategy.noWatermarks(),
                                "source")
                        .keyBy(v -> v % 2);

        KeyedPartitionStream<Integer, Integer> keyed2 =
                env.fromSource(
                                Sources.collection(Arrays.asList(1, 2, 3, 4, 5, 6)),
                                WatermarkStrategy.noWatermarks(),
                                "source")
                        .keyBy(v -> v % 2);
        keyed2.process(BatchStreamingUnifiedFunctions.map(value -> value + 2), v -> v % 2);
        ProcessConfigurableAndKeyedPartitionStream<Integer, Integer> keyedTwoInputStream =
                keyed1.connectAndProcess(
                        keyed2,
                        new TwoInputStreamProcessFunction<Integer, Integer, Integer>() {
                            @Override
                            public void processFirstInputRecord(
                                    Integer record, Collector<Integer> output, RuntimeContext ctx)
                                    throws Exception {
                                output.collect(record + 2);
                            }

                            @Override
                            public void processSecondInputRecord(
                                    Integer record, Collector<Integer> output, RuntimeContext ctx)
                                    throws Exception {
                                output.collect(record + 2);
                            }
                        },
                        v -> v % 2);
        keyedTwoInputStream
                .process(new EmitRecordWithKeyProcessFunction<>())
                .sinkTo(Sinks.consumer((tsStr) -> System.out.println(tsStr)));
        // Expected:
        // Keyed-TwoInput-Process -> KeyedProcess -> Sink: Writer
        env.execute();
    }

    private static void twoOutput() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        KeyedPartitionStream<Integer, Integer> keyedStream =
                env.fromSource(
                                Sources.collection(Arrays.asList(1, 2, 3, 4, 5, 6)),
                                WatermarkStrategy.noWatermarks(),
                                "source")
                        .keyBy(v -> v % 2);
        KeyedPartitionStream.ProcessConfigurableAndTwoKeyedPartitionStreams<
                        Integer, Integer, String>
                twoOutputStream =
                        keyedStream.process(
                                new TwoOutputStreamProcessFunction<Integer, Integer, String>() {
                                    @Override
                                    public void processRecord(
                                            Integer record,
                                            Collector<Integer> output1,
                                            Collector<String> output2,
                                            RuntimeContext ctx) {
                                        output1.collect(record + 2);
                                        output2.collect(String.valueOf(record + 2));
                                    }
                                },
                                v -> v % 2,
                                v -> Integer.parseInt(v) % 2);
        twoOutputStream
                .getFirst()
                .process(new EmitRecordWithKeyProcessFunction<>())
                .sinkTo(Sinks.consumer((tsStr) -> System.out.println(tsStr)));
        twoOutputStream
                .getSecond()
                .process(new EmitRecordWithKeyProcessFunction<>())
                .sinkTo(Sinks.consumer((tsStr) -> System.out.println(tsStr)));
        // Expected:
        // Two-Output-Operator -> (KeyedProcess -> Sink: Writer, KeyedProcess -> Sink: Writer)
        env.execute();
    }

    private static class EmitRecordWithKeyProcessFunction<T>
            implements SingleStreamProcessFunction<T, String> {
        @Override
        public void processRecord(T record, Collector<String> output, RuntimeContext ctx)
                throws Exception {
            Optional<Integer> currentKey = ctx.getCurrentKey();
            output.collect(String.format("record %s with key %s", record, currentKey.get()));
        }
    }
}
