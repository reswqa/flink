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

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.processfunction.api.Collector;
import org.apache.flink.processfunction.api.ExecutionEnvironment;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.builtin.BatchStreamingUnifiedFunctions;
import org.apache.flink.processfunction.api.builtin.Sinks;
import org.apache.flink.processfunction.api.builtin.Sources;
import org.apache.flink.processfunction.api.builtin.Windows;
import org.apache.flink.processfunction.api.function.ReduceFunction;
import org.apache.flink.processfunction.api.function.WindowProcessFunction;
import org.apache.flink.processfunction.api.stream.GlobalStream;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream;
import org.apache.flink.processfunction.api.windowing.window.Window;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.function.SupplierFunction;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

class WindowOperatorTest implements Serializable {
    @Test
    void testProcess() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Long> source =
                env.fromSource(
                        Sources.supplier(
                                new SupplierFunction<Long>() {
                                    private long curTime = 1689847907000L;

                                    @Override
                                    public Long get() {
                                        curTime = curTime + Time.seconds(1).toMilliseconds();
                                        return curTime;
                                    }
                                }),
                        WatermarkStrategy.forGenerator(PerElementWatermarkGenerator.getSupplier())
                                .withTimestampAssigner(
                                        (TimestampAssignerSupplier<Long>)
                                                context -> new ElementValueTimestampAssigner()),
                        "source");
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<String> process =
                source.keyBy(x -> 0)
                        .process(
                                Windows.builder(
                                                // TODO It seems that this type hint is necessary.
                                                Windows.TimeWindows.<Long>ofTumbling(
                                                        Time.seconds(5),
                                                        Windows.TimeWindows.TimeType.EVENT))
                                        // .withTrigger()
                                        // .withEvictor
                                        .apply(
                                                new WindowProcessFunction<
                                                        Iterable<Long>, String, Window>() {
                                                    @Override
                                                    public void processRecord(
                                                            Iterable<Long> record,
                                                            Collector<String> output,
                                                            RuntimeContext ctx,
                                                            WindowContext<Window> windowContext)
                                                            throws Exception {
                                                        Window window = windowContext.window();
                                                        // handle records;
                                                        List<Long> collect =
                                                                IterableUtils.toStream(record)
                                                                        .collect(
                                                                                Collectors
                                                                                        .toList());
                                                        output.collect(collect.toString());
                                                    }
                                                }));
        process.sinkTo(
                Sinks.consumer(
                        (x) -> {
                            System.out.println(x);
                        }));
        env.execute();
    }

    @Test
    void testReduce() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                source =
                        env.fromSource(
                                Sources.supplier(
                                        new SupplierFunction<ValueWithTimestamp>() {
                                            private long curTime = 1689847907000L;

                                            private int value = 0;

                                            @Override
                                            public ValueWithTimestamp get() {
                                                curTime =
                                                        curTime + Time.seconds(1).toMilliseconds();
                                                return new ValueWithTimestamp(curTime, value++);
                                            }
                                        }),
                                WatermarkStrategy.forGenerator(
                                                PerElementValueWithTimestampWatermarkGenerator
                                                        .getSupplier())
                                        .withTimestampAssigner(
                                                (TimestampAssignerSupplier<ValueWithTimestamp>)
                                                        context ->
                                                                new ValueWithTimestampAssigner()),
                                "source");
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Integer> process =
                source.process(BatchStreamingUnifiedFunctions.map(x -> x.value))
                        .keyBy(x -> 0)
                        .process(
                                Windows.builder(
                                                // TODO It seems that this type hint is necessary.
                                                Windows.TimeWindows.<Integer>ofTumbling(
                                                        Time.seconds(5),
                                                        Windows.TimeWindows.TimeType.EVENT))
                                        .reduce((ReduceFunction<Integer>) Integer::sum));
        process.sinkTo(
                Sinks.consumer(
                        (x) -> {
                            System.out.println(x);
                        }));
        env.execute();
    }

    @Test
    void testReduceWithProcessFunction() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                source =
                        env.fromSource(
                                Sources.supplier(
                                        new SupplierFunction<ValueWithTimestamp>() {
                                            private long curTime = 1689847907000L;

                                            private int value = 0;

                                            @Override
                                            public ValueWithTimestamp get() {
                                                curTime =
                                                        curTime + Time.seconds(1).toMilliseconds();
                                                return new ValueWithTimestamp(curTime, value++);
                                            }
                                        }),
                                WatermarkStrategy.forGenerator(
                                                PerElementValueWithTimestampWatermarkGenerator
                                                        .getSupplier())
                                        .withTimestampAssigner(
                                                (TimestampAssignerSupplier<ValueWithTimestamp>)
                                                        context ->
                                                                new ValueWithTimestampAssigner()),
                                "source");
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Integer> process =
                source.process(BatchStreamingUnifiedFunctions.map(x -> x.value))
                        .keyBy(x -> 0)
                        .process(
                                Windows.builder(
                                                // TODO It seems that this type hint is necessary.
                                                Windows.TimeWindows.<Integer>ofTumbling(
                                                        Time.seconds(5),
                                                        Windows.TimeWindows.TimeType.EVENT))
                                        .reduce(
                                                (ReduceFunction<Integer>) Integer::sum,
                                                new WindowProcessFunction<
                                                        Integer, Integer, Window>() {
                                                    @Override
                                                    public void processRecord(
                                                            Integer record,
                                                            Collector<Integer> output,
                                                            RuntimeContext ctx,
                                                            WindowContext<Window> windowContext)
                                                            throws Exception {
                                                        output.collect(record);
                                                    }
                                                }));
        process.sinkTo(
                Sinks.consumer(
                        (x) -> {
                            System.out.println(x);
                        }));
        env.execute();
    }

    @Test
    void testNonKeyed() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                source =
                        env.fromSource(
                                Sources.supplier(
                                        new SupplierFunction<ValueWithTimestamp>() {
                                            private long curTime = 1689847907000L;

                                            private int value = 0;

                                            @Override
                                            public ValueWithTimestamp get() {
                                                curTime =
                                                        curTime + Time.seconds(1).toMilliseconds();
                                                return new ValueWithTimestamp(curTime, value++);
                                            }
                                        }),
                                WatermarkStrategy.forGenerator(
                                                PerElementValueWithTimestampWatermarkGenerator
                                                        .getSupplier())
                                        .withTimestampAssigner(
                                                (TimestampAssignerSupplier<ValueWithTimestamp>)
                                                        context ->
                                                                new ValueWithTimestampAssigner()),
                                "source");
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Integer> process =
                source.process(BatchStreamingUnifiedFunctions.map(x -> x.value))
                        .process(
                                Windows.builder(
                                                // TODO It seems that this type hint is necessary.
                                                Windows.TimeWindows.<Integer>ofTumbling(
                                                        Time.seconds(5),
                                                        Windows.TimeWindows.TimeType.EVENT))
                                        .reduce(
                                                new ReduceFunction<Integer>() {
                                                    @Override
                                                    public Integer reduce(
                                                            Integer value1, Integer value2)
                                                            throws Exception {
                                                        return value1 + value2;
                                                    }
                                                },
                                                new WindowProcessFunction<
                                                        Integer, Integer, Window>() {
                                                    @Override
                                                    public void processRecord(
                                                            Integer record,
                                                            Collector<Integer> output,
                                                            RuntimeContext ctx,
                                                            WindowContext<Window> windowContext)
                                                            throws Exception {
                                                        output.collect(record);
                                                    }
                                                }));
        process.sinkTo(
                Sinks.consumer(
                        (x) -> {
                            System.out.println(x);
                        }));
        env.execute();
    }

    @Test
    void testGlobal() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                source =
                        env.fromSource(
                                Sources.supplier(
                                        new SupplierFunction<ValueWithTimestamp>() {
                                            private long curTime = 1689847907000L;

                                            private int value = 0;

                                            @Override
                                            public ValueWithTimestamp get() {
                                                curTime =
                                                        curTime + Time.seconds(1).toMilliseconds();
                                                return new ValueWithTimestamp(curTime, value++);
                                            }
                                        }),
                                WatermarkStrategy.forGenerator(
                                                PerElementValueWithTimestampWatermarkGenerator
                                                        .getSupplier())
                                        .withTimestampAssigner(
                                                (TimestampAssignerSupplier<ValueWithTimestamp>)
                                                        context ->
                                                                new ValueWithTimestampAssigner()),
                                "source");
        GlobalStream.ProcessConfigurableAndGlobalStream<Integer> process =
                source.process(BatchStreamingUnifiedFunctions.map(x -> x.value))
                        .withParallelism(2)
                        .coalesce()
                        .process(
                                Windows.builder(
                                                // TODO It seems that this type hint is necessary.
                                                Windows.TimeWindows.<Integer>ofTumbling(
                                                        Time.seconds(5),
                                                        Windows.TimeWindows.TimeType.EVENT))
                                        .reduce(
                                                new ReduceFunction<Integer>() {
                                                    @Override
                                                    public Integer reduce(
                                                            Integer value1, Integer value2)
                                                            throws Exception {
                                                        return value1 + value2;
                                                    }
                                                },
                                                new WindowProcessFunction<
                                                        Integer, Integer, Window>() {
                                                    @Override
                                                    public void processRecord(
                                                            Integer record,
                                                            Collector<Integer> output,
                                                            RuntimeContext ctx,
                                                            WindowContext<Window> windowContext)
                                                            throws Exception {
                                                        output.collect(record);
                                                    }
                                                }));
        process.sinkTo(
                Sinks.consumer(
                        (x) -> {
                            System.out.println(x);
                        }));
        env.execute();
    }

    private static class PerElementWatermarkGenerator implements WatermarkGenerator<Long> {

        private long maxTimestamp;

        public PerElementWatermarkGenerator() {
            maxTimestamp = Long.MIN_VALUE;
        }

        @Override
        public void onEvent(Long event, long eventTimestamp, WatermarkOutput output) {
            if (eventTimestamp > maxTimestamp) {
                maxTimestamp = eventTimestamp;
                output.emitWatermark(new Watermark(eventTimestamp));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}

        public static WatermarkGeneratorSupplier<Long> getSupplier() {
            return (ctx) -> new PerElementWatermarkGenerator();
        }
    }

    private static class ElementValueTimestampAssigner implements TimestampAssigner<Long> {

        @Override
        public long extractTimestamp(Long element, long recordTimestamp) {
            return element;
        }
    }

    public static class PerElementValueWithTimestampWatermarkGenerator
            implements WatermarkGenerator<ValueWithTimestamp> {

        private long maxTimestamp;

        public PerElementValueWithTimestampWatermarkGenerator() {
            maxTimestamp = Long.MIN_VALUE;
        }

        @Override
        public void onEvent(ValueWithTimestamp event, long eventTimestamp, WatermarkOutput output) {
            if (eventTimestamp > maxTimestamp) {
                maxTimestamp = eventTimestamp;
                output.emitWatermark(new Watermark(eventTimestamp));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}

        public static WatermarkGeneratorSupplier<ValueWithTimestamp> getSupplier() {
            return (ctx) -> new PerElementValueWithTimestampWatermarkGenerator();
        }
    }

    public static class ValueWithTimestampAssigner
            implements TimestampAssigner<ValueWithTimestamp> {

        @Override
        public long extractTimestamp(ValueWithTimestamp element, long recordTimestamp) {
            return element.timestamp;
        }
    }

    public static class ValueWithTimestamp {
        private final long timestamp;

        private final int value;

        public ValueWithTimestamp(long timestamp, int value) {
            this.timestamp = timestamp;
            this.value = value;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public int getValue() {
            return value;
        }
    }
}
