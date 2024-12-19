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

package org.apache.flink.datastream.impl.operators;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.dsv2.WrappedSink;
import org.apache.flink.api.connector.dsv2.WrappedSource;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.context.RuntimeContext;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.join.ReduceFunction;
import org.apache.flink.datastream.api.extension.window.WindowExtension;
import org.apache.flink.datastream.api.extension.window.WindowProcessFunction;
import org.apache.flink.datastream.api.extension.window.window.Window;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.GlobalStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.util.IterableUtils;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

public class WindowOperatorTest implements Serializable {
    @Test
    void testProcess() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp> source = env.fromSource(
                new WrappedSource<ValueWithTimestamp>(new DataGeneratorSource<ValueWithTimestamp>(
                        new TestGeneratorFunction(),
                        100_000,
                        TypeInformation.of(ValueWithTimestamp.class))),
                "source");

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<String> process =
                source.process(EventTimeExtension.extractEventTime(element -> element.timestamp))
                        .process(new OneInputStreamProcessFunction<ValueWithTimestamp, Long>() {
                            @Override
                            public void processRecord(
                                    ValueWithTimestamp record,
                                    Collector<Long> output,
                                    PartitionedContext ctx) throws Exception {
                                output.collect((long) record.getValue());
                            }
                        })
                        .keyBy(x -> x)
                        .process(
                                WindowExtension.apply(
                                        WindowExtension.TimeWindows.ofTumbling(
                                                Duration.ofSeconds(5),
                                                WindowExtension.TimeWindows.TimeType.EVENT),
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
                                                                .collect(Collectors.toList());
                                                output.collect(collect.toString());
                                            }
                                        }));

        process.toSink(new WrappedSink<>(new PrintSink<>()));
        env.execute("test process");
    }

    @Test
    void testReduce() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp> source = env.fromSource(
                new WrappedSource<ValueWithTimestamp>(new DataGeneratorSource<ValueWithTimestamp>(
                        new TestGeneratorFunction(),
                        100_000,
                        TypeInformation.of(ValueWithTimestamp.class))),
                "source");

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Long> process = source
                .process(EventTimeExtension.extractEventTime(element -> element.timestamp))
                .process(new OneInputStreamProcessFunction<ValueWithTimestamp, Long>() {
                    @Override
                    public void processRecord(
                            ValueWithTimestamp record,
                            Collector<Long> output,
                            PartitionedContext ctx) throws Exception {
                        output.collect((long) record.getValue());
                    }
                })
                .keyBy(x -> x)
                .process(
                        WindowExtension.apply(
                                WindowExtension.TimeWindows.ofTumbling(
                                        Duration.ofSeconds(5),
                                        WindowExtension.TimeWindows.TimeType.EVENT),
                                (ReduceFunction<Long>) Long::sum));
        process.toSink(new WrappedSink<>(new PrintSink<>()));
        env.execute("test reduce");
    }

    @Test
    void testReduceWithProcessFunction() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp> source = env.fromSource(
                new WrappedSource<ValueWithTimestamp>(new DataGeneratorSource<ValueWithTimestamp>(
                        new TestGeneratorFunction(),
                        100_000,
                        TypeInformation.of(ValueWithTimestamp.class))),
                "source");

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Long> process = source
                .process(EventTimeExtension.extractEventTime(element -> element.timestamp))
                .process(new OneInputStreamProcessFunction<ValueWithTimestamp, Long>() {
                    @Override
                    public void processRecord(
                            ValueWithTimestamp record,
                            Collector<Long> output,
                            PartitionedContext ctx) throws Exception {
                        output.collect((long) record.getValue());
                    }
                })
                .keyBy(x -> x)
                        .process(
                                WindowExtension.apply(
                                        WindowExtension.TimeWindows.<Long>ofTumbling(
                                                Duration.ofSeconds(5),
                                                WindowExtension.TimeWindows.TimeType.EVENT),
                                        (ReduceFunction<Long>) Long::sum,
                                        new WindowProcessFunction<Long, Long, Window>() {
                                            @Override
                                            public void processRecord(
                                                    Long record,
                                                    Collector<Long> output,
                                                    RuntimeContext ctx,
                                                    WindowContext<Window> windowContext)
                                                    throws Exception {
                                                output.collect(record);
                                            }
                                        }));
        process.toSink(new WrappedSink<>(new PrintSink<>()));
        env.execute("test reduce with process");
    }

    @Test
    void testNonKeyed() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp> source = env.fromSource(
                new WrappedSource<ValueWithTimestamp>(new DataGeneratorSource<ValueWithTimestamp>(
                        new TestGeneratorFunction(),
                        100_000,
                        TypeInformation.of(ValueWithTimestamp.class))),
                "source");

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Long> process = source
                .process(EventTimeExtension.extractEventTime(element -> element.timestamp))
                .process(new OneInputStreamProcessFunction<ValueWithTimestamp, Long>() {
                    @Override
                    public void processRecord(
                            ValueWithTimestamp record,
                            Collector<Long> output,
                            PartitionedContext ctx) throws Exception {
                        output.collect((long) record.getValue());
                    }
                })
                        .process(
                                WindowExtension.apply(
                                        WindowExtension.TimeWindows.ofTumbling(
                                                Duration.ofSeconds(5),
                                                WindowExtension.TimeWindows.TimeType.EVENT),
                                        new ReduceFunction<Long>() {
                                            @Override
                                            public Long reduce(Long value1, Long value2)
                                                    throws Exception {
                                                return value1 + value2;
                                            }
                                        },
                                        new WindowProcessFunction<Long, Long, Window>() {
                                            @Override
                                            public void processRecord(
                                                    Long record,
                                                    Collector<Long> output,
                                                    RuntimeContext ctx,
                                                    WindowContext<Window> windowContext)
                                                    throws Exception {
                                                output.collect(record);
                                            }
                                        }));
        process.toSink(new WrappedSink<>(new PrintSink<>()));
        env.execute("test non keyed");
    }

    @Test
    void testGlobal() throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getInstance();
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp> source = env.fromSource(
                new WrappedSource<ValueWithTimestamp>(new DataGeneratorSource<ValueWithTimestamp>(
                        new TestGeneratorFunction(),
                        100_000,
                        TypeInformation.of(ValueWithTimestamp.class))),
                "source");

        GlobalStream.ProcessConfigurableAndGlobalStream<Long> process = source
                .process(EventTimeExtension.extractEventTime(element -> element.timestamp))
                .process(new OneInputStreamProcessFunction<ValueWithTimestamp, Long>() {
                    @Override
                    public void processRecord(
                            ValueWithTimestamp record,
                            Collector<Long> output,
                            PartitionedContext ctx) throws Exception {
                        output.collect((long) record.getValue());
                    }
                }).withParallelism(2)
                .global()
                .process(
                        WindowExtension.apply(
                                WindowExtension.TimeWindows.ofTumbling(
                                        Duration.ofSeconds(5),
                                        WindowExtension.TimeWindows.TimeType.EVENT),
                                new ReduceFunction<Long>() {
                                    @Override
                                    public Long reduce(Long value1, Long value2)
                                            throws Exception {
                                        return value1 + value2;
                                    }
                                },
                                new WindowProcessFunction<Long, Long, Window>() {
                                    @Override
                                    public void processRecord(
                                            Long record,
                                            Collector<Long> output,
                                            RuntimeContext ctx,
                                            WindowContext<Window> windowContext)
                                            throws Exception {
                                        output.collect(record);
                                    }
                                }));
        process.toSink(new WrappedSink<>(new PrintSink<>()));
        env.execute("test global");
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

    public static class TestGeneratorFunction implements GeneratorFunction<Long, ValueWithTimestamp>  {

        private long curTime = 1689847907000L;

        private int elementValue = 0;

        @Override
        public ValueWithTimestamp map(Long value) throws Exception {
            curTime =
                    curTime + Duration.ofSeconds(1).toMillis();
            return new ValueWithTimestamp(curTime, elementValue++);
        }
    }
}
