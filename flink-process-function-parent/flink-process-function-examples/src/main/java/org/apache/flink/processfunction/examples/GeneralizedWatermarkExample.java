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

import org.apache.flink.api.common.eventtime.ProcessWatermark;
import org.apache.flink.api.common.eventtime.ProcessWatermarkDeclaration;
import org.apache.flink.api.common.eventtime.WatermarkAlignmentStrategy;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.processfunction.api.Collector;
import org.apache.flink.processfunction.api.ExecutionEnvironment;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.builtin.Sinks;
import org.apache.flink.processfunction.api.builtin.Sources;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

public class GeneralizedWatermarkExample {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Integer> source =
                env.fromSource(
                                Sources.collection(Arrays.asList(1, 2, 3, 4, 5)),
                                WatermarkStrategy.noWatermarks(),
                                "source")
                        .withParallelism(2);
        source.process(
                        new SingleStreamProcessFunction<Integer, Integer>() {
                            @Override
                            public void processRecord(
                                    Integer record, Collector<Integer> output, RuntimeContext ctx) {
                                output.collect(record);
                                ctx.emitWatermark(new MyWatermark((long) record));
                            }

                            @Override
                            public Set<ProcessWatermarkDeclaration> usesWatermarks() {
                                return Collections.singleton(
                                        new ProcessWatermarkDeclaration(
                                                MyWatermark.class,
                                                () -> MyWatermark.MIN_WATER_MARK,
                                                () -> MyWatermark.MAX_WATER_MARK,
                                                new WatermarkAlignmentStrategy(
                                                        WatermarkAlignmentStrategy
                                                                .WatermarkAlignmentMode
                                                                .NON_BLOCKING),
                                                MyWatermarkSerializer.INSTANCE));
                            }
                        })
                .withParallelism(2)
                .process(
                        new SingleStreamProcessFunction<Integer, Integer>() {
                            @Override
                            public void processRecord(
                                    Integer record, Collector<Integer> output, RuntimeContext ctx)
                                    throws Exception {}

                            @Override
                            public void onWatermark(
                                    ProcessWatermark<?> watermark,
                                    Collector<Integer> output,
                                    RuntimeContext context) {
                                if (watermark instanceof MyWatermark)
                                    System.out.println(
                                            "received watermark value : "
                                                    + ((MyWatermark) watermark).getValue());
                                context.emitWatermark(watermark);
                            }
                        })
                .sinkTo(Sinks.consumer((tsStr) -> System.out.println(tsStr)))
                .withParallelism(1);
        env.execute();
    }

    private static class MyWatermark implements ProcessWatermark<MyWatermark> {

        private static final MyWatermark MIN_WATER_MARK = new MyWatermark(0L);

        private static final MyWatermark MAX_WATER_MARK = new MyWatermark(Long.MAX_VALUE);

        private final Long value;

        public MyWatermark(Long value) {
            this.value = value;
        }

        public Long getValue() {
            return value;
        }

        public int compareTo(MyWatermark o) {
            return this.value.compareTo(o.value);
        }
    }

    private static class MyWatermarkSerializer extends TypeSerializer<MyWatermark> {
        public static final MyWatermarkSerializer INSTANCE = new MyWatermarkSerializer();

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TypeSerializer<MyWatermark> duplicate() {
            return INSTANCE;
        }

        @Override
        public MyWatermark createInstance() {
            return MyWatermark.MIN_WATER_MARK;
        }

        @Override
        public MyWatermark copy(MyWatermark from) {
            // ths is immutable
            return from;
        }

        @Override
        public MyWatermark copy(MyWatermark from, MyWatermark reuse) {
            // ths is immutable
            return from;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(MyWatermark record, DataOutputView target) throws IOException {
            target.writeLong(record.getValue());
        }

        @Override
        public MyWatermark deserialize(DataInputView source) throws IOException {
            return new MyWatermark(source.readLong());
        }

        @Override
        public MyWatermark deserialize(MyWatermark reuse, DataInputView source) throws IOException {
            return new MyWatermark(source.readLong());
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeLong(source.readLong());
        }

        @Override
        public int hashCode() {
            return this.getClass().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj.getClass().equals(this.getClass());
        }

        @Override
        public TypeSerializerSnapshot<MyWatermark> snapshotConfiguration() {
            return null;
        }
    }
}
