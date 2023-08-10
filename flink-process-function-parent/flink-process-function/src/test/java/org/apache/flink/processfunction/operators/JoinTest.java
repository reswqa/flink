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

import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.processfunction.api.Collector;
import org.apache.flink.processfunction.api.ExecutionEnvironment;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.builtin.BatchStreamingUnifiedFunctions;
import org.apache.flink.processfunction.api.builtin.Joins;
import org.apache.flink.processfunction.api.builtin.Sinks;
import org.apache.flink.processfunction.api.builtin.Sources;
import org.apache.flink.processfunction.api.builtin.Windows;
import org.apache.flink.processfunction.api.function.JoinFunction;
import org.apache.flink.processfunction.api.stream.KeyedPartitionStream;
import org.apache.flink.processfunction.api.windowing.window.Window;
import org.apache.flink.processfunction.operators.WindowOperatorTest.PerElementValueWithTimestampWatermarkGenerator;
import org.apache.flink.processfunction.operators.WindowOperatorTest.ValueWithTimestamp;
import org.apache.flink.processfunction.operators.WindowOperatorTest.ValueWithTimestampAssigner;
import org.apache.flink.util.function.SupplierFunction;

import org.junit.jupiter.api.Test;

import java.io.Serializable;

class JoinTest implements Serializable {
    @Test
    void testJoin() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        KeyedPartitionStream<Integer, Integer> source =
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
                                "source")
                        .process(BatchStreamingUnifiedFunctions.map(ValueWithTimestamp::getValue))
                        .keyBy(x -> x % 2);

        KeyedPartitionStream<Integer, Integer> joinSource =
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
                                "source")
                        .process(BatchStreamingUnifiedFunctions.map(ValueWithTimestamp::getValue))
                        .keyBy(x -> x % 2);

        source.connectAndProcess(
                        joinSource,
                        Joins.withWindow(
                                        Windows.<Integer, Integer, Window>twoInputBuilder(
                                                Windows.TimeWindows.ofTumbling(
                                                        Time.seconds(5),
                                                        Windows.TimeWindows.TimeType.EVENT)))
                                .join(
                                        new JoinFunction<Integer, Integer, String>() {
                                            @Override
                                            public void processRecord(
                                                    Integer leftRecord,
                                                    Integer rightRecord,
                                                    Collector<String> output,
                                                    RuntimeContext ctx)
                                                    throws Exception {
                                                output.collect(
                                                        String.format(
                                                                "joined: (%s, %s)",
                                                                leftRecord, rightRecord));
                                            }
                                        },
                                        Joins.JoinType.INNER))
                .sinkTo(Sinks.consumer((x) -> System.out.println(x)));
        env.execute();
    }
}
