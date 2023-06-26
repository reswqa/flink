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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ProcessOperator}. */
class ProcessOperatorTest {
    @Test
    void testKeyedProcessOperatorGetCurrentKey() throws Exception {
        ProcessOperator<Integer, Integer> processOperator =
                new ProcessOperator<>(
                        new SingleStreamProcessFunction<Integer, Integer>() {
                            @Override
                            public void processRecord(
                                    Integer record, Consumer<Integer> output, RuntimeContext ctx)
                                    throws Exception {
                                Optional<Integer> currentKey = ctx.getCurrentKey();
                                assertThat(currentKey).hasValue(record);
                                output.accept(record + 1);
                            }
                        });

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Integer>) value -> value,
                        Types.INT)) {
            testHarness.open();
            testHarness.processElement(new StreamRecord<>(1));
        }
    }

    @Test
    void testNonKeyedProcessOperatorGetCurrentKey() throws Exception {
        ProcessOperator<Integer, Integer> processOperator =
                new ProcessOperator<>(
                        new SingleStreamProcessFunction<Integer, Integer>() {
                            @Override
                            public void processRecord(
                                    Integer record, Consumer<Integer> output, RuntimeContext ctx)
                                    throws Exception {
                                assertThat(ctx.getCurrentKey()).isNotPresent();
                                output.accept(record + 1);
                            }
                        });

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                new OneInputStreamOperatorTestHarness<>(processOperator)) {
            testHarness.open();
            testHarness.processElement(new StreamRecord<>(1));
        }
    }
}
