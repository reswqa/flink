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
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
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

    @Test
    void testNonKeyedProcessOperatorEndOfPartition() throws Exception {
        CompletableFuture<Void> notifiedEndOfPartition = new CompletableFuture<>();

        MockEnvironment mockEnvironment =
                new MockEnvironmentBuilder().setJobType(JobType.BATCH).build();

        ProcessOperator<Integer, Integer> processOperator =
                new ProcessOperator<>(
                        new SingleStreamProcessFunction<Integer, Integer>() {
                            @Override
                            public void processRecord(
                                    Integer record, Consumer<Integer> output, RuntimeContext ctx)
                                    throws Exception {
                                // do nothing.
                            }

                            @Override
                            public void endOfPartition(
                                    Consumer<Integer> output, RuntimeContext ctx) {
                                assertThat(notifiedEndOfPartition).isNotDone();
                                notifiedEndOfPartition.complete(null);
                            }
                        });

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                new OneInputStreamOperatorTestHarness<>(processOperator, mockEnvironment)) {
            testHarness.open();
            testHarness.processElement(new StreamRecord<>(1));
            testHarness.processElement(new StreamRecord<>(2));
            testHarness.processElement(new StreamRecord<>(3));
            testHarness.endInput();
        }

        assertThat(notifiedEndOfPartition).isCompleted();
    }

    @Test
    void testSortedKeyedProcessOperatorEndOfPartition() throws Exception {
        boolean sortInputs = true;
        MockEnvironment mockEnvironment =
                new MockEnvironmentBuilder().setJobType(JobType.BATCH).build();

        List<Integer> expectedKeys = new ArrayList<>();
        List<Integer> processedLastRecordBeforeEndOfPartition = Arrays.asList(6, 3);

        KeyedProcessOperator<Integer, Integer, Integer> processOperator =
                new KeyedProcessOperator<>(
                        new SingleStreamProcessFunction<Integer, Integer>() {
                            Integer currentRecord;

                            @Override
                            public void processRecord(
                                    Integer record, Consumer<Integer> output, RuntimeContext ctx)
                                    throws Exception {
                                currentRecord = record;
                            }

                            @Override
                            public void endOfPartition(
                                    Consumer<Integer> output, RuntimeContext ctx) {
                                assertThat(processedLastRecordBeforeEndOfPartition)
                                        .element(expectedKeys.size())
                                        .isEqualTo(currentRecord);
                                assertThat(ctx.<Integer>getCurrentKey()).isPresent();
                                expectedKeys.add(ctx.<Integer>getCurrentKey().get());
                            }
                        },
                        sortInputs);

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Integer>) value -> value % 2,
                        Types.INT,
                        mockEnvironment)) {
            testHarness.open();
            // testHarness will not sort the input, so we manually pass records sorted by key.
            testHarness.processElement(new StreamRecord<>(2)); // key = 0
            testHarness.processElement(new StreamRecord<>(4)); // key = 0
            testHarness.processElement(new StreamRecord<>(6)); // key = 0
            testHarness.processElement(new StreamRecord<>(1)); // key = 1
            testHarness.processElement(new StreamRecord<>(3)); // key = 1
            testHarness.endInput();
        }

        assertThat(expectedKeys).containsExactly(0, 1);
    }

    @Test
    void testNotSortedKeyedProcessOperatorEndOfPartition() throws Exception {
        boolean sortInputs = false;
        MockEnvironment mockEnvironment =
                new MockEnvironmentBuilder().setJobType(JobType.BATCH).build();

        List<Integer> expectedKeys = new ArrayList<>();
        AtomicInteger numProcessedRecords = new AtomicInteger(0);

        KeyedProcessOperator<Integer, Integer, Integer> processOperator =
                new KeyedProcessOperator<>(
                        new SingleStreamProcessFunction<Integer, Integer>() {
                            @Override
                            public void processRecord(
                                    Integer record, Consumer<Integer> output, RuntimeContext ctx)
                                    throws Exception {
                                numProcessedRecords.incrementAndGet();
                            }

                            @Override
                            public void endOfPartition(
                                    Consumer<Integer> output, RuntimeContext ctx) {
                                assertThat(numProcessedRecords).hasValue(5);
                                assertThat(ctx.<Integer>getCurrentKey()).isPresent();
                                expectedKeys.add(ctx.<Integer>getCurrentKey().get());
                            }
                        },
                        sortInputs);

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Integer>) value -> value % 2,
                        Types.INT,
                        mockEnvironment)) {
            testHarness.open();
            testHarness.processElement(new StreamRecord<>(2)); // key = 0
            testHarness.processElement(new StreamRecord<>(3)); // key = 1
            testHarness.processElement(new StreamRecord<>(6)); // key = 0
            testHarness.processElement(new StreamRecord<>(1)); // key = 1
            testHarness.processElement(new StreamRecord<>(4)); // key = 0
            testHarness.endInput();
        }

        assertThat(expectedKeys).containsExactlyInAnyOrder(0, 1);
    }
}
