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
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

class TwoInputProcessOperatorTest {
    @Test
    void testKeyedSortedTwoInputProcessEndOfPartition() throws Exception {
        boolean sortInputs = true;
        MockEnvironment mockEnvironment =
                new MockEnvironmentBuilder().setJobType(JobType.BATCH).build();

        List<Integer> expectedKeys1 = new ArrayList<>();
        List<Integer> expectedKeys2 = new ArrayList<>();
        List<Integer> processedLastRecordBeforeEndOfPartition1 = Arrays.asList(4, 3);
        List<Long> processedLastRecordBeforeEndOfPartition2 = Arrays.asList(8L, 1L);

        KeyedTwoInputProcessOperator<Integer, Integer, Long, Integer> processOperator =
                new KeyedTwoInputProcessOperator<>(
                        new TwoInputStreamProcessFunction<Integer, Long, Integer>() {
                            private int firstInputRecord;

                            private long secondInputRecord;

                            @Override
                            public void processFirstInputRecord(
                                    Integer record, Consumer<Integer> output, RuntimeContext ctx)
                                    throws Exception {
                                firstInputRecord = record;
                            }

                            @Override
                            public void processSecondInputRecord(
                                    Long record, Consumer<Integer> output, RuntimeContext ctx)
                                    throws Exception {
                                secondInputRecord = record;
                            }

                            @Override
                            public void endOfFirstInputPartition(
                                    Consumer<Integer> output, RuntimeContext ctx) {
                                assertThat(processedLastRecordBeforeEndOfPartition1)
                                        .element(expectedKeys1.size())
                                        .isEqualTo(firstInputRecord);
                                assertThat(ctx.<Integer>getCurrentKey()).isPresent();
                                expectedKeys1.add(ctx.<Integer>getCurrentKey().get());
                            }

                            @Override
                            public void endOfSecondInputPartition(
                                    Consumer<Integer> output, RuntimeContext ctx) {
                                assertThat(processedLastRecordBeforeEndOfPartition2)
                                        .element(expectedKeys2.size())
                                        .isEqualTo(secondInputRecord);
                                assertThat(ctx.<Integer>getCurrentKey()).isPresent();
                                expectedKeys2.add(ctx.<Integer>getCurrentKey().get());
                            }
                        },
                        sortInputs);

        try (KeyedTwoInputStreamOperatorTestHarness<Integer, Integer, Long, Integer> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Integer>) value -> value % 2,
                        (KeySelector<Long, Integer>) value -> Math.toIntExact(value % 2),
                        Types.INT,
                        mockEnvironment)) {
            testHarness.open();
            // testHarness will not sort the input, so we manually pass records sorted by key.
            testHarness.processElement1(new StreamRecord<>(2)); // key = 0
            testHarness.processElement1(new StreamRecord<>(4)); // key = 0
            testHarness.processElement2(new StreamRecord<>(6L)); // key = 0
            testHarness.processElement2(new StreamRecord<>(8L)); // key = 0
            testHarness.processElement2(new StreamRecord<>(1L)); // key = 1
            testHarness.processElement1(new StreamRecord<>(3)); // key = 1
            testHarness.endInput(1);
            testHarness.endInput(2);
        }

        assertThat(expectedKeys1).containsExactly(0, 1);
        assertThat(expectedKeys2).containsExactly(0, 1);
    }
}
