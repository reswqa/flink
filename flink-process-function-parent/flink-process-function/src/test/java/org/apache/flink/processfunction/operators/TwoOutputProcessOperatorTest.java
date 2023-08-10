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

import org.apache.flink.processfunction.api.Collector;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

public class TwoOutputProcessOperatorTest {
    @Test
    void testNonKeyedTwoOutputProcessEndOfPartition() throws Exception {
        CompletableFuture<Void> notifiedEndOfPartition = new CompletableFuture<>();

        MockEnvironment mockEnvironment =
                new MockEnvironmentBuilder().setJobType(JobType.BATCH).build();
        OutputTag<Long> sideOutputTag = new OutputTag<Long>("side-output") {};

        TwoOutputProcessOperator<Integer, Integer, Long> processOperator =
                new TwoOutputProcessOperator<>(
                        new TwoOutputStreamProcessFunction<Integer, Integer, Long>() {
                            @Override
                            public void processRecord(
                                    Integer record,
                                    Collector<Integer> output1,
                                    Collector<Long> output2,
                                    RuntimeContext ctx) {
                                // do nothing
                            }

                            @Override
                            public void endOfPartition(
                                    Collector<Integer> output1,
                                    Collector<Long> output2,
                                    RuntimeContext ctx) {
                                assertThat(notifiedEndOfPartition).isNotDone();
                                notifiedEndOfPartition.complete(null);
                            }
                        },
                        sideOutputTag);

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
}
