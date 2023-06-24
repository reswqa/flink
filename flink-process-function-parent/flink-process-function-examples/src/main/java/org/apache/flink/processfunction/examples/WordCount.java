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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.processfunction.api.ExecutionEnvironment;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.builtin.BatchStreamingUnifiedFunctions;
import org.apache.flink.processfunction.api.builtin.Sources;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream;

import java.util.Arrays;
import java.util.Random;
import java.util.function.Consumer;

public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        boolean isStreamingMode = false;
        NonKeyedPartitionStream<String> source;
        if (isStreamingMode) {
            env.tmpSetRuntimeMode(RuntimeExecutionMode.STREAMING);
            source =
                    env.fromSource(
                            Sources.supplier(
                                    () -> {
                                        // generate lines of text, each line contains 5 words
                                        // (letter)
                                        Random random = new Random();
                                        StringBuilder sb = new StringBuilder();
                                        for (int i = 0; i < 5; ++i) {
                                            sb.append((char) ('A' + random.nextInt(26)))
                                                    .append(' ');
                                        }
                                        return sb.toString();
                                    }));
        } else {
            env.tmpSetRuntimeMode(RuntimeExecutionMode.BATCH);
            source = env.fromSource(Sources.collection(Arrays.asList("A", "B", "A", "C")));
        }
        source.process(new Tokenizer())
                .keyBy(WordAndCount::getWord)
                .process(
                        BatchStreamingUnifiedFunctions.reduce(
                                (wc1, wc2) -> new WordAndCount(wc1.word, wc1.count + wc2.count)))
                // Don't use Lambda reference as PrintStream is not serializable.
                .tmpToConsumerSink((wc) -> System.out.println(wc));
        env.execute();
    }

    public static final class Tokenizer
            implements SingleStreamProcessFunction<String, WordAndCount> {
        @Override
        public void processRecord(
                String record, Consumer<WordAndCount> output, RuntimeContext ctx) {
            String[] tokens = record.split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    output.accept(new WordAndCount(token, 1));
                }
            }
        }
    }

    public static final class WordAndCount {
        private final String word;
        private final int count;

        public WordAndCount(String word, int count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return this.word;
        }

        public int getCount() {
            return this.count;
        }

        @Override
        public String toString() {
            return word + " " + count;
        }
    }
}
