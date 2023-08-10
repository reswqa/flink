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

package org.apache.flink.processfunction.functions;

import org.apache.flink.processfunction.api.Collector;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.function.JoinFunction;
import org.apache.flink.processfunction.api.function.TwoInputWindowProcessFunction;
import org.apache.flink.processfunction.api.windowing.window.Window;

public abstract class OutJoinedWindowProcessFunction<IN1, IN2, OUT, W extends Window>
        implements TwoInputWindowProcessFunction<Iterable<IN1>, Iterable<IN2>, OUT, W> {
    private final JoinFunction<IN1, IN2, OUT> joinFunction;

    OutJoinedWindowProcessFunction(JoinFunction<IN1, IN2, OUT> joinFunction) {
        this.joinFunction = joinFunction;
    }

    @Override
    public void processRecord(
            Iterable<IN1> input1,
            Iterable<IN2> input2,
            Collector<OUT> output,
            RuntimeContext ctx,
            WindowContext<W> windowContext)
            throws Exception {
        join(input1, input2, output, ctx);
    }

    protected void outputJoinedRecord(
            IN1 left, IN2 right, Collector<OUT> output, RuntimeContext ctx) throws Exception {
        joinFunction.processRecord(left, right, output, ctx);
    }

    protected abstract void join(
            Iterable<IN1> input1, Iterable<IN2> input2, Collector<OUT> output, RuntimeContext ctx)
            throws Exception;

    public JoinFunction<IN1, IN2, OUT> getJoinFunction() {
        return joinFunction;
    }

    /** Left Outer Join. */
    public static class LeftOuterJoinedWindowProcessFunction<IN1, IN2, OUT, W extends Window>
            extends OutJoinedWindowProcessFunction<IN1, IN2, OUT, W> {
        public LeftOuterJoinedWindowProcessFunction(JoinFunction<IN1, IN2, OUT> joinFunction) {
            super(joinFunction);
        }

        @Override
        protected void join(
                Iterable<IN1> left, Iterable<IN2> right, Collector<OUT> output, RuntimeContext ctx)
                throws Exception {
            if (!left.iterator().hasNext()) {
                return;
            }
            if (!right.iterator().hasNext()) {
                for (IN1 record : left) {
                    outputJoinedRecord(record, null, output, ctx);
                }
            } else {
                for (IN1 leftRecord : left) {
                    for (IN2 rightRecord : right) {
                        outputJoinedRecord(leftRecord, rightRecord, output, ctx);
                    }
                }
            }
        }
    }

    /** Right Outer Join. */
    public static class RightOuterJoinedWindowProcessFunction<IN1, IN2, OUT, W extends Window>
            extends OutJoinedWindowProcessFunction<IN1, IN2, OUT, W> {
        public RightOuterJoinedWindowProcessFunction(JoinFunction<IN1, IN2, OUT> joinFunction) {
            super(joinFunction);
        }

        @Override
        protected void join(
                Iterable<IN1> left, Iterable<IN2> right, Collector<OUT> output, RuntimeContext ctx)
                throws Exception {
            if (!right.iterator().hasNext()) {
                return;
            }
            if (!left.iterator().hasNext()) {
                for (IN2 record : right) {
                    outputJoinedRecord(null, record, output, ctx);
                }
            } else {
                for (IN1 leftRecord : left) {
                    for (IN2 rightRecord : right) {
                        outputJoinedRecord(leftRecord, rightRecord, output, ctx);
                    }
                }
            }
        }
    }

    /** Full Outer Join. */
    public static class FullOuterJoinedWindowProcessFunction<IN1, IN2, OUT, W extends Window>
            extends OutJoinedWindowProcessFunction<IN1, IN2, OUT, W> {
        public FullOuterJoinedWindowProcessFunction(JoinFunction<IN1, IN2, OUT> joinFunction) {
            super(joinFunction);
        }

        @Override
        protected void join(
                Iterable<IN1> left, Iterable<IN2> right, Collector<OUT> output, RuntimeContext ctx)
                throws Exception {
            if (!right.iterator().hasNext() && !left.iterator().hasNext()) {
                return;
            }
            if (!left.iterator().hasNext()) {
                for (IN2 record : right) {
                    outputJoinedRecord(null, record, output, ctx);
                }
            } else if (!right.iterator().hasNext()) {
                for (IN1 record : left) {
                    outputJoinedRecord(record, null, output, ctx);
                }
            } else {
                for (IN1 leftRecord : left) {
                    for (IN2 rightRecord : right) {
                        outputJoinedRecord(leftRecord, rightRecord, output, ctx);
                    }
                }
            }
        }
    }
}
