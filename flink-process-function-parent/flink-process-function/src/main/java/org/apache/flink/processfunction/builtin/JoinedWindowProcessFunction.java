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

package org.apache.flink.processfunction.builtin;

import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.function.JoinFunction;
import org.apache.flink.processfunction.api.function.TwoInputWindowProcessFunction;
import org.apache.flink.processfunction.api.windowing.window.Window;

import java.util.function.Consumer;

public class JoinedWindowProcessFunction<IN1, IN2, OUT, W extends Window>
        implements TwoInputWindowProcessFunction<Iterable<IN1>, Iterable<IN2>, OUT, W> {
    private final JoinFunction<IN1, IN2, OUT> joinFunction;

    JoinedWindowProcessFunction(JoinFunction<IN1, IN2, OUT> joinFunction) {
        this.joinFunction = joinFunction;
    }

    @Override
    public void processRecord(
            Iterable<IN1> input1,
            Iterable<IN2> input2,
            Consumer<OUT> output,
            RuntimeContext ctx,
            WindowContext<W> windowContext)
            throws Exception {
        for (IN1 left : input1) {
            for (IN2 right : input2) {
                joinFunction.processRecord(left, right, output, ctx);
            }
        }
    }

    public JoinFunction<IN1, IN2, OUT> getJoinFunction() {
        return joinFunction;
    }
}
