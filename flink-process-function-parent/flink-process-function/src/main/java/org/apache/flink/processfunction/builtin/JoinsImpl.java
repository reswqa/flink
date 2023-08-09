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

import org.apache.flink.processfunction.api.builtin.Joins;
import org.apache.flink.processfunction.api.builtin.Windows;
import org.apache.flink.processfunction.api.function.JoinFunction;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.processfunction.api.windowing.window.Window;
import org.apache.flink.processfunction.functions.InternalTwoInputWindowFunction;
import org.apache.flink.processfunction.functions.JoinedWindowProcessFunction;
import org.apache.flink.processfunction.functions.OutJoinedWindowProcessFunction;

public class JoinsImpl {
    public static <IN1, IN2, OUT, W extends Window>
            TwoInputStreamProcessFunction<IN1, IN2, OUT> join(
                    JoinFunction<IN1, IN2, OUT> joinFunction,
                    Joins.JoinType joinType,
                    Windows.TwoInputWindowBuilder<IN1, IN2, W> windowBuilder) {
        switch (joinType) {
            case INNER:
                return new InternalTwoInputWindowFunction<>(
                        new JoinedWindowProcessFunction<>(joinFunction),
                        windowBuilder.getAssigner(),
                        windowBuilder.getTrigger());
            case LEFT_OUTER:
                return new InternalTwoInputWindowFunction<>(
                        new OutJoinedWindowProcessFunction.LeftOuterJoinedWindowProcessFunction<>(
                                joinFunction),
                        windowBuilder.getAssigner(),
                        windowBuilder.getTrigger());
            case RIGHT_OUTER:
                return new InternalTwoInputWindowFunction<>(
                        new OutJoinedWindowProcessFunction.RightOuterJoinedWindowProcessFunction<>(
                                joinFunction),
                        windowBuilder.getAssigner(),
                        windowBuilder.getTrigger());
            case FULL_OUTER:
                return new InternalTwoInputWindowFunction<>(
                        new OutJoinedWindowProcessFunction.FullOuterJoinedWindowProcessFunction<>(
                                joinFunction),
                        windowBuilder.getAssigner(),
                        windowBuilder.getTrigger());
            default:
                throw new UnsupportedOperationException("Unsupported join type : " + joinType);
        }
    }
}
