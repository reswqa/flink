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

package org.apache.flink.datastream.impl.extension.join;


import org.apache.flink.datastream.api.extension.join.JoinExtension;
import org.apache.flink.datastream.api.extension.join.JoinFunction;
import org.apache.flink.datastream.api.extension.window.WindowExtension;
import org.apache.flink.datastream.api.extension.window.window.Window;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.join.function.JoinedWindowProcessFunction;
import org.apache.flink.datastream.impl.extension.join.function.OutJoinedWindowProcessFunction;
import org.apache.flink.datastream.impl.extension.window.function.InternalTwoInputWindowFunction;

public class JoinExtensionImpl {
    public static <IN1, IN2, OUT, W extends Window>
    TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> join(
            JoinFunction<IN1, IN2, OUT> joinFunction,
            JoinExtension.JoinType joinType,
            WindowExtension.TwoInputWindowBuilder<IN1, IN2, W> windowBuilder) {
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
