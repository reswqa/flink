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

package org.apache.flink.processfunction.api.builtin;

import org.apache.flink.processfunction.api.function.JoinFunction;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;

public class Joins {
    public static final Class<?> INSTANCE;

    static {
        try {
            INSTANCE = Class.forName("org.apache.flink.processfunction.builtin.JoinsImpl");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    "Please ensure that flink-process-function in your class path");
        }
    }

    /** Non-Window join. */
    @SuppressWarnings("unchecked")
    public <IN1, IN2, OUT> TwoInputStreamProcessFunction<IN1, IN2, OUT> join(
            JoinFunction<IN1, IN2, OUT> joinFunction, JoinType joinType) {
        try {
            return (TwoInputStreamProcessFunction<IN1, IN2, OUT>)
                    INSTANCE.getMethod(
                                    "join",
                                    JoinFunction.class,
                                    JoinType.class,
                                    Windows.TwoInputWindowBuilder.class)
                            .invoke(
                                    null,
                                    joinFunction,
                                    joinType,
                                    Windows.GlobalWindows.createTwoInput());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public enum JoinType {
        INNER,
        LEFT_OUTER,
        RIGHT_OUTER,
        FULL_OUTER
    }
}
