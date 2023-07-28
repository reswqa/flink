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

package org.apache.flink.processfunction.api.function;

import org.apache.flink.api.common.eventtime.ProcessWatermarkDeclaration;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.state.StateDeclaration;
import org.apache.flink.processfunction.api.windowing.window.Window;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

public interface TwoInputWindowProcessFunction<IN1, IN2, OUT, W extends Window> extends Function {
    void processRecord(
            IN1 input1,
            IN2 input2,
            Consumer<OUT> output,
            RuntimeContext ctx,
            WindowContext<W> windowContext)
            throws Exception;

    // TODO merge this common part with WindowProcessFunction.
    /**
     * Callback when a window is about to be cleaned up. It is the time to deletes any state in the
     * {@code Context} when the Window expires (the watermark passes its {@code maxTimestamp} +
     * {@code allowedLateness}).
     *
     * @param window The window which is to be cleared.
     */
    default void endOfWindow(W window) throws Exception {}

    default Set<StateDeclaration> useWindowStates() {
        return Collections.emptySet();
    }

    default Set<StateDeclaration> usesStates() {
        return Collections.emptySet();
    }

    default Set<ProcessWatermarkDeclaration> usesWatermarks() {
        return Collections.emptySet();
    }

    interface WindowContext<W> {
        /** Returns the window that is being evaluated. */
        W window();

        <T> Optional<ListState<T>> getWindowState(
                StateDeclaration.ListStateDeclaration stateDeclaration) throws Exception;

        <T> Optional<ValueState<T>> getWindowState(
                StateDeclaration.ValueStateDeclaration stateDeclaration) throws Exception;

        <KEY, V> Optional<MapState<KEY, V>> getWindowState(
                StateDeclaration.MapStateDeclaration stateDeclaration) throws Exception;
    }
}
