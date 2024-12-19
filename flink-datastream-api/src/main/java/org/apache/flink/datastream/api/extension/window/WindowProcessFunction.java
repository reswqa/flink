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

package org.apache.flink.datastream.api.extension.window;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDeclaration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDeclaration;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.RuntimeContext;
import org.apache.flink.datastream.api.extension.window.window.Window;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/** Special process function for window. */
public interface WindowProcessFunction<IN, OUT, W extends Window> extends Function {

    void processRecord(
            IN record, Collector<OUT> output, RuntimeContext ctx, WindowContext<W> windowContext)
            throws Exception;

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

    /**
     * Explicitly declare watermarks upfront. Each specific watermark must be declared in this
     * method before it can be used.
     *
     * @return all watermark declarations used by this application.
     */
    default Collection<? extends WatermarkDeclaration> watermarkDeclarations() {
        return Collections.emptySet();
    }

    interface WindowContext<W> {
        /** Returns the window that is being evaluated. */
        W window();

        <T> Optional<ListState<T>> getWindowState(
                ListStateDeclaration stateDeclaration) throws Exception;

        <T> Optional<ValueState<T>> getWindowState(
                ValueStateDeclaration stateDeclaration) throws Exception;

        <KEY, V> Optional<MapState<KEY, V>> getWindowState(
                MapStateDeclaration stateDeclaration) throws Exception;
    }
}
