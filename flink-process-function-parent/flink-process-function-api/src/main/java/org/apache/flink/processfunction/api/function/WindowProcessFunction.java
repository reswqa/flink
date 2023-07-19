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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.processfunction.api.state.StateDeclaration;
import org.apache.flink.processfunction.api.windowing.window.Window;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/** Special process function for window. */
public abstract class WindowProcessFunction<IN, OUT, W extends Window>
        implements SingleStreamProcessFunction<IN, OUT> {

    private WindowContext<W> windowContext;

    public void setContext(WindowContext<W> context) {
        this.windowContext = context;
    }

    public WindowContext<W> getWindowContext() {
        if (this.windowContext != null) {
            return this.windowContext;
        } else {
            throw new IllegalStateException("The window context has not been initialized.");
        }
    }

    public Set<StateDeclaration> useWindowStates() {
        return Collections.emptySet();
    }

    /**
     * Callback when a window is about to be cleaned up. It is the time to deletes any state in the
     * {@code Context} when the Window expires (the watermark passes its {@code maxTimestamp} +
     * {@code allowedLateness}).
     *
     * @param window The window which is to be cleared.
     * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
     */
    public void endOfWindow(W window) throws Exception {}

    public interface WindowContext<W> {
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
