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

package org.apache.flink.processfunction.api.windowing.trigger;

import org.apache.flink.api.common.eventtime.ProcessWatermark;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.processfunction.api.state.StateDeclaration;
import org.apache.flink.processfunction.api.windowing.window.Window;

import java.io.Serializable;
import java.util.Optional;

/**
 * A {@code Trigger} determines when a pane of a window should be evaluated to emit the results for
 * that part of the window.
 *
 * <p>A pane is the bucket of elements that have the same key (assigned by the {@link
 * org.apache.flink.api.java.functions.KeySelector}) and same {@link Window}. An element can be in
 * multiple panes if it was assigned to multiple windows by the {@link
 * org.apache.flink.processfunction.api.windowing.assigner.WindowAssigner}. These panes all have
 * their own instance of the {@code Trigger}.
 *
 * <p>Triggers must not maintain state internally since they can be re-created or reused for
 * different keys. All necessary state should be persisted using the state abstraction available on
 * the {@link TriggerContext}.
 *
 * <p>When used with a {@link
 * org.apache.flink.processfunction.api.windowing.assigner.MergingWindowAssigner} the {@code
 * Trigger} must return {@code true} from {@link #canMerge()} and {@link #onMerge(Window,
 * OnMergeContext)} most be properly implemented.
 *
 * @param <T> The type of elements on which this {@code Trigger} works.
 * @param <W> The type of {@link Window Windows} on which this {@code Trigger} can operate.
 */
public abstract class Trigger<T, W extends Window> implements Serializable {

    private static final long serialVersionUID = -4104633972991191369L;

    /**
     * Called for every element that gets added to a pane. The result of this will determine whether
     * the pane is evaluated to emit results.
     *
     * @param element The element that arrived.
     * @param timestamp The timestamp of the element that arrived.
     * @param window The window to which the element is being added.
     * @param ctx A context object that can be used to register timer callbacks.
     */
    public abstract TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx)
            throws Exception;

    /**
     * Called when a processing-time timer that was set using the trigger context fires.
     *
     * @param time The timestamp at which the timer fired.
     * @param window The window for which the timer fired.
     * @param ctx A context object that can be used to register timer callbacks.
     */
    public abstract TriggerResult onProcessingTime(long time, W window, TriggerContext ctx)
            throws Exception;

    /**
     * Called when a watermark that was set using the trigger context fires.
     *
     * @param watermark The received watermark.
     * @param window The window for which the timer fired.
     * @param ctx A context object that can be used to register timer callbacks.
     */
    public abstract TriggerResult onWatermark(
            ProcessWatermark<?> watermark, W window, TriggerContext ctx) throws Exception;

    /**
     * Returns true if this trigger supports merging of trigger state and can therefore be used with
     * a {@link org.apache.flink.processfunction.api.windowing.assigner.MergingWindowAssigner}.
     *
     * <p>If this returns {@code true} you must properly implement {@link #onMerge(Window,
     * OnMergeContext)}
     */
    public boolean canMerge() {
        return false;
    }

    /**
     * Called when several windows have been merged into one window by the {@link
     * org.apache.flink.processfunction.api.windowing.assigner.WindowAssigner}.
     *
     * @param window The new window that results from the merge.
     * @param ctx A context object that can be used to register timer callbacks and access state.
     */
    public void onMerge(W window, OnMergeContext ctx) throws Exception {
        throw new UnsupportedOperationException("This trigger does not support merging.");
    }

    /**
     * Clears any state that the trigger might still hold for the given window. This is called when
     * a window is purged. Timers set using {@link
     * TriggerContext#registerWatermarkListener(ProcessWatermark)}} and {@link
     * TriggerContext#registerProcessingTimeTimer(long)} should be deleted here as well as state
     * acquired using {@code TriggerContext#getWindowState}.
     */
    public abstract void clear(W window, TriggerContext ctx) throws Exception;

    // ------------------------------------------------------------------------

    /**
     * A context object that is given to {@link Trigger} methods to allow them to register timer
     * callbacks and deal with state.
     */
    public interface TriggerContext {

        /** Returns the current processing time. */
        long getCurrentProcessingTime();

        /** Returns the current watermark time. */
        long getCurrentWatermark();

        /**
         * Register a system time callback. When the current system time passes the specified time
         * {@link Trigger#onProcessingTime(long, Window, TriggerContext)} is called with the time
         * specified here.
         *
         * @param time The time at which to invoke {@link Trigger#onProcessingTime(long, Window,
         *     TriggerContext)}
         */
        void registerProcessingTimeTimer(long time);

        /**
         * Register an watermark callback. When the current watermark passes the specified watermark
         * {@link Trigger#onWatermark(ProcessWatermark, Window, TriggerContext)} is called with the
         * time specified here.
         *
         * @param watermark The watermark at which to invoke {@link
         *     Trigger#onWatermark(ProcessWatermark, Window, TriggerContext)}
         */
        void registerWatermarkListener(ProcessWatermark<?> watermark);

        /** Delete the processing time trigger for the given time. */
        void deleteProcessingTimeTimer(long time);

        void deleteWatermarkListener(ProcessWatermark<?> watermark);

        /**
         * Retrieves a {@link ListState} object that can be used to interact with fault-tolerant
         * state that is scoped to the window and key of the current trigger invocation.
         */
        <T> Optional<ListState<T>> getWindowState(
                StateDeclaration.ListStateDeclaration stateDeclaration);

        /**
         * Retrieves a {@link MapState} object that can be used to interact with fault-tolerant
         * state that is scoped to the window and key of the current trigger invocation.
         */
        <KEY, V> Optional<MapState<KEY, V>> getWindowState(
                StateDeclaration.MapStateDeclaration stateDeclaration);

        /**
         * Retrieves a {@link ValueState} object that can be used to interact with fault-tolerant
         * state that is scoped to the window and key of the current trigger invocation.
         */
        <T> Optional<ValueState<T>> getWindowState(
                StateDeclaration.ValueStateDeclaration stateDeclaration);

        MetricGroup getMetricGroup();
    }

    /**
     * Extension of {@link TriggerContext} that is given to {@link Trigger#onMerge(Window,
     * OnMergeContext)}.
     */
    public interface OnMergeContext extends TriggerContext {
        void mergeWindowState(StateDeclaration StateDeclaration);
    }

    /**
     * Result type for trigger methods. This determines what happens with the window, for example
     * whether the window function should be called, or the window should be discarded.
     *
     * <p>If a {@link Trigger} returns {@link #FIRE} or {@link #FIRE_AND_PURGE} but the window does
     * not contain any data the window function will not be invoked, i.e. no data will be produced
     * for the window.
     */
    public enum TriggerResult {

        /** No action is taken on the window. */
        CONTINUE(false, false),

        /** {@code FIRE_AND_PURGE} evaluates the window function and emits the window result. */
        FIRE_AND_PURGE(true, true),

        /**
         * On {@code FIRE}, the window is evaluated and results are emitted. The window is not
         * purged, though, all elements are retained.
         */
        FIRE(true, false),

        /**
         * All elements in the window are cleared and the window is discarded, without evaluating
         * the window function or emitting any elements.
         */
        PURGE(false, true);

        // ------------------------------------------------------------------------

        private final boolean fire;
        private final boolean purge;

        TriggerResult(boolean fire, boolean purge) {
            this.purge = purge;
            this.fire = fire;
        }

        public boolean isFire() {
            return fire;
        }

        public boolean isPurge() {
            return purge;
        }
    }
}
