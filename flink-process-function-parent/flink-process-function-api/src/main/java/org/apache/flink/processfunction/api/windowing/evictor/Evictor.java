/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.processfunction.api.windowing.evictor;

import org.apache.flink.api.common.eventtime.TimestampedValue;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.processfunction.api.windowing.assigner.WindowAssigner;
import org.apache.flink.processfunction.api.windowing.trigger.Trigger;
import org.apache.flink.processfunction.api.windowing.window.Window;

import java.io.Serializable;

/**
 * An {@code Evictor} can remove elements from a pane before/after the evaluation of WindowFunction
 * and after the window evaluation gets triggered by a {@link Trigger}
 *
 * <p>A pane is the bucket of elements that have the same key (assigned by the {@link KeySelector})
 * and same {@link Window}. An element can be in multiple panes of it was assigned to multiple
 * windows by the {@link WindowAssigner}. These panes all have their own instance of the {@code
 * Evictor}.
 *
 * @param <T> The type of elements that this {@code Evictor} can evict.
 * @param <W> The type of {@link Window Windows} on which this {@code Evictor} can operate.
 */
public interface Evictor<T, W extends Window> extends Serializable {

    /**
     * Optionally evicts elements. Called before windowing function.
     *
     * @param elements The elements currently in the pane.
     * @param size The current number of elements in the pane.
     * @param window The {@link Window}
     * @param evictorContext The context for the Evictor
     */
    void evictBefore(
            Iterable<TimestampedValue<T>> elements,
            int size,
            W window,
            EvictorContext evictorContext);

    /**
     * Optionally evicts elements. Called after windowing function.
     *
     * @param elements The elements currently in the pane.
     * @param size The current number of elements in the pane.
     * @param window The {@link Window}
     * @param evictorContext The context for the Evictor
     */
    void evictAfter(
            Iterable<TimestampedValue<T>> elements,
            int size,
            W window,
            EvictorContext evictorContext);

    /** A context object that is given to {@link Evictor} methods. */
    interface EvictorContext {

        /** Returns the current processing time. */
        long getCurrentProcessingTime();

        /** Returns the current watermark time. */
        long getCurrentWatermark();

        // TODO metrics group.
    }
}
