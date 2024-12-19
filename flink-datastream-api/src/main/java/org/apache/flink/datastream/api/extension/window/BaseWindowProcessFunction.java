package org.apache.flink.datastream.api.extension.window;

import org.apache.flink.api.common.state.ListStateDeclaration;
import org.apache.flink.api.common.state.MapStateDeclaration;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.datastream.api.extension.window.window.Window;
import org.apache.flink.datastream.api.function.ProcessFunction;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * Base interface for functions that are evaluated over windows.
 *
 * @param <W> The type of {@code Window} that this window function can be applied on.
 */
public interface BaseWindowProcessFunction<W extends Window> extends ProcessFunction {

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

    interface WindowContext<W> {
        /** Returns the window that is being evaluated. */
        W window();

        /**
         * Retrieves a {@link ListState} object that can be used to interact with fault-tolerant
         * state that is scoped to the window and key of the current trigger invocation.
         */
        <T> Optional<ListState<T>> getWindowState(ListStateDeclaration<T> stateDeclaration)
                throws Exception;

        /**
         * Retrieves a {@link MapState} object that can be used to interact with fault-tolerant
         * state that is scoped to the window and key of the current trigger invocation.
         */
        <KEY, V> Optional<MapState<KEY, V>> getWindowState(
                MapStateDeclaration<KEY, V> stateDeclaration) throws Exception;

        /**
         * Retrieves a {@link ValueState} object that can be used to interact with fault-tolerant
         * state that is scoped to the window and key of the current trigger invocation.
         */
        <T> Optional<ValueState<T>> getWindowState(ValueStateDeclaration<T> stateDeclaration)
                throws Exception;
    }
}
