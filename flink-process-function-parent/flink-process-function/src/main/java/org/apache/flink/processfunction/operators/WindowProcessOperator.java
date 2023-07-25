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

package org.apache.flink.processfunction.operators;

import org.apache.flink.api.common.eventtime.EventTimestampWatermark;
import org.apache.flink.api.common.eventtime.GeneralizedWatermark;
import org.apache.flink.api.common.eventtime.ProcessWatermark;
import org.apache.flink.api.common.eventtime.TimestampWatermark;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.processfunction.api.function.WindowProcessFunction;
import org.apache.flink.processfunction.api.state.StateDeclaration;
import org.apache.flink.processfunction.api.windowing.assigner.MergingWindowAssigner;
import org.apache.flink.processfunction.api.windowing.assigner.WindowAssigner;
import org.apache.flink.processfunction.api.windowing.trigger.Trigger;
import org.apache.flink.processfunction.api.windowing.trigger.Trigger.TriggerResult;
import org.apache.flink.processfunction.api.windowing.window.Window;
import org.apache.flink.processfunction.functions.InternalWindowFunction;
import org.apache.flink.processfunction.state.ListStateDeclarationImpl;
import org.apache.flink.processfunction.state.StateDeclarationConverter;
import org.apache.flink.processfunction.windows.MergingWindowSet;
import org.apache.flink.processfunction.windows.ParallelismAwareKeySelector;
import org.apache.flink.processfunction.windows.window.BoundedWindow;
import org.apache.flink.processfunction.windows.window.TimeWindow;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalAppendingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMergingState;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.Collection;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Process operator for window. */
public class WindowProcessOperator<K, IN, ACC, OUT, W extends Window>
        extends KeyedProcessOperator<K, IN, OUT> implements Triggerable<K, W> {

    private static final long serialVersionUID = 1L;

    // ------------------------------------------------------------------------
    // Configuration values and user functions
    // ------------------------------------------------------------------------

    protected final WindowAssigner<? super IN, W> windowAssigner;

    private final Trigger<? super IN, ? super W> trigger;

    protected transient WindowTriggerContext triggerContext = new WindowTriggerContext(null, null);

    protected transient WindowFunctionContext windowFunctionContext;

    private final StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor;

    /** For serializing the key in checkpoints. */
    protected final TypeSerializer<K> keySerializer;

    /** For serializing the window in checkpoints. */
    protected final TypeSerializer<W> windowSerializer;

    /**
     * The allowed lateness for elements. This is used for:
     *
     * <ul>
     *   <li>Deciding if an element should be dropped from a window due to lateness.
     *   <li>Clearing the state of a window if the system time passes the {@code window.maxTimestamp
     *       + allowedLateness} landmark.
     * </ul>
     */
    protected final long allowedLateness;

    /**
     * {@link OutputTag} to use for late arriving events. Elements for which {@code
     * window.maxTimestamp + allowedLateness} is smaller than the current watermark will be emitted
     * to this.
     */
    protected final OutputTag<IN> lateDataOutputTag;

    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";

    protected transient Counter numLateRecordsDropped;

    // ------------------------------------------------------------------------
    // State that is not checkpointed
    // ------------------------------------------------------------------------

    /** The state in which the window contents is stored. Each window is a namespace */
    private transient InternalAppendingState<K, W, IN, ACC, ACC> windowState;

    private transient WindowStateStore windowStateStore;

    /**
     * The {@link #windowState}, typed to merging state for merging windows. Null if the window
     * state is not mergeable.
     */
    private transient InternalMergingState<K, W, IN, ACC, ACC> windowMergingState;

    /** The state that holds the merging window metadata (the sets that describe what is merged). */
    private transient InternalListState<K, VoidNamespace, Tuple2<W, W>> mergingSetsState;

    protected transient InternalWindowAssignerContext windowAssignerContext;

    // ------------------------------------------------------------------------
    // State that needs to be checkpointed
    // ------------------------------------------------------------------------

    protected transient InternalTimerService<W> internalTimerService;

    private final WindowProcessFunction<ACC, OUT, W> windowFunction;

    public WindowProcessOperator(
            InternalWindowFunction<IN, ACC, OUT, W> windowFunction,
            WindowAssigner<? super IN, W> windowAssigner,
            Trigger<? super IN, ? super W> trigger,
            TypeSerializer<W> windowSerializer,
            TypeSerializer<K> keySerializer,
            StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor,
            long allowedLateness,
            OutputTag<IN> lateDataOutputTag,
            boolean sortInputs) {
        super(windowFunction, sortInputs);

        checkArgument(allowedLateness >= 0);

        checkArgument(
                windowStateDescriptor == null || windowStateDescriptor.isSerializerInitialized(),
                "window state serializer is not properly initialized");

        this.windowFunction = windowFunction.getWindowProcessFunction();
        this.windowAssigner = windowAssigner;
        this.trigger = trigger;
        this.windowSerializer = windowSerializer;
        this.keySerializer = keySerializer;
        this.windowStateDescriptor = windowStateDescriptor;
        this.allowedLateness = allowedLateness;
        this.lateDataOutputTag = lateDataOutputTag;

        setChainingStrategy(ChainingStrategy.ALWAYS);
    }

    @Override
    public void open() throws Exception {
        super.open();

        // inject parallelism for ParallelismAwareKeySelector.
        if (getStateKeySelector1() instanceof ParallelismAwareKeySelector) {
            ((ParallelismAwareKeySelector<?>) getStateKeySelector1())
                    .setParallelismSupplier(() -> getRuntimeContext().getIndexOfThisSubtask());
        }

        numLateRecordsDropped = metrics.counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);

        internalTimerService =
                getInternalTimerService("process-window-timers", windowSerializer, this);

        triggerContext = new WindowTriggerContext(null, null);
        windowFunctionContext = new WindowFunctionContext();
        windowStateStore = new WindowStateStore();

        windowAssignerContext = new InternalWindowAssignerContext();

        // create (or restore) the state that hold the actual window contents
        // NOTE - the state may be null in the case of the overriding evicting window operator
        if (windowStateDescriptor != null) {
            windowState =
                    (InternalAppendingState<K, W, IN, ACC, ACC>)
                            getOrCreateKeyedState(windowSerializer, windowStateDescriptor);
        }

        // create the typed and helper states for merging windows
        if (windowAssigner instanceof MergingWindowAssigner) {

            // store a typed reference for the state of merging windows - sanity check
            if (windowState instanceof InternalMergingState) {
                windowMergingState = (InternalMergingState<K, W, IN, ACC, ACC>) windowState;
            } else if (windowState != null) {
                throw new IllegalStateException(
                        "The window uses a merging assigner, but the window state is not mergeable.");
            }

            @SuppressWarnings("unchecked")
            final Class<Tuple2<W, W>> typedTuple = (Class<Tuple2<W, W>>) (Class<?>) Tuple2.class;

            final TupleSerializer<Tuple2<W, W>> tupleSerializer =
                    new TupleSerializer<>(
                            typedTuple, new TypeSerializer[] {windowSerializer, windowSerializer});

            final ListStateDescriptor<Tuple2<W, W>> mergingSetsStateDescriptor =
                    new ListStateDescriptor<>("merging-window-set", tupleSerializer);

            // get the state that stores the merging sets
            mergingSetsState =
                    (InternalListState<K, VoidNamespace, Tuple2<W, W>>)
                            getOrCreateKeyedState(
                                    VoidNamespaceSerializer.INSTANCE, mergingSetsStateDescriptor);
            mergingSetsState.setCurrentNamespace(VoidNamespace.INSTANCE);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        triggerContext = null;
        windowFunctionContext = null;
        windowAssignerContext = null;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        final Collection<W> elementWindows =
                windowAssigner.assignWindows(
                        element.getValue(), element.getTimestamp(), windowAssignerContext);

        // if element is handled by none of assigned elementWindows
        boolean isSkippedElement = true;

        final K key = this.<K>getKeyedStateBackend().getCurrentKey();

        if (windowAssigner instanceof MergingWindowAssigner) {
            MergingWindowSet<W> mergingWindows = getMergingWindowSet();

            for (W window : elementWindows) {

                // adding the new window might result in a merge, in that case the actualWindow
                // is the merged window and we work with that. If we don't merge then
                // actualWindow == window
                W actualWindow =
                        mergingWindows.addWindow(
                                window,
                                new MergingWindowSet.MergeFunction<W>() {
                                    @Override
                                    public void merge(
                                            W mergeResult,
                                            Collection<W> mergedWindows,
                                            W stateWindowResult,
                                            Collection<W> mergedStateWindows)
                                            throws Exception {
                                        triggerContext.key = key;
                                        triggerContext.window = mergeResult;

                                        triggerContext.onMerge(mergeResult, mergedWindows);

                                        for (W m : mergedWindows) {
                                            triggerContext.window = m;
                                            triggerContext.clear();
                                            if (m instanceof BoundedWindow) {
                                                ((BoundedWindow<?>) m)
                                                        .unRegisterCleaner(triggerContext);
                                            }
                                        }

                                        // merge the merged state windows into the newly resulting
                                        // state window
                                        windowMergingState.mergeNamespaces(
                                                stateWindowResult, mergedStateWindows);
                                    }
                                });

                // drop if the window is already late
                if (isWindowLate(actualWindow)) {
                    mergingWindows.retireWindow(actualWindow);
                    continue;
                }
                isSkippedElement = false;

                W stateWindow = mergingWindows.getStateWindow(actualWindow);
                if (stateWindow == null) {
                    throw new IllegalStateException(
                            "Window " + window + " is not in in-flight window set.");
                }

                windowState.setCurrentNamespace(stateWindow);
                windowState.add(element.getValue());

                triggerContext.key = key;
                triggerContext.window = actualWindow;

                TriggerResult triggerResult = triggerContext.onElement(element);

                if (triggerResult.isFire()) {
                    ACC contents = windowState.get();
                    if (contents == null) {
                        continue;
                    }
                    emitWindowContents(actualWindow, contents);
                }

                if (triggerResult.isPurge()) {
                    windowState.clear();
                }
                if (window instanceof BoundedWindow) {
                    ((BoundedWindow<?>) window).registerCleaner(triggerContext);
                }
            }

            // need to make sure to update the merging state in state
            mergingWindows.persist();
        } else {
            for (W window : elementWindows) {

                // drop if the window is already late
                if (isWindowLate(window)) {
                    continue;
                }
                isSkippedElement = false;

                windowState.setCurrentNamespace(window);
                windowState.add(element.getValue());

                triggerContext.key = key;
                triggerContext.window = window;

                TriggerResult triggerResult = triggerContext.onElement(element);

                if (triggerResult.isFire()) {
                    ACC contents = windowState.get();
                    if (contents == null) {
                        continue;
                    }
                    emitWindowContents(window, contents);
                }

                if (triggerResult.isPurge()) {
                    windowState.clear();
                }
                if (window instanceof BoundedWindow) {
                    ((BoundedWindow<?>) window).registerCleaner(triggerContext);
                }
            }
        }

        // side output input event if element not handled by any window late arriving tag has been
        // set windowAssigner is event time and current timestamp + allowed lateness no less than
        // element timestamp.
        if (isSkippedElement && isElementLate(element)) {
            if (lateDataOutputTag != null) {
                sideOutput(element);
            } else {
                this.numLateRecordsDropped.inc();
            }
        }
    }

    @Override
    public void onEventTime(InternalTimer<K, W> timer) throws Exception {
        triggerContext.key = timer.getKey();
        triggerContext.window = timer.getNamespace();

        MergingWindowSet<W> mergingWindows;

        if (windowAssigner instanceof MergingWindowAssigner) {
            mergingWindows = getMergingWindowSet();
            W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
            if (stateWindow == null) {
                // Timer firing for non-existent window, this can only happen if a
                // trigger did not clean up timers. We have already cleared the merging
                // window and therefore the Trigger state, however, so nothing to do.
                return;
            } else {
                windowState.setCurrentNamespace(stateWindow);
            }
        } else {
            windowState.setCurrentNamespace(triggerContext.window);
            mergingWindows = null;
        }

        TriggerResult triggerResult =
                triggerContext.onWatermark(new EventTimestampWatermark(timer.getTimestamp()));

        if (triggerResult.isFire()) {
            ACC contents = windowState.get();
            if (contents != null) {
                emitWindowContents(triggerContext.window, contents);
            }
        }

        if (triggerResult.isPurge()) {
            windowState.clear();
        }

        // TODO this can be removed in the future
        if (windowAssigner.isEventTime()) {
            if (triggerContext.window instanceof TimeWindow) {
                windowAssignerContext.processWatermark =
                        new EventTimestampWatermark(timer.getTimestamp());
                if (((TimeWindow) triggerContext.window).isBoundaryReached(windowAssignerContext)) {
                    clearAllState(triggerContext.window, windowState, mergingWindows);
                }
            }
        }

        if (mergingWindows != null) {
            // need to make sure to update the merging state in state
            mergingWindows.persist();
        }
    }

    @Override
    public void processWatermark(GeneralizedWatermark mark) throws Exception {
        if (mark instanceof TimestampWatermark) {
            if (timeServiceManager != null) {
                timeServiceManager.advanceWatermark(
                        new Watermark(((TimestampWatermark) mark).getTimestamp()));
            }
            // TODO should we trigger userFunction.onWatermark also for timestamp watermark?
            // always push timestamp watermark to output be keep the original behavior.
            output.emitWatermark(mark);
            return;
        }
        // TODO handle process watermark: This required something like timeService, maybe called
        // watermarkManager to manage all generalized watermarks for this task.
        /*if (mark instanceof ProcessWatermarkWrapper) {
            triggerContext.onWatermark(((ProcessWatermarkWrapper) mark).getProcessWatermark());
        }*/
    }

    @Override
    public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
        triggerContext.key = timer.getKey();
        triggerContext.window = timer.getNamespace();

        MergingWindowSet<W> mergingWindows;

        if (windowAssigner instanceof MergingWindowAssigner) {
            mergingWindows = getMergingWindowSet();
            W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
            if (stateWindow == null) {
                // Timer firing for non-existent window, this can only happen if a
                // trigger did not clean up timers. We have already cleared the merging
                // window and therefore the Trigger state, however, so nothing to do.
                return;
            } else {
                windowState.setCurrentNamespace(stateWindow);
            }
        } else {
            windowState.setCurrentNamespace(triggerContext.window);
            mergingWindows = null;
        }

        TriggerResult triggerResult = triggerContext.onProcessingTime(timer.getTimestamp());

        if (triggerResult.isFire()) {
            ACC contents = windowState.get();
            if (contents != null) {
                emitWindowContents(triggerContext.window, contents);
            }
        }

        if (triggerResult.isPurge()) {
            windowState.clear();
        }

        if (!windowAssigner.isEventTime() && triggerContext.window instanceof TimeWindow) {
            if (((TimeWindow) triggerContext.window).isBoundaryReached(windowAssignerContext)) {
                clearAllState(triggerContext.window, windowState, mergingWindows);
            }
        }

        if (mergingWindows != null) {
            // need to make sure to update the merging state in state
            mergingWindows.persist();
        }
    }

    /**
     * Drops all state for the given window and calls {@link Trigger#clear(Window,
     * Trigger.TriggerContext)}.
     *
     * <p>The caller must ensure that the correct key is set in the state backend and the
     * triggerContext object.
     */
    private void clearAllState(
            W window, AppendingState<IN, ACC> windowState, MergingWindowSet<W> mergingWindows)
            throws Exception {
        windowState.clear();
        triggerContext.clear();
        windowFunctionContext.window = window;
        windowFunction.endOfWindow(window);
        if (mergingWindows != null) {
            mergingWindows.retireWindow(window);
            mergingWindows.persist();
        }
    }

    /**
     * Emits the contents of the given window using the {@link
     * org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction}.
     */
    @SuppressWarnings("unchecked")
    private void emitWindowContents(W window, ACC contents) throws Exception {
        // only time window touch the time concept.
        if (window instanceof TimeWindow) {
            outputCollector.setAbsoluteTimestamp(((TimeWindow) window).maxTimeStamp());
        }
        windowFunctionContext.window = window;
        windowFunction.processRecord(contents, outputCollector, context, windowFunctionContext);
    }

    /**
     * Write skipped late arriving element to SideOutput.
     *
     * @param element skipped late arriving element to side output
     */
    protected void sideOutput(StreamRecord<IN> element) {
        output.collect(lateDataOutputTag, element);
    }

    /**
     * Retrieves the {@link MergingWindowSet} for the currently active key. The caller must ensure
     * that the correct key is set in the state backend.
     *
     * <p>The caller must also ensure to properly persist changes to state using {@link
     * MergingWindowSet#persist()}.
     */
    protected MergingWindowSet<W> getMergingWindowSet() throws Exception {
        @SuppressWarnings("unchecked")
        MergingWindowAssigner<? super IN, W> mergingAssigner =
                (MergingWindowAssigner<? super IN, W>) windowAssigner;
        return new MergingWindowSet<>(mergingAssigner, mergingSetsState);
    }

    /**
     * Returns {@code true} if the watermark is after the end timestamp plus the allowed lateness of
     * the given window.
     */
    // TODO Provide a common later strategy.
    protected boolean isWindowLate(W window) {
        if (window instanceof TimeWindow) {
            return ((TimeWindow) window).isWindowLate(internalTimerService.currentWatermark());
        }
        return false;
    }

    /**
     * Decide if a record is currently late, based on current watermark and allowed lateness.
     *
     * @param element The element to check
     * @return The element for which should be considered when sideoutputs
     */
    protected boolean isElementLate(StreamRecord<IN> element) {
        return (windowAssigner.isEventTime())
                && (element.getTimestamp() + allowedLateness
                        <= internalTimerService.currentWatermark());
    }

    // -------------------------------------------------------------------------
    //                             Window State Store
    // -------------------------------------------------------------------------

    private class WindowStateStore {
        @SuppressWarnings("unchecked")
        public <T> Optional<ListState<T>> getWindowState(
                StateDeclaration.ListStateDeclaration stateDeclaration, W namespace) {
            if (!windowFunction.useWindowStates().contains(stateDeclaration)) {
                return Optional.empty();
            }

            ListStateDescriptor<T> listStateDescriptor =
                    StateDeclarationConverter.getListStateDescriptor(
                            (ListStateDeclarationImpl<T>) stateDeclaration);

            StateDeclaration.RedistributionMode redistributionMode =
                    stateDeclaration.getRedistributionMode();
            if (redistributionMode == StateDeclaration.RedistributionMode.NONE) {
                try {
                    return Optional.ofNullable(
                            WindowProcessOperator.this.getPartitionedState(
                                    namespace, windowSerializer, listStateDescriptor));
                } catch (Exception e) {
                    return Optional.empty();
                }
            } else {
                throw new UnsupportedOperationException(
                        "RedistributionMode "
                                + redistributionMode.name()
                                + " is not supported for window state.");
            }
        }

        public <KEY, V> Optional<MapState<KEY, V>> getWindowState(
                StateDeclaration.MapStateDeclaration stateDeclaration, W namespace) {
            // TODO impl
            return null;
        }

        public <T> Optional<ValueState<T>> getWindowState(
                StateDeclaration.ValueStateDeclaration stateDeclaration, W namespace) {
            // TODO impl;
            return null;
        }
    }

    // -------------------------------------------------------------------------
    //                             Trigger Context
    // -------------------------------------------------------------------------

    /**
     * {@code Context} is a utility for handling {@link Trigger} invocations. It can be reused by
     * setting the {@code key} and {@code window} fields. No internal state must be kept in the
     * {@code Context}
     */
    private class WindowTriggerContext implements Trigger.OnMergeContext {
        protected K key;
        protected W window;

        protected Collection<W> mergedWindows;

        public WindowTriggerContext(K key, W window) {
            this.key = key;
            this.window = window;
        }

        @Override
        public void mergeWindowState(StateDeclaration stateDeclaration) {
            if (mergedWindows != null && mergedWindows.size() > 0) {
                try {
                    State rawState =
                            getKeyedStateBackend()
                                    .getOrCreateKeyedState(
                                            windowSerializer,
                                            StateDeclarationConverter.getStateDescriptor(
                                                    stateDeclaration));

                    if (rawState instanceof InternalMergingState) {
                        @SuppressWarnings("unchecked")
                        InternalMergingState<K, W, ?, ?, ?> mergingState =
                                (InternalMergingState<K, W, ?, ?, ?>) rawState;
                        mergingState.mergeNamespaces(window, mergedWindows);
                    } else {
                        throw new IllegalArgumentException(
                                "The given state declaration does not refer to a mergeable state (MergingState)");
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Error while merging state.", e);
                }
            }
        }

        @Override
        public MetricGroup getMetricGroup() {
            return WindowProcessOperator.this.getMetricGroup();
        }

        public long getCurrentWatermark() {
            return internalTimerService.currentWatermark();
        }

        @Override
        public long getCurrentProcessingTime() {
            return internalTimerService.currentProcessingTime();
        }

        @Override
        public void registerProcessingTimeTimer(long time) {
            internalTimerService.registerProcessingTimeTimer(window, time);
        }

        @Override
        public void registerWatermarkListener(ProcessWatermark<?> watermark) {
            if (watermark instanceof EventTimestampWatermark) {
                internalTimerService.registerEventTimeTimer(
                        window, ((EventTimestampWatermark) watermark).getTimestamp());
            }
            // TODO handle generalized watermark.
        }

        public void deleteProcessingTimeTimer(long time) {
            internalTimerService.deleteProcessingTimeTimer(window, time);
        }

        @Override
        public void deleteWatermarkListener(ProcessWatermark<?> watermark) {
            if (watermark instanceof EventTimestampWatermark) {
                internalTimerService.deleteProcessingTimeTimer(
                        window, ((EventTimestampWatermark) watermark).getTimestamp());
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> Optional<ListState<T>> getWindowState(
                StateDeclaration.ListStateDeclaration stateDeclaration) {
            return windowStateStore.getWindowState(stateDeclaration, window);
        }

        @Override
        public <KEY, V> Optional<MapState<KEY, V>> getWindowState(
                StateDeclaration.MapStateDeclaration stateDeclaration) {
            return windowStateStore.getWindowState(stateDeclaration, window);
        }

        @Override
        public <T> Optional<ValueState<T>> getWindowState(
                StateDeclaration.ValueStateDeclaration stateDeclaration) {
            return windowStateStore.getWindowState(stateDeclaration, window);
        }

        public TriggerResult onElement(StreamRecord<IN> element) throws Exception {
            return trigger.onElement(element.getValue(), element.getTimestamp(), window, this);
        }

        public TriggerResult onProcessingTime(long time) throws Exception {
            return trigger.onProcessingTime(time, window, this);
        }

        public TriggerResult onWatermark(ProcessWatermark<?> watermark) throws Exception {
            return trigger.onWatermark(watermark, window, this);
        }

        public void onMerge(W mergeResult, Collection<W> mergedWindows) throws Exception {
            this.mergedWindows = mergedWindows;
            trigger.onMerge(window, this);
        }

        public void clear() throws Exception {
            trigger.clear(window, this);
        }

        @Override
        public String toString() {
            return "Context{" + "key=" + key + ", window=" + window + '}';
        }
    }

    // -------------------------------------------------------------------------
    //                          Window Assigner Context
    // -------------------------------------------------------------------------
    private class InternalWindowAssignerContext extends WindowAssigner.WindowAssignerContext {
        private ProcessWatermark<?> processWatermark;

        @Override
        public long getCurrentProcessingTime() {
            return internalTimerService.currentProcessingTime();
        }

        @Override
        public ProcessWatermark<?> getCurrentWatermark() {
            return processWatermark;
        }
    };

    // -------------------------------------------------------------------------
    //                             Window Context
    // -------------------------------------------------------------------------
    public class WindowFunctionContext implements WindowProcessFunction.WindowContext<W> {
        private W window;

        @Override
        public W window() {
            return window;
        }

        @Override
        public <T> Optional<ListState<T>> getWindowState(
                StateDeclaration.ListStateDeclaration stateDeclaration) throws Exception {
            return windowStateStore.getWindowState(stateDeclaration, window);
        }

        @Override
        public <T> Optional<ValueState<T>> getWindowState(
                StateDeclaration.ValueStateDeclaration stateDeclaration) throws Exception {
            return windowStateStore.getWindowState(stateDeclaration, window);
        }

        @Override
        public <KEY, V> Optional<MapState<KEY, V>> getWindowState(
                StateDeclaration.MapStateDeclaration stateDeclaration) throws Exception {
            return windowStateStore.getWindowState(stateDeclaration, window);
        }
    }
}
