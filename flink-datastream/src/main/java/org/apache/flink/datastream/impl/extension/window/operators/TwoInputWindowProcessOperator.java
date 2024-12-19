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

package org.apache.flink.datastream.impl.extension.window.operators;

import org.apache.flink.api.common.state.ListStateDeclaration;
import org.apache.flink.api.common.state.MapStateDeclaration;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.state.v2.AppendingState;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeinfo.TypeSerializer;
import org.apache.flink.datastream.api.extension.window.TwoInputWindowProcessFunction;
import org.apache.flink.datastream.api.extension.window.assigner.WindowAssigner;
import org.apache.flink.datastream.api.extension.window.trigger.Trigger;
import org.apache.flink.datastream.api.extension.window.utils.TaggedUnion;
import org.apache.flink.datastream.api.extension.window.window.Window;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.impl.extension.window.ParallelismAwareKeySelector;
import org.apache.flink.datastream.impl.extension.window.function.InternalTwoInputWindowFunction;
import org.apache.flink.datastream.impl.extension.window.utils.WindowStateStore;
import org.apache.flink.datastream.impl.extension.window.window.BoundedWindow;
import org.apache.flink.datastream.impl.extension.window.window.TimeWindowImpl;
import org.apache.flink.datastream.impl.operators.BaseKeyedTwoInputNonBroadcastProcessOperator;
import org.apache.flink.datastream.impl.utils.StateDeclarationConverter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.internal.InternalMergingState;
import org.apache.flink.runtime.state.v2.StateDescriptor;
import org.apache.flink.runtime.state.v2.internal.InternalPartitionedState;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Optional;

/** Operator for {@link TwoInputWindowProcessFunction} in {@link KeyedPartitionStream}. */
public class TwoInputWindowProcessOperator<K, IN1, IN2, ACC1, ACC2, OUT, W extends Window>
        extends BaseKeyedTwoInputNonBroadcastProcessOperator<K, IN1, IN2, OUT>
        implements Triggerable<K, W> {

    protected static final Logger LOG =
            LoggerFactory.getLogger(TwoInputWindowProcessOperator.class);

    private static final long serialVersionUID = 1L;

    // ------------------------------------------------------------------------
    // Configuration values and user functions
    // ------------------------------------------------------------------------

    protected final WindowAssigner<? super TaggedUnion<IN1, IN2>, W> windowAssigner;

    private final Trigger<? super TaggedUnion<IN1, IN2>, ? super W> trigger;

    private final StateDescriptor<? extends AppendingState<IN1, ACC1, ACC1>>
            leftWindowStateDescriptor;

    private final StateDescriptor<? extends AppendingState<IN2, ACC2, ACC2>>
            rightWindowStateDescriptor;

    /** For serializing the window in checkpoints. */
    protected final TypeSerializer<W> windowSerializer;

    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";

    protected transient Counter numLateRecordsDropped;

    // ------------------------------------------------------------------------
    // State that is not checkpointed
    // ------------------------------------------------------------------------

    /** The state in which the window contents is stored. Each window is a namespace */
    private transient AppendingState<IN1, ACC1, ACC1> leftWindowState;

    /** The state in which the window contents is stored. Each window is a namespace */
    private transient AppendingState<IN2, ACC2, ACC2> rightWindowState;

    private transient WindowStateStore windowStateStore;

    private final ACC1 acc1InitValue;

    private final ACC2 acc2InitValue;

    // ------------------------------------------------------------------------
    //                                Context
    // ------------------------------------------------------------------------

    protected transient WindowTriggerContext triggerContext;

    protected transient WindowFunctionContext windowFunctionContext;

    protected transient InternalWindowAssignerContext windowAssignerContext;

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

    protected final OutputTag<IN1> leftLateOutputTag;

    protected final OutputTag<IN2> rightLateOutputTag;

    // ------------------------------------------------------------------------
    // State that needs to be checkpointed
    // ------------------------------------------------------------------------

    protected transient InternalTimerService<W> internalTimerService;

    private final TwoInputWindowProcessFunction<ACC1, ACC2, OUT, W> windowProcessFunction;

    public TwoInputWindowProcessOperator(
            InternalTwoInputWindowFunction<IN1, IN2, ACC1, ACC2, OUT, W> windowFunction,
            WindowAssigner<? super TaggedUnion<IN1, IN2>, W> windowAssigner,
            Trigger<? super TaggedUnion<IN1, IN2>, ? super W> trigger,
            TypeSerializer<W> windowSerializer,
            StateDescriptor<? extends AppendingState<IN1, ACC1, ACC1>> windowStateDescriptor1,
            StateDescriptor<? extends AppendingState<IN2, ACC2, ACC2>> windowStateDescriptor2,
            long allowedLateness,
            OutputTag<IN1> leftLateOutputTag,
            OutputTag<IN2> rightLateOutputTag,
            ACC1 acc1InitValue,
            ACC2 acc2InitValue) {
        super(windowFunction);
        this.windowProcessFunction = windowFunction.getWindowProcessFunction();
        this.windowAssigner = windowAssigner;
        this.trigger = trigger;
        this.windowSerializer = windowSerializer;
        this.leftWindowStateDescriptor = windowStateDescriptor1;
        this.rightWindowStateDescriptor = windowStateDescriptor2;
        this.allowedLateness = allowedLateness;
        this.leftLateOutputTag = leftLateOutputTag;
        this.rightLateOutputTag = rightLateOutputTag;
        this.acc1InitValue = acc1InitValue;
        this.acc2InitValue = acc2InitValue;

        // TODO:
        //        setChainingStrategy(ChainingStrategy.ALWAYS);
    }

    @Override
    public void open() throws Exception {
        super.open();

        // inject parallelism for ParallelismAwareKeySelector.
        if (getStateKeySelector1() instanceof ParallelismAwareKeySelector) {
            ((ParallelismAwareKeySelector<?>) getStateKeySelector1())
                    .setParallelismSupplier(
                            () -> getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
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
        if (leftWindowStateDescriptor != null) {
            leftWindowState =
                    (AppendingState<IN1, ACC1, ACC1>)
                            getOrCreateKeyedState(
                                    windowSerializer.createInstance(),
                                    windowSerializer,
                                    leftWindowStateDescriptor);
        }

        if (rightWindowStateDescriptor != null) {
            rightWindowState =
                    (AppendingState<IN2, ACC2, ACC2>)
                            getOrCreateKeyedState(
                                    windowSerializer.createInstance(),
                                    windowSerializer,
                                    rightWindowStateDescriptor);
        }
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        final Collection<W> elementWindows =
                windowAssigner.assignWindows(
                        TaggedUnion.one(element.getValue()),
                        element.getTimestamp(),
                        windowAssignerContext);

        // if element is handled by none of assigned elementWindows
        boolean isSkippedElement = true;

        final K key = (K) this.getCurrentKey();

        for (W window : elementWindows) {
            // drop if the window is already late
            if (isWindowLate(window)) {
                continue;
            }
            isSkippedElement = false;

            ((InternalPartitionedState<W>) leftWindowState).setCurrentNamespace(window);
            leftWindowState.add(element.getValue());

            triggerContext.key = key;
            triggerContext.window = window;

            Trigger.TriggerResult triggerResult = triggerContext.onElement1(element);

            if (triggerResult.isFire()) {
                ACC1 leftContent = leftWindowState.get();
                ACC2 rightContent = rightWindowState.get();
                if (leftContent == null && rightContent == null) {
                    continue;
                }
                if (leftContent == null) {
                    leftContent = acc1InitValue;
                }
                if (rightContent == null) {
                    rightContent = acc2InitValue;
                }
                emitWindowContents(window, leftContent, rightContent);
            }

            if (triggerResult.isPurge()) {
                leftWindowState.clear();
                rightWindowState.clear();
            }
            if (window instanceof BoundedWindow) {
                ((BoundedWindow<?>) window).registerCleaner(triggerContext);
            }
        }

        // side output input event if element not handled by any window late arriving tag has been
        // set windowAssigner is event time and current timestamp + allowed lateness no less than
        // element timestamp.
        if (isSkippedElement && isElementLate(element)) {
            if (leftLateOutputTag != null) {
                leftSideOutput(element);
            } else {
                this.numLateRecordsDropped.inc();
            }
        }
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        final Collection<W> elementWindows =
                windowAssigner.assignWindows(
                        TaggedUnion.two(element.getValue()),
                        element.getTimestamp(),
                        windowAssignerContext);

        // if element is handled by none of assigned elementWindows
        boolean isSkippedElement = true;

        final K key = (K) this.getCurrentKey();

        for (W window : elementWindows) {
            // drop if the window is already late
            if (isWindowLate(window)) {
                continue;
            }
            isSkippedElement = false;

            ((InternalPartitionedState<W>) rightWindowState).setCurrentNamespace(window);
            rightWindowState.add(element.getValue());

            triggerContext.key = key;
            triggerContext.window = window;

            Trigger.TriggerResult triggerResult = triggerContext.onElement2(element);

            if (triggerResult.isFire()) {
                ACC1 leftContent = leftWindowState.get();
                ACC2 rightContent = rightWindowState.get();
                if (leftContent == null && rightContent == null) {
                    continue;
                }
                if (leftContent == null) {
                    leftContent = acc1InitValue;
                }
                if (rightContent == null) {
                    rightContent = acc2InitValue;
                }
                emitWindowContents(window, leftContent, rightContent);
            }

            if (triggerResult.isPurge()) {
                leftWindowState.clear();
                rightWindowState.clear();
            }
            if (window instanceof BoundedWindow) {
                ((BoundedWindow<?>) window).registerCleaner(triggerContext);
            }
        }

        // side output input event if element not handled by any window late arriving tag has been
        // set windowAssigner is event time and current timestamp + allowed lateness no less than
        // element timestamp.
        if (isSkippedElement && isElementLate(element)) {
            if (rightLateOutputTag != null) {
                rightSideOutput(element);
            } else {
                this.numLateRecordsDropped.inc();
            }
        }
    }

    @Override
    public void onEventTime(InternalTimer<K, W> timer) throws Exception {
        triggerContext.key = timer.getKey();
        triggerContext.window = timer.getNamespace();

        ((InternalPartitionedState<W>) leftWindowState).setCurrentNamespace(triggerContext.window);
        ((InternalPartitionedState<W>) rightWindowState).setCurrentNamespace(triggerContext.window);

        Trigger.TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());

        if (triggerResult.isFire()) {
            ACC1 leftContent = leftWindowState.get();
            ACC2 rightContent = rightWindowState.get();
            if (leftContent != null || rightContent != null) {
                if (leftContent == null) {
                    leftContent = acc1InitValue;
                }
                if (rightContent == null) {
                    rightContent = acc2InitValue;
                }
                emitWindowContents(triggerContext.window, leftContent, rightContent);
            }
        }

        if (triggerResult.isPurge()) {
            leftWindowState.clear();
            rightWindowState.clear();
        }

        // TODO this can be removed in the future
        if (windowAssigner.isEventTime() && triggerContext.window instanceof TimeWindowImpl) {
            windowAssignerContext.currentEventTime = timer.getTimestamp();
            if (((TimeWindowImpl) triggerContext.window).isBoundaryReached(windowAssignerContext)) {
                clearAllState(triggerContext.window, leftWindowState, rightWindowState);
            }
        }
    }

    @Override
    public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
        triggerContext.key = timer.getKey();
        triggerContext.window = timer.getNamespace();

        ((InternalPartitionedState<W>) leftWindowState).setCurrentNamespace(triggerContext.window);
        ((InternalPartitionedState<W>) rightWindowState).setCurrentNamespace(triggerContext.window);

        Trigger.TriggerResult triggerResult = triggerContext.onProcessingTime(timer.getTimestamp());

        if (triggerResult.isFire()) {
            ACC1 leftContent = leftWindowState.get();
            ACC2 rightContent = rightWindowState.get();
            if (leftContent != null || rightContent != null) {
                if (leftContent == null) {
                    leftContent = acc1InitValue;
                }
                if (rightContent == null) {
                    rightContent = acc2InitValue;
                }
                emitWindowContents(triggerContext.window, leftContent, rightContent);
            }
        }

        if (triggerResult.isPurge()) {
            leftWindowState.clear();
            rightWindowState.clear();
        }

        if (!windowAssigner.isEventTime() && triggerContext.window instanceof TimeWindowImpl) {
            if (((TimeWindowImpl) triggerContext.window).isBoundaryReached(windowAssignerContext)) {
                clearAllState(triggerContext.window, leftWindowState, rightWindowState);
            }
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
            W window,
            AppendingState<IN1, ACC1, ACC1> leftState,
            AppendingState<IN2, ACC2, ACC2> rightState)
            throws Exception {
        leftState.clear();
        rightState.clear();
        triggerContext.clear();
        windowFunctionContext.window = window;
        windowProcessFunction.endOfWindow(window);
    }

    /** Emits the contents of the given window using the {@link InternalTwoInputWindowFunction}. */
    @SuppressWarnings("unchecked")
    private void emitWindowContents(W window, ACC1 leftContents, ACC2 rightContents)
            throws Exception {
        // only time window touch the time concept.
        if (window instanceof TimeWindowImpl) {
            collector.setTimestamp(((TimeWindowImpl) window).maxTimeStamp());
        }
        windowFunctionContext.window = window;
        windowProcessFunction.processRecord(
                leftContents, rightContents, collector, partitionedContext, windowFunctionContext);
    }

    /**
     * Write skipped late arriving element to SideOutput.
     *
     * @param element skipped late arriving element to side output
     */
    protected void leftSideOutput(StreamRecord<IN1> element) {
        output.collect(leftLateOutputTag, element);
    }

    protected void rightSideOutput(StreamRecord<IN2> element) {
        output.collect(rightLateOutputTag, element);
    }

    /**
     * Returns {@code true} if the watermark is after the end timestamp plus the allowed lateness of
     * the given window.
     */
    // TODO Provide a common later strategy.
    protected boolean isWindowLate(W window) {
        if (window instanceof TimeWindowImpl) {
            return ((TimeWindowImpl) window).isWindowLate(internalTimerService.currentWatermark());
        }
        return false;
    }

    /**
     * Decide if a record is currently late, based on current watermark and allowed lateness.
     *
     * @param element The element to check
     * @return The element for which should be considered when sideoutputs
     */
    protected boolean isElementLate(StreamRecord<?> element) {
        return (windowAssigner.isEventTime())
                && (element.getTimestamp() + allowedLateness
                        <= internalTimerService.currentWatermark());
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
            return TwoInputWindowProcessOperator.this.getMetricGroup();
        }

        public long getCurrentEventTime() {
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
        public void registerEventTimeListener(long time) {
            internalTimerService.registerEventTimeTimer(window, time);
            // TODO handle generalized watermark.
        }

        public void deleteProcessingTimeTimer(long time) {
            internalTimerService.deleteProcessingTimeTimer(window, time);
        }

        @Override
        public void deleteEventTimeListener(long time) {
            internalTimerService.deleteProcessingTimeTimer(window, time);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> Optional<ListState<T>> getWindowState(ListStateDeclaration<T> stateDeclaration) {
            return windowStateStore.getWindowState(stateDeclaration, window);
        }

        @Override
        public <KEY, V> Optional<MapState<KEY, V>> getWindowState(
                MapStateDeclaration<KEY, V> stateDeclaration) {
            return windowStateStore.getWindowState(stateDeclaration, window);
        }

        @Override
        public <T> Optional<ValueState<T>> getWindowState(
                ValueStateDeclaration<T> stateDeclaration) {
            return windowStateStore.getWindowState(stateDeclaration, window);
        }

        public Trigger.TriggerResult onElement1(StreamRecord<IN1> element) throws Exception {
            return trigger.onElement(
                    TaggedUnion.one(element.getValue()), element.getTimestamp(), window, this);
        }

        public Trigger.TriggerResult onElement2(StreamRecord<IN2> element) throws Exception {
            return trigger.onElement(
                    TaggedUnion.two(element.getValue()), element.getTimestamp(), window, this);
        }

        public Trigger.TriggerResult onProcessingTime(long time) throws Exception {
            return trigger.onProcessingTime(time, window, this);
        }

        public Trigger.TriggerResult onEventTime(long time) throws Exception {
            return trigger.onEventTime(time, window, this);
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
        private long currentEventTime;

        @Override
        public long getCurrentProcessingTime() {
            return internalTimerService.currentProcessingTime();
        }

        @Override
        public long getCurrentEventTime() {
            return currentEventTime;
        }
    }

    // -------------------------------------------------------------------------
    //                             Window Context
    // -------------------------------------------------------------------------
    public class WindowFunctionContext implements TwoInputWindowProcessFunction.WindowContext<W> {
        private W window;

        @Override
        public W window() {
            return window;
        }

        @Override
        public <T> Optional<ListState<T>> getWindowState(ListStateDeclaration<T> stateDeclaration)
                throws Exception {
            return windowStateStore.getWindowState(stateDeclaration, window);
        }

        @Override
        public <KEY, V> Optional<MapState<KEY, V>> getWindowState(
                MapStateDeclaration<KEY, V> stateDeclaration) throws Exception {
            return windowStateStore.getWindowState(stateDeclaration, window);
        }

        @Override
        public <T> Optional<ValueState<T>> getWindowState(ValueStateDeclaration<T> stateDeclaration)
                throws Exception {
            return windowStateStore.getWindowState(stateDeclaration, window);
        }
    }
}
