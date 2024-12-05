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

package org.apache.flink.datastream.impl.operators;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.ProcessingTimeManager;
import org.apache.flink.datastream.api.context.TwoOutputNonPartitionedContext;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeManager;
import org.apache.flink.datastream.api.extension.eventtime.TwoOutputEventTimerCallback;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.impl.common.KeyCheckedOutputCollector;
import org.apache.flink.datastream.impl.common.OutputCollector;
import org.apache.flink.datastream.impl.common.TimestampCollector;
import org.apache.flink.datastream.impl.context.DefaultProcessingTimeManager;
import org.apache.flink.datastream.impl.context.DefaultTwoOutputNonPartitionedContext;
import org.apache.flink.datastream.impl.context.TwoOutputEventTimeManager;
import org.apache.flink.datastream.impl.operators.extension.eventtime.WithEventTimeExtension;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.v2.MapStateDescriptor;
import org.apache.flink.runtime.state.v2.adaptor.MapStateAdaptor;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkState;

/** */
public class KeyedTwoOutputProcessOperator<KEY, IN, OUT_MAIN, OUT_SIDE>
        extends TwoOutputProcessOperator<IN, OUT_MAIN, OUT_SIDE>
        implements Triggerable<KEY, VoidNamespace>, WithEventTimeExtension {
    private transient InternalTimerService<VoidNamespace> timerService;

    // TODO Restore this keySet when task initialized from checkpoint.
    private transient Set<Object> keySet;

    @Nullable private final KeySelector<OUT_MAIN, KEY> mainOutKeySelector;

    @Nullable private final KeySelector<OUT_SIDE, KEY> sideOutKeySelector;

    // -------------------- Event Time Extension --------------------------------
    /**
     * The fields below are only created when event time is enabled. Currently, this occurs only
     * when the {@link EventTimeExtension#getEventTimeManager(NonPartitionedContext)} method enables
     * the event time extension for a specific operator.
     */
    private boolean eventTimeExtensionEnable = false;

    // used to store user-defined event timer callback
    private transient MapStateAdaptor<KEY, VoidNamespace, Long, TwoOutputEventTimerCallback>
            eventTimerCallbackMapState;

    private transient EventTimeManager eventTimeManager;

    public KeyedTwoOutputProcessOperator(
            TwoOutputStreamProcessFunction<IN, OUT_MAIN, OUT_SIDE> userFunction,
            OutputTag<OUT_SIDE> outputTag) {
        this(userFunction, outputTag, null, null);
    }

    public KeyedTwoOutputProcessOperator(
            TwoOutputStreamProcessFunction<IN, OUT_MAIN, OUT_SIDE> userFunction,
            OutputTag<OUT_SIDE> outputTag,
            @Nullable KeySelector<OUT_MAIN, KEY> mainOutKeySelector,
            @Nullable KeySelector<OUT_SIDE, KEY> sideOutKeySelector) {
        super(userFunction, outputTag);
        Preconditions.checkArgument(
                (mainOutKeySelector == null && sideOutKeySelector == null)
                        || (mainOutKeySelector != null && sideOutKeySelector != null),
                "Both mainOutKeySelector and sideOutKeySelector must be null or not null.");
        this.mainOutKeySelector = mainOutKeySelector;
        this.sideOutKeySelector = sideOutKeySelector;
    }

    @Override
    public void open() throws Exception {
        this.timerService =
                getInternalTimerService("processing timer", VoidNamespaceSerializer.INSTANCE, this);
        this.keySet = new HashSet<>();
        super.open();
    }

    @Override
    protected TimestampCollector<OUT_MAIN> getMainCollector() {
        return mainOutKeySelector != null && sideOutKeySelector != null
                ? new KeyCheckedOutputCollector<>(
                        new OutputCollector<>(output),
                        mainOutKeySelector,
                        () -> (KEY) getCurrentKey())
                : new OutputCollector<>(output);
    }

    @Override
    public TimestampCollector<OUT_SIDE> getSideCollector() {
        return mainOutKeySelector != null && sideOutKeySelector != null
                ? new KeyCheckedOutputCollector<>(
                        new SideOutputCollector(output),
                        sideOutKeySelector,
                        () -> (KEY) getCurrentKey())
                : new SideOutputCollector(output);
    }

    @Override
    protected Object currentKey() {
        return getCurrentKey();
    }

    protected ProcessingTimeManager getProcessingTimeManager() {
        return new DefaultProcessingTimeManager(timerService);
    }

    @Override
    public void onEventTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
        checkState(eventTimeExtensionEnable);

        TwoOutputEventTimerCallback eventTimerCallback =
                eventTimerCallbackMapState.get(timer.getTimestamp());
        if (eventTimerCallback != null) {
            eventTimerCallback.onEventTimer(
                    timer.getTimestamp(),
                    getMainCollector(),
                    getSideCollector(),
                    partitionedContext);
            eventTimerCallbackMapState.remove(timer.getTimestamp());
        }
    }

    @Override
    public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
        userFunction.onProcessingTimer(
                timer.getTimestamp(), getMainCollector(), getSideCollector(), partitionedContext);
    }

    @Override
    protected TwoOutputNonPartitionedContext<OUT_MAIN, OUT_SIDE> getNonPartitionedContext() {
        return new DefaultTwoOutputNonPartitionedContext<>(
                this,
                context,
                partitionedContext,
                mainCollector,
                sideCollector,
                true,
                keySet,
                output,
                watermarkDeclarationMap);
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    public void setKeyContextElement1(StreamRecord record) throws Exception {
        super.setKeyContextElement1(record);
        keySet.add(getCurrentKey());
    }

    @Override
    public boolean isAsyncStateProcessingEnabled() {
        return true;
    }

    @Override
    public void initEventTimeExtension() throws Exception {
        MapStateDescriptor<Long, TwoOutputEventTimerCallback> eventTimerCallbackMapStateDescriptor =
                new MapStateDescriptor<>(
                        "event-timer-state",
                        Types.LONG,
                        TypeInformation.of(TwoOutputEventTimerCallback.class));

        eventTimerCallbackMapState =
                getOrCreateKeyedState(
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        eventTimerCallbackMapStateDescriptor);

        eventTimeManager = new TwoOutputEventTimeManager(timerService, eventTimerCallbackMapState);

        eventTimeExtensionEnable = true;
    }

    @Override
    public EventTimeManager getEventTimeManager() {
        checkState(eventTimeExtensionEnable);
        return eventTimeManager;
    }
}
