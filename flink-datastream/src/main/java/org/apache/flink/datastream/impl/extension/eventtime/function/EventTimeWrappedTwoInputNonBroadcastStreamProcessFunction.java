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

package org.apache.flink.datastream.impl.extension.eventtime.function;

import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkHandlingResult;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.timer.EventTimeManager;
import org.apache.flink.datastream.api.extension.eventtime.timer.TwoInputNonBroadcastEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.eventtime.InternalEventTimeUtils;
import org.apache.flink.datastream.impl.extension.eventtime.timer.DefaultEventTimeManager;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.watermark.extension.eventtime.EventTimeWatermarkHandler;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Set;

/**
 * The wrapped {@link TwoInputNonBroadcastEventTimeStreamProcessFunction} that take care of
 * event-time alignment with idleness.
 */
public class EventTimeWrappedTwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT>
        implements TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> {
    private final TwoInputNonBroadcastEventTimeStreamProcessFunction<IN1, IN2, OUT>
            wrappedUserFunction;

    private EventTimeManager eventTimeManager;

    protected transient EventTimeWatermarkHandler eventTimeWatermarkHandler;

    public EventTimeWrappedTwoInputNonBroadcastStreamProcessFunction(
            TwoInputNonBroadcastEventTimeStreamProcessFunction<IN1, IN2, OUT> wrappedUserFunction) {
        this.wrappedUserFunction = Preconditions.checkNotNull(wrappedUserFunction);
    }

    @Override
    public void open(NonPartitionedContext<OUT> ctx) throws Exception {
        wrappedUserFunction.initEventTimeProcessFunction(eventTimeManager);
        wrappedUserFunction.open(ctx);
    }

    // This method have to invoke before open
    public void initEventTimeExtension(
            EventTimeManager eventTimeManager,
            Output<?> output,
            InternalTimeServiceManager<?> timeServiceManager) {
        this.eventTimeManager = eventTimeManager;
        ((DefaultEventTimeManager) this.eventTimeManager).setCanRegisterTimer(true);

        eventTimeWatermarkHandler = new EventTimeWatermarkHandler(2, output, timeServiceManager);
    }

    @Override
    public void processRecordFromFirstInput(
            IN1 record, Collector<OUT> output, PartitionedContext ctx) throws Exception {
        wrappedUserFunction.processRecordFromFirstInput(record, output, ctx);
    }

    @Override
    public void processRecordFromSecondInput(
            IN2 record, Collector<OUT> output, PartitionedContext ctx) throws Exception {
        wrappedUserFunction.processRecordFromSecondInput(record, output, ctx);
    }

    @Override
    public void endFirstInput(NonPartitionedContext<OUT> ctx) {
        wrappedUserFunction.endFirstInput(ctx);
    }

    @Override
    public void endSecondInput(NonPartitionedContext<OUT> ctx) {
        wrappedUserFunction.endSecondInput(ctx);
    }

    @Override
    public void onProcessingTimer(long timestamp, Collector<OUT> output, PartitionedContext ctx) {
        wrappedUserFunction.onProcessingTimer(timestamp, output, ctx);
    }

    @Override
    public WatermarkHandlingResult onWatermarkFromFirstInput(
            Watermark watermark, Collector<OUT> output, NonPartitionedContext<OUT> ctx)
            throws Exception {
        if (EventTimeExtension.isEventTimeWatermark(watermark)
                || EventTimeExtension.isIdleStatusWatermark(watermark)) {
            EventTimeWatermarkHandler.EventTimeUpdateStatus eventTimeUpdateStatus =
                    InternalEventTimeUtils.processWatermark(
                            watermark, 0, eventTimeWatermarkHandler);
            if (eventTimeUpdateStatus.isEventTimeUpdated()) {
                wrappedUserFunction.onEventTimeWatermark(
                        eventTimeUpdateStatus.getNewEventTime(), output, ctx);
            }
            return WatermarkHandlingResult.POLL;
        } else {
            return wrappedUserFunction.onWatermarkFromFirstInput(watermark, output, ctx);
        }
    }

    @Override
    public WatermarkHandlingResult onWatermarkFromSecondInput(
            Watermark watermark, Collector<OUT> output, NonPartitionedContext<OUT> ctx)
            throws Exception {
        if (EventTimeExtension.isEventTimeWatermark(watermark)
                || EventTimeExtension.isIdleStatusWatermark(watermark)) {
            EventTimeWatermarkHandler.EventTimeUpdateStatus eventTimeUpdateStatus =
                    InternalEventTimeUtils.processWatermark(
                            watermark, 1, eventTimeWatermarkHandler);
            if (eventTimeUpdateStatus.isEventTimeUpdated()) {
                wrappedUserFunction.onEventTimeWatermark(
                        eventTimeUpdateStatus.getNewEventTime(), output, ctx);
            }
            return WatermarkHandlingResult.POLL;
        } else {
            return wrappedUserFunction.onWatermarkFromSecondInput(watermark, output, ctx);
        }
    }

    public void onEventTime(long timestamp, Collector<OUT> output, PartitionedContext ctx) {
        wrappedUserFunction.onEventTimer(timestamp, output, ctx);
    }

    @Override
    public void close() throws Exception {
        wrappedUserFunction.close();
    }

    @Override
    public Set<StateDeclaration> usesStates() {
        return wrappedUserFunction.usesStates();
    }

    @Override
    public Collection<? extends WatermarkDeclaration> watermarkDeclarations() {
        return wrappedUserFunction.watermarkDeclarations();
    }

    public TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> getWrappedUserFunction() {
        return wrappedUserFunction;
    }
}
