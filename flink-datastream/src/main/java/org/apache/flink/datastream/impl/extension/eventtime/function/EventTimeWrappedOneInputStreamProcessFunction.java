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
import org.apache.flink.datastream.api.extension.eventtime.timer.OneInputEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.eventtime.InternalEventTimeUtils;
import org.apache.flink.datastream.impl.extension.eventtime.timer.DefaultEventTimeManager;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.watermark.extension.eventtime.EventTimeWatermarkHandler;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Set;

/**
 * The wrapped {@link OneInputEventTimeStreamProcessFunction} that take care of event-time alignment
 * with idleness.
 */
public class EventTimeWrappedOneInputStreamProcessFunction<IN, OUT>
        implements OneInputStreamProcessFunction<IN, OUT> {

    private final OneInputEventTimeStreamProcessFunction<IN, OUT> wrappedUserFunction;

    private EventTimeManager eventTimeManager;

    protected transient EventTimeWatermarkHandler eventTimeWatermarkHandler;

    public EventTimeWrappedOneInputStreamProcessFunction(
            OneInputEventTimeStreamProcessFunction<IN, OUT> wrappedUserFunction) {
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

        eventTimeWatermarkHandler = new EventTimeWatermarkHandler(1, output, timeServiceManager);
    }

    @Override
    public void processRecord(IN record, Collector<OUT> output, PartitionedContext ctx)
            throws Exception {
        wrappedUserFunction.processRecord(record, output, ctx);
    }

    @Override
    public void endInput(NonPartitionedContext<OUT> ctx) {
        wrappedUserFunction.endInput(ctx);
    }

    @Override
    public void onProcessingTimer(long timestamp, Collector<OUT> output, PartitionedContext ctx) {
        wrappedUserFunction.onProcessingTimer(timestamp, output, ctx);
    }

    @Override
    public WatermarkHandlingResult onWatermark(
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
            return wrappedUserFunction.onWatermark(watermark, output, ctx);
        }
    }

    public void onEventTime(long timestamp, Collector<OUT> output, PartitionedContext ctx) {
        wrappedUserFunction.onEventTimer(timestamp, output, ctx);
    }

    @Override
    public Set<StateDeclaration> usesStates() {
        return wrappedUserFunction.usesStates();
    }

    @Override
    public Collection<? extends WatermarkDeclaration> watermarkDeclarations() {
        return wrappedUserFunction.watermarkDeclarations();
    }

    @Override
    public void close() throws Exception {
        wrappedUserFunction.close();
    }

    public OneInputStreamProcessFunction<IN, OUT> getWrappedUserFunction() {
        return wrappedUserFunction;
    }
}
