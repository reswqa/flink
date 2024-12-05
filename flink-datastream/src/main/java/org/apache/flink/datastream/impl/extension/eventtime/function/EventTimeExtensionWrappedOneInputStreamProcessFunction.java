package org.apache.flink.datastream.impl.extension.eventtime.function;

import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkHandlingResult;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.timer.EventTimeManager;
import org.apache.flink.datastream.api.extension.eventtime.timer.EventTimeProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.timer.OneInputEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.eventtime.InternalEventTimeUtils;
import org.apache.flink.datastream.impl.extension.eventtime.timer.DefaultEventTimeManager;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.watermark.extension.eventtime.EventTimeWatermarkHandler;

import java.util.Collection;
import java.util.Set;

public class EventTimeExtensionWrappedOneInputStreamProcessFunction<IN, OUT>
        implements OneInputStreamProcessFunction<IN, OUT> {

    private final OneInputStreamProcessFunction<IN, OUT> wrappedUserFunction;

    private EventTimeManager eventTimeManager;

    protected transient EventTimeWatermarkHandler eventTimeWatermarkHandler;

    public EventTimeExtensionWrappedOneInputStreamProcessFunction(
            OneInputStreamProcessFunction<IN, OUT> wrappedUserFunction) {
        this.wrappedUserFunction = wrappedUserFunction;
    }

    @Override
    public void open(NonPartitionedContext<OUT> ctx) throws Exception {
        ((EventTimeProcessFunction) wrappedUserFunction).initEventTimeExtension(eventTimeManager);
        wrappedUserFunction.open(ctx);
    }

    // should be executed before open
    public void initEventTimeExtension(
            EventTimeManager eventTimeManager,
            Output<?> output,
            InternalTimeServiceManager<?> timeServiceManager) {
        this.eventTimeManager = eventTimeManager;
        if (wrappedUserFunction instanceof OneInputEventTimeStreamProcessFunction) {
            ((DefaultEventTimeManager) this.eventTimeManager).setCanRegisterTimer(true);
        }

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
        // TODO InternalEventTimeUtils.processWatermark then send it to user function
        if (EventTimeExtension.isEventTimeWatermark(watermark.getIdentifier())
                && (wrappedUserFunction instanceof OneInputEventTimeStreamProcessFunction)) {
            //
            ((OneInputEventTimeStreamProcessFunction<IN, OUT>) wrappedUserFunction)
                    .onEventTimeWatermark(((LongWatermark) watermark).getValue(), output, ctx);
        }

        if (InternalEventTimeUtils.processWatermark(watermark, 0, eventTimeWatermarkHandler)) {
            return WatermarkHandlingResult.PEEK;
        } else {
            return wrappedUserFunction.onWatermark(watermark, output, ctx);
        }
    }

    public void onEventTime(long timestamp, Collector<OUT> output, PartitionedContext ctx) {
        ((OneInputEventTimeStreamProcessFunction<IN, OUT>) wrappedUserFunction)
                .onEventTimer(timestamp, output, ctx);
    }

    @Override
    public Set<StateDeclaration> usesStates() {
        // TODO: may declare time service state
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
