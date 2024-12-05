package org.apache.flink.datastream.impl.extension.eventtime.function;

import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkHandlingResult;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.TwoOutputNonPartitionedContext;
import org.apache.flink.datastream.api.context.TwoOutputPartitionedContext;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.timer.EventTimeManager;
import org.apache.flink.datastream.api.extension.eventtime.timer.EventTimeProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.timer.TwoOutputEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.eventtime.InternalEventTimeUtils;
import org.apache.flink.datastream.impl.extension.eventtime.timer.DefaultEventTimeManager;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.watermark.extension.eventtime.EventTimeWatermarkHandler;

import java.util.Collection;
import java.util.Set;

public class EventTimeExtensionWrappedTwoOutputStreamProcessFunction<IN, OUT1, OUT2>
        implements TwoOutputStreamProcessFunction<IN, OUT1, OUT2> {

    private final TwoOutputStreamProcessFunction<IN, OUT1, OUT2> wrappedUserFunction;

    private EventTimeManager eventTimeManager;

    protected transient EventTimeWatermarkHandler eventTimeWatermarkHandler;

    public EventTimeExtensionWrappedTwoOutputStreamProcessFunction(
            TwoOutputStreamProcessFunction<IN, OUT1, OUT2> wrappedUserFunction) {
        this.wrappedUserFunction = wrappedUserFunction;
    }

    @Override
    public void open(TwoOutputNonPartitionedContext<OUT1, OUT2> ctx) throws Exception {
        ((EventTimeProcessFunction) wrappedUserFunction).initEventTimeExtension(eventTimeManager);
        wrappedUserFunction.open(ctx);
    }

    // should be executed before open
    public void initEventTimeExtension(
            EventTimeManager eventTimeManager,
            Output<?> output,
            InternalTimeServiceManager<?> timeServiceManager) {
        this.eventTimeManager = eventTimeManager;
        if (wrappedUserFunction instanceof TwoOutputEventTimeStreamProcessFunction) {
            ((DefaultEventTimeManager) this.eventTimeManager).setCanRegisterTimer(true);
        }

        eventTimeWatermarkHandler = new EventTimeWatermarkHandler(1, output, timeServiceManager);
    }

    @Override
    public void processRecord(
            IN record,
            Collector<OUT1> output1,
            Collector<OUT2> output2,
            TwoOutputPartitionedContext ctx)
            throws Exception {
        wrappedUserFunction.processRecord(record, output1, output2, ctx);
    }

    @Override
    public void endInput(TwoOutputNonPartitionedContext<OUT1, OUT2> ctx) {
        wrappedUserFunction.endInput(ctx);
    }

    @Override
    public void onProcessingTimer(
            long timestamp,
            Collector<OUT1> output1,
            Collector<OUT2> output2,
            TwoOutputPartitionedContext ctx) {
        wrappedUserFunction.onProcessingTimer(timestamp, output1, output2, ctx);
    }

    @Override
    public WatermarkHandlingResult onWatermark(
            Watermark watermark,
            Collector<OUT1> output1,
            Collector<OUT2> output2,
            TwoOutputNonPartitionedContext<OUT1, OUT2> ctx)
            throws Exception {
        // TODO InternalEventTimeUtils.processWatermark then send it to user function
        if (EventTimeExtension.isEventTimeWatermark(watermark.getIdentifier())
                && (wrappedUserFunction instanceof TwoOutputEventTimeStreamProcessFunction)) {
            //
            ((TwoOutputEventTimeStreamProcessFunction<IN, OUT1, OUT2>) wrappedUserFunction)
                    .onEventTimeWatermark(
                            ((LongWatermark) watermark).getValue(), output1, output2, ctx);
        }

        if (InternalEventTimeUtils.processWatermark(watermark, 0, eventTimeWatermarkHandler)) {
            return WatermarkHandlingResult.PEEK;
        } else {
            return wrappedUserFunction.onWatermark(watermark, output1, output2, ctx);
        }
    }

    public void onEventTime(
            long timestamp,
            Collector<OUT1> output1,
            Collector<OUT2> output2,
            TwoOutputPartitionedContext ctx) {
        ((TwoOutputEventTimeStreamProcessFunction<IN, OUT1, OUT2>) wrappedUserFunction)
                .onEventTimer(timestamp, output1, output2, ctx);
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

    public TwoOutputStreamProcessFunction<IN, OUT1, OUT2> getWrappedUserFunction() {
        return wrappedUserFunction;
    }
}
