package org.apache.flink.datastream.impl.extension.eventtime;

import org.apache.flink.api.common.watermark.BoolWatermark;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.strategy.EventTimeWatermarkStrategy;
import org.apache.flink.datastream.api.extension.eventtime.timer.OneInputEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.timer.TwoInputBroadcastEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.timer.TwoInputNonBroadcastEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.timer.TwoOutputEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.eventtime.function.EventTimeWrappedOneInputStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.eventtime.function.EventTimeWrappedTwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.eventtime.function.EventTimeWrappedTwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.eventtime.function.EventTimeWrappedTwoOutputStreamProcessFunction;
import org.apache.flink.datastream.impl.watermark.ExtractEventTimeProcessFunction;
import org.apache.flink.streaming.runtime.watermark.extension.eventtime.EventTimeWatermarkHandler;
import org.apache.flink.streaming.runtime.watermark.extension.eventtime.EventTimeWatermarkHandler.EventTimeUpdateStatus;

/** The internal utils for event-time extension. */
public class InternalEventTimeUtils {
    public static <T>
            OneInputStreamProcessFunction<T, T> generateProcessFunctionWithWatermarkStrategy(
                    EventTimeWatermarkStrategy<T> strategy) {
        return new ExtractEventTimeProcessFunction<>(strategy);
    }

    public static <IN, OUT> OneInputStreamProcessFunction<IN, OUT> wrapProcessFunction(
            OneInputEventTimeStreamProcessFunction<IN, OUT> processFunction) {
        return new EventTimeWrappedOneInputStreamProcessFunction<>(processFunction);
    }

    public static <IN, OUT1, OUT2>
            TwoOutputStreamProcessFunction<IN, OUT1, OUT2> wrapProcessFunction(
                    TwoOutputEventTimeStreamProcessFunction<IN, OUT1, OUT2> processFunction) {
        return new EventTimeWrappedTwoOutputStreamProcessFunction<>(processFunction);
    }

    public static <IN1, IN2, OUT>
            TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> wrapProcessFunction(
                    TwoInputNonBroadcastEventTimeStreamProcessFunction<IN1, IN2, OUT>
                            processFunction) {
        return new EventTimeWrappedTwoInputNonBroadcastStreamProcessFunction<>(processFunction);
    }

    public static <IN1, IN2, OUT>
            TwoInputBroadcastStreamProcessFunction<IN1, IN2, OUT> wrapProcessFunction(
                    TwoInputBroadcastEventTimeStreamProcessFunction<IN1, IN2, OUT>
                            processFunction) {
        return new EventTimeWrappedTwoInputBroadcastStreamProcessFunction<>(processFunction);
    }

    public static boolean isEventTimeOrIdleStatusWatermark(String watermarkIdentifier) {
        return EventTimeExtension.isEventTimeWatermark(watermarkIdentifier)
                || EventTimeExtension.isIdleStatusWatermark(watermarkIdentifier);
    }

    /**
     * Process EventTimeWatermark/IdleStatusWatermark.
     *
     * <p>It's caller's responsibility to check whether the watermark is
     * EventTimeWatermark/IdleStatusWatermark.
     *
     * @return the status of event time watermark update.
     */
    public static EventTimeUpdateStatus processWatermark(
            Watermark watermark,
            int inputIndex,
            EventTimeWatermarkHandler eventTimeWatermarkHandler)
            throws Exception {
        if (EventTimeExtension.isEventTimeWatermark(watermark.getIdentifier())) {
            long timestamp = ((LongWatermark) watermark).getValue();
            return eventTimeWatermarkHandler.processEventTime(timestamp, inputIndex);
        } else if (EventTimeExtension.isIdleStatusWatermark(watermark.getIdentifier())) {
            boolean isIdle = ((BoolWatermark) watermark).getValue();
            eventTimeWatermarkHandler.processEventTimeIdleStatus(isIdle, inputIndex);
        }
        return EventTimeUpdateStatus.noUpdate();
    }
}
