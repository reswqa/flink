package org.apache.flink.datastream.impl.extension.eventtime;

import org.apache.flink.api.common.watermark.BoolWatermark;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.strategy.EventTimeWatermarkStrategy;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.eventtime.function.EventTimeExtensionWrappedOneInputStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.eventtime.function.EventTimeExtensionWrappedTwoOutputStreamProcessFunction;
import org.apache.flink.datastream.impl.watermark.ExtractEventTimeProcessFunction;
import org.apache.flink.streaming.runtime.watermark.extension.eventtime.EventTimeWatermarkHandler;

/** The implementation of {@link EventTimeWatermarks}. */
public class InternalEventTimeUtils {
    public static <T>
            OneInputStreamProcessFunction<T, T> generateProcessFunctionWithWatermarkStrategy(
                    EventTimeWatermarkStrategy<T> strategy) {
        return new ExtractEventTimeProcessFunction<>(strategy);
    }

    public static <IN, OUT> OneInputStreamProcessFunction<IN, OUT> wrapAsEventTimeProcessFunction(
            OneInputStreamProcessFunction<IN, OUT> processFunction) {
        return new EventTimeExtensionWrappedOneInputStreamProcessFunction<>(processFunction);
    }

    public static <IN, OUT1, OUT2>
            TwoOutputStreamProcessFunction<IN, OUT1, OUT2> wrapAsEventTimeProcessFunction(
                    TwoOutputStreamProcessFunction<IN, OUT1, OUT2> processFunction) {
        return new EventTimeExtensionWrappedTwoOutputStreamProcessFunction(processFunction);
    }

    public static boolean processWatermark(
            Watermark watermark,
            int inputIndex,
            EventTimeWatermarkHandler eventTimeWatermarkHandler)
            throws Exception {
        if (EventTimeExtension.isEventTimeWatermark(watermark.getIdentifier())) {
            long timestamp = ((LongWatermark) watermark).getValue();
            eventTimeWatermarkHandler.processEventTime(timestamp, inputIndex);
            return true;
        } else if (EventTimeExtension.isIdleStatusWatermark(watermark.getIdentifier())) {
            boolean isIdle = ((BoolWatermark) watermark).getValue();
            eventTimeWatermarkHandler.processEventTimeIdleStatus(isIdle, inputIndex);
            return true;
        }
        return false;
    }
}
