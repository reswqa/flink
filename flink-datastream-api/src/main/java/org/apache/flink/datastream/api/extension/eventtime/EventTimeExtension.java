package org.apache.flink.datastream.api.extension.eventtime;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.watermark.BoolWatermarkDeclaration;
import org.apache.flink.api.common.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkDeclarations;
import org.apache.flink.datastream.api.extension.eventtime.strategy.EventTimeWatermarkGeneratorBuilder;
import org.apache.flink.datastream.api.extension.eventtime.strategy.EventTimeWatermarkStrategy;
import org.apache.flink.datastream.api.extension.eventtime.timer.EventTimeManager;
import org.apache.flink.datastream.api.extension.eventtime.timer.EventTimeProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.timer.OneInputEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.timer.TwoInputBroadcastEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.timer.TwoInputNonBroadcastEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.timer.TwoOutputEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.EventTimeExtractor;

/**
 * The entry point for the event-time extension, which provides the following functionality:
 *
 * <ul>
 *   <li>defines the event-time watermark.
 *   <li>provides the {@link EventTimeWatermarkGeneratorBuilder} to facilitate the generation of
 *       event time watermarks.
 *   <li>provides a tool to encapsulate a user-defined {@link EventTimeProcessFunction} to provide
 *       the relevant components of the event-time extension.
 * </ul>
 */
@Experimental
public class EventTimeExtension {

    private static final Class<?> INSTANCE;

    static {
        try {
            INSTANCE =
                    Class.forName(
                            "org.apache.flink.datastream.impl.extension.eventtime.InternalEventTimeUtils");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Please ensure that flink-datastream in your class path");
        }
    }

    public static <T> EventTimeWatermarkGeneratorBuilder<T> newWatermarkGeneratorBuilder(
            EventTimeExtractor<T> eventTimeExtractor) {
        return new EventTimeWatermarkGeneratorBuilder<>(eventTimeExtractor);
    }

    public static <T>
            OneInputStreamProcessFunction<T, T> generateProcessFunctionWithWatermarkStrategy(
                    EventTimeWatermarkStrategy<T> strategy) {
        try {
            return (OneInputStreamProcessFunction<T, T>)
                    INSTANCE.getMethod(
                                    "generateProcessFunctionWithWatermarkStrategy",
                                    EventTimeWatermarkStrategy.class)
                            .invoke(null, strategy);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Wrap the user-defined {@link EventTimeProcessFunction}, which will provide related components
     * such as {@link EventTimeManager} and declare the necessary built-in state required for the
     * Timer, etc.
     */
    public static <IN, OUT> OneInputStreamProcessFunction<IN, OUT> wrapProcessFunction(
            OneInputEventTimeStreamProcessFunction<IN, OUT> processFunction) {
        // TODO: check whether the stream is keyed
        try {
            return (OneInputStreamProcessFunction<IN, OUT>)
                    INSTANCE.getMethod(
                                    "wrapProcessFunction",
                                    OneInputEventTimeStreamProcessFunction.class)
                            .invoke(null, processFunction);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <IN, OUT1, OUT2>
            TwoOutputStreamProcessFunction<IN, OUT1, OUT2> wrapProcessFunction(
                    TwoOutputEventTimeStreamProcessFunction<IN, OUT1, OUT2> processFunction) {
        // TODO: check whether the stream is keyed
        try {
            return (TwoOutputStreamProcessFunction<IN, OUT1, OUT2>)
                    INSTANCE.getMethod(
                                    "wrapProcessFunction",
                                    TwoOutputEventTimeStreamProcessFunction.class)
                            .invoke(null, processFunction);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <IN1, IN2, OUT>
            TwoInputNonBroadcastEventTimeStreamProcessFunction<IN1, IN2, OUT> wrapProcessFunction(
                    TwoInputNonBroadcastEventTimeStreamProcessFunction<IN1, IN2, OUT>
                            processFunction) {
        // TODO: check whether the stream is keyed
        try {
            return (TwoInputNonBroadcastEventTimeStreamProcessFunction<IN1, IN2, OUT>)
                    INSTANCE.getMethod(
                                    "wrapProcessFunction",
                                    TwoInputNonBroadcastEventTimeStreamProcessFunction.class)
                            .invoke(null, processFunction);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <IN1, IN2, OUT>
            TwoInputBroadcastEventTimeStreamProcessFunction<IN1, IN2, OUT> wrapProcessFunction(
                    TwoInputBroadcastEventTimeStreamProcessFunction<IN1, IN2, OUT>
                            processFunction) {
        // TODO: check whether the stream is keyed
        try {
            return (TwoInputBroadcastEventTimeStreamProcessFunction<IN1, IN2, OUT>)
                    INSTANCE.getMethod(
                                    "wrapProcessFunction",
                                    TwoInputBroadcastEventTimeStreamProcessFunction.class)
                            .invoke(null, processFunction);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static final LongWatermarkDeclaration EVENT_TIME_WATERMARK_DECLARATION =
            WatermarkDeclarations.newBuilder("BUILTIN_API_EVENT_TIME")
                    .typeLong()
                    .combineFunctionMin()
                    .defaultHandlingStrategyForward()
                    .build();

    public static final BoolWatermarkDeclaration IDLE_STATUS_WATERMARK_DECLARATION =
            WatermarkDeclarations.newBuilder("BUILTIN_API_EVENT_TIME_IDLE")
                    .typeBool()
                    .combineFunctionAND()
                    .defaultHandlingStrategyForward()
                    .build();

    public static boolean isEventTimeWatermark(String watermarkIdentifier) {
        return watermarkIdentifier.equals(EVENT_TIME_WATERMARK_DECLARATION.getIdentifier());
    }

    public static boolean isIdleStatusWatermark(String watermarkIdentifier) {
        return watermarkIdentifier.equals(IDLE_STATUS_WATERMARK_DECLARATION.getIdentifier());
    }

    public static boolean isEventTimeWatermark(Watermark watermark) {
        return watermark.getIdentifier().equals(EVENT_TIME_WATERMARK_DECLARATION.getIdentifier());
    }

    public static boolean isIdleStatusWatermark(Watermark watermark) {
        return watermark.getIdentifier().equals(IDLE_STATUS_WATERMARK_DECLARATION.getIdentifier());
    }
}
