package org.apache.flink.datastream.api.extension.eventtime;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.watermark.BoolWatermarkDeclaration;
import org.apache.flink.api.common.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkDeclarations;
import org.apache.flink.datastream.api.extension.eventtime.strategy.EventTimeWatermarkGeneratorBuilder;
import org.apache.flink.datastream.api.extension.eventtime.strategy.EventTimeWatermarkStrategy;
import org.apache.flink.datastream.api.extension.eventtime.timer.EventTimeManager;
import org.apache.flink.datastream.api.extension.eventtime.timer.EventTimeProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.timer.OneInputEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.timer.TwoOutputEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.EventTimeExtractor;

/**
 * The entry point for the Event Time extension, which provides the following functionality:
 *
 * <ul>
 *   <li>defines the event time watermark.
 *   <li>provides the {@link EventTimeWatermarkGeneratorBuilder} to facilitate the generation of
 *       event time watermarks.
 *   <li>provides a tool method to encapsulate a user-defined {@link EventTimeProcessFunction} to
 *       provide the relevant components of the EventTime Extension.
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

    public static <T> EventTimeWatermarkGeneratorBuilder<T> newEventTimeWatermarkGeneratorBuilder(
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
    public static <IN, OUT> OneInputStreamProcessFunction<IN, OUT> wrapAsEventTimeProcessFunction(
            OneInputEventTimeStreamProcessFunction<IN, OUT> processFunction) {
        // TODO: check whether the stream is keyed

        if (!(processFunction instanceof EventTimeProcessFunction)) {
            throw new IllegalArgumentException(
                    "The processFunction must be an instance of EventTimeProcessFunction");
        }

        if (processFunction instanceof TwoOutputEventTimeStreamProcessFunction) {
            throw new IllegalArgumentException(
                    "The ProcessFunction should implement OneOutputEventTimeProcessFunction rather than TwoOutputEventTimeProcessFunction.");
        }

        try {
            return (OneInputStreamProcessFunction<IN, OUT>)
                    INSTANCE.getMethod(
                                    "wrapAsEventTimeProcessFunction",
                                    OneInputStreamProcessFunction.class)
                            .invoke(null, processFunction);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <IN, OUT1, OUT2>
            TwoOutputStreamProcessFunction<IN, OUT1, OUT2> wrapAsEventTimeProcessFunction(
                    TwoOutputStreamProcessFunction<IN, OUT1, OUT2> processFunction) {
        // TODO: check whether the stream is keyed

        if (!(processFunction instanceof EventTimeProcessFunction)) {
            throw new IllegalArgumentException(
                    "The processFunction must be an instance of EventTimeProcessFunction");
        }

        if (processFunction instanceof OneInputEventTimeStreamProcessFunction) {
            throw new IllegalArgumentException(
                    "The ProcessFunction should implement TwoOutputEventTimeProcessFunction rather than OneOutputEventTimeProcessFunction.");
        }

        try {
            return (TwoOutputStreamProcessFunction<IN, OUT1, OUT2>)
                    INSTANCE.getMethod(
                                    "wrapAsEventTimeProcessFunction",
                                    TwoOutputStreamProcessFunction.class)
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
}
