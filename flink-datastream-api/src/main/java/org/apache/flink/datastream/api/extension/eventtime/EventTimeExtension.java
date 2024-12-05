package org.apache.flink.datastream.api.extension.eventtime;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.watermark.BoolWatermarkDeclaration;
import org.apache.flink.api.common.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkDeclarations;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.TwoOutputNonPartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.EventTimeExtractor;

/**
 * The Util class allows users to utilize event time extensions, such as extracting and forwarding
 * event time. It also provides access to the {@link EventTimeManager}, which enables the
 * registration of event timers.
 */
@Experimental
public class EventTimeExtension {

    public static final LongWatermarkDeclaration EVENT_TIME_WATERMARK_DECLARATION =
            WatermarkDeclarations.newBuilder("EVENT_TIME_EXTENSION_EVENT_TIME")
                    .typeLong()
                    .combineFunctionMin()
                    .defaultHandlingStrategyForward()
                    .build();

    public static final BoolWatermarkDeclaration IDLE_STATUS_WATERMARK_DECLARATION =
            WatermarkDeclarations.newBuilder("EVENT_TIME_EXTENSION_IDLE_STATUS")
                    .typeBool()
                    .combineFunctionAND()
                    .defaultHandlingStrategyForward()
                    .build();

    private static final Class<?> INSTANCE;

    static {
        try {
            INSTANCE =
                    Class.forName(
                            "org.apache.flink.datastream.impl.extension.eventtime.EventTimeExtensionImpl");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Please ensure that flink-datastream in your class path");
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> OneInputStreamProcessFunction<T, T> extractEventTime(
            EventTimeExtractor<T> eventTimeExtractor) {
        return extractEventTimeAndWatermark(
                eventTimeExtractor, EventTimeWatermarkStrategy.noEventTimeWatermark());
    }

    @SuppressWarnings("unchecked")
    public static <T> OneInputStreamProcessFunction<T, T> extractEventTimeAndWatermark(
            EventTimeExtractor<T> eventTimeExtractor,
            EventTimeWatermarkStrategy<T> eventTimeWatermarkStrategy) {
        try {
            return (OneInputStreamProcessFunction<T, T>)
                    INSTANCE.getMethod(
                                    "extractEventTimeAndWatermark",
                                    EventTimeExtractor.class,
                                    EventTimeWatermarkStrategy.class)
                            .invoke(null, eventTimeExtractor, eventTimeWatermarkStrategy);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** This method should be invoked within {@code ProcessFunction#open}. */
    @SuppressWarnings("unchecked")
    public static <T> EventTimeManager getEventTimeManager(NonPartitionedContext<T> ctx) {
        try {
            return (EventTimeManager)
                    INSTANCE.getMethod("getEventTimeManager", NonPartitionedContext.class)
                            .invoke(null, ctx);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** This method should be invoked within {@code ProcessFunction#open}. */
    @SuppressWarnings("unchecked")
    public static <T1, T2> EventTimeManager getEventTimeManager(
            TwoOutputNonPartitionedContext<T1, T2> ctx) {
        try {
            return (EventTimeManager)
                    INSTANCE.getMethod("getEventTimeManager", TwoOutputNonPartitionedContext.class)
                            .invoke(null, ctx);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isEventTimeWatermarkDeclaration(String watermarkIdentifier) {
        return watermarkIdentifier.equals(EVENT_TIME_WATERMARK_DECLARATION.getIdentifier());
    }

    public static boolean isIdleStatusWatermarkDeclaration(String watermarkIdentifier) {
        return watermarkIdentifier.equals(IDLE_STATUS_WATERMARK_DECLARATION.getIdentifier());
    }
}
