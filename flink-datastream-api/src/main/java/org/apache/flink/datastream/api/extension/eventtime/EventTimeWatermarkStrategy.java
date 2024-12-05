package org.apache.flink.datastream.api.extension.eventtime;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.clock.RelativeClock;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;

/** The EventTimeWatermarkStrategy defines how to generate {@link EventTimeWatermarkGenerator}. */
@Experimental
public interface EventTimeWatermarkStrategy<T> extends Serializable {

    /**
     * Instantiates a EventTimeWatermarkGenerator that generates event time watermarks according to
     * this strategy.
     */
    EventTimeWatermarkGenerator<T> createEventTimeWatermarkGenerator(
            EventTimeWatermarkGeneratorContext context);

    @SuppressWarnings("unchecked")
    static <T> EventTimeWatermarkStrategy<T> noEventTimeWatermark() {
        return ctx -> {
            try {
                Class<?> clazz =
                        Class.forName(
                                "org.apache.flink.datastream.impl.extension.eventtime.NoEventTimeWatermarkGenerator");
                return (EventTimeWatermarkGenerator) clazz.getConstructor().newInstance();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(
                        "Please ensure that flink-datastream in your class path");
            } catch (InvocationTargetException
                    | InstantiationException
                    | IllegalAccessException
                    | NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @SuppressWarnings("unchecked")
    static <T> EventTimeWatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness) {
        return ctx -> {
            try {
                Class<?> clazz =
                        Class.forName(
                                "org.apache.flink.datastream.impl.extension.eventtime.BoundedOutOfOrdernessEventTimeWatermarkGenerator");
                return (EventTimeWatermarkGenerator)
                        clazz.getConstructor(
                                        EventTimeWatermarkStrategy
                                                .EventTimeWatermarkGeneratorContext.class,
                                        Duration.class)
                                .newInstance(ctx, maxOutOfOrderness);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(
                        "Please ensure that flink-datastream in your class path");
            } catch (InvocationTargetException
                    | NoSuchMethodException
                    | IllegalAccessException
                    | InstantiationException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @SuppressWarnings("unchecked")
    default EventTimeWatermarkStrategy<T> withIdleness(Duration idleTimeout) {
        return ctx -> {
            try {
                Class<?> clazz =
                        Class.forName(
                                "org.apache.flink.datastream.impl.extension.eventtime.EventTimeWatermarkGeneratorWithIdleness");
                return (EventTimeWatermarkGenerator)
                        clazz.getConstructor(
                                        EventTimeWatermarkStrategy
                                                .EventTimeWatermarkGeneratorContext.class,
                                        EventTimeWatermarkStrategy.class,
                                        Duration.class)
                                .newInstance(ctx, this, idleTimeout);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(
                        "Please ensure that flink-datastream in your class path");
            } catch (InvocationTargetException
                    | NoSuchMethodException
                    | IllegalAccessException
                    | InstantiationException e) {
                throw new RuntimeException(e);
            }
        };
    }

    /**
     * Additional information available to {@link
     * EventTimeWatermarkStrategy#createEventTimeWatermarkGenerator}. This can be access to {@link
     * MetricGroup MetricGroups}, for example.
     */
    interface EventTimeWatermarkGeneratorContext {

        /**
         * Returns the metric group for the context in which the created {@link
         * EventTimeWatermarkGenerator} is used.
         *
         * <p>Instances of this class can be used to register new metrics with Flink and to create a
         * nested hierarchy based on the group names. See {@link MetricGroup} for more information
         * for the metrics system.
         *
         * @see MetricGroup
         */
        MetricGroup getMetricGroup();

        /**
         * Returns a {@link RelativeClock}.
         *
         * @see RelativeClock
         */
        RelativeClock getInputActivityClock();
    }
}
