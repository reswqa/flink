package org.apache.flink.datastream.impl.extension.eventtime;

import org.apache.flink.api.common.eventtime.WatermarksWithIdleness;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeWatermarkGenerator;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeWatermarkOutput;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeWatermarkStrategy;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link EventTimeWatermarkGenerator} that wraps another {@link EventTimeWatermarkGenerator} and
 * detects idleness of the wrapped generator.
 *
 * <p>For more details, please refer to {@link WatermarksWithIdleness}.
 */
public class EventTimeWatermarkGeneratorWithIdleness<T> implements EventTimeWatermarkGenerator<T> {

    private final EventTimeWatermarkGenerator<T> baseWatermarkGenerator;

    private WatermarksWithIdleness.IdlenessTimer idlenessTimer;

    private boolean isIdleNow = false;

    private Duration idleTimeout;

    /**
     * Creates a new WatermarksWithIdleness generator to the given generator idleness detection with
     * the given timeout.
     *
     * @param baseWatermarkGenerator The original watermark generator.
     * @param idleTimeout The timeout for the idleness detection.
     * @param clock The clock that will be used to measure idleness period. It is expected that this
     *     clock will hide periods when this {@link WatermarkGenerator} has been blocked from making
     *     any progress despite availability of records on the input.
     */
    public EventTimeWatermarkGeneratorWithIdleness(
            EventTimeWatermarkStrategy.EventTimeWatermarkGeneratorContext context,
            EventTimeWatermarkStrategy<T> baseWatermarkStrategy,
            Duration idleTimeout) {
        checkNotNull(idleTimeout, "idleTimeout");
        checkArgument(
                !(idleTimeout.isZero() || idleTimeout.isNegative()),
                "idleTimeout must be greater than zero");
        this.idlenessTimer =
                new WatermarksWithIdleness.IdlenessTimer(
                        context.getInputActivityClock(), idleTimeout);
        this.baseWatermarkGenerator =
                checkNotNull(baseWatermarkStrategy).createEventTimeWatermarkGenerator(context);
        this.idleTimeout = idleTimeout;
    }

    @Override
    public void onEvent(T event, long eventTimestamp, EventTimeWatermarkOutput output) {
        baseWatermarkGenerator.onEvent(event, eventTimestamp, output);
        idlenessTimer.activity();
        isIdleNow = false;
    }

    @Override
    public void onPeriodicEmit(EventTimeWatermarkOutput output) {
        if (idlenessTimer.checkIfIdle()) {
            if (!isIdleNow) {
                output.emitEventTimeWatermarkIdle(true);
                isIdleNow = true;
            }
        } else {
            baseWatermarkGenerator.onPeriodicEmit(output);
        }
    }
}
