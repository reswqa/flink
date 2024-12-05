package org.apache.flink.datastream.impl.extension.eventtime;

import org.apache.flink.datastream.api.extension.eventtime.EventTimeWatermarkGenerator;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeWatermarkOutput;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeWatermarkStrategy;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A watermark generator that produces watermarks with a maximum out-of-order bound. For more
 * details, please refer to {@link
 * org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks}.
 */
public class BoundedOutOfOrdernessEventTimeWatermarkGenerator<T>
        implements EventTimeWatermarkGenerator<T> {
    /** The maximum timestamp encountered so far. */
    private long maxTimestamp;

    /** The maximum out-of-orderness that this watermark generator assumes. */
    private final long outOfOrdernessMillis;

    public BoundedOutOfOrdernessEventTimeWatermarkGenerator(
            EventTimeWatermarkStrategy.EventTimeWatermarkGeneratorContext context,
            Duration maxOutOfOrderness) {
        checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
        checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");

        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();

        // start so that our lowest watermark would be Long.MIN_VALUE.
        this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
    }

    // ------------------------------------------------------------------------

    @Override
    public void onEvent(T event, long eventTimestamp, EventTimeWatermarkOutput output) {
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(EventTimeWatermarkOutput output) {
        output.emitEventTimeWatermark(maxTimestamp - outOfOrdernessMillis - 1);
    }
}
