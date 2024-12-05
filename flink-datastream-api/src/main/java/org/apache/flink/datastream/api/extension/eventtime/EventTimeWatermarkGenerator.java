package org.apache.flink.datastream.api.extension.eventtime;

import org.apache.flink.annotation.Experimental;

import java.io.Serializable;

/**
 * The {@code EventTimeWatermarkGenerator} generates event time watermarks either based on events or
 * periodically (in a fixed interval).
 */
@Experimental
public interface EventTimeWatermarkGenerator<T> extends Serializable {

    /**
     * Called for every event, allows the watermark generator to examine and remember the event
     * timestamps, or to emit a event time watermark based on the event itself.
     */
    void onEvent(T event, long eventTimestamp, EventTimeWatermarkOutput output);

    /**
     * Called periodically, and might emit a new event time watermark, or not.
     *
     * <p>The interval in which this method is called and event time watermarks are generated
     * depends on {@code ExecutionConfig#getAutoWatermarkInterval()}.
     */
    void onPeriodicEmit(EventTimeWatermarkOutput output);
}
