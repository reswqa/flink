package org.apache.flink.datastream.api.extension.eventtime;

import org.apache.flink.annotation.Experimental;

/**
 * A utility class is employed to enable the {@link EventTimeWatermarkGenerator} to dispatch event
 * time watermarks and idle status watermarks.
 */
@Experimental
public interface EventTimeWatermarkOutput {

    /** Emit event time watermark to downstream. */
    void emitEventTimeWatermark(long eventTimeWatermark);

    /** Emit idle status watermark to downstream. */
    void emitEventTimeWatermarkIdle(boolean isIdle);
}
