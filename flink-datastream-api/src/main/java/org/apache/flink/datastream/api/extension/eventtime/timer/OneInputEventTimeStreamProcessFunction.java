package org.apache.flink.datastream.api.extension.eventtime.timer;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;

/** A {@code EventTimeProcessFunction} interface for {@link OneInputStreamProcessFunction}. */
@Experimental
public interface OneInputEventTimeStreamProcessFunction<IN, OUT>
        extends EventTimeProcessFunction, OneInputStreamProcessFunction<IN, OUT> {

    default void onEventTimeWatermark(
            long watermarkTimestamp, Collector<OUT> output, NonPartitionedContext<OUT> ctx)
            throws Exception {}

    default void onEventTimer(long timestamp, Collector<OUT> output, PartitionedContext ctx) {}
}
