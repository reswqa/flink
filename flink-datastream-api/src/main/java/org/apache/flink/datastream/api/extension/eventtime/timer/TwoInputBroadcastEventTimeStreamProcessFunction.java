package org.apache.flink.datastream.api.extension.eventtime.timer;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;

/** The {@link TwoInputBroadcastStreamProcessFunction} that extends with event time support. */
@Experimental
public interface TwoInputBroadcastEventTimeStreamProcessFunction<IN1, IN2, OUT>
        extends EventTimeProcessFunction, TwoInputBroadcastStreamProcessFunction<IN1, IN2, OUT> {

    default void onEventTimeWatermark(
            long watermarkTimestamp, Collector<OUT> output, NonPartitionedContext<OUT> ctx)
            throws Exception {}

    default void onEventTimer(long timestamp, Collector<OUT> output, PartitionedContext ctx) {}
}
