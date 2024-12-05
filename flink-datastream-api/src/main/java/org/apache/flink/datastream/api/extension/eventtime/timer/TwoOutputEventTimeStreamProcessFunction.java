package org.apache.flink.datastream.api.extension.eventtime.timer;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.TwoOutputNonPartitionedContext;
import org.apache.flink.datastream.api.context.TwoOutputPartitionedContext;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;

/** A {@code EventTimeProcessFunction} interface for {@link TwoOutputStreamProcessFunction}. */
@Experimental
public interface TwoOutputEventTimeStreamProcessFunction<IN, OUT1, OUT2>
        extends EventTimeProcessFunction, TwoOutputStreamProcessFunction<IN, OUT1, OUT2> {

    default void onEventTimeWatermark(
            long watermarkTimestamp,
            Collector<OUT1> output1,
            Collector<OUT2> output2,
            TwoOutputNonPartitionedContext<OUT1, OUT2> ctx)
            throws Exception {}

    default void onEventTimer(
            long timestamp,
            Collector<OUT1> output1,
            Collector<OUT2> output2,
            TwoOutputPartitionedContext ctx) {}
}
