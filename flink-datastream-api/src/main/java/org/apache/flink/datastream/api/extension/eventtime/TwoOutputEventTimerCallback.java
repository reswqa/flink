package org.apache.flink.datastream.api.extension.eventtime;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.TwoOutputPartitionedContext;

import java.io.Serializable;

/**
 * The event timer callback of {@link EventTimeManager#registerTimer(long,
 * TwoOutputEventTimerCallback)}.
 */
@Experimental
public interface TwoOutputEventTimerCallback<OUT1, OUT2> extends Serializable {

    void onEventTimer(
            long timestamp,
            Collector<OUT1> output1,
            Collector<OUT2> output2,
            TwoOutputPartitionedContext ctx);
}
