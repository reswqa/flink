package org.apache.flink.datastream.api.extension.eventtime;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;

import java.io.Serializable;

/** The event timer callback of {@link EventTimeManager#registerTimer(long, EventTimerCallback)}. */
@Experimental
public interface EventTimerCallback<OUT> extends Serializable {

    void onEventTimer(long timestamp, Collector<OUT> output, PartitionedContext ctx);
}
