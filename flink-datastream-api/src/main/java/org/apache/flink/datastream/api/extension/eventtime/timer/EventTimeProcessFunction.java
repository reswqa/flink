package org.apache.flink.datastream.api.extension.eventtime.timer;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.datastream.api.function.ProcessFunction;

// TODO: distinguish between one output and two output
// TODO: check keyed
/**
 * The base interface for event time process functions, indicating that the process function will
 * use event time extensions, such as registering event timers and handle event time watermarks.
 * Note that user-defined process functions should implement this sub-interface rather than this
 * interface.
 */
@Experimental
public interface EventTimeProcessFunction extends ProcessFunction {
    void initEventTimeExtension(EventTimeManager eventTimeManager);
}
