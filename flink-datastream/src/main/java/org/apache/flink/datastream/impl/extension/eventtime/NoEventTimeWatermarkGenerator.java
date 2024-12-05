package org.apache.flink.datastream.impl.extension.eventtime;

import org.apache.flink.datastream.api.extension.eventtime.EventTimeWatermarkGenerator;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeWatermarkOutput;

/** The default implementation of {@link EventTimeWatermarkGenerator}. */
public class NoEventTimeWatermarkGenerator<T> implements EventTimeWatermarkGenerator<T> {

    @Override
    public void onEvent(T event, long eventTimestamp, EventTimeWatermarkOutput output) {}

    @Override
    public void onPeriodicEmit(EventTimeWatermarkOutput output) {}
}
