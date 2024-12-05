package org.apache.flink.datastream.api.stream;

import org.apache.flink.annotation.Experimental;

import java.io.Serializable;

/** A user function designed to extract event time from an event. */
@Experimental
public interface EventTimeExtractor<T> extends Serializable {

    /** Extract the event time from the event, with the result provided in milliseconds. */
    long extractTimestamp(T event);
}
