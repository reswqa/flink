package org.apache.flink.datastream.api.extension.window.window;

/**
 * A {@link Window} that windows elements into sessions based on the timestamp of the elements. Note
 * that windows cannot overlap.
 */
public interface SessionWindow extends TimeWindow {}
