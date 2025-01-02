package org.apache.flink.datastream.api.extension.window.window;

/**
 * A {@link Window} that represents a time interval from {@code start} (inclusive) to {@code end}
 * (exclusive).
 */
public interface TimeWindow extends Window {

    /**
     * Gets the starting timestamp of the window. This is the first timestamp that belongs to this
     * window.
     *
     * @return The starting timestamp of this window.
     */
    long getStart();

    /**
     * Gets the end timestamp of this window. The end timestamp is exclusive, meaning it is the
     * first timestamp that does not belong to this window any more.
     *
     * @return The exclusive end timestamp of this window.
     */
    long getEnd();
}
