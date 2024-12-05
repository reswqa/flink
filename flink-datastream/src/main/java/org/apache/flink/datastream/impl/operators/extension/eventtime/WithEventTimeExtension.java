package org.apache.flink.datastream.impl.operators.extension.eventtime;

import org.apache.flink.datastream.api.extension.eventtime.EventTimeManager;

/**
 * An interface representing an operator can utilize event time extension, it is currently only used
 * in keyed operators.
 */
public interface WithEventTimeExtension {

    /**
     * Initialize all components related to the even time extension; this method should be invoked
     * within the operator's {@code open} method.
     */
    void initEventTimeExtension() throws Exception;

    /** Get the {@link EventTimeManager} instance. */
    EventTimeManager getEventTimeManager();
}
