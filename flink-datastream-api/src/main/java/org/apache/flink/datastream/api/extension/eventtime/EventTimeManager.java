/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.datastream.api.extension.eventtime;

import org.apache.flink.annotation.Experimental;

/**
 * This utility class allows users to register and delete event timers, as well as retrieve event
 * times. Note that it is only used in the KeyedProcessFunction.
 */
@Experimental
public interface EventTimeManager {
    /**
     * Register an event timer for this process function. The EventTimerCallback will be invoked if
     * the timer exists.
     *
     * @param timestamp to trigger timer callback.
     * @param callback to be invoked when the timer fires.
     */
    void registerTimer(long timestamp, EventTimerCallback callback);

    /**
     * Register an event timer for this process function. The TwoOutputEventTimerCallback will be
     * invoked if the timer exists.
     *
     * @param timestamp to trigger timer callback.
     * @param callback to be invoked when the timer fires.
     */
    void registerTimer(long timestamp, TwoOutputEventTimerCallback callback);

    /**
     * Deletes the event-time timer with the given trigger timestamp. This method has only an effect
     * if such a timer was previously registered and did not already expire.
     *
     * @param timestamp indicates the timestamp of the timer to delete.
     */
    void deleteTimer(long timestamp);

    /**
     * Get the current event time.
     *
     * @return current event time.
     */
    long currentTime();
}
