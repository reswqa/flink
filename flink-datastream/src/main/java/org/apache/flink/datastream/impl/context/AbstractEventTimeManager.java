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

package org.apache.flink.datastream.impl.context;

import org.apache.flink.datastream.api.extension.eventtime.EventTimeManager;
import org.apache.flink.datastream.api.extension.eventtime.EventTimerCallback;
import org.apache.flink.datastream.api.extension.eventtime.TwoOutputEventTimerCallback;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.v2.adaptor.MapStateAdaptor;
import org.apache.flink.streaming.api.operators.InternalTimerService;

/** The abstract implementation of {@link EventTimeManager}. */
public abstract class AbstractEventTimeManager implements EventTimeManager {
    /** The timer service of operator, used in register event timer. */
    protected final InternalTimerService<VoidNamespace> timerService;

    /**
     * The internal map state, used in store event timer callback. key is event time, value is
     * user-defined callback, {@link EventTimerCallback} or {@link TwoOutputEventTimerCallback}.
     */
    protected final MapStateAdaptor eventTimerCallbackMapState;

    public AbstractEventTimeManager(
            InternalTimerService<VoidNamespace> timerService,
            MapStateAdaptor eventTimerCallbackMapState) {
        this.timerService = timerService;
        this.eventTimerCallbackMapState = eventTimerCallbackMapState;
    }

    @Override
    public void deleteTimer(long timestamp) {
        timerService.deleteEventTimeTimer(VoidNamespace.INSTANCE, timestamp);
    }

    @Override
    public long currentTime() {
        return timerService.currentWatermark();
    }
}
