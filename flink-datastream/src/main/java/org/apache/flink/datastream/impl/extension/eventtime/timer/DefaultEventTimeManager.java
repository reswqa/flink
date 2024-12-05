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

package org.apache.flink.datastream.impl.extension.eventtime.timer;

import org.apache.flink.datastream.api.extension.eventtime.timer.EventTimeManager;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimerService;

import java.util.function.Supplier;

/** The implementation of {@link EventTimeManager}, used in one output keyed operator. */
public class DefaultEventTimeManager implements EventTimeManager {

    /** The timer service of operator, used in register event timer. */
    protected final InternalTimerService<VoidNamespace> timerService;

    private boolean canRegisterTimer = false;

    private Supplier<Object> currentKeySupplier;

    public DefaultEventTimeManager(
            InternalTimerService<VoidNamespace> timerService, Supplier<Object> currentKeySupplier) {
        this.timerService = timerService;
        this.currentKeySupplier = currentKeySupplier;
    }

    @Override
    public void registerTimer(long timestamp) {
        if (!canRegisterTimer) {
            throw new IllegalArgumentException(
                    "The wrapped process function must implement EventTimeExtensionWithTimerCallback or EventTimeExtensionWithTwoOutputTimerCallback");
        }
        if (currentKeySupplier.get() == null) {
            throw new IllegalArgumentException(
                    "The current key is null, register should be execute in ProcessFunction#processRecord or other methods which have PartitionedContext parameter, to ensure it is executed in correct key context.");
        }
        timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, timestamp);
    }

    @Override
    public void deleteTimer(long timestamp) {
        timerService.deleteEventTimeTimer(VoidNamespace.INSTANCE, timestamp);
    }

    @Override
    public long currentTime() {
        return timerService.currentWatermark();
    }

    public void setCanRegisterTimer(boolean canRegisterTimer) {
        this.canRegisterTimer = canRegisterTimer;
    }
}
