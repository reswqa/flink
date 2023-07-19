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

package org.apache.flink.processfunction.windows.trigger;

import org.apache.flink.api.common.eventtime.EventTimestampWatermark;
import org.apache.flink.api.common.eventtime.ProcessWatermark;
import org.apache.flink.processfunction.api.windowing.trigger.Trigger;
import org.apache.flink.processfunction.windows.window.TimeWindow;

public class EventTimeTrigger extends Trigger<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private EventTimeTrigger() {}

    @Override
    public TriggerResult onElement(
            Object element, long timestamp, TimeWindow window, Trigger.TriggerContext ctx)
            throws Exception {
        if (window.maxTimeStamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        } else {
            ctx.registerWatermarkListener(new EventTimestampWatermark(window.maxTimeStamp()));
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onWatermark(
            ProcessWatermark<?> watermark, TimeWindow window, Trigger.TriggerContext ctx) {
        if (watermark instanceof EventTimestampWatermark) {
            return ((EventTimestampWatermark) watermark).getTimestamp() == window.maxTimeStamp()
                    ? TriggerResult.FIRE
                    : TriggerResult.CONTINUE;
        }
        // ignore other watermark.
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, Trigger.TriggerContext ctx)
            throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
        ctx.deleteWatermarkListener(new EventTimestampWatermark(window.maxTimeStamp()));
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window, Trigger.OnMergeContext ctx) {
        // only register a timer if the watermark is not yet past the end of the merged window
        // this is in line with the logic in onElement(). If the watermark is past the end of
        // the window onElement() will fire and setting a timer here would fire the window twice.
        long windowMaxTimestamp = window.maxTimeStamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerWatermarkListener(new EventTimestampWatermark(windowMaxTimestamp));
        }
    }

    @Override
    public String toString() {
        return "EventTimeTrigger()";
    }

    /**
     * Creates an event-time trigger that fires once the watermark passes the end of the window.
     *
     * <p>Once the trigger fires all elements are discarded. Elements that arrive late immediately
     * trigger window evaluation with just this one element.
     */
    public static EventTimeTrigger create() {
        return new EventTimeTrigger();
    }
}
