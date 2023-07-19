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

package org.apache.flink.processfunction.windows.assigner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.SerializerContext;
import org.apache.flink.api.common.eventtime.ProcessWatermark;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.processfunction.api.windowing.assigner.WindowAssigner;
import org.apache.flink.processfunction.api.windowing.trigger.Trigger;
import org.apache.flink.processfunction.windows.window.GlobalWindow;

import java.util.Collection;
import java.util.Collections;

public class GlobalWindowAssigner extends WindowAssigner<Object, GlobalWindow> {
    private static final long serialVersionUID = 1L;

    private GlobalWindowAssigner() {}

    @Override
    public Collection<GlobalWindow> assignWindows(
            Object element, long timestamp, WindowAssigner.WindowAssignerContext context) {
        return Collections.singletonList(GlobalWindow.get());
    }

    @Override
    public Trigger<Object, GlobalWindow> getDefaultTrigger() {
        return new GlobalWindowAssigner.NeverTrigger();
    }

    @Override
    public String toString() {
        return "GlobalWindows()";
    }

    /**
     * Creates a new {@code GlobalWindows} {@link WindowAssigner} that assigns all elements to the
     * same {@link GlobalWindow}.
     *
     * @return The global window policy.
     */
    public static GlobalWindowAssigner create() {
        return new GlobalWindowAssigner();
    }

    /** A trigger that never fires, as default Trigger for GlobalWindows. */
    @Internal
    public static class NeverTrigger extends Trigger<Object, GlobalWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public TriggerResult onElement(
                Object element, long timestamp, GlobalWindow window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onWatermark(
                ProcessWatermark<?> watermark, GlobalWindow window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {}

        @Override
        public void onMerge(GlobalWindow window, OnMergeContext ctx) {}
    }

    @Override
    public TypeSerializer<GlobalWindow> getWindowSerializer(SerializerContext serializerContext) {
        return new GlobalWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return false;
    }
}
