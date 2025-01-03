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

package org.apache.flink.datastream.impl.extension.window.assigner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeSerializer;
import org.apache.flink.datastream.api.extension.window.assigner.WindowAssigner;
import org.apache.flink.datastream.api.extension.window.trigger.Trigger;
import org.apache.flink.datastream.api.extension.window.window.GlobalWindow;
import org.apache.flink.datastream.impl.extension.window.window.GlobalWindowImpl;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;

import java.util.Collection;
import java.util.Collections;

/** A special {@link WindowAssigner} for {@link GlobalWindow}. */
public class GlobalWindowAssigner extends WindowAssigner<Object, GlobalWindowImpl> {

    private static final long serialVersionUID = 1L;

    private GlobalWindowAssigner() {}

    @Override
    public Collection<GlobalWindowImpl> assignWindows(
            Object element, long timestamp, WindowAssigner.WindowAssignerContext context) {
        return Collections.singletonList(GlobalWindowImpl.get());
    }

    @Override
    public Trigger<Object, GlobalWindowImpl> getDefaultTrigger() {
        //        return new GlobalWindowAssigner.NeverTrigger();
        return new GlobalWindowAssigner.EndOfStreamTrigger();
    }

    @Override
    public String toString() {
        return "GlobalWindowAssigner()";
    }

    /**
     * Creates a new {@code GlobalWindows} {@link WindowAssigner} that assigns all elements to the
     * same {@link GlobalWindow}.
     *
     * @return The global window assigner.
     */
    public static GlobalWindowAssigner create() {
        return new GlobalWindowAssigner();
    }

    /** A trigger that never fires, as default Trigger for {@link GlobalWindow}s. */
    @Internal
    public static class NeverTrigger extends Trigger<Object, GlobalWindowImpl> {
        private static final long serialVersionUID = 1L;

        @Override
        public TriggerResult onElement(
                Object element, long timestamp, GlobalWindowImpl window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        public TriggerResult onEventTime(long time, GlobalWindowImpl window, TriggerContext ctx)
                throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(
                long time, GlobalWindowImpl window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindowImpl window, TriggerContext ctx) throws Exception {}

        @Override
        public void onMerge(GlobalWindowImpl window, OnMergeContext ctx) {}
    }

    @Internal
    public static class EndOfStreamTrigger extends Trigger<Object, GlobalWindowImpl> {
        private static final long serialVersionUID = 1L;

        @Override
        public TriggerResult onElement(
                Object element, long timestamp, GlobalWindowImpl window, TriggerContext ctx) {
            ctx.registerEventTimeListener(Long.MAX_VALUE);
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, GlobalWindowImpl window, TriggerContext ctx) {
            return time == Long.MAX_VALUE ? TriggerResult.FIRE : TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(
                long time, GlobalWindowImpl window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindowImpl window, TriggerContext ctx) throws Exception {}

        @Override
        public void onMerge(GlobalWindowImpl window, OnMergeContext ctx) {}
    }

    @Override
    public TypeSerializer<GlobalWindowImpl> getWindowSerializer() {
        return new GlobalWindowImpl.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return false;
    }
}
