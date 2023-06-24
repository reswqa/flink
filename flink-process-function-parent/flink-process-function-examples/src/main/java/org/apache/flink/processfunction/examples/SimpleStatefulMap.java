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

package org.apache.flink.processfunction.examples;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.States;
import org.apache.flink.api.common.state.States.ListStateDeclaration;
import org.apache.flink.api.common.state.States.StateDeclaration;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.processfunction.api.ExecutionEnvironment;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.builtin.Sinks;
import org.apache.flink.processfunction.api.builtin.Sources;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;

import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;

/** Usage: Must be executed with flink-process-function and flink-dist jar in classpath. */
public class SimpleStatefulMap {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.fromSource(Sources.supplier(System::currentTimeMillis))
                .process(new CalcTimeDiffFunc())
                // Don't use Lambda reference as PrintStream is not serializable.
                .sinkTo(
                        Sinks.consumer(
                                (timeDiff) ->
                                        System.out.printf(
                                                "%d milliseconds since last timestamp. \n",
                                                timeDiff)));
        env.execute();
    }

    private static class CalcTimeDiffFunc implements SingleStreamProcessFunction<Long, Long> {
        static final String STATE_ID = "lastTimestamp";

        static final ListStateDeclaration<Long> LIST_STATE_DECLARATION =
                States.ofList(STATE_ID, TypeDescriptors.LONG);

        @Override
        public void processRecord(Long record, Consumer<Long> output, RuntimeContext ctx)
                throws Exception {
            ListState<Long> state = ctx.getState(LIST_STATE_DECLARATION);
            if (!state.get().iterator().hasNext()) {
                // for first record
                output.accept(0L);
            } else {
                long diff = record - state.get().iterator().next();
                output.accept(diff);
            }
            state.update(Collections.singletonList(record));
        }

        @Override
        public Set<StateDeclaration> usesStates() {
            return Collections.singleton(LIST_STATE_DECLARATION);
        }
    }
}
