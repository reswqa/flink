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

import org.apache.flink.processfunction.api.ExecutionEnvironment;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.State;
import org.apache.flink.processfunction.api.StateDescriptor;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

/** Usage: Must be executed with flink-process-function and flink-dist jar in classpath. */
public class SimpleStatefulMap {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.tmpFromSupplierSource(System::currentTimeMillis)
                .process(new CalcTimeDiffFunc())
                // Don't use Lambda reference as PrintStream is not serializable.
                .tmpToConsumerSink(
                        (timeDiff) -> System.out.println("%d milliseconds since last timestamp."));
        env.execute();
    }

    private static class CalcTimeDiffFunc implements SingleStreamProcessFunction<Long, Long> {
        static final String STATE_ID = "lastTimestamp";

        @Override
        public void processRecord(Long record, Consumer<Long> output, RuntimeContext ctx) {
            State state = ctx.getState(STATE_ID);
            // TODO:
            //  long diff = record - state.getValue();
            //  state.setValue(record)
            //  output.accept(diff);
        }

        @Override
        public Map<String, StateDescriptor> usesStates() {
            // TODO: state descriptor for type LONG
            return Collections.singletonMap(STATE_ID, null);
        }
    }
}
