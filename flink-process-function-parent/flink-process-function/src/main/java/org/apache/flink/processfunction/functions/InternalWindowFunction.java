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

package org.apache.flink.processfunction.functions;

import org.apache.flink.processfunction.api.Collector;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.api.function.WindowProcessFunction;
import org.apache.flink.processfunction.api.state.StateDeclaration;
import org.apache.flink.processfunction.api.windowing.assigner.WindowAssigner;
import org.apache.flink.processfunction.api.windowing.trigger.Trigger;
import org.apache.flink.processfunction.api.windowing.window.Window;

import java.util.Set;

/** This is only a container for window related things. */
public class InternalWindowFunction<IN, ACC, OUT, W extends Window>
        implements SingleStreamProcessFunction<IN, OUT> {
    private final WindowProcessFunction<ACC, OUT, W> windowProcessFunction;

    private final WindowAssigner<IN, W> assigner;

    private final Trigger<IN, W> trigger;

    public InternalWindowFunction(
            WindowProcessFunction<ACC, OUT, W> windowProcessFunction,
            WindowAssigner<IN, W> assigner,
            Trigger<IN, W> trigger) {
        this.windowProcessFunction = windowProcessFunction;
        this.assigner = assigner;
        this.trigger = trigger;
    }

    @Override
    public void processRecord(IN record, Collector<OUT> output, RuntimeContext ctx)
            throws Exception {
        // Do nothing as this will translator to windowOperator instead of processOperator.
    }

    public WindowAssigner<IN, W> getAssigner() {
        return assigner;
    }

    public Trigger<IN, W> getTrigger() {
        return trigger;
    }

    public WindowProcessFunction<ACC, OUT, W> getWindowProcessFunction() {
        return windowProcessFunction;
    }

    @Override
    public Set<StateDeclaration> usesStates() {
        return windowProcessFunction.usesStates();
    }
}
