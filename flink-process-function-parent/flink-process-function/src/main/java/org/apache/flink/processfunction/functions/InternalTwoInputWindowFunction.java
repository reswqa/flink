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

import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoInputWindowProcessFunction;
import org.apache.flink.processfunction.api.state.StateDeclaration;
import org.apache.flink.processfunction.api.windowing.assigner.WindowAssigner;
import org.apache.flink.processfunction.api.windowing.evictor.Evictor;
import org.apache.flink.processfunction.api.windowing.trigger.Trigger;
import org.apache.flink.processfunction.api.windowing.window.Window;
import org.apache.flink.processfunction.windows.utils.Unions.TaggedUnion;

import javax.annotation.Nullable;

import java.util.Set;
import java.util.function.Consumer;

public class InternalTwoInputWindowFunction<IN1, IN2, ACC1, ACC2, OUT, W extends Window>
        implements TwoInputStreamProcessFunction<IN1, IN2, OUT> {

    private final TwoInputWindowProcessFunction<ACC1, ACC2, OUT, W> windowProcessFunction;

    private final WindowAssigner<TaggedUnion<IN1, IN2>, W> assigner;

    private final Trigger<TaggedUnion<IN1, IN2>, W> trigger;

    @Nullable Evictor<TaggedUnion<IN1, IN2>, W> evictor;

    public InternalTwoInputWindowFunction(
            TwoInputWindowProcessFunction<ACC1, ACC2, OUT, W> windowProcessFunction,
            WindowAssigner<TaggedUnion<IN1, IN2>, W> assigner,
            Trigger<TaggedUnion<IN1, IN2>, W> trigger,
            @Nullable Evictor<TaggedUnion<IN1, IN2>, W> evictor) {
        this.windowProcessFunction = windowProcessFunction;
        this.assigner = assigner;
        this.trigger = trigger;
        this.evictor = evictor;
    }

    @Override
    public void processFirstInputRecord(IN1 record, Consumer<OUT> output, RuntimeContext ctx)
            throws Exception {
        // Do nothing as this will translator to windowOperator instead of processOperator.
    }

    @Override
    public void processSecondInputRecord(IN2 record, Consumer<OUT> output, RuntimeContext ctx)
            throws Exception {
        // Do nothing as this will translator to windowOperator instead of processOperator.
    }

    public TwoInputWindowProcessFunction<ACC1, ACC2, OUT, W> getWindowProcessFunction() {
        return windowProcessFunction;
    }

    @Override
    public Set<StateDeclaration> usesStates() {
        return windowProcessFunction.usesStates();
    }

    public WindowAssigner<TaggedUnion<IN1, IN2>, W> getAssigner() {
        return assigner;
    }

    public Trigger<TaggedUnion<IN1, IN2>, W> getTrigger() {
        return trigger;
    }

    @Nullable
    public Evictor<TaggedUnion<IN1, IN2>, W> getEvictor() {
        return evictor;
    }
}
