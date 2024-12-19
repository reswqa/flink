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

package org.apache.flink.datastream.impl.extension.window.function;


import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.extension.window.TwoInputWindowProcessFunction;
import org.apache.flink.datastream.api.extension.window.assigner.WindowAssigner;
import org.apache.flink.datastream.api.extension.window.trigger.Trigger;
import org.apache.flink.datastream.api.extension.window.utils.TaggedUnion;
import org.apache.flink.datastream.api.extension.window.window.Window;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;

import java.util.Set;

public class InternalTwoInputWindowFunction<IN1, IN2, ACC1, ACC2, OUT, W extends Window>
        implements TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> {

    private final TwoInputWindowProcessFunction<ACC1, ACC2, OUT, W> windowProcessFunction;

    private final WindowAssigner<TaggedUnion<IN1, IN2>, W> assigner;

    private final Trigger<TaggedUnion<IN1, IN2>, W> trigger;

    public InternalTwoInputWindowFunction(
            TwoInputWindowProcessFunction<ACC1, ACC2, OUT, W> windowProcessFunction,
            WindowAssigner<TaggedUnion<IN1, IN2>, W> assigner,
            Trigger<TaggedUnion<IN1, IN2>, W> trigger) {
        this.windowProcessFunction = windowProcessFunction;
        this.assigner = assigner;
        this.trigger = trigger;
    }

    @Override
    public void processRecordFromFirstInput(IN1 record, Collector<OUT> output, PartitionedContext ctx)
            throws Exception {
        // Do nothing as this will translator to windowOperator instead of processOperator.
    }

    @Override
    public void processRecordFromSecondInput(IN2 record, Collector<OUT> output, PartitionedContext ctx)
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
}
