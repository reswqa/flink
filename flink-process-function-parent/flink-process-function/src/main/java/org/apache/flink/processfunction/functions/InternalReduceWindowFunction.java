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

import org.apache.flink.processfunction.api.builtin.BatchStreamingUnifiedFunctions;
import org.apache.flink.processfunction.api.function.WindowProcessFunction;
import org.apache.flink.processfunction.api.state.StateDeclaration;
import org.apache.flink.processfunction.api.windowing.assigner.WindowAssigner;
import org.apache.flink.processfunction.api.windowing.evictor.Evictor;
import org.apache.flink.processfunction.api.windowing.trigger.Trigger;
import org.apache.flink.processfunction.api.windowing.window.Window;

import javax.annotation.Nullable;

import java.util.Set;

public class InternalReduceWindowFunction<IN, W extends Window>
        extends InternalWindowFunction<IN, IN, IN, W> {
    private final BatchStreamingUnifiedFunctions.ReduceFunction<IN> reduceFunction;

    public InternalReduceWindowFunction(
            WindowProcessFunction<IN, IN, W> windowProcessFunction,
            WindowAssigner<IN, W> assigner,
            Trigger<IN, W> trigger,
            BatchStreamingUnifiedFunctions.ReduceFunction<IN> reduceFunction,
            @Nullable Evictor<IN, W> evictor) {
        super(windowProcessFunction, assigner, trigger, evictor);
        this.reduceFunction = reduceFunction;
    }

    public BatchStreamingUnifiedFunctions.ReduceFunction<IN> getReduceFunction() {
        return reduceFunction;
    }

    @Override
    public Set<StateDeclaration> usesStates() {
        return getWindowProcessFunction().usesStates();
    }
}
