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

package org.apache.flink.processfunction;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.processfunction.api.DataStream;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.connector.ConsumerSinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ConsumerFunction;

public class DataStreamImpl<T> implements DataStream<T> {
    private final ExecutionEnvironmentImpl environment;
    private final Transformation<T> transformation;

    public DataStreamImpl(ExecutionEnvironmentImpl environment, Transformation<T> transformation) {
        this.environment =
                Preconditions.checkNotNull(environment, "Execution Environment must not be null.");
        this.transformation =
                Preconditions.checkNotNull(
                        transformation, "Stream Transformation must not be null.");
    }

    @Override
    public <OUT> DataStream<OUT> process(SingleStreamProcessFunction<T, OUT> processFunction) {
        // TODO: Add implementation that calls processFunction.processRecord() in runtime
        return null;
    }

    @Override
    public void tmpToConsumerSink(ConsumerFunction<T> consumer) {
        // read the output type of the input Transform to coax out errors about MissingTypeInfo
        transformation.getOutputType();

        ConsumerSinkFunction<T> sinkFunction = new ConsumerSinkFunction<>(consumer);

        // TODO Supports clean closure
        StreamSink<T> sinkOperator = new StreamSink<>(sinkFunction);

        PhysicalTransformation<T> sinkTransformation =
                new LegacySinkTransformation<>(
                        transformation,
                        "Consumer Sink",
                        sinkOperator,
                        // TODO Supports configure parallelism
                        1,
                        true);

        environment.addOperator(sinkTransformation);
    }
}
