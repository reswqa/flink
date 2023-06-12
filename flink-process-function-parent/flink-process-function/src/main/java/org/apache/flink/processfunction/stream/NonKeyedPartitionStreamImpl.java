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

package org.apache.flink.processfunction.stream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.processfunction.DataStream;
import org.apache.flink.processfunction.ExecutionEnvironmentImpl;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.processfunction.api.stream.BroadcastStream;
import org.apache.flink.processfunction.api.stream.GlobalStream;
import org.apache.flink.processfunction.api.stream.KeyedPartitionStream;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream;
import org.apache.flink.processfunction.connector.ConsumerSinkFunction;
import org.apache.flink.processfunction.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.util.function.ConsumerFunction;

import java.util.function.Function;

public class NonKeyedPartitionStreamImpl<T> extends DataStream<T>
        implements NonKeyedPartitionStream<T> {

    public NonKeyedPartitionStreamImpl(
            ExecutionEnvironmentImpl environment, Transformation<T> transformation) {
        super(environment, transformation);
    }

    @Override
    public <OUT> NonKeyedPartitionStream<OUT> process(
            SingleStreamProcessFunction<T, OUT> processFunction) {
        TypeInformation<OUT> outType =
                TypeExtractor.getUnaryOperatorReturnType(
                        processFunction,
                        SingleStreamProcessFunction.class,
                        0,
                        1,
                        new int[] {1, 0},
                        getType(),
                        Utils.getCallLocationName(),
                        true);
        ProcessOperator<T, OUT> operator = new ProcessOperator<>(processFunction);

        return transform("Process", outType, operator);
    }

    @Override
    public <OUT1, OUT2> TwoOutputStreams<OUT1, OUT2> process(
            TwoOutputStreamProcessFunction<T, OUT1, OUT2> processFunction) {
        // TODO Impl.
        return null;
    }

    @Override
    public <T_OTHER, OUT> NonKeyedPartitionStream<OUT> connectAndProcess(
            NonKeyedPartitionStream<T_OTHER> other,
            TwoInputStreamProcessFunction<T, T_OTHER, OUT> processFunction) {
        // TODO Impl.
        return null;
    }

    @Override
    public <T_OTHER, OUT> NonKeyedPartitionStream<OUT> connectAndProcess(
            BroadcastStream<T_OTHER> other,
            TwoInputStreamProcessFunction<T, T_OTHER, OUT> processFunction) {
        // TODO Impl.
        return null;
    }

    @Override
    public GlobalStream<T> coalesce() {
        // TODO Impl.
        return null;
    }

    @Override
    public <K> KeyedPartitionStream<K, T> keyBy(Function<T, K> keySelector) {
        // TODO Impl.
        return null;
    }

    @Override
    public NonKeyedPartitionStream<T> shuffle() {
        // TODO Impl.
        return null;
    }

    @Override
    public BroadcastStream<T> broadcast() {
        // TODO Impl.
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

    private <R> NonKeyedPartitionStream<R> transform(
            String operatorName,
            TypeInformation<R> outputTypeInfo,
            OneInputStreamOperator<T, R> operator) {
        // read the output type of the input Transform to coax out errors about MissingTypeInfo
        transformation.getOutputType();

        OneInputTransformation<T, R> resultTransform =
                new OneInputTransformation<>(
                        this.transformation,
                        operatorName,
                        SimpleUdfStreamOperatorFactory.of(operator),
                        outputTypeInfo,
                        // TODO Supports set parallelism.
                        1,
                        true);

        NonKeyedPartitionStream<R> returnStream =
                new NonKeyedPartitionStreamImpl<>(environment, resultTransform);

        environment.addOperator(resultTransform);

        return returnStream;
    }
}
