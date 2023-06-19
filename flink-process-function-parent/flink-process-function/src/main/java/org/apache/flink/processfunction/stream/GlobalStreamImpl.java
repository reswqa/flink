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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.processfunction.DataStream;
import org.apache.flink.processfunction.ExecutionEnvironmentImpl;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.processfunction.api.stream.BroadcastStream;
import org.apache.flink.processfunction.api.stream.GlobalStream;
import org.apache.flink.processfunction.api.stream.KeyedPartitionStream;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream;
import org.apache.flink.processfunction.operators.ProcessOperator;
import org.apache.flink.processfunction.operators.TwoInputProcessOperator;
import org.apache.flink.processfunction.operators.TwoOutputProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.function.ConsumerFunction;

/** Implementation for {@link GlobalStream}. */
public class GlobalStreamImpl<T> extends DataStream<T> implements GlobalStream<T> {

    public GlobalStreamImpl(
            ExecutionEnvironmentImpl environment, Transformation<T> transformation) {
        super(environment, transformation);
    }

    @Override
    public <OUT> GlobalStream<OUT> process(SingleStreamProcessFunction<T, OUT> processFunction) {
        TypeInformation<OUT> outType =
                StreamUtils.getOutputTypeForProcessFunction(processFunction, getType());
        ProcessOperator<T, OUT> operator = new ProcessOperator<>(processFunction);

        return transform("Global Process", outType, operator);
    }

    @Override
    public <OUT1, OUT2> TwoOutputStreams<OUT1, OUT2> process(
            TwoOutputStreamProcessFunction<T, OUT1, OUT2> processFunction) {
        OutputTag<OUT2> secondOutputTag = new OutputTag<OUT2>("Second-Output") {};

        TypeInformation<OUT1> firstOutputType =
                StreamUtils.getFirstOutputType(processFunction, getType());
        TwoOutputProcessOperator<T, OUT1, OUT2> operator =
                new TwoOutputProcessOperator<>(processFunction, secondOutputTag);
        GlobalStream<OUT1> firstStream =
                transform("Two-Output-Operator", firstOutputType, operator);
        GlobalStream<OUT2> secondStream =
                new GlobalStreamImpl<>(environment, getSideOutputTransform(secondOutputTag));
        return GlobalTwoOutputStream.of(firstStream, secondStream);
    }

    @Override
    public <T_OTHER, OUT> GlobalStream<OUT> connectAndProcess(
            BroadcastStream<T_OTHER> other,
            TwoInputStreamProcessFunction<T, T_OTHER, OUT> processFunction) {
        TypeInformation<OUT> outTypeInfo =
                StreamUtils.getOutputTypeForTwoInputProcessFunction(
                        processFunction,
                        getType(),
                        ((BroadcastStreamImpl<T_OTHER>) other).getType());
        TwoInputProcessOperator<T, T_OTHER, OUT> processOperator =
                new TwoInputProcessOperator<>(processFunction);
        Transformation<OUT> outTransformation =
                StreamUtils.getTwoInputTransform(
                        "Broadcast-Global-TwoInput-Process",
                        this,
                        (BroadcastStreamImpl<T_OTHER>) other,
                        outTypeInfo,
                        processOperator);
        // Operator parallelism should always be 1 for global stream.
        // parallelismConfigured should be true to avoid overwritten by AdaptiveBatchScheduler.
        outTransformation.setParallelism(1, true);
        return new GlobalStreamImpl<>(environment, outTransformation);
    }

    @Override
    public <K> KeyedPartitionStream<K, T> keyBy(KeySelector<T, K> keySelector) {
        return new KeyedPartitionStreamImpl<>(this, keySelector);
    }

    @Override
    public NonKeyedPartitionStream<T> shuffle() {
        return new NonKeyedPartitionStreamImpl<>(
                environment,
                new PartitionTransformation<>(getTransformation(), new ShufflePartitioner<>()));
    }

    @Override
    public BroadcastStream<T> broadcast() {
        return new BroadcastStreamImpl<>(environment, getTransformation());
    }

    @Override
    public void tmpToConsumerSink(ConsumerFunction<T> consumer) {
        Transformation<T> sinkTransformation =
                StreamUtils.getConsumerSinkTransform(transformation, consumer);
        sinkTransformation.setParallelism(1, true);
        environment.addOperator(sinkTransformation);
    }

    private <R> GlobalStream<R> transform(
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
                        // Operator parallelism should always be 1 for global stream.
                        1,
                        // parallelismConfigured should be true to avoid overwritten by
                        // AdaptiveBatchScheduler.
                        true);

        GlobalStream<R> returnStream = new GlobalStreamImpl<>(environment, resultTransform);

        environment.addOperator(resultTransform);

        return returnStream;
    }

    private static class GlobalTwoOutputStream<OUT1, OUT2> implements TwoOutputStreams<OUT1, OUT2> {

        private final GlobalStream<OUT1> firstStream;

        private final GlobalStream<OUT2> secondStream;

        public static <OUT1, OUT2> GlobalTwoOutputStream<OUT1, OUT2> of(
                GlobalStream<OUT1> firstStream, GlobalStream<OUT2> secondStream) {
            return new GlobalTwoOutputStream<>(firstStream, secondStream);
        }

        private GlobalTwoOutputStream(
                GlobalStream<OUT1> firstStream, GlobalStream<OUT2> secondStream) {
            this.firstStream = firstStream;
            this.secondStream = secondStream;
        }

        @Override
        public GlobalStream<OUT1> getFirst() {
            return firstStream;
        }

        @Override
        public GlobalStream<OUT2> getSecond() {
            return secondStream;
        }
    }
}
