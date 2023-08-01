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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.processfunction.ExecutionEnvironmentImpl;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.processfunction.api.stream.BroadcastStream;
import org.apache.flink.processfunction.api.stream.GlobalStream;
import org.apache.flink.processfunction.api.stream.KeyedPartitionStream;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream;
import org.apache.flink.processfunction.api.stream.ProcessConfigurable;
import org.apache.flink.processfunction.functions.InternalWindowFunction;
import org.apache.flink.processfunction.operators.ProcessOperator;
import org.apache.flink.processfunction.operators.TwoInputProcessOperator;
import org.apache.flink.processfunction.operators.TwoOutputProcessOperator;
import org.apache.flink.streaming.api.datastream.CustomSinkOperatorUidHashes;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.PfSinkTransformation;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.util.OutputTag;

/** Implementation for {@link GlobalStream}. */
public class GlobalStreamImpl<T>
        extends ProcessConfigurableDataStream<T, GlobalStream.ProcessConfigurableAndGlobalStream<T>>
        implements GlobalStream.ProcessConfigurableAndGlobalStream<T> {

    public GlobalStreamImpl(
            ExecutionEnvironmentImpl environment, Transformation<T> transformation) {
        super(environment, transformation);
    }

    @Override
    public <OUT> ProcessConfigurableAndGlobalStream<OUT> process(
            SingleStreamProcessFunction<T, OUT> processFunction) {
        TypeInformation<OUT> outType =
                StreamUtils.getOutputTypeForProcessFunction(processFunction, getType());

        if (processFunction instanceof InternalWindowFunction) {
            // Transform to keyed stream.
            KeyedPartitionStreamImpl<Byte, T> keyedStream =
                    new KeyedPartitionStreamImpl<>(
                            this, getTransformation(), new NullByteKeySelector<>(), Types.BYTE);
            Transformation<OUT> outTransformation =
                    keyedStream.transformWindow(outType, processFunction, false);
            outTransformation.setParallelism(1, true);
            return new GlobalStreamImpl<>(keyedStream.environment, outTransformation);
        } else {
            ProcessOperator<T, OUT> operator = new ProcessOperator<>(processFunction);
            return transform("Global Process", outType, operator);
        }
    }

    @Override
    public <OUT1, OUT2> ProcessConfigurableAndTwoGlobalStreams<OUT1, OUT2> process(
            TwoOutputStreamProcessFunction<T, OUT1, OUT2> processFunction) {
        Tuple2<TypeInformation<OUT1>, TypeInformation<OUT2>> twoOutputType =
                StreamUtils.getTwoOutputType(processFunction, getType());
        TypeInformation<OUT1> firstOutputType = twoOutputType.f0;
        TypeInformation<OUT2> secondTOutputType = twoOutputType.f1;
        OutputTag<OUT2> secondOutputTag = new OutputTag<OUT2>("Second-Output", secondTOutputType);

        TwoOutputProcessOperator<T, OUT1, OUT2> operator =
                new TwoOutputProcessOperator<>(processFunction, secondOutputTag);
        GlobalStreamImpl<OUT1> firstStream =
                transform("Two-Output-Operator", firstOutputType, operator);
        GlobalStreamImpl<OUT2> secondStream =
                new GlobalStreamImpl<>(
                        environment, firstStream.getSideOutputTransform(secondOutputTag));
        return GlobalTwoOutputStream.of(firstStream, secondStream);
    }

    @Override
    public <T_OTHER, OUT> ProcessConfigurableAndGlobalStream<OUT> connectAndProcess(
            GlobalStream<T_OTHER> other,
            TwoInputStreamProcessFunction<T, T_OTHER, OUT> processFunction) {
        TypeInformation<OUT> outTypeInfo =
                StreamUtils.getOutputTypeForTwoInputProcessFunction(
                        processFunction, getType(), ((GlobalStreamImpl<T_OTHER>) other).getType());
        TwoInputProcessOperator<T, T_OTHER, OUT> processOperator =
                new TwoInputProcessOperator<>(processFunction);
        Transformation<OUT> outTransformation =
                StreamUtils.getTwoInputTransform(
                        "Global-Global-TwoInput-Process",
                        this,
                        (GlobalStreamImpl<T_OTHER>) other,
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
    public ProcessConfigurable<?> sinkTo(Sink<T> sink) {
        // read the output type of the input Transform to coax out errors about MissingTypeInfo
        transformation.getOutputType();
        PfSinkTransformation<T, T> sinkTransformation =
                new PfSinkTransformation<>(
                        this,
                        sink,
                        getType(),
                        "Sink",
                        getEnvironment().getParallelism(),
                        false,
                        CustomSinkOperatorUidHashes.DEFAULT);
        // Force set parallelism to 1 for global stream.
        sinkTransformation.setParallelism(1, true);
        this.getEnvironment().addOperator(sinkTransformation);
        return new NonKeyedPartitionStreamImpl<>(environment, sinkTransformation);
    }

    @Override
    public GlobalStream.ProcessConfigurableAndGlobalStream<T> withParallelism(int parallelism) {
        throw new UnsupportedOperationException(
                "Set parallelism for global stream is not supported.");
    }

    private <R> GlobalStreamImpl<R> transform(
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

        GlobalStreamImpl<R> returnStream = new GlobalStreamImpl<>(environment, resultTransform);

        environment.addOperator(resultTransform);

        return returnStream;
    }

    private static class GlobalTwoOutputStream<OUT1, OUT2>
            extends ProcessConfigurableDataStream<
                    OUT1, ProcessConfigurableAndTwoGlobalStreams<OUT1, OUT2>>
            implements ProcessConfigurableAndTwoGlobalStreams<OUT1, OUT2> {

        private final GlobalStream<OUT1> firstStream;

        private final GlobalStream<OUT2> secondStream;

        public static <OUT1, OUT2> GlobalTwoOutputStream<OUT1, OUT2> of(
                GlobalStreamImpl<OUT1> firstStream, GlobalStreamImpl<OUT2> secondStream) {
            return new GlobalTwoOutputStream<>(firstStream, secondStream);
        }

        private GlobalTwoOutputStream(
                GlobalStreamImpl<OUT1> firstStream, GlobalStreamImpl<OUT2> secondStream) {
            super(firstStream.environment, firstStream.transformation);
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
