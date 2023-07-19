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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.processfunction.api.builtin.BatchStreamingUnifiedFunctions;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.processfunction.api.stream.BroadcastStream;
import org.apache.flink.processfunction.api.stream.GlobalStream;
import org.apache.flink.processfunction.api.stream.KeyedPartitionStream;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream.ProcessConfigurableAndTwoNonKeyedPartitionStreams;
import org.apache.flink.processfunction.api.stream.ProcessConfigurable;
import org.apache.flink.processfunction.api.windowing.assigner.WindowAssigner;
import org.apache.flink.processfunction.functions.InternalReduceWindowFunction;
import org.apache.flink.processfunction.functions.InternalWindowFunction;
import org.apache.flink.processfunction.functions.SingleStreamReduceFunction;
import org.apache.flink.processfunction.operators.KeyedProcessOperator;
import org.apache.flink.processfunction.operators.KeyedTwoInputProcessOperator;
import org.apache.flink.processfunction.operators.KeyedTwoOutputProcessOperator;
import org.apache.flink.processfunction.operators.WindowProcessOperator;
import org.apache.flink.processfunction.stream.NonKeyedPartitionStreamImpl.NonKeyedTwoOutputStream;
import org.apache.flink.streaming.api.datastream.CustomSinkOperatorUidHashes;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.PfSinkTransformation;
import org.apache.flink.streaming.api.transformations.ReduceTransformation;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.util.OutputTag;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Implementation for {@link KeyedPartitionStream}. */
public class KeyedPartitionStreamImpl<K, V>
        extends ProcessConfigurableDataStream<
                V, KeyedPartitionStream.ProcessConfigurableAndKeyedPartitionStream<K, V>>
        implements KeyedPartitionStream.ProcessConfigurableAndKeyedPartitionStream<K, V> {

    /**
     * The key selector that can get the key by which the stream if partitioned from the elements.
     */
    private final KeySelector<V, K> keySelector;

    /** The type of the key by which the stream is partitioned. */
    private final TypeInformation<K> keyType;

    public KeyedPartitionStreamImpl(DataStream<V> dataStream, KeySelector<V, K> keySelector) {
        this(
                dataStream,
                keySelector,
                TypeExtractor.getKeySelectorTypes(keySelector, dataStream.getType()));
    }

    public KeyedPartitionStreamImpl(
            DataStream<V> dataStream, KeySelector<V, K> keySelector, TypeInformation<K> keyType) {
        this(
                dataStream,
                new PartitionTransformation<>(
                        dataStream.getTransformation(),
                        new KeyGroupStreamPartitioner<>(
                                keySelector,
                                StreamGraphGenerator.DEFAULT_LOWER_BOUND_MAX_PARALLELISM)),
                keySelector,
                keyType);
    }

    /**
     * This can construct a keyed stream directly without partitionTransformation to avoid shuffle.
     */
    public KeyedPartitionStreamImpl(
            DataStream<V> dataStream,
            Transformation<V> partitionTransformation,
            KeySelector<V, K> keySelector,
            TypeInformation<K> keyType) {
        super(dataStream.getEnvironment(), partitionTransformation);
        this.keySelector = keySelector;
        this.keyType = keyType;
    }

    @Override
    public <OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> process(
            SingleStreamProcessFunction<V, OUT> processFunction) {
        TypeInformation<OUT> outType;
        outType = StreamUtils.getOutputTypeForProcessFunction(processFunction, getType());

        Transformation<OUT> transform;
        if (processFunction instanceof SingleStreamReduceFunction) {
            // reduce process
            //noinspection unchecked
            transform =
                    (Transformation<OUT>)
                            transformReduce((SingleStreamReduceFunction<V>) processFunction);
        } else if (processFunction instanceof InternalWindowFunction) {
            transform = transformWindow(outType, processFunction);
        } else {
            Configuration configuration = getEnvironment().getConfiguration();
            boolean sortInputs = configuration.get(ExecutionOptions.SORT_INPUTS);
            // universal process
            KeyedProcessOperator<K, V, OUT> operator =
                    new KeyedProcessOperator<>(processFunction, sortInputs);
            transform = oneInputTransformWithOperator("KeyedProcess", outType, operator);
        }

        return new NonKeyedPartitionStreamImpl<>(environment, transform);
    }

    public <OUT> Transformation<OUT> transformWindow(
            TypeInformation<OUT> outType, SingleStreamProcessFunction<V, OUT> processFunction) {
        Transformation<OUT> transform;
        if (processFunction instanceof InternalReduceWindowFunction) {
            InternalReduceWindowFunction<V, ?> internalWindowFunction =
                    (InternalReduceWindowFunction<V, ?>) processFunction;
            WindowAssigner<V, ?> assigner = internalWindowFunction.getAssigner();
            ReducingStateDescriptor<V> stateDesc =
                    new ReducingStateDescriptor<>(
                            "window-reduce-state",
                            // TODO We can directly pass Functions.ReduceFunction after remove old
                            // datastream api.
                            (ReduceFunction<V>) internalWindowFunction.getReduceFunction()::reduce,
                            getType().createSerializer(environment.getExecutionConfig()));

            WindowProcessOperator windowProcessOperator =
                    new WindowProcessOperator(
                            internalWindowFunction,
                            assigner,
                            internalWindowFunction.getTrigger(),
                            assigner.getWindowSerializer(environment.getExecutionConfig()),
                            keyType.createSerializer(environment.getExecutionConfig()),
                            stateDesc,
                            0L,
                            null,
                            false);
            transform = oneInputTransformWithOperator("Window", outType, windowProcessOperator);
        } else if (processFunction instanceof InternalWindowFunction) {
            InternalWindowFunction<V, ?, OUT, ?> internalWindowFunction =
                    (InternalWindowFunction<V, ?, OUT, ?>) processFunction;
            WindowAssigner<V, ?> assigner = internalWindowFunction.getAssigner();
            ListStateDescriptor<V> stateDesc =
                    new ListStateDescriptor<>(
                            "window-iterator-state",
                            getType().createSerializer(environment.getExecutionConfig()));

            WindowProcessOperator windowProcessOperator =
                    new WindowProcessOperator(
                            internalWindowFunction,
                            assigner,
                            internalWindowFunction.getTrigger(),
                            assigner.getWindowSerializer(environment.getExecutionConfig()),
                            keyType.createSerializer(environment.getExecutionConfig()),
                            stateDesc,
                            0L,
                            null,
                            false);
            transform = oneInputTransformWithOperator("Window", outType, windowProcessOperator);
        } else {
            throw new IllegalArgumentException("Unsupported window function " + processFunction);
        }
        return transform;
    }

    @Override
    public <OUT1, OUT2> ProcessConfigurableAndTwoKeyedPartitionStreams<K, OUT1, OUT2> process(
            TwoOutputStreamProcessFunction<V, OUT1, OUT2> processFunction,
            KeySelector<OUT1, K> keySelector1,
            KeySelector<OUT2, K> keySelector2) {
        Tuple2<TypeInformation<OUT1>, TypeInformation<OUT2>> twoOutputType =
                StreamUtils.getTwoOutputType(processFunction, getType());
        TypeInformation<OUT1> firstOutputType = twoOutputType.f0;
        TypeInformation<OUT2> secondTOutputType = twoOutputType.f1;
        OutputTag<OUT2> secondOutputTag = new OutputTag<>("Second-Output", secondTOutputType);

        Configuration configuration = getEnvironment().getConfiguration();
        boolean sortInputs = configuration.get(ExecutionOptions.SORT_INPUTS);

        KeyedTwoOutputProcessOperator<K, V, OUT1, OUT2> operator =
                new KeyedTwoOutputProcessOperator<>(
                        processFunction, secondOutputTag, sortInputs, keySelector1, keySelector2);
        Transformation<OUT1> mainOutputTransform =
                oneInputTransformWithOperator("Two-Output-Operator", firstOutputType, operator);
        NonKeyedPartitionStreamImpl<OUT1> nonKeyedMainOutputStream =
                new NonKeyedPartitionStreamImpl<>(environment, mainOutputTransform);
        Transformation<OUT2> sideOutputTransform =
                nonKeyedMainOutputStream.getSideOutputTransform(secondOutputTag);
        NonKeyedPartitionStreamImpl<OUT2> nonKeyedSideStream =
                new NonKeyedPartitionStreamImpl<>(environment, sideOutputTransform);

        // Note: Construct a keyed stream directly without partitionTransformation to avoid
        // shuffle.
        KeyedPartitionStreamImpl<K, OUT1> keyedMainOutputStream =
                new KeyedPartitionStreamImpl<>(
                        nonKeyedMainOutputStream,
                        mainOutputTransform,
                        keySelector1,
                        TypeExtractor.getKeySelectorTypes(
                                keySelector1, nonKeyedMainOutputStream.getType()));
        KeyedPartitionStreamImpl<K, OUT2> keyedSideOutputStream =
                new KeyedPartitionStreamImpl<>(
                        nonKeyedSideStream,
                        sideOutputTransform,
                        keySelector2,
                        TypeExtractor.getKeySelectorTypes(
                                keySelector2, nonKeyedSideStream.getType()));
        return KeyedTwoOutputStream.of(keyedMainOutputStream, keyedSideOutputStream);
    }

    private Transformation<V> transformReduce(SingleStreamReduceFunction<V> processFunction) {
        BatchStreamingUnifiedFunctions.ReduceFunction<V> reduceFunction =
                processFunction.getReduceFunction();
        ReduceTransformation<V, K> reduce =
                new ReduceTransformation<>(
                        "Keyed Reduce",
                        environment.getParallelism(),
                        transformation,
                        // TODO Supports clean closure.
                        // We can directly pass Functions.ReduceFunction after remove old datastream
                        // api.
                        (ReduceFunction<V>) reduceFunction::reduce,
                        keySelector,
                        keyType,
                        true);
        environment.addOperator(reduce);
        return reduce;
    }

    @Override
    public <OUT> ProcessConfigurableAndKeyedPartitionStream<K, OUT> process(
            SingleStreamProcessFunction<V, OUT> processFunction,
            KeySelector<OUT, K> newKeySelector) {
        TypeInformation<OUT> outType =
                StreamUtils.getOutputTypeForProcessFunction(processFunction, getType());
        Configuration configuration = getEnvironment().getConfiguration();
        boolean sortInputs = configuration.get(ExecutionOptions.SORT_INPUTS);
        // TODO Supports checking key for non-process operator(i.e. ReduceOperator).
        KeyedProcessOperator<K, V, OUT> operator =
                new KeyedProcessOperator<>(
                        processFunction, sortInputs, checkNotNull(newKeySelector));
        Transformation<OUT> transform =
                oneInputTransformWithOperator("KeyedProcess", outType, operator);
        NonKeyedPartitionStreamImpl<OUT> outputStream =
                new NonKeyedPartitionStreamImpl<>(environment, transform);
        // Note: Construct a keyed stream directly without partitionTransformation to avoid
        // shuffle.
        return new KeyedPartitionStreamImpl<>(
                outputStream,
                transform,
                newKeySelector,
                TypeExtractor.getKeySelectorTypes(newKeySelector, outputStream.getType()));
    }

    @Override
    public <OUT1, OUT2> ProcessConfigurableAndTwoNonKeyedPartitionStreams<OUT1, OUT2> process(
            TwoOutputStreamProcessFunction<V, OUT1, OUT2> processFunction) {
        Tuple2<TypeInformation<OUT1>, TypeInformation<OUT2>> twoOutputType =
                StreamUtils.getTwoOutputType(processFunction, getType());
        TypeInformation<OUT1> firstOutputType = twoOutputType.f0;
        TypeInformation<OUT2> secondTOutputType = twoOutputType.f1;
        OutputTag<OUT2> secondOutputTag = new OutputTag<>("Second-Output", secondTOutputType);

        Configuration configuration = getEnvironment().getConfiguration();
        boolean sortInputs = configuration.get(ExecutionOptions.SORT_INPUTS);

        KeyedTwoOutputProcessOperator<K, V, OUT1, OUT2> operator =
                new KeyedTwoOutputProcessOperator<>(
                        processFunction, secondOutputTag, sortInputs, null, null);
        Transformation<OUT1> firstTransformation =
                oneInputTransformWithOperator("Two-Output-Operator", firstOutputType, operator);
        NonKeyedPartitionStreamImpl<OUT1> firstStream =
                new NonKeyedPartitionStreamImpl<>(environment, firstTransformation);
        NonKeyedPartitionStreamImpl<OUT2> secondStream =
                new NonKeyedPartitionStreamImpl<>(
                        environment, firstStream.getSideOutputTransform(secondOutputTag));
        return NonKeyedTwoOutputStream.of(firstStream, secondStream);
    }

    @Override
    public <T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            KeyedPartitionStream<K, T_OTHER> other,
            TwoInputStreamProcessFunction<V, T_OTHER, OUT> processFunction) {
        TypeInformation<OUT> outTypeInfo =
                StreamUtils.getOutputTypeForTwoInputProcessFunction(
                        processFunction,
                        getType(),
                        ((KeyedPartitionStreamImpl<K, T_OTHER>) other).getType());
        Configuration configuration = getEnvironment().getConfiguration();
        boolean sortInputs = configuration.get(ExecutionOptions.SORT_INPUTS);
        KeyedTwoInputProcessOperator<K, V, T_OTHER, OUT> processOperator =
                new KeyedTwoInputProcessOperator<>(processFunction, sortInputs);
        Transformation<OUT> outTransformation =
                StreamUtils.getTwoInputTransform(
                        "Keyed-TwoInput-Process",
                        this,
                        (KeyedPartitionStreamImpl<K, T_OTHER>) other,
                        outTypeInfo,
                        processOperator);
        return new NonKeyedPartitionStreamImpl<>(environment, outTransformation);
    }

    @Override
    public <T_OTHER, OUT> ProcessConfigurableAndKeyedPartitionStream<K, OUT> connectAndProcess(
            KeyedPartitionStream<K, T_OTHER> other,
            TwoInputStreamProcessFunction<V, T_OTHER, OUT> processFunction,
            KeySelector<OUT, K> newKeySelector) {
        TypeInformation<OUT> outTypeInfo =
                StreamUtils.getOutputTypeForTwoInputProcessFunction(
                        processFunction,
                        getType(),
                        ((KeyedPartitionStreamImpl<K, T_OTHER>) other).getType());

        Configuration configuration = getEnvironment().getConfiguration();
        boolean sortInputs = configuration.get(ExecutionOptions.SORT_INPUTS);
        KeyedTwoInputProcessOperator<K, V, T_OTHER, OUT> processOperator =
                new KeyedTwoInputProcessOperator<>(processFunction, sortInputs, newKeySelector);
        Transformation<OUT> outTransformation =
                StreamUtils.getTwoInputTransform(
                        "Keyed-TwoInput-Process",
                        this,
                        (KeyedPartitionStreamImpl<K, T_OTHER>) other,
                        outTypeInfo,
                        processOperator);
        NonKeyedPartitionStreamImpl<OUT> nonKeyedOutputStream =
                new NonKeyedPartitionStreamImpl<>(environment, outTransformation);
        // Note: Construct a keyed stream directly without partitionTransformation to avoid
        // shuffle.
        return new KeyedPartitionStreamImpl<>(
                nonKeyedOutputStream,
                outTransformation,
                newKeySelector,
                TypeExtractor.getKeySelectorTypes(newKeySelector, nonKeyedOutputStream.getType()));
    }

    @Override
    public <T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            BroadcastStream<T_OTHER> other,
            TwoInputStreamProcessFunction<V, T_OTHER, OUT> processFunction) {
        TypeInformation<OUT> outTypeInfo =
                StreamUtils.getOutputTypeForTwoInputProcessFunction(
                        processFunction,
                        getType(),
                        ((BroadcastStreamImpl<T_OTHER>) other).getType());
        Configuration configuration = getEnvironment().getConfiguration();
        boolean sortInputs = configuration.get(ExecutionOptions.SORT_INPUTS);
        KeyedTwoInputProcessOperator<K, V, T_OTHER, OUT> processOperator =
                new KeyedTwoInputProcessOperator<>(processFunction, sortInputs);
        Transformation<OUT> outTransformation =
                StreamUtils.getTwoInputTransform(
                        "Broadcast-Keyed-TwoInput-Process",
                        this,
                        (BroadcastStreamImpl<T_OTHER>) other,
                        outTypeInfo,
                        processOperator);
        return new NonKeyedPartitionStreamImpl<>(environment, outTransformation);
    }

    @Override
    public GlobalStream<V> coalesce() {
        return new GlobalStreamImpl<>(
                environment,
                new PartitionTransformation<>(transformation, new GlobalPartitioner<>()));
    }

    @Override
    public <NEW_KEY> KeyedPartitionStream<NEW_KEY, V> keyBy(KeySelector<V, NEW_KEY> keySelector) {
        // Create a new keyed stream with different key selector.
        return new KeyedPartitionStreamImpl<>(this, keySelector);
    }

    @Override
    public NonKeyedPartitionStream<V> shuffle() {
        return new NonKeyedPartitionStreamImpl<>(
                environment,
                new PartitionTransformation<>(getTransformation(), new ShufflePartitioner<>()));
    }

    @Override
    public BroadcastStream<V> broadcast() {
        return new BroadcastStreamImpl<>(environment, getTransformation());
    }

    @Override
    public ProcessConfigurable<?> sinkTo(Sink<V> sink) {
        // read the output type of the input Transform to coax out errors about MissingTypeInfo
        transformation.getOutputType();
        PfSinkTransformation<V, V> sinkTransformation =
                new PfSinkTransformation<>(
                        this,
                        sink,
                        getType(),
                        "Sink",
                        getEnvironment().getParallelism(),
                        false,
                        CustomSinkOperatorUidHashes.DEFAULT);
        this.getEnvironment().addOperator(sinkTransformation);
        return new NonKeyedPartitionStreamImpl<>(environment, sinkTransformation);
    }

    public TypeInformation<K> getKeyType() {
        return keyType;
    }

    public KeySelector<V, K> getKeySelector() {
        return keySelector;
    }

    private <R> Transformation<R> oneInputTransformWithOperator(
            String operatorName,
            TypeInformation<R> outputTypeInfo,
            OneInputStreamOperator<V, R> operator) {
        // read the output type of the input Transform to coax out errors about MissingTypeInfo
        transformation.getOutputType();

        OneInputTransformation<V, R> resultTransform =
                new OneInputTransformation<>(
                        this.transformation,
                        operatorName,
                        SimpleUdfStreamOperatorFactory.of(operator),
                        outputTypeInfo,
                        environment.getParallelism(),
                        false);

        environment.addOperator(resultTransform);

        // inject the key selector and key type
        resultTransform.setStateKeySelector(keySelector);
        resultTransform.setStateKeyType(keyType);

        return resultTransform;
    }

    static class KeyedTwoOutputStream<K, OUT1, OUT2>
            extends ProcessConfigurableDataStream<
                    OUT1, ProcessConfigurableAndTwoKeyedPartitionStreams<K, OUT1, OUT2>>
            implements ProcessConfigurableAndTwoKeyedPartitionStreams<K, OUT1, OUT2> {

        private final KeyedPartitionStream<K, OUT1> firstStream;

        private final KeyedPartitionStream<K, OUT2> secondStream;

        public static <K, OUT1, OUT2> KeyedTwoOutputStream<K, OUT1, OUT2> of(
                KeyedPartitionStreamImpl<K, OUT1> firstStream,
                KeyedPartitionStreamImpl<K, OUT2> secondStream) {
            return new KeyedTwoOutputStream<>(firstStream, secondStream);
        }

        private KeyedTwoOutputStream(
                KeyedPartitionStreamImpl<K, OUT1> firstStream,
                KeyedPartitionStreamImpl<K, OUT2> secondStream) {
            super(firstStream.environment, firstStream.transformation);
            this.firstStream = firstStream;
            this.secondStream = secondStream;
        }

        @Override
        public KeyedPartitionStream<K, OUT1> getFirst() {
            return firstStream;
        }

        @Override
        public KeyedPartitionStream<K, OUT2> getSecond() {
            return secondStream;
        }
    }
}
