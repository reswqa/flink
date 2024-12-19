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

package org.apache.flink.datastream.impl.stream;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.dsv2.Sink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.datastream.api.extension.window.assigner.WindowAssigner;
import org.apache.flink.datastream.api.extension.window.utils.TaggedUnion;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.BroadcastStream;
import org.apache.flink.datastream.api.stream.GlobalStream;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream.ProcessConfigurableAndTwoNonKeyedPartitionStream;
import org.apache.flink.datastream.api.stream.ProcessConfigurable;
import org.apache.flink.datastream.impl.attribute.AttributeParser;
import org.apache.flink.datastream.impl.extension.window.function.InternalReduceWindowFunction;
import org.apache.flink.datastream.impl.extension.window.function.InternalTwoInputWindowFunction;
import org.apache.flink.datastream.impl.extension.window.function.InternalWindowFunction;
import org.apache.flink.datastream.impl.extension.window.operators.TwoInputWindowProcessOperator;
import org.apache.flink.datastream.impl.extension.window.operators.WindowProcessOperator;
import org.apache.flink.datastream.impl.operators.KeyedProcessOperator;
import org.apache.flink.datastream.impl.operators.KeyedTwoInputBroadcastProcessOperator;
import org.apache.flink.datastream.impl.operators.KeyedTwoInputNonBroadcastProcessOperator;
import org.apache.flink.datastream.impl.operators.KeyedTwoOutputProcessOperator;
import org.apache.flink.datastream.impl.utils.StreamUtils;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.DataStreamV2SinkTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.util.OutputTag;

import java.util.Collections;
import java.util.HashSet;

import static org.apache.flink.datastream.impl.utils.StreamUtils.validateStates;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The implementation of {@link KeyedPartitionStream}. */
public class KeyedPartitionStreamImpl<K, V> extends AbstractDataStream<V>
        implements KeyedPartitionStream<K, V> {

    /**
     * The key selector that can get the key by which the stream if partitioned from the elements.
     */
    private final KeySelector<V, K> keySelector;

    /** The type of the key by which the stream is partitioned. */
    private final TypeInformation<K> keyType;

    public KeyedPartitionStreamImpl(
            AbstractDataStream<V> dataStream, KeySelector<V, K> keySelector) {
        this(
                dataStream,
                keySelector,
                TypeExtractor.getKeySelectorTypes(keySelector, dataStream.getType()));
    }

    public KeyedPartitionStreamImpl(
            AbstractDataStream<V> dataStream,
            KeySelector<V, K> keySelector,
            TypeInformation<K> keyType) {
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
            AbstractDataStream<V> dataStream,
            Transformation<V> partitionTransformation,
            KeySelector<V, K> keySelector,
            TypeInformation<K> keyType) {
        super(dataStream.getEnvironment(), partitionTransformation);
        this.keySelector = keySelector;
        this.keyType = keyType;
    }

    @Override
    public <OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> process(
            OneInputStreamProcessFunction<V, OUT> processFunction) {
        validateStates(
                processFunction.usesStates(),
                new HashSet<>(
                        Collections.singletonList(StateDeclaration.RedistributionMode.IDENTICAL)));

        TypeInformation<OUT> outType;
        outType = StreamUtils.getOutputTypeForOneInputProcessFunction(processFunction, getType());

        Transformation<OUT> transform;
        Configuration configuration = getEnvironment().getConfiguration();
        boolean sortInputs = configuration.get(ExecutionOptions.SORT_INPUTS);

        if (processFunction instanceof InternalWindowFunction) {
            transform = transformWindow(outType, processFunction);
        } else {
            KeyedProcessOperator<K, V, OUT> operator = new KeyedProcessOperator<>(processFunction);
            transform =
                    StreamUtils.getOneInputKeyedTransformation(
                            "KeyedProcess", this, outType, operator, keySelector, keyType);
        }
        transform.setAttribute(AttributeParser.parseAttribute(processFunction));
        environment.addOperator(transform);
        return StreamUtils.wrapWithConfigureHandle(
                new NonKeyedPartitionStreamImpl<>(environment, transform));
    }

    public <OUT, OTHER_V> Transformation<OUT> transformTwoInputWindow(
            TypeInformation<OUT> outType,
            TwoInputNonBroadcastStreamProcessFunction<V, OTHER_V, OUT> processFunction,
            KeyedPartitionStreamImpl<K, OTHER_V> otherStream) {
        // for iterable window.
        if (processFunction instanceof InternalTwoInputWindowFunction) {
            InternalTwoInputWindowFunction<V, OTHER_V, ?, ?, OUT, ?> internalWindowFunction =
                    (InternalTwoInputWindowFunction<V, OTHER_V, ?, ?, OUT, ?>) processFunction;
            WindowAssigner<TaggedUnion<V, OTHER_V>, ?> assigner =
                    internalWindowFunction.getAssigner();
            ListStateDescriptor<V> leftStateDesc =
                    new ListStateDescriptor<>(
                            "two-input-window-iterator-left-state",
                            getType().createSerializer(environment.getExecutionConfig().getSerializerConfig()));
            ListStateDescriptor<V> rightStateDesc =
                    new ListStateDescriptor<>(
                            "two-input-window-iterator-right-state",
                            getType().createSerializer(environment.getExecutionConfig().getSerializerConfig()));

            TwoInputWindowProcessOperator windowProcessOperator =
                    new TwoInputWindowProcessOperator(
                            internalWindowFunction,
                            assigner,
                            internalWindowFunction.getTrigger(),
                            assigner.getWindowSerializer(),
                            leftStateDesc,
                            rightStateDesc,
                            0,
                            null,
                            null,
                            Collections.EMPTY_LIST,
                            Collections.EMPTY_LIST);
            return StreamUtils.getTwoInputTransformation(
                    "Keyed-TwoInput-Window", this, otherStream, outType, windowProcessOperator);
        } else {
            throw new UnsupportedOperationException(
                    processFunction + "is not supported as two input window.");
        }
    }

    public <OUT> Transformation<OUT> transformWindow(
            TypeInformation<OUT> outType,
            OneInputStreamProcessFunction<V, OUT> processFunction) {
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
                            getType().createSerializer(environment.getExecutionConfig().getSerializerConfig()));

            WindowProcessOperator windowProcessOperator =
                    new WindowProcessOperator(
                            internalWindowFunction,
                            assigner,
                            internalWindowFunction.getTrigger(),
                            assigner.getWindowSerializer(),
                            keyType.createSerializer(environment.getExecutionConfig().getSerializerConfig()),
                            stateDesc,
                            0L,
                            null);
            transform = oneInputTransformWithOperator("Window", outType, windowProcessOperator);
        } else if (processFunction instanceof InternalWindowFunction) {
            InternalWindowFunction<V, ?, OUT, ?> internalWindowFunction =
                    (InternalWindowFunction<V, ?, OUT, ?>) processFunction;
            WindowAssigner<V, ?> assigner = internalWindowFunction.getAssigner();
            ListStateDescriptor<V> stateDesc =
                    new ListStateDescriptor<>(
                            "window-iterator-state",
                            getType().createSerializer(environment.getExecutionConfig().getSerializerConfig()));

            WindowProcessOperator windowProcessOperator =
                    new WindowProcessOperator(
                            internalWindowFunction,
                            assigner,
                            internalWindowFunction.getTrigger(),
                            assigner.getWindowSerializer(),
                            keyType.createSerializer(environment.getExecutionConfig().getSerializerConfig()),
                            stateDesc,
                            0L,
                            null);
            transform = oneInputTransformWithOperator("Window", outType, windowProcessOperator);
        } else {
            throw new IllegalArgumentException("Unsupported window function " + processFunction);
        }
        return transform;
    }


    @Override
    public <OUT> ProcessConfigurableAndKeyedPartitionStream<K, OUT> process(
            OneInputStreamProcessFunction<V, OUT> processFunction,
            KeySelector<OUT, K> newKeySelector) {
        validateStates(
                processFunction.usesStates(),
                new HashSet<>(
                        Collections.singletonList(StateDeclaration.RedistributionMode.IDENTICAL)));

        TypeInformation<OUT> outType =
                StreamUtils.getOutputTypeForOneInputProcessFunction(processFunction, getType());
        KeyedProcessOperator<K, V, OUT> operator =
                new KeyedProcessOperator<>(processFunction, checkNotNull(newKeySelector));
        Transformation<OUT> transform =
                StreamUtils.getOneInputKeyedTransformation(
                        "KeyedProcess", this, outType, operator, keySelector, keyType);
        NonKeyedPartitionStreamImpl<OUT> outputStream =
                new NonKeyedPartitionStreamImpl<>(environment, transform);
        transform.setAttribute(AttributeParser.parseAttribute(processFunction));
        environment.addOperator(transform);
        // Construct a keyed stream directly without partitionTransformation to avoid shuffle.
        return StreamUtils.wrapWithConfigureHandle(
                new KeyedPartitionStreamImpl<>(
                        outputStream,
                        transform,
                        newKeySelector,
                        TypeExtractor.getKeySelectorTypes(newKeySelector, outputStream.getType())));
    }

    @Override
    public <OUT1, OUT2> ProcessConfigurableAndTwoKeyedPartitionStreams<K, OUT1, OUT2> process(
            TwoOutputStreamProcessFunction<V, OUT1, OUT2> processFunction,
            KeySelector<OUT1, K> keySelector1,
            KeySelector<OUT2, K> keySelector2) {
        validateStates(
                processFunction.usesStates(),
                new HashSet<>(
                        Collections.singletonList(StateDeclaration.RedistributionMode.IDENTICAL)));

        Tuple2<TypeInformation<OUT1>, TypeInformation<OUT2>> twoOutputType =
                StreamUtils.getOutputTypesForTwoOutputProcessFunction(processFunction, getType());
        TypeInformation<OUT1> firstOutputType = twoOutputType.f0;
        TypeInformation<OUT2> secondOutputType = twoOutputType.f1;
        OutputTag<OUT2> secondOutputTag = new OutputTag<>("Second-Output", secondOutputType);

        KeyedTwoOutputProcessOperator<K, V, OUT1, OUT2> operator =
                new KeyedTwoOutputProcessOperator<>(
                        processFunction, secondOutputTag, keySelector1, keySelector2);
        Transformation<OUT1> mainOutputTransform =
                StreamUtils.getOneInputKeyedTransformation(
                        "Two-Output-Process",
                        this,
                        firstOutputType,
                        operator,
                        keySelector,
                        keyType);
        mainOutputTransform.setAttribute(AttributeParser.parseAttribute(processFunction));
        NonKeyedPartitionStreamImpl<OUT1> nonKeyedMainOutputStream =
                new NonKeyedPartitionStreamImpl<>(environment, mainOutputTransform);
        Transformation<OUT2> sideOutputTransform =
                nonKeyedMainOutputStream.getSideOutputTransform(secondOutputTag);
        NonKeyedPartitionStreamImpl<OUT2> nonKeyedSideStream =
                new NonKeyedPartitionStreamImpl<>(environment, sideOutputTransform);

        // Construct a keyed stream directly without partitionTransformation to avoid shuffle.
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
        environment.addOperator(mainOutputTransform);
        return new ProcessConfigurableAndTwoKeyedPartitionStreamsImpl<>(
                environment, mainOutputTransform, keyedMainOutputStream, keyedSideOutputStream);
    }

    @Override
    public <OUT1, OUT2> ProcessConfigurableAndTwoNonKeyedPartitionStream<OUT1, OUT2> process(
            TwoOutputStreamProcessFunction<V, OUT1, OUT2> processFunction) {
        validateStates(
                processFunction.usesStates(),
                new HashSet<>(
                        Collections.singletonList(StateDeclaration.RedistributionMode.IDENTICAL)));

        Tuple2<TypeInformation<OUT1>, TypeInformation<OUT2>> twoOutputType =
                StreamUtils.getOutputTypesForTwoOutputProcessFunction(processFunction, getType());
        TypeInformation<OUT1> firstOutputType = twoOutputType.f0;
        TypeInformation<OUT2> secondOutputType = twoOutputType.f1;
        OutputTag<OUT2> secondOutputTag = new OutputTag<>("Second-Output", secondOutputType);

        KeyedTwoOutputProcessOperator<K, V, OUT1, OUT2> operator =
                new KeyedTwoOutputProcessOperator<>(processFunction, secondOutputTag);
        Transformation<OUT1> firstTransformation =
                StreamUtils.getOneInputKeyedTransformation(
                        "Two-Output-Process",
                        this,
                        firstOutputType,
                        operator,
                        keySelector,
                        keyType);
        firstTransformation.setAttribute(AttributeParser.parseAttribute(processFunction));
        NonKeyedPartitionStreamImpl<OUT1> firstStream =
                new NonKeyedPartitionStreamImpl<>(environment, firstTransformation);
        NonKeyedPartitionStreamImpl<OUT2> secondStream =
                new NonKeyedPartitionStreamImpl<>(
                        environment, firstStream.getSideOutputTransform(secondOutputTag));
        environment.addOperator(firstTransformation);
        return new ProcessConfigurableAndTwoNonKeyedPartitionStreamImpl<>(
                environment, firstTransformation, firstStream, secondStream);
    }

    @Override
    public <T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            KeyedPartitionStream<K, T_OTHER> other,
            TwoInputNonBroadcastStreamProcessFunction<V, T_OTHER, OUT> processFunction) {
        validateStates(
                processFunction.usesStates(),
                new HashSet<>(
                        Collections.singletonList(StateDeclaration.RedistributionMode.IDENTICAL)));
        other =
                other instanceof ProcessConfigurableAndKeyedPartitionStreamImpl
                        ? ((ProcessConfigurableAndKeyedPartitionStreamImpl) other)
                                .getKeyedPartitionStream()
                        : other;
        TypeInformation<OUT> outTypeInfo =
                StreamUtils.getOutputTypeForTwoInputNonBroadcastProcessFunction(
                        processFunction,
                        getType(),
                        ((KeyedPartitionStreamImpl<K, T_OTHER>) other).getType());

        Transformation<OUT> outTransformation;
        if (processFunction instanceof InternalTwoInputWindowFunction) {
            outTransformation =
                    transformTwoInputWindow(
                            outTypeInfo,
                            processFunction,
                            (KeyedPartitionStreamImpl<K, T_OTHER>) other);
        } else {
            KeyedTwoInputNonBroadcastProcessOperator<K, V, T_OTHER, OUT> processOperator =
                    new KeyedTwoInputNonBroadcastProcessOperator<>(processFunction);
            outTransformation =
                    StreamUtils.getTwoInputTransformation(
                            "Keyed-TwoInput-Process",
                            this,
                            (KeyedPartitionStreamImpl<K, T_OTHER>) other,
                            outTypeInfo,
                            processOperator);
        }
        outTransformation.setAttribute(AttributeParser.parseAttribute(processFunction));
        environment.addOperator(outTransformation);
        return StreamUtils.wrapWithConfigureHandle(
                new NonKeyedPartitionStreamImpl<>(environment, outTransformation));
    }

    @Override
    public <T_OTHER, OUT> ProcessConfigurableAndKeyedPartitionStream<K, OUT> connectAndProcess(
            KeyedPartitionStream<K, T_OTHER> other,
            TwoInputNonBroadcastStreamProcessFunction<V, T_OTHER, OUT> processFunction,
            KeySelector<OUT, K> newKeySelector) {
        validateStates(
                processFunction.usesStates(),
                new HashSet<>(
                        Collections.singletonList(StateDeclaration.RedistributionMode.IDENTICAL)));

        other =
                other instanceof ProcessConfigurableAndKeyedPartitionStreamImpl
                        ? ((ProcessConfigurableAndKeyedPartitionStreamImpl) other)
                                .getKeyedPartitionStream()
                        : other;

        TypeInformation<OUT> outTypeInfo =
                StreamUtils.getOutputTypeForTwoInputNonBroadcastProcessFunction(
                        processFunction,
                        getType(),
                        ((KeyedPartitionStreamImpl<K, T_OTHER>) other).getType());

        KeyedTwoInputNonBroadcastProcessOperator<K, V, T_OTHER, OUT> processOperator =
                new KeyedTwoInputNonBroadcastProcessOperator<>(processFunction, newKeySelector);
        Transformation<OUT> outTransformation =
                StreamUtils.getTwoInputTransformation(
                        "Keyed-TwoInput-Process",
                        this,
                        (KeyedPartitionStreamImpl<K, T_OTHER>) other,
                        outTypeInfo,
                        processOperator);
        outTransformation.setAttribute(AttributeParser.parseAttribute(processFunction));
        NonKeyedPartitionStreamImpl<OUT> nonKeyedOutputStream =
                new NonKeyedPartitionStreamImpl<>(environment, outTransformation);
        environment.addOperator(outTransformation);
        // Construct a keyed stream directly without partitionTransformation to avoid shuffle.
        return StreamUtils.wrapWithConfigureHandle(
                new KeyedPartitionStreamImpl<>(
                        nonKeyedOutputStream,
                        outTransformation,
                        newKeySelector,
                        TypeExtractor.getKeySelectorTypes(
                                newKeySelector, nonKeyedOutputStream.getType())));
    }

    @Override
    public <T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            BroadcastStream<T_OTHER> other,
            TwoInputBroadcastStreamProcessFunction<V, T_OTHER, OUT> processFunction) {
        // no state redistribution mode check is required here, since all redistribution modes are
        // acceptable

        TypeInformation<OUT> outTypeInfo =
                StreamUtils.getOutputTypeForTwoInputBroadcastProcessFunction(
                        processFunction,
                        getType(),
                        ((BroadcastStreamImpl<T_OTHER>) other).getType());
        KeyedTwoInputBroadcastProcessOperator<K, V, T_OTHER, OUT> processOperator =
                new KeyedTwoInputBroadcastProcessOperator<>(processFunction);
        Transformation<OUT> outTransformation =
                StreamUtils.getTwoInputTransformation(
                        "Keyed-TwoInput-Broadcast-Process",
                        this,
                        // we should always take the broadcast input as second input.
                        (BroadcastStreamImpl<T_OTHER>) other,
                        outTypeInfo,
                        processOperator);
        outTransformation.setAttribute(AttributeParser.parseAttribute(processFunction));
        environment.addOperator(outTransformation);
        return StreamUtils.wrapWithConfigureHandle(
                new NonKeyedPartitionStreamImpl<>(environment, outTransformation));
    }

    @Override
    public <T_OTHER, OUT> ProcessConfigurableAndKeyedPartitionStream<K, OUT> connectAndProcess(
            BroadcastStream<T_OTHER> other,
            TwoInputBroadcastStreamProcessFunction<V, T_OTHER, OUT> processFunction,
            KeySelector<OUT, K> newKeySelector) {
        // no state redistribution mode check is required here, since all redistribution modes are
        // acceptable

        TypeInformation<OUT> outTypeInfo =
                StreamUtils.getOutputTypeForTwoInputBroadcastProcessFunction(
                        processFunction,
                        getType(),
                        ((BroadcastStreamImpl<T_OTHER>) other).getType());
        KeyedTwoInputBroadcastProcessOperator<K, V, T_OTHER, OUT> processOperator =
                new KeyedTwoInputBroadcastProcessOperator<>(
                        processFunction, checkNotNull(newKeySelector));
        Transformation<OUT> outTransformation =
                StreamUtils.getTwoInputTransformation(
                        "Keyed-TwoInput-Broadcast-Process",
                        this,
                        // we should always take the broadcast input as second input.
                        (BroadcastStreamImpl<T_OTHER>) other,
                        outTypeInfo,
                        processOperator);

        NonKeyedPartitionStreamImpl<OUT> outputStream =
                new NonKeyedPartitionStreamImpl<>(environment, outTransformation);
        outTransformation.setAttribute(AttributeParser.parseAttribute(processFunction));
        environment.addOperator(outTransformation);
        // Construct a keyed stream directly without partitionTransformation to avoid shuffle.
        return StreamUtils.wrapWithConfigureHandle(
                new KeyedPartitionStreamImpl<>(
                        outputStream,
                        outTransformation,
                        newKeySelector,
                        TypeExtractor.getKeySelectorTypes(newKeySelector, outputStream.getType())));
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

    public TypeInformation<K> getKeyType() {
        return keyType;
    }

    public KeySelector<V, K> getKeySelector() {
        return keySelector;
    }

    @Override
    public ProcessConfigurable<?> toSink(Sink<V> sink) {
        DataStreamV2SinkTransformation<V, V> sinkTransformation =
                StreamUtils.addSinkOperator(this, sink, getType());
        return StreamUtils.wrapWithConfigureHandle(
                new NonKeyedPartitionStreamImpl<>(environment, sinkTransformation));
    }

    // ---------------------
    //   Partitioning
    // ---------------------

    @Override
    public GlobalStream<V> global() {
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
}
