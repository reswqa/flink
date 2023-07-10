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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.GeneralizedWatermarkDeclaration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.client.deployment.executors.LocalExecutorFactory;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.processfunction.api.ExecutionEnvironment;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream;
import org.apache.flink.processfunction.connector.FromCollectionSource;
import org.apache.flink.processfunction.connector.SupplierSource;
import org.apache.flink.processfunction.stream.NonKeyedPartitionStreamImpl;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.streaming.api.graph.StreamGraphGenerator.DEFAULT_TIME_CHARACTERISTIC;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class ExecutionEnvironmentImpl extends ExecutionEnvironment {
    private final List<Transformation<?>> transformations = new ArrayList<>();

    // Todo merge execution config and configuration.
    private final ExecutionConfig executionConfig = new ExecutionConfig();

    private Configuration configuration = new Configuration();

    private final Map<Class<?>, GeneralizedWatermarkDeclaration> generalizedWatermarkSpecs =
            new HashMap<>();

    public static ExecutionEnvironmentImpl newInstance() {
        return new ExecutionEnvironmentImpl();
    }

    ExecutionEnvironmentImpl() {
        // TODO re-consider the class loader.
        configure(configuration, null);
    }

    @Override
    public void execute() throws Exception {
        StreamGraph streamGraph = getStreamGraph();
        streamGraph.setJobName("Process Function Api Test Job");
        execute(streamGraph);
        // TODO Supports cache for DataStream.
    }

    @Override
    public <OUT>
            NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<OUT> fromSource(
                    Source<OUT, ?, ?> source,
                    WatermarkStrategy<OUT> watermarkStrategy,
                    String sourceName) {
        TypeInformation<OUT> outputType = null;
        if (source instanceof FromCollectionSource) {
            return transformToLegacyFromCollectionSource(
                    ((FromCollectionSource<OUT>) source).getDatas());
        } else if (source instanceof SupplierSource) {
            // Technically speaking, it does not need to be a special case. Some additional work has
            // been done here to avoid the caller manually setting the type information of this
            // source.
            SupplierFunction<OUT> supplierFunction =
                    ((SupplierSource<OUT>) source).getSupplierFunction();
            outputType =
                    TypeExtractor.getUnaryOperatorReturnType(
                            supplierFunction,
                            SupplierFunction.class,
                            -1,
                            0,
                            TypeExtractor.NO_INDEX,
                            null,
                            null,
                            false);
        }

        final TypeInformation<OUT> resolvedTypeInfo =
                getSourceTypeInfo(source, sourceName, Source.class, outputType);

        SourceTransformation<OUT, ?, ?> sourceTransformation =
                new SourceTransformation<>(
                        sourceName,
                        source,
                        // TODO revisit event-time supports later.
                        watermarkStrategy,
                        resolvedTypeInfo,
                        getParallelism(),
                        false);
        return new NonKeyedPartitionStreamImpl<>(this, sourceTransformation);
    }

    @Override
    public ExecutionEnvironment tmpSetRuntimeMode(RuntimeExecutionMode runtimeMode) {
        checkNotNull(runtimeMode);
        configuration.set(ExecutionOptions.RUNTIME_MODE, runtimeMode);
        return this;
    }

    @Override
    public ExecutionEnvironment tmpWithConfiguration(Configuration configuration) {
        this.configuration = checkNotNull(configuration);
        return this;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public ExecutionConfig getExecutionConfig() {
        return executionConfig;
    }

    public int getParallelism() {
        return executionConfig.getParallelism();
    }

    public ExecutionEnvironmentImpl setParallelism(int parallelism) {
        executionConfig.setParallelism(parallelism);
        return this;
    }

    public List<Transformation<?>> getTransformations() {
        return transformations;
    }

    // -----------------------------------------------
    //              Internal Methods
    // -----------------------------------------------

    private <OUT>
            NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<OUT>
                    transformToLegacyFromCollectionSource(Collection<OUT> data) {
        Preconditions.checkNotNull(data, "Collection must not be null");
        if (data.isEmpty()) {
            throw new IllegalArgumentException("Collection must not be empty");
        }

        OUT first = data.iterator().next();
        if (first == null) {
            throw new IllegalArgumentException("Collection must not contain null elements");
        }

        TypeInformation<OUT> typeInfo;
        try {
            typeInfo = TypeExtractor.getForObject(first);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not create TypeInformation for type "
                            + first.getClass()
                            + "; please specify the TypeInformation manually via "
                            + "StreamExecutionEnvironment#fromElements(Collection, TypeInformation)",
                    e);
        }
        return transformToLegacyFromCollectionSource(data, typeInfo);
    }

    public <OUT>
            NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<OUT>
                    transformToLegacyFromCollectionSource(
                            Collection<OUT> data, TypeInformation<OUT> typeInfo) {
        Preconditions.checkNotNull(data, "Collection must not be null");

        // must not have null elements and mixed elements
        FromElementsFunction.checkCollection(data, typeInfo.getTypeClass());

        SourceFunction<OUT> function = new FromElementsFunction<>(data);
        return addLegacySource(function, "Collection Source", typeInfo, Boundedness.BOUNDED);
    }

    private <OUT> NonKeyedPartitionStreamImpl<OUT> addLegacySource(
            SourceFunction<OUT> sourceFunction,
            String sourceName,
            TypeInformation<OUT> typeInformation,
            Boundedness boundedness) {
        final StreamSource<OUT, ?> sourceOperator = new StreamSource<>(sourceFunction);
        return new NonKeyedPartitionStreamImpl<>(
                this,
                new LegacySourceTransformation<>(
                        sourceName,
                        sourceOperator,
                        typeInformation,
                        // TODO Supports configure parallelism
                        1,
                        boundedness,
                        true));
    }

    private <OUT, T extends TypeInformation<OUT>> T getSourceTypeInfo(
            Object source,
            String sourceName,
            Class<?> baseSourceClass,
            TypeInformation<OUT> typeInfo) {
        TypeInformation<OUT> resolvedTypeInfo = typeInfo;
        if (resolvedTypeInfo == null && source instanceof ResultTypeQueryable) {
            resolvedTypeInfo = ((ResultTypeQueryable<OUT>) source).getProducedType();
        }
        if (resolvedTypeInfo == null) {
            try {
                resolvedTypeInfo =
                        TypeExtractor.createTypeInfo(
                                baseSourceClass, source.getClass(), 0, null, null);
            } catch (final InvalidTypesException e) {
                resolvedTypeInfo = (TypeInformation<OUT>) new MissingTypeInfo(sourceName, e);
            }
        }
        return (T) resolvedTypeInfo;
    }

    public void addOperator(Transformation<?> transformation) {
        Preconditions.checkNotNull(transformation, "transformation must not be null.");
        this.transformations.add(transformation);
    }

    public void registerGeneralizedWatermark(GeneralizedWatermarkDeclaration watermarkDeclaration) {
        this.generalizedWatermarkSpecs.put(
                watermarkDeclaration.getWatermarkClazz(), watermarkDeclaration);
    }

    private void execute(StreamGraph streamGraph) throws Exception {
        final JobClient jobClient = executeAsync(streamGraph);

        try {
            if (configuration.getBoolean(DeploymentOptions.ATTACHED)) {
                jobClient.getJobExecutionResult().get();
            }
            // TODO Supports handle JobExecutionResult, including execution time and accumulator.

            // TODO Supports job listeners.
        } catch (Throwable t) {
            // get() on the JobExecutionResult Future will throw an ExecutionException. This
            // behaviour was largely not there in Flink versions before the PipelineExecutor
            // refactoring so we should strip that exception.
            Throwable strippedException = ExceptionUtils.stripExecutionException(t);
            ExceptionUtils.rethrowException(strippedException);
        }
    }

    private JobClient executeAsync(StreamGraph streamGraph) throws Exception {
        checkNotNull(streamGraph, "StreamGraph cannot be null.");
        final PipelineExecutor executor = getPipelineExecutor();

        CompletableFuture<JobClient> jobClientFuture =
                executor.execute(streamGraph, configuration, getClass().getClassLoader());

        try {
            // TODO Supports job listeners.
            // TODO Supports DataStream collect.
            return jobClientFuture.get();
        } catch (ExecutionException executionException) {
            final Throwable strippedException =
                    ExceptionUtils.stripExecutionException(executionException);
            throw new FlinkException(
                    String.format("Failed to execute job '%s'.", streamGraph.getJobName()),
                    strippedException);
        }
    }

    /** Get {@link StreamGraph} and clear all transformations. */
    private StreamGraph getStreamGraph() {
        final StreamGraph streamGraph = getStreamGraphGenerator(transformations).generate();
        transformations.clear();
        return streamGraph;
    }

    private StreamGraphGenerator getStreamGraphGenerator(List<Transformation<?>> transformations) {
        if (transformations.size() <= 0) {
            throw new IllegalStateException(
                    "No operators defined in streaming topology. Cannot execute.");
        }

        // We copy the transformation so that newly added transformations cannot intervene with the
        // stream graph generation.
        return new StreamGraphGenerator(
                        new ArrayList<>(transformations),
                        executionConfig,
                        new CheckpointConfig(),
                        configuration)
                // TODO Re-Consider should we expose the logic of controlling chains to users.
                .setChaining(true)
                .setTimeCharacteristic(DEFAULT_TIME_CHARACTERISTIC)
                .setGeneralizedWatermarkSpecs(generalizedWatermarkSpecs);
    }

    private PipelineExecutor getPipelineExecutor() {
        // TODO Get executor factory via SPI.
        PipelineExecutorFactory executorFactory = new LocalExecutorFactory();
        // TODO Local executor only expect attached mode, remove this after other executor
        // supported.
        configuration.set(DeploymentOptions.ATTACHED, true);
        return executorFactory.getExecutor(configuration);
    }

    @PublicEvolving
    public void configure(ReadableConfig configuration, ClassLoader classLoader) {
        // TODO not all configure are merged.
        configuration
                .getOptional(ExecutionOptions.RUNTIME_MODE)
                .ifPresent(
                        runtimeMode ->
                                this.configuration.set(ExecutionOptions.RUNTIME_MODE, runtimeMode));

        configuration
                .getOptional(ExecutionOptions.BATCH_SHUFFLE_MODE)
                .ifPresent(
                        shuffleMode ->
                                this.configuration.set(
                                        ExecutionOptions.BATCH_SHUFFLE_MODE, shuffleMode));

        configuration
                .getOptional(ExecutionOptions.SORT_INPUTS)
                .ifPresent(
                        sortInputs ->
                                this.configuration.set(ExecutionOptions.SORT_INPUTS, sortInputs));
        configuration
                .getOptional(ExecutionOptions.USE_BATCH_STATE_BACKEND)
                .ifPresent(
                        sortInputs ->
                                this.configuration.set(
                                        ExecutionOptions.USE_BATCH_STATE_BACKEND, sortInputs));
        configuration
                .getOptional(PipelineOptions.NAME)
                .ifPresent(jobName -> this.configuration.set(PipelineOptions.NAME, jobName));

        configuration
                .getOptional(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH)
                .ifPresent(
                        flag ->
                                this.configuration.set(
                                        ExecutionCheckpointingOptions
                                                .ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH,
                                        flag));

        configuration
                .getOptional(PipelineOptions.JARS)
                .ifPresent(jars -> this.configuration.set(PipelineOptions.JARS, jars));

        configuration
                .getOptional(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_ENABLED)
                .ifPresent(
                        flag ->
                                this.configuration.set(
                                        BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_ENABLED,
                                        flag));

        executionConfig.configure(configuration, classLoader);
    }
}
