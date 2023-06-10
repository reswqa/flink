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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.client.deployment.executors.LocalExecutorFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.processfunction.api.ExecutionEnvironment;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream;
import org.apache.flink.processfunction.connector.SupplierSourceFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.streaming.api.graph.StreamGraphGenerator.DEFAULT_TIME_CHARACTERISTIC;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class ExecutionEnvironmentImpl extends ExecutionEnvironment {
    private final List<Transformation<?>> transformations = new ArrayList<>();

    private final ExecutionConfig config = new ExecutionConfig();

    private final Configuration configuration = new Configuration();

    public static ExecutionEnvironmentImpl newInstance() {
        return new ExecutionEnvironmentImpl();
    }

    @Override
    public void execute() throws Exception {
        StreamGraph streamGraph = getStreamGraph();
        streamGraph.setJobName("Process Function Api Test Job");
        execute(streamGraph);
        // TODO Supports cache for DataStream.
    }

    @Override
    public <OUT> NonKeyedPartitionStream<OUT> tmpFromSupplierSource(
            SupplierFunction<OUT> supplier) {
        final String sourceName = "Supplier Source";
        // TODO Supports clean closure
        final SupplierSourceFunction<OUT> sourceFunction = new SupplierSourceFunction<>(supplier);
        final TypeInformation<OUT> resolvedTypeInfo =
                TypeExtractor.getUnaryOperatorReturnType(
                        supplier,
                        SupplierFunction.class,
                        -1,
                        0,
                        TypeExtractor.NO_INDEX,
                        null,
                        null,
                        false);

        final StreamSource<OUT, ?> sourceOperator = new StreamSource<>(sourceFunction);
        return new DataStreamImpl<>(
                this,
                new LegacySourceTransformation<>(
                        sourceName,
                        sourceOperator,
                        resolvedTypeInfo,
                        // TODO Supports configure parallelism
                        1,
                        Boundedness.CONTINUOUS_UNBOUNDED,
                        true));
    }

    public void addOperator(Transformation<?> transformation) {
        Preconditions.checkNotNull(transformation, "transformation must not be null.");
        this.transformations.add(transformation);
    }

    // -----------------------------------------------
    //              Internal Methods
    // -----------------------------------------------

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
                        config,
                        new CheckpointConfig(),
                        configuration)
                // TODO Re-Consider should we expose the logic of controlling chains to users.
                .setChaining(true)
                .setTimeCharacteristic(DEFAULT_TIME_CHARACTERISTIC);
    }

    private PipelineExecutor getPipelineExecutor() {
        // TODO Get executor factory via SPI.
        PipelineExecutorFactory executorFactory = new LocalExecutorFactory();
        // TODO Local executor only expect attached mode, remove this after other executor
        // supported.
        configuration.set(DeploymentOptions.ATTACHED, true);
        return executorFactory.getExecutor(configuration);
    }
}
