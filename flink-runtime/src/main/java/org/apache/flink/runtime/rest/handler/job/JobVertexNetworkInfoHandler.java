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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.JobVertexNetworkInfo;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_GROUP_BUFFERS;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_GROUP_INPUT;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_GROUP_NETTY;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_GROUP_OUTPUT;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_GROUP_SHUFFLE;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_INPUT_EXCLUSIVE_BUFFERS_USAGE;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_INPUT_FLOATING_BUFFERS_USAGE;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_OUTPUT_POOL_USAGE;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_OUTPUT_QUEUE_LENGTH;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_OUTPUT_QUEUE_SIZE;

public class JobVertexNetworkInfoHandler
        extends AbstractRestHandler<
                RestfulGateway,
                EmptyRequestBody,
                JobVertexNetworkInfo,
                JobVertexMessageParameters> {
    private static final String SEPARATOR = ".";

    private static final String NETTY_PREFIX =
            METRIC_GROUP_SHUFFLE + SEPARATOR + METRIC_GROUP_NETTY + SEPARATOR;

    private static final String OUTPUT_BUFFER_PREFIX =
            NETTY_PREFIX
                    + SEPARATOR
                    + METRIC_GROUP_OUTPUT
                    + SEPARATOR
                    + METRIC_GROUP_BUFFERS
                    + SEPARATOR;

    private static final String INPUT_BUFFER_PREFIX =
            NETTY_PREFIX
                    + SEPARATOR
                    + METRIC_GROUP_INPUT
                    + SEPARATOR
                    + METRIC_GROUP_BUFFERS
                    + SEPARATOR;

    private static final String OUTPUT_QUEUE_SIZE = OUTPUT_BUFFER_PREFIX + METRIC_OUTPUT_QUEUE_SIZE;

    private static final String OUTPUT_QUEUE_LENGTH =
            OUTPUT_BUFFER_PREFIX + METRIC_OUTPUT_QUEUE_LENGTH;

    private static final String OUTPUT_POOL_USAGE = OUTPUT_BUFFER_PREFIX + METRIC_OUTPUT_POOL_USAGE;

    private static final String INPUT_QUEUE_SIZE = INPUT_BUFFER_PREFIX + METRIC_OUTPUT_QUEUE_SIZE;

    private static final String INPUT_QUEUE_LENGTH =
            INPUT_BUFFER_PREFIX + METRIC_OUTPUT_QUEUE_LENGTH;

    private static final String INPUT_POOL_USAGE = INPUT_BUFFER_PREFIX + METRIC_OUTPUT_POOL_USAGE;

    private static final String INPUT_EXCLUSIVE_BUFFERS_USAGE =
            INPUT_BUFFER_PREFIX + METRIC_INPUT_EXCLUSIVE_BUFFERS_USAGE;

    private static final String INPUT_FLOATING_BUFFERS_USAGE =
            INPUT_BUFFER_PREFIX + METRIC_INPUT_FLOATING_BUFFERS_USAGE;

    private final MetricFetcher metricFetcher;

    public JobVertexNetworkInfoHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, JobVertexNetworkInfo, JobVertexMessageParameters>
                    messageHeaders,
            MetricFetcher metricFetcher) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        this.metricFetcher = metricFetcher;
    }

    @Override
    protected CompletableFuture<JobVertexNetworkInfo> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        metricFetcher.update();

        final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
        final JobVertexID jobVertexId = request.getPathParameter(JobVertexIdPathParameter.class);

        MetricStore.TaskMetricStore taskMetricStore =
                metricFetcher
                        .getMetricStore()
                        .getTaskMetricStore(jobId.toString(), jobVertexId.toString());
        Map<String, Map<Integer, Integer>> jobRepresentativeExecutions =
                metricFetcher.getMetricStore().getRepresentativeAttempts().get(jobId.toString());
        Map<Integer, Integer> representativeAttempts =
                jobRepresentativeExecutions != null
                        ? jobRepresentativeExecutions.get(jobVertexId.toString())
                        : null;

        return CompletableFuture.completedFuture(
                taskMetricStore != null
                        ? createJobVertexNetworkInfo(taskMetricStore, representativeAttempts)
                        : JobVertexNetworkInfo.deprecated());
    }

    private JobVertexNetworkInfo createJobVertexNetworkInfo(
            MetricStore.TaskMetricStore taskMetricStore,
            Map<Integer, Integer> representativeAttempts) {
        List<JobVertexNetworkInfo.SubtaskNetworkInfo> subtaskNetworkInfos =
                createSubtaskNetworkInfo(taskMetricStore, representativeAttempts);
        return new JobVertexNetworkInfo(metricFetcher.getLastUpdateTime(), subtaskNetworkInfos);
    }

    private List<JobVertexNetworkInfo.SubtaskNetworkInfo> createSubtaskNetworkInfo(
            MetricStore.TaskMetricStore taskMetricStore,
            Map<Integer, Integer> representativeAttempts) {
        Map<Integer, MetricStore.SubtaskMetricStore> subtaskMetricStores =
                taskMetricStore.getAllSubtaskMetricStores();
        List<JobVertexNetworkInfo.SubtaskNetworkInfo> result =
                new ArrayList<>(subtaskMetricStores.size());
        for (Map.Entry<Integer, MetricStore.SubtaskMetricStore> entry :
                subtaskMetricStores.entrySet()) {
            int subtaskIndex = entry.getKey();
            MetricStore.SubtaskMetricStore subtaskMetricStore = entry.getValue();
            Map<Integer, MetricStore.ComponentMetricStore> allAttemptsMetricStores =
                    subtaskMetricStore.getAllAttemptsMetricStores();
            if (allAttemptsMetricStores.isEmpty() || allAttemptsMetricStores.size() == 1) {
                result.add(
                        createSubtaskAttemptNetworkInfo(
                                subtaskIndex, null, subtaskMetricStore, null));
            } else {
                int representativeAttempt =
                        representativeAttempts == null
                                ? -1
                                : representativeAttempts.getOrDefault(subtaskIndex, -1);
                if (!allAttemptsMetricStores.containsKey(representativeAttempt)) {
                    // allAttemptsMetricStores is not empty here
                    representativeAttempt = allAttemptsMetricStores.keySet().iterator().next();
                }
                List<JobVertexNetworkInfo.SubtaskNetworkInfo> otherConcurrentAttempts =
                        new ArrayList<>(allAttemptsMetricStores.size() - 1);
                for (Map.Entry<Integer, MetricStore.ComponentMetricStore> attemptStore :
                        allAttemptsMetricStores.entrySet()) {
                    if (attemptStore.getKey() == representativeAttempt) {
                        continue;
                    }
                    otherConcurrentAttempts.add(
                            createSubtaskAttemptNetworkInfo(
                                    subtaskIndex,
                                    attemptStore.getKey(),
                                    attemptStore.getValue(),
                                    null));
                }
                result.add(
                        createSubtaskAttemptNetworkInfo(
                                subtaskIndex,
                                representativeAttempt,
                                allAttemptsMetricStores.get(representativeAttempt),
                                otherConcurrentAttempts));
            }
        }
        result.sort(Comparator.comparingInt(JobVertexNetworkInfo.SubtaskNetworkInfo::getSubtask));
        return result;
    }

    private JobVertexNetworkInfo.SubtaskNetworkInfo createSubtaskAttemptNetworkInfo(
            int subtaskIndex,
            @Nullable Integer attemptNumber,
            MetricStore.ComponentMetricStore metricStore,
            @Nullable List<JobVertexNetworkInfo.SubtaskNetworkInfo> otherConcurrentAttempts) {
        long outputQueueSize = getOutputQueueSize(metricStore);
        int outputQueueLength = getOutputQueueLength(metricStore);
        float outputPoolUsage = getOutputPoolUsage(metricStore);
        long inputQueueSize = getInputQueueSize(metricStore);
        int inputQueueLength = getInputQueueLength(metricStore);
        float inputPoolUsage = getInputPoolUsage(metricStore);
        float inputExclusiveBufferUsage = getInputExclusiveBufferUsage(metricStore);
        float inputFloatingBufferUsage = getInputFloatingBufferUsage(metricStore);
        return new JobVertexNetworkInfo.SubtaskNetworkInfo(
                subtaskIndex,
                attemptNumber,
                outputQueueSize,
                outputQueueLength,
                outputPoolUsage,
                inputQueueSize,
                inputQueueLength,
                inputPoolUsage,
                inputExclusiveBufferUsage,
                inputFloatingBufferUsage,
                otherConcurrentAttempts);
    }

    private long getOutputQueueSize(MetricStore.ComponentMetricStore metricStore) {
        return Long.parseLong(metricStore.getMetric(OUTPUT_QUEUE_SIZE, "0"));
    }

    private int getOutputQueueLength(MetricStore.ComponentMetricStore metricStore) {
        return Integer.parseInt(metricStore.getMetric(OUTPUT_QUEUE_LENGTH, "0"));
    }

    private float getOutputPoolUsage(MetricStore.ComponentMetricStore metricStore) {
        return Float.parseFloat(metricStore.getMetric(OUTPUT_POOL_USAGE, "0"));
    }

    private long getInputQueueSize(MetricStore.ComponentMetricStore metricStore) {
        return Long.parseLong(metricStore.getMetric(INPUT_QUEUE_SIZE, "0"));
    }

    private int getInputQueueLength(MetricStore.ComponentMetricStore metricStore) {
        return Integer.parseInt(metricStore.getMetric(INPUT_QUEUE_LENGTH, "0"));
    }

    private float getInputPoolUsage(MetricStore.ComponentMetricStore metricStore) {
        return Float.parseFloat(metricStore.getMetric(INPUT_POOL_USAGE, "0"));
    }

    private float getInputExclusiveBufferUsage(MetricStore.ComponentMetricStore metricStore) {
        return Float.parseFloat(metricStore.getMetric(INPUT_EXCLUSIVE_BUFFERS_USAGE, "0"));
    }

    private float getInputFloatingBufferUsage(MetricStore.ComponentMetricStore metricStore) {
        return Float.parseFloat(metricStore.getMetric(INPUT_FLOATING_BUFFERS_USAGE, "0"));
    }
}
