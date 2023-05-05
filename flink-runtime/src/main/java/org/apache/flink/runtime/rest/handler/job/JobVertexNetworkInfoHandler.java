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

public class JobVertexNetworkInfoHandler
        extends AbstractRestHandler<
                RestfulGateway,
                EmptyRequestBody,
                JobVertexNetworkInfo,
                JobVertexMessageParameters> {
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
        int outputQueueSize = getOutputQueueSize(metricStore);
        return new JobVertexNetworkInfo.SubtaskNetworkInfo(
                subtaskIndex, attemptNumber, outputQueueSize, otherConcurrentAttempts);
    }

    private int getOutputQueueSize(MetricStore.ComponentMetricStore metricStore) {
        return Integer.parseInt(
                metricStore.getMetric("Shuffle.Netty.Output.Buffers.outputQueueSize", "0"));
    }
}
