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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.handler.job.JobVertexNetworkInfoHandler;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Response type of the {@link JobVertexNetworkInfoHandler}. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobVertexNetworkInfo implements ResponseBody {
    public static final String FIELD_NAME_END_TIMESTAMP = "end-timestamp";
    public static final String FIELD_NAME_SUBTASKS = "subtasks";

    /** Immutable singleton instance denoting that the back pressure stats are not available. */
    private static final JobVertexNetworkInfo DEPRECATED_JOB_VERTEX_NETWORK_INFO =
            new JobVertexNetworkInfo(null, null);

    @JsonProperty(FIELD_NAME_END_TIMESTAMP)
    private final Long endTimestamp;

    @JsonProperty(FIELD_NAME_SUBTASKS)
    private final List<SubtaskNetworkInfo> subtasks;

    @JsonCreator
    public JobVertexNetworkInfo(
            @JsonProperty(FIELD_NAME_END_TIMESTAMP) Long endTimestamp,
            @JsonProperty(FIELD_NAME_SUBTASKS) List<SubtaskNetworkInfo> subtasks) {
        this.endTimestamp = endTimestamp;
        this.subtasks = subtasks;
    }

    public static JobVertexNetworkInfo deprecated() {
        return DEPRECATED_JOB_VERTEX_NETWORK_INFO;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobVertexNetworkInfo that = (JobVertexNetworkInfo) o;
        return Objects.equals(endTimestamp, that.endTimestamp)
                && Objects.equals(subtasks, that.subtasks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endTimestamp, subtasks);
    }

    @Nullable
    public Long getEndTimestamp() {
        return endTimestamp;
    }

    @Nullable
    public List<SubtaskNetworkInfo> getSubtasks() {
        return subtasks == null ? null : Collections.unmodifiableList(subtasks);
    }

    // ---------------------------------------------------------------------------------
    // Static helper classes
    // ---------------------------------------------------------------------------------

    /** Nested class to encapsulate the sub tasks network information. */
    public static final class SubtaskNetworkInfo {

        public static final String FIELD_NAME_SUBTASK = "subtask";
        public static final String FIELD_NAME_ATTEMPT_NUMBER = "attempt-number";
        public static final String FIELD_NAME_OUTPUT_QUEUE_SIZE = "output-queue-size";
        public static final String FIELD_NAME_OTHER_CONCURRENT_ATTEMPTS =
                "other-concurrent-attempts";

        @JsonProperty(FIELD_NAME_SUBTASK)
        private final int subtask;

        @JsonProperty(FIELD_NAME_ATTEMPT_NUMBER)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @Nullable
        private final Integer attemptNumber;

        @JsonProperty(FIELD_NAME_OUTPUT_QUEUE_SIZE)
        private final int outputQueueSize;

        @JsonProperty(FIELD_NAME_OTHER_CONCURRENT_ATTEMPTS)
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @Nullable
        private final List<SubtaskNetworkInfo> otherConcurrentAttempts;

        // otherConcurrentAttempts and attemptNumber are Nullable since Jackson will assign null if
        // the fields are absent while parsing
        public SubtaskNetworkInfo(
                @JsonProperty(FIELD_NAME_SUBTASK) int subtask,
                @JsonProperty(FIELD_NAME_ATTEMPT_NUMBER) @Nullable Integer attemptNumber,
                @JsonProperty(FIELD_NAME_OUTPUT_QUEUE_SIZE) int outputQueueSize,
                @JsonProperty(FIELD_NAME_OTHER_CONCURRENT_ATTEMPTS) @Nullable
                        List<SubtaskNetworkInfo> otherConcurrentAttempts) {
            this.subtask = subtask;
            this.attemptNumber = attemptNumber;
            this.outputQueueSize = outputQueueSize;
            this.otherConcurrentAttempts = otherConcurrentAttempts;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SubtaskNetworkInfo that = (SubtaskNetworkInfo) o;
            return subtask == that.subtask
                    && Objects.equals(attemptNumber, that.attemptNumber)
                    && outputQueueSize == that.outputQueueSize
                    && Objects.equals(otherConcurrentAttempts, that.otherConcurrentAttempts);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    subtask,
                    attemptNumber,
                    outputQueueSize,
                    outputQueueSize,
                    otherConcurrentAttempts);
        }

        public int getSubtask() {
            return subtask;
        }

        public int getOutputQueueSize() {
            return outputQueueSize;
        }

        @Nullable
        public Integer getAttemptNumber() {
            return attemptNumber;
        }

        @Nullable
        public List<SubtaskNetworkInfo> getOtherConcurrentAttempts() {
            return otherConcurrentAttempts;
        }
    }
}
