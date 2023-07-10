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

package org.apache.flink.api.common.typeutils;

import java.io.IOException;
import java.util.Objects;
import org.apache.flink.api.common.eventtime.ProcessWatermark;
import org.apache.flink.api.common.eventtime.ProcessWatermarkWrapper;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public class ProcessWatermarkWrapperTypeSerializer
        extends GeneralizedWatermarkTypeSerializer<ProcessWatermarkWrapper> {
    private final TypeSerializer<ProcessWatermark> processWatermarkTypeSerializer;

    public ProcessWatermarkWrapperTypeSerializer(
            TypeSerializer<ProcessWatermark> processWatermarkTypeSerializer) {
        this.processWatermarkTypeSerializer = processWatermarkTypeSerializer;
    }

    @Override
    public TypeSerializer<ProcessWatermarkWrapper> duplicate() {
        return new ProcessWatermarkWrapperTypeSerializer(processWatermarkTypeSerializer);
    }

    @Override
    public ProcessWatermarkWrapper createInstance() {
        return new ProcessWatermarkWrapper(processWatermarkTypeSerializer.createInstance());
    }

    @Override
    public ProcessWatermarkWrapper copy(ProcessWatermarkWrapper from) {
        return new ProcessWatermarkWrapper(
                processWatermarkTypeSerializer.copy(from.getProcessWatermark()));
    }

    @Override
    public ProcessWatermarkWrapper copy(
            ProcessWatermarkWrapper from, ProcessWatermarkWrapper reuse) {
        return new ProcessWatermarkWrapper(
                processWatermarkTypeSerializer.copy(
                        from.getProcessWatermark(), reuse.getProcessWatermark()));
    }

    @Override
    public int getLength() {
        return processWatermarkTypeSerializer.getLength();
    }

    @Override
    public void serialize(ProcessWatermarkWrapper record, DataOutputView target)
            throws IOException {
        processWatermarkTypeSerializer.serialize(record.getProcessWatermark(), target);
    }

    @Override
    public ProcessWatermarkWrapper deserialize(DataInputView source) throws IOException {
        return new ProcessWatermarkWrapper(processWatermarkTypeSerializer.deserialize(source));
    }

    @Override
    public ProcessWatermarkWrapper deserialize(ProcessWatermarkWrapper reuse, DataInputView source)
            throws IOException {
        return new ProcessWatermarkWrapper(
                processWatermarkTypeSerializer.deserialize(reuse.getProcessWatermark(), source));
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        processWatermarkTypeSerializer.copy(source, target);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProcessWatermarkWrapperTypeSerializer that = (ProcessWatermarkWrapperTypeSerializer) o;
        return Objects.equals(processWatermarkTypeSerializer, that.processWatermarkTypeSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processWatermarkTypeSerializer);
    }
}
