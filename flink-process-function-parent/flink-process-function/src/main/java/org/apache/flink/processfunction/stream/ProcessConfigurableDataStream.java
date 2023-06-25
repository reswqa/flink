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

import org.apache.flink.api.common.operators.util.OperatorValidationUtils;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.processfunction.ExecutionEnvironmentImpl;
import org.apache.flink.processfunction.api.stream.ProcessConfigurable;

/** A {@link DataStream} but process configurable. */
public class ProcessConfigurableDataStream<T, S extends ProcessConfigurable<S>>
        extends DataStream<T> implements ProcessConfigurable<S> {
    public ProcessConfigurableDataStream(
            ExecutionEnvironmentImpl environment, Transformation<T> transformation) {
        super(environment, transformation);
    }

    /**
     * Sets an ID for this transformation.
     *
     * <p>The specified ID is used to assign the same ID across job submissions (for example when
     * starting a job from a savepoint).
     *
     * <p><strong>Important</strong>: this ID needs to be unique per transformation and job.
     * Otherwise, job submission will fail.
     *
     * @param uid The unique user-specified ID of this transformation.
     * @return The processConfigurable with the specified ID.
     */
    @Override
    public S withUid(String uid) {
        transformation.setUid(uid);
        return (S) this;
    }

    /**
     * Sets the name of the current data stream. This name is used by the visualization and logging
     * during runtime.
     *
     * @return The named processConfigurable.
     */
    @Override
    public S withName(String name) {
        transformation.setName(name);
        return (S) this;
    }

    /**
     * Sets the parallelism for this transformation.
     *
     * @param parallelism The parallelism for this transformation.
     * @return The processConfigurable with set parallelism.
     */
    @Override
    public S withParallelism(int parallelism) {
        OperatorValidationUtils.validateParallelism(parallelism, true);
        transformation.setParallelism(parallelism);
        return (S) this;
    }
}
