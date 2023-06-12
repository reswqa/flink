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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.util.Preconditions;

/**
 * Base class for all streams.
 *
 * <p>Note: This is only used for internal implementation. It must not leak to user face api.
 */
public abstract class DataStream<T> {
    protected final ExecutionEnvironmentImpl environment;

    protected final Transformation<T> transformation;

    public DataStream(ExecutionEnvironmentImpl environment, Transformation<T> transformation) {
        this.environment =
                Preconditions.checkNotNull(environment, "Execution Environment must not be null.");
        this.transformation =
                Preconditions.checkNotNull(
                        transformation, "Stream Transformation must not be null.");
    }

    /**
     * Gets the type of the stream.
     *
     * @return The type of the DataStream.
     */
    public TypeInformation<T> getType() {
        return transformation.getOutputType();
    }

    /** This is only used for internal implementation. It must not leak to user face api. */
    public Transformation<T> getTransformation() {
        return transformation;
    }

    public ExecutionEnvironmentImpl getEnvironment() {
        return environment;
    }
}
