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

package org.apache.flink.process.impl;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;

import java.util.Collections;
import java.util.List;

/** Mock {@link Transformation} for testing. */
public class TestingTransformation<T> extends Transformation<T> {

    public TestingTransformation(String name, TypeInformation<T> outputType, int parallelism) {
        super(name, outputType, parallelism);
    }

    @Override
    public List<Transformation<?>> getTransitivePredecessors() {
        return Collections.emptyList();
    }

    @Override
    public List<Transformation<?>> getInputs() {
        return Collections.emptyList();
    }
}
