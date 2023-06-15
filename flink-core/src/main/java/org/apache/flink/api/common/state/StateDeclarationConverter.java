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

package org.apache.flink.api.common.state;

import org.apache.flink.api.common.state.States.ListStateDeclaration;
import org.apache.flink.api.common.state.States.StateDeclaration;
import org.apache.flink.api.common.state.States.ValueStateDeclaration;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeInformationUtils;

/** Utils to convert {@link StateDeclaration} to {@link StateDescriptor}. */
public class StateDeclarationConverter {
    public static <T> ListStateDescriptor<T> getListStateDescriptor(
            ListStateDeclaration<T> stateDeclaration) {
        //noinspection unchecked
        return new ListStateDescriptor<>(
                stateDeclaration.getName(),
                (TypeInformation<T>)
                        TypeInformationUtils.fromTypeDescriptor(
                                stateDeclaration.getElementTypeDescriptor()));
    }

    public static <T> ValueStateDescriptor<T> getValueStateDescriptor(
            ValueStateDeclaration<T> stateDeclaration) {
        //noinspection unchecked
        return new ValueStateDescriptor<>(
                stateDeclaration.getName(),
                (TypeInformation<T>)
                        TypeInformationUtils.fromTypeDescriptor(
                                stateDeclaration.getTypeDescriptor()));
    }
}
