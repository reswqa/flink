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

package org.apache.flink.processfunction.state;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeInformationUtils;
import org.apache.flink.processfunction.api.state.StateDeclaration;

/** Utils to convert {@link StateDeclaration} to {@link StateDescriptor}. */
public class StateDeclarationConverter {
    public static StateDescriptor<?, ?> getStateDescriptor(StateDeclaration stateDeclaration) {
        if (stateDeclaration instanceof ListStateDeclarationImpl) {
            return new ListStateDescriptor<>(
                    stateDeclaration.getName(),
                    TypeInformationUtils.fromTypeDescriptor(
                            ((ListStateDeclarationImpl<?>) stateDeclaration)
                                    .getElementTypeDescriptor()));
        } else if (stateDeclaration instanceof MapStateDeclarationImpl) {
            return new MapStateDescriptor<>(
                    stateDeclaration.getName(),
                    TypeInformationUtils.fromTypeDescriptor(
                            ((MapStateDeclarationImpl<?, ?>) stateDeclaration)
                                    .getKeyTypeDescriptor()),
                    TypeInformationUtils.fromTypeDescriptor(
                            ((MapStateDeclarationImpl<?, ?>) stateDeclaration)
                                    .getValueTypeDescriptor()));
        } else if (stateDeclaration instanceof ValueStateDeclarationImpl) {
            return new ValueStateDescriptor<>(
                    stateDeclaration.getName(),
                    TypeInformationUtils.fromTypeDescriptor(
                            ((ValueStateDeclarationImpl<?>) stateDeclaration).getTypeDescriptor()));
        } else {
            throw new IllegalArgumentException(
                    "state declaration : " + stateDeclaration + " is not supported at the moment.");
        }
    }

    public static <T> ListStateDescriptor<T> getListStateDescriptor(
            ListStateDeclarationImpl<T> stateDeclaration) {
        //noinspection unchecked
        return new ListStateDescriptor<>(
                stateDeclaration.getName(),
                (TypeInformation<T>)
                        TypeInformationUtils.fromTypeDescriptor(
                                stateDeclaration.getElementTypeDescriptor()));
    }

    public static <T> ValueStateDescriptor<T> getValueStateDescriptor(
            ValueStateDeclarationImpl<T> stateDeclaration) {
        //noinspection unchecked
        return new ValueStateDescriptor<>(
                stateDeclaration.getName(),
                (TypeInformation<T>)
                        TypeInformationUtils.fromTypeDescriptor(
                                stateDeclaration.getTypeDescriptor()));
    }

    public static <K, V> MapStateDescriptor<K, V> getMapStateDescriptor(
            MapStateDeclarationImpl<K, V> stateDeclaration) {
        //noinspection unchecked
        return new MapStateDescriptor<K, V>(
                stateDeclaration.getName(),
                (TypeInformation<K>)
                        TypeInformationUtils.fromTypeDescriptor(
                                stateDeclaration.getKeyTypeDescriptor()),
                (TypeInformation<V>)
                        TypeInformationUtils.fromTypeDescriptor(
                                stateDeclaration.getValueTypeDescriptor()));
    }
}
