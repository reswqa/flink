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

package org.apache.flink.datastream.impl.utils;

import org.apache.flink.api.common.state.ListStateDeclaration;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDeclaration;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.java.typeutils.TypeExtractor;

/** Utils to convert {@link StateDeclaration} to {@link StateDescriptor}. */
public class StateDeclarationConverter {
    public static StateDescriptor<?, ?> getStateDescriptor(StateDeclaration stateDeclaration) {
        if (stateDeclaration instanceof ListStateDeclaration) {
            return getListStateDescriptor((ListStateDeclaration<?>) stateDeclaration);
        } else if (stateDeclaration instanceof MapStateDeclaration) {
            return getMapStateDescriptor((MapStateDeclaration<?, ?>) stateDeclaration);
        } else if (stateDeclaration instanceof ValueStateDeclaration) {
            return getValueStateDescriptor((ValueStateDeclaration<?>) stateDeclaration);
        } else {
            throw new IllegalArgumentException(
                    "state declaration : " + stateDeclaration + " is not supported at the moment.");
        }
    }

    public static <T> ListStateDescriptor<T> getListStateDescriptor(
            ListStateDeclaration<T> stateDeclaration) {
        //noinspection unchecked
        return new ListStateDescriptor<>(
                stateDeclaration.getName(),
                TypeExtractor.createTypeInfo(stateDeclaration.getTypeDescriptor().getTypeClass()));
    }

    public static <T> ValueStateDescriptor<T> getValueStateDescriptor(
            ValueStateDeclaration<T> stateDeclaration) {
        //noinspection unchecked
        return new ValueStateDescriptor<>(
                stateDeclaration.getName(),
                TypeExtractor.createTypeInfo(stateDeclaration.getTypeDescriptor().getTypeClass()));
    }

    public static <K, V> MapStateDescriptor<K, V> getMapStateDescriptor(
            MapStateDeclaration<K, V> stateDeclaration) {
        //noinspection unchecked
        return new MapStateDescriptor<K, V>(
                stateDeclaration.getName(),
                TypeExtractor.createTypeInfo(stateDeclaration.getKeyTypeDescriptor().getTypeClass()),
                TypeExtractor.createTypeInfo(stateDeclaration.getValueTypeDescriptor().getTypeClass())
                );
    }
}
