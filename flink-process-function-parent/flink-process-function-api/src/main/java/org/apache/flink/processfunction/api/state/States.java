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

package org.apache.flink.processfunction.api.state;

import org.apache.flink.api.common.typeinfo.TypeDescriptor;
import org.apache.flink.processfunction.api.state.StateDeclaration.ListStateDeclaration;
import org.apache.flink.processfunction.api.state.StateDeclaration.ValueStateDeclaration;

public class States {
    private static final Class<?> INSTANCE;

    static {
        try {
            INSTANCE = Class.forName("org.apache.flink.processfunction.state.StatesImpl");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    "Please ensure that flink-process-function in your class path");
        }
    }

    public static <T> ListStateDeclaration listState(
            String name, TypeDescriptor<T> typeDescriptor) {
        try {
            return (ListStateDeclaration)
                    INSTANCE.getMethod("listState", String.class, TypeDescriptor.class)
                            .invoke(null, name, typeDescriptor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> ListStateDeclaration splitRedistributionListState(
            String name, TypeDescriptor<T> typeDescriptor) {
        try {
            return (ListStateDeclaration)
                    INSTANCE.getMethod(
                                    "splitRedistributionListState",
                                    String.class,
                                    TypeDescriptor.class)
                            .invoke(null, name, typeDescriptor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> ListStateDeclaration unionRedistributionListState(
            String name, TypeDescriptor<T> typeDescriptor) {
        try {
            return (ListStateDeclaration)
                    INSTANCE.getMethod(
                                    "unionRedistributionListState",
                                    String.class,
                                    TypeDescriptor.class)
                            .invoke(null, name, typeDescriptor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> ValueStateDeclaration valueState(
            String name, TypeDescriptor<T> typeDescriptor) {
        try {
            return (ValueStateDeclaration)
                    INSTANCE.getMethod("valueState", String.class, TypeDescriptor.class)
                            .invoke(null, name, typeDescriptor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <K, V> ValueStateDeclaration broadcastMapState(
            String name,
            TypeDescriptor<K> keyTypeDescriptor,
            TypeDescriptor<V> valueTypeDescriptor) {
        try {
            return (ValueStateDeclaration)
                    INSTANCE.getMethod(
                                    "broadcastMapState",
                                    String.class,
                                    TypeDescriptor.class,
                                    TypeDescriptor.class)
                            .invoke(null, name, keyTypeDescriptor, valueTypeDescriptor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
