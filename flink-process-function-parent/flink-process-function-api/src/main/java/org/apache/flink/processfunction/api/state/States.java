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
    // ------------------------------------------------------------------------
    //  Operator State
    // ------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    public static <T> ListStateDeclaration<T> operatorListState(
            String name, TypeDescriptor<T> typeDescriptor) {
        try {
            return (ListStateDeclaration<T>)
                    INSTANCE.getMethod("operatorListState", String.class, TypeDescriptor.class)
                            .invoke(null, name, typeDescriptor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ------------------------------------------------------------------------
    //  Keyed State
    // ------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    public static <T> ListStateDeclaration<T> keyedListState(
            String name, TypeDescriptor<T> typeDescriptor) {
        try {
            return (ListStateDeclaration<T>)
                    INSTANCE.getMethod("keyedListState", String.class, TypeDescriptor.class)
                            .invoke(null, name, typeDescriptor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> ValueStateDeclaration<T> keyedValueState(
            String name, TypeDescriptor<T> typeDescriptor) {
        try {
            return (ValueStateDeclaration<T>)
                    INSTANCE.getMethod("keyedValueState", String.class, TypeDescriptor.class)
                            .invoke(null, name, typeDescriptor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
