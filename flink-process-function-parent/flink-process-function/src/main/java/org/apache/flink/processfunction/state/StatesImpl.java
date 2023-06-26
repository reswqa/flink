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

import org.apache.flink.api.common.typeinfo.TypeDescriptor;
import org.apache.flink.processfunction.api.state.StateDeclaration;
import org.apache.flink.processfunction.api.state.StateDeclaration.Scope;

/** Provider for all {@link StateDeclaration}. */
public class StatesImpl {

    // ------------------------------------------------------------------------
    //  Operator State
    // ------------------------------------------------------------------------

    public static <T> StateDeclaration.ListStateDeclaration<T> operatorListState(
            String name, TypeDescriptor<T> typeDescriptor) {
        return new ListStateDeclarationImpl<>(name, typeDescriptor, Scope.OPERATOR);
    }

    // ------------------------------------------------------------------------
    //  Keyed State
    // ------------------------------------------------------------------------

    public static <T> StateDeclaration.ListStateDeclaration<T> keyedListState(
            String name, TypeDescriptor<T> typeDescriptor) {
        return new ListStateDeclarationImpl<>(name, typeDescriptor, Scope.KEYED);
    }

    public static <T> StateDeclaration.ValueStateDeclaration<T> keyedValueState(
            String name, TypeDescriptor<T> typeDescriptor) {
        return new ValueStateDeclarationImpl<>(name, typeDescriptor);
    }
}
