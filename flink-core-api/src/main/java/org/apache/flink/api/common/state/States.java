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

import org.apache.flink.api.common.typeinfo.TypeDescriptor;

import java.util.Objects;

/** All related classes of state declaration. */
public class States {
    public static <T> ListStateDeclaration<T> ofList(
            String name, TypeDescriptor<T> elementTypeDescriptor) {
        return new ListStateDeclaration<>(name, elementTypeDescriptor);
    }

    public static <T> ValueStateDeclaration<T> ofValue(
            String name, TypeDescriptor<T> typeDescriptor) {
        return new ValueStateDeclaration<>(name, typeDescriptor);
    }

    /** Declaration for state. */
    public abstract static class StateDeclaration {
        private final String name;

        public StateDeclaration(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            StateDeclaration that = (StateDeclaration) o;
            return Objects.equals(getName(), that.getName());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getName());
        }
    }

    public static class ListStateDeclaration<T> extends StateDeclaration {

        private final TypeDescriptor<T> elementTypeDescriptor;

        private ListStateDeclaration(String name, TypeDescriptor<T> elementTypeDescriptor) {
            super(name);
            this.elementTypeDescriptor = elementTypeDescriptor;
        }

        public TypeDescriptor<T> getElementTypeDescriptor() {
            return elementTypeDescriptor;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            ListStateDeclaration<?> that = (ListStateDeclaration<?>) o;
            return Objects.equals(getElementTypeDescriptor(), that.getElementTypeDescriptor());
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), getElementTypeDescriptor());
        }
    }

    public static class ValueStateDeclaration<T> extends StateDeclaration {

        private final TypeDescriptor<T> typeDescriptor;

        private ValueStateDeclaration(String name, TypeDescriptor<T> typeDescriptor) {
            super(name);
            this.typeDescriptor = typeDescriptor;
        }

        public TypeDescriptor<T> getTypeDescriptor() {
            return typeDescriptor;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            ValueStateDeclaration<?> that = (ValueStateDeclaration<?>) o;
            return Objects.equals(typeDescriptor, that.typeDescriptor);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), typeDescriptor);
        }
    }
}
