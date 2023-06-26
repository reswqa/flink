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

import org.apache.flink.processfunction.api.state.StateDeclaration;

import java.util.Objects;

public class AbstractStateDeclaration implements StateDeclaration {

    private final String name;

    private final RedistributionMode redistributionMode;

    public AbstractStateDeclaration(String name, RedistributionMode redistributionMode) {
        this.name = name;
        this.redistributionMode = redistributionMode;
    }

    public String getName() {
        return name;
    }

    public RedistributionMode getRedistributionMode() {
        return redistributionMode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractStateDeclaration that = (AbstractStateDeclaration) o;
        return Objects.equals(getName(), that.getName())
                && getRedistributionMode() == that.getRedistributionMode();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getRedistributionMode());
    }
}
