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

package org.apache.flink.api.common;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Abstract class for a custom user configuration object registered at the execution config.
 *
 * <p>This user config is accessible at runtime through
 * getRuntimeContext().getExecutionConfig().GlobalJobParameters()
 */
public class GlobalJobParameters implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Convert UserConfig into a {@code Map<String, String>} representation. This can be used by the
     * runtime, for example for presenting the user config in the web frontend.
     *
     * @return Key/Value representation of the UserConfig
     */
    public Map<String, String> toMap() {
        return Collections.emptyMap();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash();
    }
}
