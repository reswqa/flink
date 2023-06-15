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

package org.apache.flink.processfunction.api;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.States.ListStateDeclaration;
import org.apache.flink.api.common.state.States.ValueStateDeclaration;
import org.apache.flink.api.common.state.ValueState;

public interface RuntimeContext {
    <T> ListState<T> getState(ListStateDeclaration<T> stateDeclaration) throws Exception;

    <T> ValueState<T> getState(ValueStateDeclaration<T> stateDeclaration) throws Exception;

    // TODO provide some method related to keyContext. For example: getCurrentKey. I'm not sure if
    // these methods should be placed here?
}
