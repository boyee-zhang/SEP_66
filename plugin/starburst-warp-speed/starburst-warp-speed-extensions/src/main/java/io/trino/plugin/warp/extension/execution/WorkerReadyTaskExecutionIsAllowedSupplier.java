/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.warp.extension.execution;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.WorkerNodeManager;

import java.util.function.BooleanSupplier;

import static java.util.Objects.requireNonNull;

@Singleton
public class WorkerReadyTaskExecutionIsAllowedSupplier
        implements BooleanSupplier
{
    private final WorkerNodeManager workerNodeManager;

    @Inject
    public WorkerReadyTaskExecutionIsAllowedSupplier(WorkerNodeManager workerNodeManager)
    {
        this.workerNodeManager = requireNonNull(workerNodeManager);
    }

    @Override
    public boolean getAsBoolean()
    {
        return workerNodeManager.isWorkerReady();
    }
}
