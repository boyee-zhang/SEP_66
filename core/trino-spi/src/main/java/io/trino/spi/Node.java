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
package io.trino.spi;

public interface Node
{
    String getHost();

    HostAddress getHostAndPort();

    /**
     * The unique id of the deployment slot in which this binary is running.  This id should
     * represent the physical deployment location and should not change.
     */
    String getNodeIdentifier();

    /**
     * The unique id of this Java VM instance.  This id will change every time the VM is restarted.
     */
    String getInstanceId();

    String getVersion();

    boolean isCoordinator();
}
