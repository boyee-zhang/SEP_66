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
package io.trino.plugin.barbapi;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class BarbApiConfig
{
    private String email;
    private String password;

    @NotNull
    public String getEmail()
    {
        return email;
    }

    @Config("email")
    public BarbApiConfig setEmail(String email)
    {
        this.email = email;
        return this;
    }

    @NotNull
    public String getPassword()
    {
        return password;
    }

    @Config("password")
    public BarbApiConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }
}
