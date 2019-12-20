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
package io.prestosql.plugin.jdbc.credential;

import io.prestosql.plugin.jdbc.JdbcIdentity;

import javax.inject.Inject;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ConfigFileBasedCredentialProvider
        implements CredentialProvider
{
    private final Optional<String> connectionUser;
    private final Optional<String> connectionPassword;

    @Inject
    public ConfigFileBasedCredentialProvider(CredentialConfig credentialsConfig)
    {
        requireNonNull(credentialsConfig, "credentialsConfig is null");
        this.connectionUser = credentialsConfig.getConnectionUser();
        this.connectionPassword = credentialsConfig.getConnectionPassword();
    }

    @Override
    public Optional<String> getConnectionUser(Optional<JdbcIdentity> jdbcIdentity)
    {
        return connectionUser;
    }

    @Override
    public Optional<String> getConnectionPassword(Optional<JdbcIdentity> jdbcIdentity)
    {
        return connectionPassword;
    }
}
