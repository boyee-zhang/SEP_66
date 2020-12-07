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
package io.prestosql.plugin.redshift;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.BaseJdbcConfig.LegacyGenericColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.ForBaseJdbc;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import org.postgresql.Driver;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class RedshiftClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(RedshiftClient.class).in(Scopes.SINGLETON);

        // TODO implement correct type mapping for Redshift connector
        configBinder(binder).bindConfigDefaults(BaseJdbcConfig.class, config -> config.setLegacyGenericColumnMapping(LegacyGenericColumnMapping.ENABLE));
    }

    @Singleton
    @Provides
    @ForBaseJdbc
    public static ConnectionFactory getConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider)
    {
        return new DriverConnectionFactory(new Driver(), config, credentialProvider);
    }
}
