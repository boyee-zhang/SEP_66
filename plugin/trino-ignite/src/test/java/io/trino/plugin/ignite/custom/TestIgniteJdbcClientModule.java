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
package io.trino.plugin.ignite.custom;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.trino.plugin.ignite.IgniteJdbcClientModule;
import io.trino.plugin.ignite.IgniteTableProperties;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;

import static io.trino.plugin.jdbc.JdbcModule.bindTablePropertiesProvider;

public class TestIgniteJdbcClientModule
        extends IgniteJdbcClientModule
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(TestIgniteTestClient.class).in(Scopes.SINGLETON);
        bindTablePropertiesProvider(binder, IgniteTableProperties.class);
        binder.install(new DecimalModule());
    }
}
