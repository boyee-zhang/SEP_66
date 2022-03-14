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
package io.trino.plugin.base.security;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.security.SystemAccessControl;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.trino.plugin.base.security.CatalogAccessControlRule.AccessMode.ALL;
import static io.trino.plugin.base.security.FileBasedAccessControlConfig.SECURITY_REFRESH_PERIOD;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileBasedSystemAccessControlModule
        extends AbstractConfigurationAwareModule
{
    private static final Logger log = Logger.get(FileBasedSystemAccessControlModule.class);
    private static final String HTTP_CLIENT_NAME = "access-control";

    public FileBasedSystemAccessControlModule(Map<String, String> config)
    {
        super();
        this.setConfigurationFactory(new ConfigurationFactory(config));
    }

    @Override
    public void setup(Binder binder)
    {
        FileBasedAccessControlConfig configuration = buildConfigObject(FileBasedAccessControlConfig.class);
        if (FileBasedAccessControlUtils.isRest(configuration)) {
            binder.bind(new TypeLiteral<Supplier<FileBasedSystemAccessControlRules>>() {})
                    .to(RestFileBasedSystemAccessControlRulesProvider.class)
                    .in(Scopes.SINGLETON);
            httpClientBinder(binder).bindHttpClient(HTTP_CLIENT_NAME, ForAccessControlRules.class)
                    .withConfigDefaults(config -> config
                            .setRequestTimeout(Duration.succinctDuration(10, TimeUnit.SECONDS))
                            .setSelectorCount(1)
                            .setMinThreads(1));
            configBinder(binder).bindConfig(HttpClientConfig.class, HTTP_CLIENT_NAME);
        }
        else {
            binder.bind(new TypeLiteral<Supplier<FileBasedSystemAccessControlRules>>() {})
                    .toProvider(() -> new LocalFileAccessControlRulesProvider<>(configuration, FileBasedSystemAccessControlRules.class))
                    .in(Scopes.SINGLETON);
        }
        configBinder(binder).bindConfig(FileBasedAccessControlConfig.class);
    }

    @Inject
    @Provides
    public SystemAccessControl getSystemAccessControl(FileBasedAccessControlConfig config,
            Supplier<FileBasedSystemAccessControlRules> rulesProvider)
    {
        String configFilePath = config.getConfigFilePath();

        if (config.getRefreshPeriod() != null) {
            Duration refreshPeriod;
            try {
                refreshPeriod = config.getRefreshPeriod();
            }
            catch (IllegalArgumentException e) {
                throw invalidRefreshPeriodException(config, configFilePath);
            }
            if (refreshPeriod.toMillis() == 0) {
                throw invalidRefreshPeriodException(config, configFilePath);
            }
            return ForwardingSystemAccessControl.of(memoizeWithExpiration(
                () -> {
                    log.info("Refreshing system access control from %s", configFilePath);
                    return create(rulesProvider);
                },
                refreshPeriod.toMillis(),
                MILLISECONDS));
        }
        return create(rulesProvider);
    }

    private static TrinoException invalidRefreshPeriodException(FileBasedAccessControlConfig config, String configFileName)
    {
        return new TrinoException(
            CONFIGURATION_INVALID,
            format("Invalid duration value '%s' for property '%s' in '%s'", config.getRefreshPeriod(), SECURITY_REFRESH_PERIOD, configFileName));
    }

    private static SystemAccessControl create(Supplier<FileBasedSystemAccessControlRules> rulesProvider)
    {
        FileBasedSystemAccessControlRules rules = rulesProvider.get();
        List<CatalogAccessControlRule> catalogAccessControlRules;
        if (rules.getCatalogRules().isPresent()) {
            ImmutableList.Builder<CatalogAccessControlRule> catalogRulesBuilder = ImmutableList.builder();
            catalogRulesBuilder.addAll(rules.getCatalogRules().get());

            // Hack to allow Trino Admin to access the "system" catalog for retrieving server status.
            // todo Change userRegex from ".*" to one particular user that Trino Admin will be restricted to run as
            catalogRulesBuilder.add(new CatalogAccessControlRule(
                    ALL,
                    Optional.of(Pattern.compile(".*")),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(Pattern.compile("system"))));
            catalogAccessControlRules = catalogRulesBuilder.build();
        }
        else {
            // if no rules are defined then all access is allowed
            catalogAccessControlRules = ImmutableList.of(CatalogAccessControlRule.ALLOW_ALL);
        }
        return FileBasedSystemAccessControl.builder()
            .setCatalogRules(catalogAccessControlRules)
            .setQueryAccessRules(rules.getQueryAccessRules())
            .setImpersonationRules(rules.getImpersonationRules())
            .setPrincipalUserMatchRules(rules.getPrincipalUserMatchRules())
            .setSystemInformationRules(rules.getSystemInformationRules())
            .setSchemaRules(rules.getSchemaRules().orElse(ImmutableList.of(CatalogSchemaAccessControlRule.ALLOW_ALL)))
            .setTableRules(rules.getTableRules().orElse(ImmutableList.of(CatalogTableAccessControlRule.ALLOW_ALL)))
            .setSessionPropertyRules(rules.getSessionPropertyRules().orElse(ImmutableList.of(SessionPropertyAccessControlRule.ALLOW_ALL)))
            .setCatalogSessionPropertyRules(rules.getCatalogSessionPropertyRules().orElse(ImmutableList.of(CatalogSessionPropertyAccessControlRule.ALLOW_ALL)))
            .build();
    }
}
