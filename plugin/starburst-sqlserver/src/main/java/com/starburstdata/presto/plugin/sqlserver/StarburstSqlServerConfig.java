/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerConfig.SqlServerAuthenticationType.PASSWORD;

public class StarburstSqlServerConfig
{
    static final String SQLSERVER_OVERRIDE_CATALOG_NAME = "sqlserver.override-catalog.name";
    static final String SQLSERVER_OVERRIDE_CATALOG_ENABLED = "sqlserver.override-catalog.enabled";

    private boolean impersonationEnabled;
    private boolean overrideCatalogEnabled;
    @Nullable
    private String overrideCatalogName;
    private SqlServerAuthenticationType authenticationType = PASSWORD;

    public enum SqlServerAuthenticationType
    {
        PASSWORD,
        PASSWORD_PASS_THROUGH,
        KERBEROS,
        KERBEROS_PASS_THROUGH,
    }

    private boolean bulkCopyForWriteLockDestinationTable;

    public boolean isImpersonationEnabled()
    {
        return impersonationEnabled;
    }

    @Config("sqlserver.impersonation.enabled")
    public StarburstSqlServerConfig setImpersonationEnabled(boolean impersonationEnabled)
    {
        this.impersonationEnabled = impersonationEnabled;
        return this;
    }

    public boolean isOverrideCatalogEnabled()
    {
        return overrideCatalogEnabled;
    }

    @Config(SQLSERVER_OVERRIDE_CATALOG_ENABLED)
    @ConfigDescription("Enable catalog override using system property")
    public StarburstSqlServerConfig setOverrideCatalogEnabled(boolean overrideCatalogEnabled)
    {
        this.overrideCatalogEnabled = overrideCatalogEnabled;
        return this;
    }

    public Optional<String> getOverrideCatalogName()
    {
        return Optional.ofNullable(overrideCatalogName);
    }

    @Config(SQLSERVER_OVERRIDE_CATALOG_NAME)
    @ConfigDescription("Name of the SQL server catalog to use as a catalog override")
    public StarburstSqlServerConfig setOverrideCatalogName(@Nullable String overrideCatalogName)
    {
        this.overrideCatalogName = overrideCatalogName;
        return this;
    }

    public SqlServerAuthenticationType getAuthenticationType()
    {
        return authenticationType;
    }

    @Config("sqlserver.authentication.type")
    @ConfigDescription("SQL Server authentication mechanism")
    public StarburstSqlServerConfig setAuthenticationType(SqlServerAuthenticationType authenticationType)
    {
        this.authenticationType = authenticationType;
        return this;
    }

    public boolean isBulkCopyForWriteLockDestinationTable()
    {
        return bulkCopyForWriteLockDestinationTable;
    }

    @Config("sqlserver.bulk-copy-for-write.lock-destination-table")
    @ConfigDescription("Obtain a Bulk Update lock on destination table on write")
    public StarburstSqlServerConfig setBulkCopyForWriteLockDestinationTable(boolean bulkCopyForWriteLockDestinationTable)
    {
        this.bulkCopyForWriteLockDestinationTable = bulkCopyForWriteLockDestinationTable;
        return this;
    }

    @PostConstruct
    public void validate()
    {
        verify(
                overrideCatalogEnabled || overrideCatalogName == null || overrideCatalogName.isBlank(),
                SQLSERVER_OVERRIDE_CATALOG_ENABLED + " needs to be set in order to use " + SQLSERVER_OVERRIDE_CATALOG_NAME + " parameter");
    }
}
