/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Map;

import static com.google.common.io.Resources.getResource;
import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createSession;
import static java.util.Objects.requireNonNull;

@Test
public abstract class BaseOracleKerberosImpersonationWithAuthToLocalTest
        extends BaseOracleImpersonationWithAuthToLocal
{
    private final Map<String, String> additionalProperties;

    protected BaseOracleKerberosImpersonationWithAuthToLocalTest(Map<String, String> additionalProperties)
    {
        this.additionalProperties = ImmutableMap.copyOf(requireNonNull(additionalProperties, "additionalProperties is null"));
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("connection-url", TestingStarburstOracleServer.getJdbcUrl())
                .put("oracle.impersonation.enabled", "true")
                .put("auth-to-local.config-file", getResource("auth-to-local.json").getPath())
                .put("auth-to-local.refresh-period", "1s")
                .put("oracle.synonyms.enabled", "true")
                .put("oracle.authentication.type", "KERBEROS")
                .put("kerberos.client.principal", "test@TESTING-KRB.STARBURSTDATA.COM")
                .put("kerberos.client.keytab", getResource("krb/client/test.keytab").getPath())
                .put("kerberos.config", getResource("krb/krb5.conf").getPath())
                .putAll(additionalProperties)
                .buildOrThrow();

        return OracleQueryRunner.builder()
                .withUnlockEnterpriseFeatures(true)
                .withConnectorProperties(properties)
                .withSessionModifier(session -> createSession(session.getIdentity().getUser() + "/admin@company.com"))
                .withTables(ImmutableList.of())
                .build();
    }

    @Override
    protected String getProxyUser()
    {
        return "test";
    }
}
