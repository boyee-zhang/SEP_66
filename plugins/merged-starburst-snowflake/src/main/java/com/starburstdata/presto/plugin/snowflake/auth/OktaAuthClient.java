/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.auth;

import com.starburstdata.presto.okta.OktaAuthenticationResult;

public interface OktaAuthClient
{
    OktaAuthenticationResult authenticate(String user, String password);

    SamlResponse obtainSamlAssertion(SamlRequest samlRequest, OktaAuthenticationResult authenticationResult);
}
