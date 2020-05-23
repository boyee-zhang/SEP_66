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
package io.prestosql.server;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.DefunctConfig;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotNull;

import java.util.Optional;

@DefunctConfig({
        "internal-communication.kerberos.enabled",
        "internal-communication.kerberos.use-canonical-hostname"
})
public class InternalCommunicationConfig
{
    private String sharedSecret;
    private boolean internalJwtEnabled = true;
    private boolean httpsRequired;
    private String keyStorePath;
    private String keyStorePassword;
    private String trustStorePath;
    private String trustStorePassword;

    @NotNull
    public Optional<String> getSharedSecret()
    {
        return Optional.ofNullable(sharedSecret);
    }

    @ConfigSecuritySensitive
    @Config("internal-communication.shared-secret")
    public InternalCommunicationConfig setSharedSecret(String sharedSecret)
    {
        this.sharedSecret = sharedSecret;
        return this;
    }

    public boolean isInternalJwtEnabled()
    {
        return internalJwtEnabled;
    }

    @Config("internal-communication.jwt.enabled")
    public InternalCommunicationConfig setInternalJwtEnabled(boolean internalJwtEnabled)
    {
        this.internalJwtEnabled = internalJwtEnabled;
        return this;
    }

    public boolean isHttpsRequired()
    {
        return httpsRequired;
    }

    @Config("internal-communication.https.required")
    public InternalCommunicationConfig setHttpsRequired(boolean httpsRequired)
    {
        this.httpsRequired = httpsRequired;
        return this;
    }

    public String getKeyStorePath()
    {
        return keyStorePath;
    }

    @Config("internal-communication.https.keystore.path")
    public InternalCommunicationConfig setKeyStorePath(String keyStorePath)
    {
        this.keyStorePath = keyStorePath;
        return this;
    }

    public String getKeyStorePassword()
    {
        return keyStorePassword;
    }

    @Config("internal-communication.https.keystore.key")
    @ConfigSecuritySensitive
    public InternalCommunicationConfig setKeyStorePassword(String keyStorePassword)
    {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    public String getTrustStorePath()
    {
        return trustStorePath;
    }

    @Config("internal-communication.https.truststore.path")
    public InternalCommunicationConfig setTrustStorePath(String trustStorePath)
    {
        this.trustStorePath = trustStorePath;
        return this;
    }

    public String getTrustStorePassword()
    {
        return trustStorePassword;
    }

    @Config("internal-communication.https.truststore.key")
    @ConfigSecuritySensitive
    public InternalCommunicationConfig setTrustStorePassword(String trustStorePassword)
    {
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    @AssertTrue(message = "Internal shared secret is required when HTTPS is enabled for internal communications")
    public boolean isRequiredSharedSecretSet()
    {
        return !isHttpsRequired() || getSharedSecret().isPresent();
    }
}
