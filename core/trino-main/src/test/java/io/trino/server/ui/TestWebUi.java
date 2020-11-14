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
package io.trino.server.ui;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.common.io.Resources;
import com.google.inject.Key;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeInfo;
import io.airlift.security.pem.PemReader;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.trino.server.security.PasswordAuthenticatorManager;
import io.trino.server.security.oauth2.OAuth2Client;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import okhttp3.FormBody;
import okhttp3.JavaNetCookieJar;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.CookieManager;
import java.net.HttpCookie;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Principal;
import java.security.PrivateKey;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.LOCATION;
import static com.google.common.net.HttpHeaders.X_FORWARDED_HOST;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PORT;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.trino.client.OkHttpUtil.setupSsl;
import static io.trino.server.security.oauth2.OAuth2CallbackResource.CALLBACK_ENDPOINT;
import static io.trino.server.security.oauth2.OAuth2Service.NONCE;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.DISABLED_LOCATION;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.FORWARD_DISABLED_LOCATION;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.LOGIN_FORM;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_LOGIN;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_LOGOUT;
import static io.trino.testing.assertions.Assert.assertEquals;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static javax.servlet.http.HttpServletResponse.SC_SEE_OTHER;
import static javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertTrue;

@Test
public class TestWebUi
{
    private static final String LOCALHOST_KEYSTORE = Resources.getResource("cert/localhost.pem").getPath();
    private static final ImmutableMap<String, String> SECURE_PROPERTIES = ImmutableMap.<String, String>builder()
            .put("http-server.https.enabled", "true")
            .put("http-server.https.keystore.path", LOCALHOST_KEYSTORE)
            .put("http-server.https.keystore.key", "")
            .put("http-server.process-forwarded", "true")
            .put("http-server.authentication.allow-insecure-over-http", "true")
            .build();
    private static final String STATE_KEY = "test-state-key";
    private static final ImmutableMap<String, String> OAUTH2_PROPERTIES = ImmutableMap.<String, String>builder()
            .putAll(SECURE_PROPERTIES)
            .put("web-ui.authentication.type", "oauth2")
            .put("http-server.authentication.oauth2.state-key", STATE_KEY)
            .put("http-server.authentication.oauth2.auth-url", "http://example.com/")
            .put("http-server.authentication.oauth2.token-url", "http://example.com/")
            .put("http-server.authentication.oauth2.client-id", "client")
            .put("http-server.authentication.oauth2.client-secret", "client-secret")
            .build();
    private static final String TEST_USER = "test-user";
    private static final String TEST_PASSWORD = "test-password";
    private static final String HMAC_KEY = Resources.getResource("hmac_key.txt").getPath();
    private static final PrivateKey JWK_PRIVATE_KEY;

    static {
        try {
            JWK_PRIVATE_KEY = PemReader.loadPrivateKey(new File(Resources.getResource("jwk/jwk-rsa-private.pem").getPath()), Optional.empty());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private OkHttpClient client;

    @BeforeClass
    public void setup()
    {
        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
                .followRedirects(false);
        setupSsl(
                clientBuilder,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(LOCALHOST_KEYSTORE),
                Optional.empty(),
                Optional.empty());
        client = clientBuilder.build();
    }

    @Test
    public void testInsecureAuthenticator()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(SECURE_PROPERTIES)
                .build()) {
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            // insecure authenticator takes any username, but does not allow any password
            testFormAuthentication(server, httpServerInfo, false);
        }
    }

    @Test
    public void testRedirectHttpToHttps()
            throws Exception
    {
        verifyRedirectHttpToHttps(false);
        verifyRedirectHttpToHttps(true);
    }

    private void verifyRedirectHttpToHttps(boolean processForwardedHeaders)
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("http-server.https.enabled", "true")
                        .put("http-server.https.keystore.path", LOCALHOST_KEYSTORE)
                        .put("http-server.https.keystore.key", "")
                        .put("http-server.authentication.type", "password")
                        .put("http-server.process-forwarded", String.valueOf(processForwardedHeaders))
                        .build())
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticator(TestWebUi::authenticate);
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            URI secureUri = httpServerInfo.getHttpsUri();
            URI insecureUri = httpServerInfo.getHttpUri();

            // https works as normal (redirects to login)
            assertRedirect(client, getUiLocation(secureUri), getLoginHtmlLocation(secureUri), false);

            // http redirects to https
            assertRedirect(client, getUiLocation(insecureUri), getUiLocation(secureUri), false);

            // Test a forwarded request
            OkHttpClient forwardedClient = client.newBuilder()
                    .addInterceptor(chain -> chain.proceed(chain.request().newBuilder()
                            .addHeader("Forwarded", "host=example.com;proto=http")
                            .build()))
                    .build();
            if (processForwardedHeaders) {
                // proxied requests are send to the disabled page because we do not know the location of the HTTPS address
                assertRedirect(forwardedClient, getUiLocation(insecureUri), getDisabledLocation(URI.create("http://example.com")), false);
            }
            else {
                // Requests with unprocessed forwarded headers get redirected to the forwarded disabled error page
                assertRedirect(forwardedClient, getUiLocation(insecureUri), getForwardDisabledLocation(insecureUri), false);
            }
        }
    }

    @Test
    public void testPasswordAuthenticator()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.type", "password")
                        .build())
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticator(TestWebUi::authenticate);
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            testFormAuthentication(server, httpServerInfo, true);
        }
    }

    private void testFormAuthentication(TestingTrinoServer server, HttpServerInfo httpServerInfo, boolean sendPasswordForHttps)
            throws Exception
    {
        testRootRedirect(httpServerInfo.getHttpUri(), client);
        testRootRedirect(httpServerInfo.getHttpsUri(), client);

        String nodeId = server.getInstance(Key.get(NodeInfo.class)).getNodeId();
        testWorkerResource(nodeId, httpServerInfo.getHttpUri(), false);
        testWorkerResource(nodeId, httpServerInfo.getHttpsUri(), sendPasswordForHttps);

        testLoggedOut(httpServerInfo.getHttpUri());
        testLoggedOut(httpServerInfo.getHttpsUri());

        testLogIn(httpServerInfo.getHttpUri(), false);
        testLogIn(httpServerInfo.getHttpsUri(), sendPasswordForHttps);

        testFailedLogin(httpServerInfo.getHttpUri(), false);
        testFailedLogin(httpServerInfo.getHttpsUri(), sendPasswordForHttps);
    }

    private static void testRootRedirect(URI baseUri, OkHttpClient client)
            throws IOException
    {
        assertRedirect(client, uriBuilderFrom(baseUri).toString(), getUiLocation(baseUri));
    }

    private void testLoggedOut(URI baseUri)
            throws IOException
    {
        testLoggedOut(baseUri, baseUri);
    }

    private void testLoggedOut(URI baseUri, URI redirectBaseUri)
            throws IOException
    {
        assertRedirect(client, getUiLocation(baseUri), getLoginHtmlLocation(redirectBaseUri));

        assertRedirect(client, getLocation(baseUri, "/ui/query.html", "abc123"), getLocation(redirectBaseUri, LOGIN_FORM, "/ui/query.html?abc123"), false);

        assertResponseCode(client, getValidApiLocation(redirectBaseUri), SC_UNAUTHORIZED);

        assertOk(client, getValidAssetsLocation(redirectBaseUri));

        assertOk(client, getValidVendorLocation(redirectBaseUri));
    }

    private void testLogIn(URI baseUri, boolean sendPassword)
            throws Exception
    {
        CookieManager cookieManager = new CookieManager();
        OkHttpClient client = this.client.newBuilder()
                .cookieJar(new JavaNetCookieJar(cookieManager))
                .build();

        String body = assertOk(client, getLoginHtmlLocation(baseUri))
                .orElseThrow(() -> new AssertionError("No response body"));
        assertThat(body).contains("action=\"/ui/login\"");
        assertThat(body).contains("method=\"post\"");

        assertThat(body).doesNotContain("// This value will be replaced");
        if (sendPassword) {
            assertThat(body).contains("var hidePassword = false;");
        }
        else {
            assertThat(body).contains("var hidePassword = true;");
        }

        logIn(baseUri, client, sendPassword);
        HttpCookie cookie = getOnlyElement(cookieManager.getCookieStore().getCookies());
        assertEquals(cookie.getPath(), "/ui");
        assertEquals(cookie.getDomain(), baseUri.getHost());
        assertEquals(cookie.getMaxAge(), -1);
        assertTrue(cookie.isHttpOnly());

        assertOk(client, getUiLocation(baseUri));

        assertOk(client, getValidApiLocation(baseUri));

        assertResponseCode(client, getLocation(baseUri, "/ui/unknown"), SC_NOT_FOUND);

        assertResponseCode(client, getLocation(baseUri, "/ui/api/unknown"), SC_NOT_FOUND);
        assertRedirect(client, getLogoutLocation(baseUri), getLoginHtmlLocation(baseUri), false);
        assertThat(cookieManager.getCookieStore().getCookies()).isEmpty();
    }

    private void testFailedLogin(URI uri, boolean passwordAllowed)
            throws IOException
    {
        testFailedLogin(uri, Optional.empty(), Optional.empty());
        testFailedLogin(uri, Optional.empty(), Optional.of(TEST_PASSWORD));
        testFailedLogin(uri, Optional.empty(), Optional.of("unknown"));

        if (passwordAllowed) {
            testFailedLogin(uri, Optional.of(TEST_USER), Optional.of("unknown"));
            testFailedLogin(uri, Optional.of("unknown"), Optional.of(TEST_PASSWORD));
            testFailedLogin(uri, Optional.of(TEST_USER), Optional.empty());
            testFailedLogin(uri, Optional.of("unknown"), Optional.empty());
        }
    }

    private void testFailedLogin(URI httpsUrl, Optional<String> username, Optional<String> password)
            throws IOException
    {
        CookieManager cookieManager = new CookieManager();
        OkHttpClient client = this.client.newBuilder()
                .cookieJar(new JavaNetCookieJar(cookieManager))
                .build();

        FormBody.Builder formData = new FormBody.Builder();
        username.ifPresent(value -> formData.add("username", value));
        password.ifPresent(value -> formData.add("password", value));
        Request request = new Request.Builder()
                .url(getLoginLocation(httpsUrl))
                .post(formData.build())
                .build();
        try (Response response = client.newCall(request).execute()) {
            assertEquals(response.code(), SC_SEE_OTHER);
            assertEquals(response.header(LOCATION), getLoginHtmlLocation(httpsUrl));
            assertTrue(cookieManager.getCookieStore().getCookies().isEmpty());
        }
    }

    private void testWorkerResource(String nodeId, URI baseUri, boolean sendPassword)
            throws Exception
    {
        OkHttpClient client = this.client.newBuilder()
                .cookieJar(new JavaNetCookieJar(new CookieManager()))
                .build();
        logIn(baseUri, client, sendPassword);

        testWorkerResource(nodeId, baseUri, client);
    }

    private static void testWorkerResource(String nodeId, URI baseUri, OkHttpClient authorizedClient)
            throws IOException
    {
        assertOk(authorizedClient, getLocation(baseUri, "/ui/api/worker/" + nodeId + "/status"));
        assertOk(authorizedClient, getLocation(baseUri, "/ui/api/worker/" + nodeId + "/thread"));
    }

    private static void logIn(URI baseUri, OkHttpClient client, boolean sendPassword)
            throws IOException
    {
        FormBody.Builder formData = new FormBody.Builder()
                .add("username", TEST_USER);
        if (sendPassword) {
            formData.add("password", TEST_PASSWORD);
        }

        Request request = new Request.Builder()
                .url(getLoginLocation(baseUri))
                .post(formData.build())
                .build();
        Response response = client.newCall(request).execute();
        assertEquals(response.code(), SC_SEE_OTHER);
        assertEquals(response.header(LOCATION), getUiLocation(baseUri));
    }

    @Test
    public void testDisabled()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("web-ui.enabled", "false")
                        .build())
                .build()) {
            server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticator(TestWebUi::authenticate);

            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            testDisabled(httpServerInfo.getHttpUri());
            testDisabled(httpServerInfo.getHttpsUri());
        }
    }

    private void testDisabled(URI baseUri)
            throws Exception
    {
        assertRedirect(client, getUiLocation(baseUri), getDisabledLocation(baseUri));

        assertRedirect(client, getLocation(baseUri, "/ui/query.html", "abc123"), getDisabledLocation(baseUri));

        assertResponseCode(client, getValidApiLocation(baseUri), SC_UNAUTHORIZED);

        assertRedirect(client, getLoginLocation(baseUri), getDisabledLocation(baseUri));

        assertRedirect(client, getLogoutLocation(baseUri), getDisabledLocation(baseUri));

        assertOk(client, getValidAssetsLocation(baseUri));

        assertOk(client, getValidVendorLocation(baseUri));
    }

    @Test
    public void testNoPasswordAuthenticator()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.type", "password")
                        .build())
                .build()) {
            // a password manager is required, so a secure request will fail
            // a real server will fail to start, but verify that we get an exception here to be safe
            FormAuthenticator formAuthenticator = server.getInstance(Key.get(FormAuthenticator.class));
            assertThatThrownBy(() -> formAuthenticator
                    .isValidCredential(TEST_USER, TEST_USER, true))
                    .hasMessage("authenticator was not loaded")
                    .isInstanceOf(IllegalStateException.class);
            assertTrue(formAuthenticator.isLoginEnabled(true));
        }
    }

    @Test
    public void testFixedAuthenticator()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("web-ui.authentication.type", "fixed")
                        .put("web-ui.user", "test-user")
                        .build())
                .build()) {
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            String nodeId = server.getInstance(Key.get(NodeInfo.class)).getNodeId();

            testAlwaysAuthorized(httpServerInfo.getHttpUri(), client, nodeId);
            testAlwaysAuthorized(httpServerInfo.getHttpsUri(), client, nodeId);

            testFixedAuthenticator(httpServerInfo.getHttpUri());
            testFixedAuthenticator(httpServerInfo.getHttpsUri());
        }
    }

    private void testFixedAuthenticator(URI baseUri)
            throws Exception
    {
        assertOk(client, getUiLocation(baseUri));

        assertOk(client, getValidApiLocation(baseUri));

        assertResponseCode(client, getLocation(baseUri, "/ui/unknown"), SC_NOT_FOUND);

        assertResponseCode(client, getLocation(baseUri, "/ui/api/unknown"), SC_NOT_FOUND);
    }

    @Test
    public void testCertAuthenticator()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.type", "certificate")
                        .put("http-server.https.truststore.path", LOCALHOST_KEYSTORE)
                        .put("http-server.https.truststore.key", "")
                        .build())
                .build()) {
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            String nodeId = server.getInstance(Key.get(NodeInfo.class)).getNodeId();

            testLogIn(httpServerInfo.getHttpUri(), false);

            testNeverAuthorized(httpServerInfo.getHttpsUri(), client);

            OkHttpClient.Builder clientBuilder = client.newBuilder();
            setupSsl(
                    clientBuilder,
                    Optional.of(LOCALHOST_KEYSTORE),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(LOCALHOST_KEYSTORE),
                    Optional.empty(),
                    Optional.empty());
            OkHttpClient clientWithCert = clientBuilder.build();
            testAlwaysAuthorized(httpServerInfo.getHttpsUri(), clientWithCert, nodeId);
        }
    }

    @Test
    public void testJwtAuthenticator()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.type", "jwt")
                        .put("http-server.authentication.jwt.key-file", HMAC_KEY)
                        .build())
                .build()) {
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            String nodeId = server.getInstance(Key.get(NodeInfo.class)).getNodeId();

            testLogIn(httpServerInfo.getHttpUri(), false);

            testNeverAuthorized(httpServerInfo.getHttpsUri(), client);

            String hmac = Files.readString(Paths.get(HMAC_KEY));
            String token = Jwts.builder()
                    .signWith(SignatureAlgorithm.HS256, hmac)
                    .setSubject("test-user")
                    .setExpiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                    .compact();

            OkHttpClient clientWithJwt = client.newBuilder()
                    .authenticator((route, response) -> response.request().newBuilder()
                            .header(AUTHORIZATION, "Bearer " + token)
                            .build())
                    .build();
            testAlwaysAuthorized(httpServerInfo.getHttpsUri(), clientWithJwt, nodeId);
        }
    }

    @Test
    public void testJwtWithJwkAuthenticator()
            throws Exception
    {
        TestingHttpServer jwkServer = createTestingJwkServer();
        jwkServer.start();
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("http-server.authentication.type", "jwt")
                        .put("http-server.authentication.jwt.key-file", jwkServer.getBaseUrl().toString())
                        .build())
                .build()) {
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            String nodeId = server.getInstance(Key.get(NodeInfo.class)).getNodeId();

            testLogIn(httpServerInfo.getHttpUri(), false);

            testNeverAuthorized(httpServerInfo.getHttpsUri(), client);

            String token = Jwts.builder()
                    .signWith(SignatureAlgorithm.RS256, JWK_PRIVATE_KEY)
                    .setHeaderParam(JwsHeader.KEY_ID, "test-rsa")
                    .setSubject("test-user")
                    .setExpiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                    .compact();

            OkHttpClient clientWithJwt = client.newBuilder()
                    .authenticator((route, response) -> response.request().newBuilder()
                            .header(AUTHORIZATION, "Bearer " + token)
                            .build())
                    .build();
            testAlwaysAuthorized(httpServerInfo.getHttpsUri(), clientWithJwt, nodeId);
        }
        finally {
            jwkServer.stop();
        }
    }

    @Test
    public void testOAuth2Authenticator()
            throws Exception
    {
        String accessToken = createTokenBuilder().compact();
        TestingHttpServer jwkServer = createTestingJwkServer();
        jwkServer.start();
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(OAUTH2_PROPERTIES)
                        .put("http-server.authentication.oauth2.jwks-url", jwkServer.getBaseUrl().toString())
                        .build())
                .setAdditionalModule(binder -> newOptionalBinder(binder, OAuth2Client.class)
                        .setBinding()
                        .toInstance(new OAuth2ClientStub(accessToken)))
                .build()) {
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            assertAuth2Authentication(httpServerInfo, accessToken);
        }
        finally {
            jwkServer.stop();
        }
    }

    @Test
    public void testOAuth2AuthenticatorWithoutOpenIdScope()
            throws Exception
    {
        String accessToken = createTokenBuilder().compact();
        TestingHttpServer jwkServer = createTestingJwkServer();
        jwkServer.start();
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(OAUTH2_PROPERTIES)
                        .put("http-server.authentication.oauth2.jwks-url", jwkServer.getBaseUrl().toString())
                        .put("http-server.authentication.oauth2.scopes", "")
                        .build())
                .setAdditionalModule(binder -> newOptionalBinder(binder, OAuth2Client.class)
                        .setBinding()
                        .toInstance(new OAuth2ClientStub(accessToken)))
                .build()) {
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));
            assertAuth2Authentication(httpServerInfo, accessToken);
        }
        finally {
            jwkServer.stop();
        }
    }

    private void assertAuth2Authentication(HttpServerInfo httpServerInfo, String accessToken)
            throws Exception
    {
        String state = Jwts.builder()
                .signWith(SignatureAlgorithm.HS256, Hashing.sha256().hashString(STATE_KEY, UTF_8).asBytes())
                .setAudience("trino_oauth_ui")
                .setExpiration(Date.from(ZonedDateTime.now().plusMinutes(10).toInstant()))
                .compact();

        CookieManager cookieManager = new CookieManager();
        OkHttpClient client = this.client.newBuilder()
                .cookieJar(new JavaNetCookieJar(cookieManager))
                .build();

        // HTTP is not allowed for OAuth
        testDisabled(httpServerInfo.getHttpUri());

        // verify HTTPS before login
        URI baseUri = httpServerInfo.getHttpsUri();
        testRootRedirect(baseUri, client);
        assertRedirect(client, getUiLocation(baseUri), "http://example.com/authorize", false);
        assertResponseCode(client, getValidApiLocation(baseUri), UNAUTHORIZED.getStatusCode());
        assertRedirect(client, getLocation(baseUri, "/ui/unknown"), "http://example.com/authorize", false);
        assertResponseCode(client, getLocation(baseUri, "/ui/api/unknown"), UNAUTHORIZED.getStatusCode());

        // login with the callback endpoint
        assertRedirect(
                client,
                uriBuilderFrom(baseUri)
                        .replacePath(CALLBACK_ENDPOINT)
                        .addParameter("code", "TEST_CODE")
                        .addParameter("state", state)
                        .toString(),
                getUiLocation(baseUri),
                false);
        HttpCookie cookie = getOnlyElement(cookieManager.getCookieStore().getCookies());
        assertEquals(cookie.getValue(), accessToken);
        assertEquals(cookie.getPath(), "/ui/");
        assertEquals(cookie.getDomain(), baseUri.getHost());
        assertTrue(cookie.getMaxAge() > 0 && cookie.getMaxAge() < MINUTES.toSeconds(5));
        assertTrue(cookie.isHttpOnly());

        // authentication cookie is now set, so UI should work
        testRootRedirect(baseUri, client);
        assertOk(client, getUiLocation(baseUri));
        assertOk(client, getUiLocation(baseUri));
        assertOk(client, getValidApiLocation(baseUri));
        assertResponseCode(client, getLocation(baseUri, "/ui/unknown"), SC_NOT_FOUND);
        assertResponseCode(client, getLocation(baseUri, "/ui/api/unknown"), SC_NOT_FOUND);

        // logout
        assertRedirect(client, getLogoutLocation(baseUri), getUiLocation(baseUri), false);
        assertThat(cookieManager.getCookieStore().getCookies()).isEmpty();
        assertRedirect(client, getUiLocation(baseUri), "http://example.com/authorize", false);
    }

    private static void testAlwaysAuthorized(URI baseUri, OkHttpClient authorizedClient, String nodeId)
            throws IOException
    {
        testRootRedirect(baseUri, authorizedClient);
        testWorkerResource(nodeId, baseUri, authorizedClient);

        assertOk(authorizedClient, getUiLocation(baseUri));

        assertOk(authorizedClient, getValidApiLocation(baseUri));

        assertRedirect(authorizedClient, getLoginHtmlLocation(baseUri), getUiLocation(baseUri), false);

        assertRedirect(authorizedClient, getLoginLocation(baseUri), getUiLocation(baseUri), false);

        assertRedirect(authorizedClient, getLogoutLocation(baseUri), getUiLocation(baseUri), false);

        assertResponseCode(authorizedClient, getLocation(baseUri, "/ui/unknown"), SC_NOT_FOUND);

        assertResponseCode(authorizedClient, getLocation(baseUri, "/ui/api/unknown"), SC_NOT_FOUND);
    }

    private static void testNeverAuthorized(URI baseUri, OkHttpClient notAuthorizedClient)
            throws IOException
    {
        testRootRedirect(baseUri, notAuthorizedClient);

        assertResponseCode(notAuthorizedClient, getUiLocation(baseUri), SC_UNAUTHORIZED);
        assertResponseCode(notAuthorizedClient, getValidApiLocation(baseUri), SC_UNAUTHORIZED);
        assertResponseCode(notAuthorizedClient, getLoginLocation(baseUri), SC_UNAUTHORIZED, true);
        assertResponseCode(notAuthorizedClient, getLogoutLocation(baseUri), SC_UNAUTHORIZED);
        assertResponseCode(notAuthorizedClient, getLocation(baseUri, "/ui/unknown"), SC_UNAUTHORIZED);
        assertResponseCode(notAuthorizedClient, getLocation(baseUri, "/ui/api/unknown"), SC_UNAUTHORIZED);
    }

    private static Optional<String> assertOk(OkHttpClient client, String url)
            throws IOException
    {
        return assertResponseCode(client, url, SC_OK);
    }

    private static void assertRedirect(OkHttpClient client, String url, String redirectLocation)
            throws IOException
    {
        assertRedirect(client, url, redirectLocation, true);
    }

    private static void assertRedirect(OkHttpClient client, String url, String redirectLocation, boolean testProxy)
            throws IOException
    {
        Request request = new Request.Builder()
                .url(url)
                .build();
        if (url.endsWith(UI_LOGIN)) {
            RequestBody formBody = new FormBody.Builder()
                    .add("username", "test")
                    .add("password", "test")
                    .build();
            request = request.newBuilder().post(formBody).build();
        }
        try (Response response = client.newCall(request).execute()) {
            assertEquals(response.code(), SC_SEE_OTHER);
            assertEquals(response.header(LOCATION), redirectLocation);
        }

        if (testProxy) {
            request = new Request.Builder()
                    .url(url)
                    .header(X_FORWARDED_PROTO, "test")
                    .header(X_FORWARDED_HOST, "my-load-balancer.local")
                    .header(X_FORWARDED_PORT, "123")
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertEquals(response.code(), SC_SEE_OTHER);
                assertEquals(
                        response.header(LOCATION),
                        uriBuilderFrom(URI.create(redirectLocation))
                                .scheme("test")
                                .host("my-load-balancer.local")
                                .port(123)
                                .toString());
            }

            request = new Request.Builder()
                    .url(url)
                    .header(X_FORWARDED_PROTO, "test")
                    .header(X_FORWARDED_HOST, "my-load-balancer.local:123")
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertEquals(response.code(), SC_SEE_OTHER);
                assertEquals(
                        response.header(LOCATION),
                        uriBuilderFrom(URI.create(redirectLocation))
                                .scheme("test")
                                .host("my-load-balancer.local")
                                .port(123)
                                .toString());
            }

            request = new Request.Builder()
                    .url(url)
                    .header(X_FORWARDED_PROTO, "test")
                    .header(X_FORWARDED_PORT, "123")
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertEquals(response.code(), SC_SEE_OTHER);
                assertEquals(
                        response.header(LOCATION),
                        uriBuilderFrom(URI.create(redirectLocation))
                                .scheme("test")
                                .port(123)
                                .toString());
            }

            request = new Request.Builder()
                    .url(url)
                    .header(X_FORWARDED_PROTO, "test")
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertEquals(response.code(), SC_SEE_OTHER);
                assertEquals(
                        response.header(LOCATION),
                        uriBuilderFrom(URI.create(redirectLocation))
                                .scheme("test")
                                .toString());
            }
            request = new Request.Builder()
                    .url(url)
                    .header(X_FORWARDED_HOST, "my-load-balancer.local")
                    .header(X_FORWARDED_PORT, "123")
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertEquals(response.code(), SC_SEE_OTHER);
                assertEquals(
                        response.header(LOCATION),
                        uriBuilderFrom(URI.create(redirectLocation))
                                .host("my-load-balancer.local")
                                .port(123)
                                .toString());
            }

            request = new Request.Builder()
                    .url(url)
                    .header(X_FORWARDED_HOST, "my-load-balancer.local:123")
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertEquals(response.code(), SC_SEE_OTHER);
                assertEquals(
                        response.header(LOCATION),
                        uriBuilderFrom(URI.create(redirectLocation))
                                .host("my-load-balancer.local")
                                .port(123)
                                .toString());
            }

            request = new Request.Builder()
                    .url(url)
                    .header(X_FORWARDED_HOST, "my-load-balancer.local")
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertEquals(response.code(), SC_SEE_OTHER);
                assertEquals(
                        response.header(LOCATION),
                        uriBuilderFrom(URI.create(redirectLocation))
                                .host("my-load-balancer.local")
                                .defaultPort()
                                .toString());
            }
        }
    }

    private static Optional<String> assertResponseCode(OkHttpClient client, String url, int expectedCode)
            throws IOException
    {
        return assertResponseCode(client, url, expectedCode, false);
    }

    private static Optional<String> assertResponseCode(OkHttpClient client,
            String url,
            int expectedCode,
            boolean postLogin)
            throws IOException
    {
        Request request = new Request.Builder()
                .url(url)
                .build();
        if (postLogin) {
            RequestBody formBody = new FormBody.Builder()
                    .add("username", "fake")
                    .add("password", "bad")
                    .build();
            request = request.newBuilder().post(formBody).build();
        }
        try (Response response = client.newCall(request).execute()) {
            assertEquals(response.code(), expectedCode, url);
            return Optional.ofNullable(response.body())
                    .map(responseBody -> {
                        try {
                            return responseBody.string();
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
    }

    private static Principal authenticate(String user, String password)
    {
        if (TEST_USER.equals(user) && TEST_PASSWORD.equals(password)) {
            return new BasicPrincipal(user);
        }
        throw new AccessDeniedException("Invalid credentials");
    }

    private static String getUiLocation(URI baseUri)
    {
        return getLocation(baseUri, "/ui/");
    }

    private static String getLoginHtmlLocation(URI baseUri)
    {
        return getLocation(baseUri, LOGIN_FORM);
    }

    private static String getLoginLocation(URI httpsUrl)
    {
        return getLocation(httpsUrl, UI_LOGIN);
    }

    private static String getLogoutLocation(URI baseUri)
    {
        return getLocation(baseUri, UI_LOGOUT);
    }

    private static String getDisabledLocation(URI baseUri)
    {
        return getLocation(baseUri, DISABLED_LOCATION);
    }

    private static String getForwardDisabledLocation(URI baseUri)
    {
        return getLocation(baseUri, FORWARD_DISABLED_LOCATION);
    }

    private static String getValidApiLocation(URI baseUri)
    {
        return getLocation(baseUri, "/ui/api/cluster");
    }

    private static String getValidAssetsLocation(URI baseUri)
    {
        return getLocation(baseUri, "/ui/assets/favicon.ico");
    }

    private static String getValidVendorLocation(URI baseUri)
    {
        return getLocation(baseUri, "/ui/vendor/bootstrap/css/bootstrap.css");
    }

    private static String getLocation(URI baseUri, String path)
    {
        return uriBuilderFrom(baseUri).replacePath(path).toString();
    }

    private static String getLocation(URI baseUri, String path, String query)
    {
        return uriBuilderFrom(baseUri).replacePath(path).replaceParameter(query).toString();
    }

    private static JwtBuilder createTokenBuilder()
    {
        Date tokenExpiration = Date.from(ZonedDateTime.now().plusMinutes(5).toInstant());
        return Jwts.builder()
                .signWith(SignatureAlgorithm.RS256, JWK_PRIVATE_KEY)
                .setHeaderParam(JwsHeader.KEY_ID, "test-rsa")
                .setSubject("test-user")
                .setExpiration(tokenExpiration);
    }

    private static TestingHttpServer createTestingJwkServer()
            throws IOException
    {
        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig().setHttpPort(0);
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, nodeInfo);

        return new TestingHttpServer(httpServerInfo, nodeInfo, config, new JwkServlet(), ImmutableMap.of());
    }

    private static class JwkServlet
            extends HttpServlet
    {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            String jwkKeys = Resources.toString(Resources.getResource("jwk/jwk-public.json"), UTF_8);
            response.getWriter().println(jwkKeys);
        }
    }

    private static class OAuth2ClientStub
            implements OAuth2Client
    {
        private final String accessToken;
        private Optional<JwtBuilder> idTokenBuilder = Optional.empty();

        public OAuth2ClientStub(String accessToken)
        {
            this.accessToken = requireNonNull(accessToken, "accessToken is null");
        }

        @Override
        public URI getAuthorizationUri(String state, URI callbackUri, Optional<String> nonceHash)
        {
            nonceHash.ifPresent(nonce -> idTokenBuilder = Optional.of(createTokenBuilder().claim(NONCE, nonce)));
            return URI.create("http://example.com/authorize");
        }

        @Override
        public AccessToken getAccessToken(String code, URI callbackUri)
        {
            if (!"TEST_CODE".equals(code)) {
                throw new IllegalArgumentException("Expected TEST_CODE");
            }
            return new AccessToken(accessToken, Optional.empty(), idTokenBuilder.map(JwtBuilder::compact));
        }
    }
}
