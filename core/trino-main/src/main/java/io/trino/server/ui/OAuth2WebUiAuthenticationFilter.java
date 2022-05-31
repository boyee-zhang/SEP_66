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

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.server.security.UserMapping;
import io.trino.server.security.UserMappingException;
import io.trino.server.security.oauth2.OAuth2Client;
import io.trino.server.security.oauth2.OAuth2Config;
import io.trino.server.security.oauth2.OAuth2Service;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.trino.server.ServletSecurityUtils.sendErrorMessage;
import static io.trino.server.ServletSecurityUtils.sendWwwAuthenticate;
import static io.trino.server.ServletSecurityUtils.setAuthenticatedIdentity;
import static io.trino.server.security.oauth2.OAuth2CallbackResource.CALLBACK_ENDPOINT;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.DISABLED_LOCATION;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.DISABLED_LOCATION_URI;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.TRINO_FORM_LOGIN;
import static io.trino.server.ui.OAuthWebUiCookie.OAUTH2_COOKIE;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;

public class OAuth2WebUiAuthenticationFilter
        implements WebUiAuthenticationFilter
{
    private static final Logger LOG = Logger.get(OAuth2WebUiAuthenticationFilter.class);

    private final String principalField;
    private final OAuth2Service service;
    private final OAuth2Client client;
    private final UserMapping userMapping;
    private final Optional<String> groupsField;

    @Inject
    public OAuth2WebUiAuthenticationFilter(OAuth2Service service, OAuth2Client client, OAuth2Config oauth2Config)
    {
        this.service = requireNonNull(service, "service is null");
        this.client = requireNonNull(client, "client is null");
        requireNonNull(oauth2Config, "oauth2Config is null");
        this.userMapping = UserMapping.createUserMapping(oauth2Config.getUserMappingPattern(), oauth2Config.getUserMappingFile());
        this.principalField = oauth2Config.getPrincipalField();
        groupsField = requireNonNull(oauth2Config.getGroupsField(), "groupsField is null");
    }

    @Override
    public void filter(ContainerRequestContext request)
    {
        String path = request.getUriInfo().getRequestUri().getPath();
        if (path.equals(DISABLED_LOCATION)) {
            return;
        }

        // Only secure connections are allowed.  We could allow insecure form login, but that
        // doesn't seem very useful if you have OAuth, and would be very complex.
        if (!request.getSecurityContext().isSecure()) {
            // send 401 to REST api calls and redirect to others
            if (path.startsWith("/ui/api/")) {
                sendWwwAuthenticate(request, "Unauthorized", ImmutableSet.of(TRINO_FORM_LOGIN));
                return;
            }
            request.abortWith(Response.seeOther(DISABLED_LOCATION_URI).build());
            return;
        }
        Optional<Map<String, Object>> claims;
        claims = getAccessToken(request);
        if (claims.isEmpty()) {
            needAuthentication(request);
            return;
        }

        try {
            Object principal = claims.get().get(principalField);
            if (!isValidPrincipal(principal)) {
                LOG.debug("Invalid principal field: %s. Expected principal to be non-empty", principalField);
                sendErrorMessage(request, UNAUTHORIZED, "Unauthorized");
                return;
            }
            String principalName = (String) principal;
            Identity.Builder builder = Identity.forUser(userMapping.mapUser(principalName));
            builder.withPrincipal(new BasicPrincipal(principalName));
            groupsField.flatMap(field -> Optional.ofNullable((List<String>) claims.get().get(field)))
                    .ifPresent(groups -> builder.withGroups(ImmutableSet.copyOf(groups)));
            setAuthenticatedIdentity(request, builder.build());
        }
        catch (UserMappingException e) {
            sendErrorMessage(request, UNAUTHORIZED, firstNonNull(e.getMessage(), "Unauthorized"));
        }
    }

    private Optional<Map<String, Object>> getAccessToken(ContainerRequestContext request)
    {
        Optional<String> accessToken = OAuthWebUiCookie.read(request.getCookies().get(OAUTH2_COOKIE));
        if (accessToken.isPresent()) {
            return client.getClaims(accessToken.get());
        }
        return Optional.empty();
    }

    private void needAuthentication(ContainerRequestContext request)
    {
        // send 401 to REST api calls and redirect to others
        if (request.getUriInfo().getRequestUri().getPath().startsWith("/ui/api/")) {
            sendWwwAuthenticate(request, "Unauthorized", ImmutableSet.of(TRINO_FORM_LOGIN));
            return;
        }
        request.abortWith(service.startOAuth2Challenge(
                request.getUriInfo().getBaseUri().resolve(CALLBACK_ENDPOINT),
                Optional.empty()));
    }

    private static boolean isValidPrincipal(Object principal)
    {
        return principal instanceof String && !((String) principal).isEmpty();
    }
}
