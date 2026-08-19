// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.authentication.plugin.oidc;

import org.apache.doris.authentication.AuthenticationException;
import org.apache.doris.authentication.AuthenticationFailureType;
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.BasicPrincipal;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.authentication.Principal;
import org.apache.doris.authentication.spi.AuthenticationPlugin;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * OIDC authentication plugin — validates Apple IDMS access tokens passed as MySQL passwords.
 *
 * <p>Flow:
 * <ol>
 *   <li>POST /userinfo — IDMS validates the token (signature, revocation) and returns claims</li>
 *   <li>Check {@code exp} from the userinfo response — token not expired</li>
 *   <li>Check {@code sub} from the userinfo response matches the MySQL username (DSID)</li>
 *   <li>Extract {@code extended_claims.allGroups} → {@link Principal#getExternalGroups()} for role mapping</li>
 * </ol>
 *
 * <p>IDMS rejects forged or expired tokens at the /userinfo step, so no local JWT signature
 * verification is needed. The {@code exp} check is an explicit guard on top of IDMS validation.
 *
 * <p>Usage:
 * <pre>{@code
 * CREATE AUTHENTICATION INTEGRATION idms_oidc
 *   TYPE = 'oidc'
 *   WITH (
 *     'userinfo_url'       = 'https://idmsac.apple.com/auth/oauth2/userinfo',
 *     'client_id'          = '66rrcbuuyvp3f6fszcv5mybsr3vai9',
 *     'client_secret'      = '<secret>',
 *     'issuer'             = 'https://idmsac.apple.com',
 *     'require_role_match' = 'true'
 *   );
 * }</pre>
 */
public class OidcAuthenticationPlugin implements AuthenticationPlugin {

    private static final Logger LOG = LogManager.getLogger(OidcAuthenticationPlugin.class);

    public static final String PLUGIN_NAME = "oidc";

    static final String PROP_USERINFO_URL   = "userinfo_url";
    static final String PROP_CLIENT_ID      = "client_id";
    static final String PROP_CLIENT_SECRET  = "client_secret";
    static final String PROP_USERNAME_CLAIM = "username_claim";
    static final String PROP_GROUPS_PATH    = "groups_path";
    static final String PROP_ISSUER         = "issuer";

    static final String DEFAULT_USERNAME_CLAIM = "sub";
    static final String DEFAULT_GROUPS_PATH    = "extended_claims.allGroups";

    private static final int HTTP_TIMEOUT_SECONDS = 10;
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(HTTP_TIMEOUT_SECONDS);

    private final HttpClient httpClient;
    // Stored during initialize() — one plugin instance per integration
    private volatile String configuredIssuer;

    public OidcAuthenticationPlugin() {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(HTTP_TIMEOUT)
                .version(java.net.http.HttpClient.Version.HTTP_2) // prefer HTTP/2 multiplexing, falls back to HTTP/1.1
                .build();
    }

    @Override
    public String name() {
        return PLUGIN_NAME;
    }

    @Override
    public String description() {
        return "OIDC authentication plugin — validates Apple IDMS access tokens";
    }

    @Override
    public boolean supports(AuthenticationRequest request) {
        if (!CredentialType.OAUTH_TOKEN.equalsIgnoreCase(request.getCredentialType())) {
            return false;
        }
        // If issuer is configured, peek at the JWT iss claim from the payload.
        // Returns false → chain moves to the next integration cleanly (no BAD_CREDENTIAL stop).
        String issuer = configuredIssuer;
        if (issuer == null || issuer.isBlank()) {
            return true;
        }
        try {
            byte[] cred = request.getCredential();
            if (cred == null || cred.length == 0) {
                return false;
            }
            JsonObject payload = decodeJwtPayload(new String(cred, StandardCharsets.UTF_8).trim());
            String tokenIssuer = payload.has("iss") ? payload.get("iss").getAsString() : null;
            return issuer.equals(tokenIssuer);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public AuthenticationResult authenticate(AuthenticationRequest request,
            AuthenticationIntegration integration) throws AuthenticationException {

        String username = request.getUsername();
        byte[] credBytes = request.getCredential();
        if (credBytes == null || credBytes.length == 0) {
            return AuthenticationResult.failure("Access token is required");
        }
        String accessToken = new String(credBytes, StandardCharsets.UTF_8).trim();

        String userinfoUrl  = integration.getProperty(PROP_USERINFO_URL,  "");
        String clientId     = integration.getProperty(PROP_CLIENT_ID,     "");
        String clientSecret = integration.getProperty(PROP_CLIENT_SECRET, "");
        String usernameClaim = integration.getProperty(PROP_USERNAME_CLAIM, DEFAULT_USERNAME_CLAIM);
        String groupsPath   = integration.getProperty(PROP_GROUPS_PATH, DEFAULT_GROUPS_PATH);

        // ── 1. POST /userinfo — IDMS validates signature + revocation ──────────
        JsonObject userinfo;
        try {
            userinfo = callUserinfo(userinfoUrl, clientId, clientSecret, accessToken);
        } catch (AuthenticationException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Userinfo call failed for user {}: {}", username, e.getMessage(), e);
            throw new AuthenticationException(
                    "Userinfo endpoint unreachable: " + e.getMessage(), e,
                    AuthenticationFailureType.SOURCE_UNAVAILABLE);
        }

        // ── 2. Check expiry from userinfo response ─────────────────────────────
        // IDMS already rejects expired tokens, but we enforce exp explicitly as a hard guard.
        if (userinfo.has("exp")) {
            long exp = userinfo.get("exp").getAsLong();
            long nowSeconds = System.currentTimeMillis() / 1000L;
            if (nowSeconds > exp) {
                LOG.info("Expired token in userinfo for user {}: exp={}, now={}", username, exp, nowSeconds);
                return AuthenticationResult.failure(AuthenticationFailureType.BAD_CREDENTIAL,
                        "Access token has expired");
            }
        }

        // ── 3. Verify DSID from userinfo sub claim matches the supplied username ─
        // IDMS returns dsid as a JSON number — getAsString() handles both string and number.
        JsonElement subElement = userinfo.has(usernameClaim) ? userinfo.get(usernameClaim) : null;
        if (subElement == null || !subElement.isJsonPrimitive()) {
            return AuthenticationResult.failure(AuthenticationFailureType.BAD_CREDENTIAL,
                    "Userinfo missing '" + usernameClaim + "' claim");
        }
        String tokenSub = subElement.getAsString();
        if (!tokenSub.equals(username)) {
            LOG.warn("DSID mismatch for login '{}': userinfo sub='{}'", username, tokenSub);
            return AuthenticationResult.failure(AuthenticationFailureType.BAD_CREDENTIAL,
                    "Token subject does not match username");
        }

        // ── 4. Extract allGroups ───────────────────────────────────────────────
        Set<String> externalGroups = extractGroups(userinfo, groupsPath);
        if (LOG.isDebugEnabled()) {
            LOG.debug("OIDC auth OK — user={} groups={}", username, externalGroups.size());
        }

        // ── 5. Build Principal — RBAC role mapping handled by fe-core ─────────
        Principal principal = BasicPrincipal.builder()
                .name(username)
                .authenticator(PLUGIN_NAME)
                .externalGroups(externalGroups)
                .build();

        return AuthenticationResult.success(principal);
    }

    @Override
    public void validate(AuthenticationIntegration integration) throws AuthenticationException {
        Map<String, String> props = integration.getProperties();
        requireProp(props, PROP_USERINFO_URL, integration.getName());
        requireProp(props, PROP_CLIENT_ID,    integration.getName());
    }

    @Override
    public void initialize(AuthenticationIntegration integration) throws AuthenticationException {
        configuredIssuer = integration.getProperty(PROP_ISSUER, "").trim();
        if (!configuredIssuer.isEmpty()) {
            LOG.info("OIDC integration '{}' will only accept tokens with iss='{}'",
                    integration.getName(), configuredIssuer);
        }
    }

    // ── Package-private for testing ───────────────────────────────────────────

    /**
     * POSTs to IDMS /userinfo. Apple's IDMS requires client_id (returns error -27122 without it).
     * IDMS validates the token's signature, expiry, and revocation before returning claims.
     * Protected so tests can override without an HTTP server.
     */
    protected JsonObject callUserinfo(String userinfoUrl, String clientId, String clientSecret,
            String accessToken) throws Exception {

        StringBuilder body = new StringBuilder()
                .append("client_id=").append(enc(clientId))
                .append("&access_token=").append(enc(accessToken))
                .append("&claims=allGroups");
        if (!clientSecret.isEmpty()) {
            body.append("&client_secret=").append(enc(clientSecret));
        }

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(userinfoUrl))
                .timeout(HTTP_TIMEOUT)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build();

        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new AuthenticationException(
                    "Userinfo returned HTTP " + resp.statusCode() + ": " + resp.body(),
                    AuthenticationFailureType.BAD_CREDENTIAL);
        }
        return JsonParser.parseString(resp.body()).getAsJsonObject();
    }

    /**
     * Walks a dotted path through the userinfo JSON and collects group strings.
     * Default path: "extended_claims.allGroups" → navigates into nested object.
     */
    static Set<String> extractGroups(JsonObject userinfo, String groupsPath) {
        Set<String> groups = new HashSet<>();
        JsonElement current = userinfo;
        for (String segment : groupsPath.split("\\.")) {
            if (current == null || !current.isJsonObject()) {
                return groups;
            }
            current = current.getAsJsonObject().get(segment);
        }
        if (current == null) {
            return groups;
        }
        if (current.isJsonArray()) {
            for (JsonElement el : current.getAsJsonArray()) {
                if (el.isJsonPrimitive()) {
                    groups.add(el.getAsString());
                }
            }
        } else if (current.isJsonPrimitive()) {
            for (String g : current.getAsString().split(",")) {
                String t = g.trim();
                if (!t.isEmpty()) {
                    groups.add(t);
                }
            }
        }
        return groups;
    }

    /** Decodes the JWT payload segment — used for issuer check in supports(). */
    static JsonObject decodeJwtPayload(String jwt) {
        String[] parts = jwt.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Not a JWT: expected 3 parts, got " + parts.length);
        }
        byte[] payloadBytes = Base64.getUrlDecoder().decode(parts[1]);
        return JsonParser.parseString(new String(payloadBytes, StandardCharsets.UTF_8)).getAsJsonObject();
    }

    private static String enc(String v) {
        return URLEncoder.encode(v, StandardCharsets.UTF_8);
    }

    private static void requireProp(Map<String, String> props, String key, String name)
            throws AuthenticationException {
        if (!props.containsKey(key) || props.get(key).isBlank()) {
            throw new AuthenticationException(
                    "'" + key + "' is required for OIDC integration '" + name + "'",
                    AuthenticationFailureType.MISCONFIGURED);
        }
    }
}
