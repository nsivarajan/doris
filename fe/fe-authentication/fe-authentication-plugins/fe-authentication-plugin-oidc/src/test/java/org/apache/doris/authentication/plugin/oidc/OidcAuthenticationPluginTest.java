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
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.CredentialType;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OidcAuthenticationPluginTest {

    // Subclass that bypasses the real HTTP call — returns whatever we configure.
    static class StubOidcPlugin extends OidcAuthenticationPlugin {
        private JsonObject stubbedUserinfo;
        private Exception stubbedError;

        void returnUserinfo(JsonObject response) {
            this.stubbedUserinfo = response;
            this.stubbedError = null;
        }

        void throwOnUserinfo(Exception e) {
            this.stubbedError = e;
            this.stubbedUserinfo = null;
        }

        @Override
        protected JsonObject callUserinfo(String userinfoUrl, String clientId,
                String clientSecret, String accessToken) throws Exception {
            if (stubbedError != null) {
                throw stubbedError;
            }
            return stubbedUserinfo;
        }
    }

    private StubOidcPlugin plugin;
    private AuthenticationIntegration integration;

    @BeforeEach
    void setUp() throws AuthenticationException {
        plugin = new StubOidcPlugin();
        integration = AuthenticationIntegration.builder()
                .name("test_oidc")
                .type("oidc")
                .property(OidcAuthenticationPlugin.PROP_USERINFO_URL,
                        "https://idmsac.apple.com/auth/oauth2/userinfo")
                .property(OidcAuthenticationPlugin.PROP_CLIENT_ID, "66rrcbuuyvp3f6fszcv5mybsr3vai9")
                .property(OidcAuthenticationPlugin.PROP_CLIENT_SECRET, "test-secret")
                .build();
        plugin.initialize(integration);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static String fakeJwt(String payloadJson) {
        String header = Base64.getUrlEncoder().withoutPadding()
                .encodeToString("{\"alg\":\"RS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
        String payload = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payloadJson.getBytes(StandardCharsets.UTF_8));
        return header + "." + payload + ".fakesignature";
    }

    private static AuthenticationRequest oauthRequest(String username, String token) {
        return AuthenticationRequest.builder()
                .username(username)
                .credentialType(CredentialType.OAUTH_TOKEN)
                .credential(token.getBytes(StandardCharsets.UTF_8))
                .build();
    }

    private static JsonObject userinfoFor(String dsid, long exp) {
        String json = String.format(
                "{\"sub\":\"%s\",\"exp\":%d,\"name\":\"Test User\","
                + "\"extended_claims\":{\"allGroups\":[\"13455565\",\"8675466\"]}}",
                dsid, exp);
        return JsonParser.parseString(json).getAsJsonObject();
    }

    // ── Plugin metadata ───────────────────────────────────────────────────────

    @Test
    void testPluginName() {
        assertEquals("oidc", plugin.name());
    }

    @Test
    void testSupportsOauthToken() throws AuthenticationException {
        plugin.initialize(integration); // no issuer configured → accept all
        AuthenticationRequest req = oauthRequest("2701084255", fakeJwt("{\"sub\":\"2701084255\"}"));
        assertTrue(plugin.supports(req));
    }

    @Test
    void testDoesNotSupportClearPassword() {
        AuthenticationRequest req = AuthenticationRequest.builder()
                .username("user")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("password".getBytes())
                .build();
        assertFalse(plugin.supports(req));
    }

    // ── authenticate: empty token ─────────────────────────────────────────────

    @Test
    void testAuthenticate_noCredential() throws AuthenticationException {
        AuthenticationRequest req = AuthenticationRequest.builder()
                .username("2701084255")
                .credentialType(CredentialType.OAUTH_TOKEN)
                .credential(new byte[0])
                .build();

        AuthenticationResult result = plugin.authenticate(req, integration);
        assertFalse(result.isSuccess());
    }

    // ── authenticate: expiry checked from userinfo response ───────────────────

    @Test
    void testAuthenticate_expiredToken_fromUserinfo() throws AuthenticationException {
        // IDMS returns exp = 1 (Unix epoch — long expired)
        plugin.returnUserinfo(userinfoFor("2701084255", 1L));

        AuthenticationResult result = plugin.authenticate(
                oauthRequest("2701084255", fakeJwt("{\"sub\":\"2701084255\"}")), integration);

        assertFalse(result.isSuccess());
        assertNotNull(result.getFailureMessage());
        assertTrue(result.getFailureMessage().contains("expired"),
                "Expected 'expired' in: " + result.getFailureMessage());
    }

    @Test
    void testAuthenticate_validToken_notExpired() throws AuthenticationException {
        long futureExp = Instant.now().getEpochSecond() + 3600;
        plugin.returnUserinfo(userinfoFor("2701084255", futureExp));

        AuthenticationResult result = plugin.authenticate(
                oauthRequest("2701084255", fakeJwt("{\"sub\":\"2701084255\"}")), integration);

        assertTrue(result.isSuccess());
    }

    // ── authenticate: DSID match checked from userinfo sub claim ─────────────

    @Test
    void testAuthenticate_dsidMismatch_fromUserinfo() throws AuthenticationException {
        long futureExp = Instant.now().getEpochSecond() + 3600;
        // userinfo returns sub = "2701084255" but login is as "9999999999"
        plugin.returnUserinfo(userinfoFor("2701084255", futureExp));

        AuthenticationResult result = plugin.authenticate(
                oauthRequest("9999999999", fakeJwt("{\"sub\":\"9999999999\"}")), integration);

        assertFalse(result.isSuccess());
        assertNotNull(result.getFailureMessage());
        assertTrue(result.getFailureMessage().contains("subject does not match"),
                "Expected 'subject does not match' in: " + result.getFailureMessage());
    }

    @Test
    void testAuthenticate_dsidMatchesNumericSubInUserinfo() throws AuthenticationException {
        // IDMS returns dsid as a JSON number — plugin must handle both number and string
        long futureExp = Instant.now().getEpochSecond() + 3600;
        String json = String.format(
                "{\"sub\":2701084255,\"dsid\":2701084255,\"exp\":%d,"
                + "\"extended_claims\":{\"allGroups\":[\"13455565\"]}}", futureExp);
        plugin.returnUserinfo(JsonParser.parseString(json).getAsJsonObject());

        AuthenticationResult result = plugin.authenticate(
                oauthRequest("2701084255", fakeJwt("{\"sub\":\"2701084255\"}")), integration);

        assertTrue(result.isSuccess());
    }

    // ── authenticate: groups extracted from userinfo ──────────────────────────

    @Test
    void testAuthenticate_groupsExtractedFromExtendedClaims() throws AuthenticationException {
        long futureExp = Instant.now().getEpochSecond() + 3600;
        plugin.returnUserinfo(userinfoFor("2701084255", futureExp));

        AuthenticationResult result = plugin.authenticate(
                oauthRequest("2701084255", fakeJwt("{\"sub\":\"2701084255\"}")), integration);

        assertTrue(result.isSuccess());
        Set<String> groups = result.getPrincipal().getExternalGroups();
        assertTrue(groups.contains("13455565"));
        assertTrue(groups.contains("8675466"));
    }

    @Test
    void testAuthenticate_noGroupsReturnedWhenExtendedClaimsMissing() throws AuthenticationException {
        long futureExp = Instant.now().getEpochSecond() + 3600;
        String json = String.format("{\"sub\":\"2701084255\",\"exp\":%d}", futureExp);
        plugin.returnUserinfo(JsonParser.parseString(json).getAsJsonObject());

        AuthenticationResult result = plugin.authenticate(
                oauthRequest("2701084255", fakeJwt("{\"sub\":\"2701084255\"}")), integration);

        assertTrue(result.isSuccess());
        assertTrue(result.getPrincipal().getExternalGroups().isEmpty());
    }

    // ── authenticate: userinfo call failure ───────────────────────────────────

    @Test
    void testAuthenticate_userinfoHttpError_throwsSourceUnavailable() {
        plugin.throwOnUserinfo(new RuntimeException("connection refused"));

        assertThrows(AuthenticationException.class, () ->
                plugin.authenticate(
                        oauthRequest("2701084255", fakeJwt("{\"sub\":\"2701084255\"}")),
                        integration));
    }

    // ── extractGroups ─────────────────────────────────────────────────────────

    @Test
    void testExtractGroups_nestedPath() {
        String json = "{\"extended_claims\":{\"allGroups\":[\"13455565\",\"8675466\"]}}";
        JsonObject userinfo = JsonParser.parseString(json).getAsJsonObject();

        Set<String> groups = OidcAuthenticationPlugin.extractGroups(
                userinfo, "extended_claims.allGroups");

        assertEquals(2, groups.size());
        assertTrue(groups.contains("13455565"));
        assertTrue(groups.contains("8675466"));
    }

    @Test
    void testExtractGroups_missingPath_returnsEmpty() {
        Set<String> groups = OidcAuthenticationPlugin.extractGroups(
                JsonParser.parseString("{\"sub\":\"123\"}").getAsJsonObject(),
                "extended_claims.allGroups");
        assertTrue(groups.isEmpty());
    }

    // ── Issuer filtering ──────────────────────────────────────────────────────

    @Test
    void testSupports_issuerMatches_acceptsToken() throws AuthenticationException {
        AuthenticationIntegration withIssuer = AuthenticationIntegration.builder()
                .name("idms_global").type("oidc")
                .property(OidcAuthenticationPlugin.PROP_USERINFO_URL, "https://idmsac.apple.com/auth/oauth2/userinfo")
                .property(OidcAuthenticationPlugin.PROP_CLIENT_ID, "66rrcbuuyvp3f6fszcv5mybsr3vai9")
                .property(OidcAuthenticationPlugin.PROP_ISSUER, "https://idmsac.apple.com")
                .build();
        plugin.initialize(withIssuer);

        String jwt = fakeJwt("{\"sub\":\"2701084255\",\"iss\":\"https://idmsac.apple.com\"}");
        assertTrue(plugin.supports(oauthRequest("2701084255", jwt)));
    }

    @Test
    void testSupports_issuerMismatch_returnsFalseCleanly() throws AuthenticationException {
        AuthenticationIntegration withIssuer = AuthenticationIntegration.builder()
                .name("idms_global").type("oidc")
                .property(OidcAuthenticationPlugin.PROP_USERINFO_URL, "https://idmsac.apple.com/auth/oauth2/userinfo")
                .property(OidcAuthenticationPlugin.PROP_CLIENT_ID, "66rrcbuuyvp3f6fszcv5mybsr3vai9")
                .property(OidcAuthenticationPlugin.PROP_ISSUER, "https://idmsac.apple.com")
                .build();
        plugin.initialize(withIssuer);

        String chinaJwt = fakeJwt("{\"sub\":\"2701084255\",\"iss\":\"https://idmsac-cn.apple.com\"}");
        assertFalse(plugin.supports(oauthRequest("2701084255", chinaJwt)));
    }

    @Test
    void testSupports_noIssuerConfigured_acceptsAllJwt() throws AuthenticationException {
        plugin.initialize(integration); // no issuer property
        String anyJwt = fakeJwt("{\"sub\":\"2701084255\",\"iss\":\"https://any-provider.com\"}");
        assertTrue(plugin.supports(oauthRequest("2701084255", anyJwt)));
    }

    // ── validate() ───────────────────────────────────────────────────────────

    @Test
    void testValidate_missingUserinfoUrl() {
        AuthenticationIntegration bad = AuthenticationIntegration.builder()
                .name("bad").type("oidc")
                .property(OidcAuthenticationPlugin.PROP_CLIENT_ID, "some-id")
                .build();
        assertThrows(AuthenticationException.class, () -> plugin.validate(bad));
    }

    @Test
    void testValidate_missingClientId() {
        AuthenticationIntegration bad = AuthenticationIntegration.builder()
                .name("bad").type("oidc")
                .property(OidcAuthenticationPlugin.PROP_USERINFO_URL, "https://example.com/userinfo")
                .build();
        assertThrows(AuthenticationException.class, () -> plugin.validate(bad));
    }

    @Test
    void testValidate_valid() throws AuthenticationException {
        plugin.validate(integration); // should not throw
    }
}
