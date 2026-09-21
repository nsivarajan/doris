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

package org.apache.doris.mysql.authenticate;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.auth.certificate.CertificateAuthDecision;
import org.apache.doris.auth.certificate.CertificateRuntimeAuthFactory;
import org.apache.doris.auth.certificate.CertificateRuntimeAuthService;
import org.apache.doris.authentication.AuthenticationFailureType;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.authentication.Principal;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.util.ClassLoaderUtils;
import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.authenticate.integration.AuthenticationIntegrationAuthenticator;
import org.apache.doris.mysql.authenticate.password.ClearPassword;
import org.apache.doris.mysql.authenticate.password.Password;
import org.apache.doris.mysql.authenticate.plugin.AuthenticationPluginAuthenticator;
import org.apache.doris.plugin.PropertiesUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;

/**
 * MySQL authenticator entry that keeps legacy auth-type compatibility while supporting plugin-based authenticators.
 *
 * <p>The compatibility rules are:
 * <ul>
 *     <li>{@code password} remains an alias of {@code default}</li>
 *     <li>built-in and customer legacy {@link AuthenticatorFactory} implementations are resolved first</li>
 *     <li>authentication plugins are used as the fallback when no legacy factory matches</li>
 * </ul>
 */
public class AuthenticatorManager {
    private static final Logger LOG = LogManager.getLogger(AuthenticatorManager.class);
    private static final String OIDC_CLIENT_PLUGIN_NAME = "authentication_openid_connect_client";
    static final String OPERATIONAL_AUTHENTICATION_FAILURE_MESSAGE =
            "Authentication failed because no configured authentication method succeeded due to service "
                    + "or configuration issues; check FE logs for details";
    private static final CertificateRuntimeAuthService CERT_RUNTIME_AUTH_SERVICE =
            CertificateRuntimeAuthFactory.getInstance();

    private static volatile Authenticator defaultAuthenticator = null;
    private static volatile Authenticator authTypeAuthenticator = null;
    private static volatile String authTypeIdentifier = null;
    private final MysqlAuthPacketCredentialExtractor authPacketCredentialExtractor =
            new MysqlAuthPacketCredentialExtractor();

    public AuthenticatorManager(String type) {
        String normalizedType = normalizeAuthTypeIdentifier(type);
        LOG.info("Authenticate type: {}", normalizedType);
        defaultAuthenticator = new DefaultAuthenticator();
        if (authTypeAuthenticator == null || !normalizedType.equalsIgnoreCase(authTypeIdentifier)) {
            synchronized (AuthenticatorManager.class) {
                if (authTypeAuthenticator == null || !normalizedType.equalsIgnoreCase(authTypeIdentifier)) {
                    try {
                        authTypeAuthenticator = loadFactoriesByName(normalizedType);
                        authTypeIdentifier = normalizedType;
                    } catch (Exception e) {
                        if (!AuthenticateType.DEFAULT.name().equalsIgnoreCase(normalizedType)) {
                            throw new IllegalStateException("Failed to load authenticator by name: "
                                    + normalizedType, e);
                        }
                        LOG.warn("Failed to load authenticator by name: {}, using default authenticator",
                                normalizedType, e);
                        authTypeAuthenticator = defaultAuthenticator;
                        authTypeIdentifier = AuthenticateType.DEFAULT.name();
                    }
                }
            }
        }
    }

    /**
     * Normalize auth-type names so old config values keep their historical meaning.
     */
    private String normalizeAuthTypeIdentifier(String type) {
        if (type == null) {
            return AuthenticateType.DEFAULT.name();
        }
        if ("password".equalsIgnoreCase(type) || AuthenticateType.DEFAULT.name().equalsIgnoreCase(type)) {
            return AuthenticateType.DEFAULT.name();
        }
        if (AuthenticateType.LDAP.name().equalsIgnoreCase(type)) {
            return AuthenticateType.LDAP.name();
        }
        return type.toLowerCase();
    }


    /**
     * Preserve the old resolution order: legacy factories first, plugin authenticator second.
     */
    private Authenticator loadFactoriesByName(String identifier) throws Exception {
        Authenticator authenticator = loadLegacyFactoryByName(identifier);
        if (authenticator != null) {
            return authenticator;
        }
        return loadPluginFactoryByName(identifier);
    }

    private Authenticator loadLegacyFactoryByName(String identifier) throws Exception {
        ServiceLoader<AuthenticatorFactory> loader = ServiceLoader.load(AuthenticatorFactory.class);
        for (AuthenticatorFactory factory : loader) {
            LOG.info("Found Authenticator Plugin Factory: {}", factory.factoryIdentifier());
            if (factory.factoryIdentifier().equalsIgnoreCase(identifier)) {
                Properties properties = PropertiesUtils.loadAuthenticationConfigFile();
                return factory.create(properties);
            }
        }
        return loadCustomerFactories(identifier);
    }

    private Authenticator loadCustomerFactories(String identifier) throws Exception {
        List<AuthenticatorFactory> factories = ClassLoaderUtils.loadServicesFromDirectory(AuthenticatorFactory.class);
        if (factories.isEmpty()) {
            return null;
        }
        for (AuthenticatorFactory factory : factories) {
            LOG.info("Found Customer Authenticator Plugin Factory: {}", factory.factoryIdentifier());
            if (factory.factoryIdentifier().equalsIgnoreCase(identifier)) {
                Properties properties = PropertiesUtils.loadAuthenticationConfigFile();
                return factory.create(properties);
            }
        }
        return null;
    }

    private Authenticator loadPluginFactoryByName(String identifier) throws Exception {
        Properties properties = PropertiesUtils.loadAuthenticationConfigFile();
        return new AuthenticationPluginAuthenticator(identifier, properties);
    }

    public boolean authenticate(ConnectContext context,
                                String userName,
                                MysqlChannel channel,
                                MysqlSerializer serializer,
                                MysqlAuthPacket authPacket,
                                MysqlHandshakePacket handshakePacket) throws IOException {

        String remoteIp = context.getMysqlChannel().getRemoteIp();
        CertificateAuthDecision certDecision = CERT_RUNTIME_AUTH_SERVICE.authenticateLive(
                userName, remoteIp, channel.getClientCertificate());
        if (certDecision.isReject()) {
            context.getState().setError(ErrorCode.ERR_ACCESS_DENIED_ERROR,
                    certDecision.getErrorMessage() == null ? "TLS certificate verification failed"
                            : certDecision.getErrorMessage());
            MysqlProto.sendResponsePacket(context);
            return false;
        }
        if (certDecision.shouldSkipPasswordVerification()) {
            context.setCurrentUserIdentity(certDecision.getUserIdentity());
            context.setRemoteIP(remoteIp);
            context.setIsTempUser(false);
            return true;
        }
        UserIdentity preferredUserIdentity = certDecision.isVerified() ? certDecision.getUserIdentity() : null;

        // mTLS path: if authentication_chain is configured and the client presented a TLS cert,
        // try to mine identity from the cert (SAN URI / OU group extraction).
        // Flow: mTLS → OIDC → LDAP → password (in chain order).
        // If mTLS cert mining fails or no mTLS plugin in chain, fall through to password/OIDC.
        // The cert is only used for identity here — transport enforcement already happened at
        // the TLS handshake level (ssl_force_client_auth=true rejects connections without cert).
        X509Certificate clientCert = channel.getClientCertificate();
        if (clientCert != null && hasAuthenticationChain()) {
            AuthenticateResponse mtlsResponse = tryMtlsChainAuth(context, remoteIp, clientCert, userName);
            if (mtlsResponse != null && mtlsResponse.isSuccess()) {
                // Compare against principal.getName() — the cert-derived username (e.g. "g_13226496").
                // getUserIdentity() reflects the DB-looked-up identity for the supplied userName, so it
                // always equals suppliedUser and cannot be used as a cert-binding check.
                Principal certPrincipal = mtlsResponse.getPrincipal();
                String certDerivedUser = certPrincipal != null ? certPrincipal.getName() : null;
                String suppliedUser = ClusterNamespace.getNameFromFullName(userName);
                if (certDerivedUser != null && certDerivedUser.equals(suppliedUser)) {
                    return finishSuccessfulAuthentication(context, remoteIp, mtlsResponse, true);
                }
                // cert belongs to a different identity — fall through to OIDC/password for suppliedUser
                LOG.debug("mTLS cert derived user '{}' != supplied username '{}' — skipping mTLS, "
                        + "falling through to OIDC/password", certDerivedUser, suppliedUser);
            }
            // non-success or null (user not found, misconfigured, transient error) — fall through to
            // OIDC/password so the supplied username can still authenticate via JWT
        }

        Authenticator primaryAuthenticator = chooseAuthenticator(userName, remoteIp);
        boolean debugEnabled = LOG.isDebugEnabled();
        long resolveStart = 0L;
        if (debugEnabled) {
            LOG.debug("AuthenticatorManager: user={}, authenticator={}",
                    userName, primaryAuthenticator.getClass().getSimpleName());
            resolveStart = System.currentTimeMillis();
        }
        Optional<AuthenticateRequest> primaryRequest = resolveAuthenticateRequest(primaryAuthenticator, userName,
                context, channel, serializer, authPacket, handshakePacket);
        if (!primaryRequest.isPresent()) {
            return false;
        }

        AuthenticateRequest request = primaryRequest.get();
        if (preferredUserIdentity != null) {
            request = request.withUserIdentity(preferredUserIdentity);
        }
        if (debugEnabled) {
            long resolveElapsed = System.currentTimeMillis() - resolveStart;
            LOG.debug("resolvePassword: user={}, elapsed={}ms", userName, resolveElapsed);
            resolveStart = System.currentTimeMillis();
        }
        remoteIp = request.getRemoteIp();
        if (isOidcAuthenticationWithoutSsl(authPacket, request)) {
            setInsecureOidcTransportError(context);
            return reportAuthenticationFailure(context, userName, remoteIp, request.getPassword(),
                    new ArrayList<>());
        }
        AuthenticateResponse primaryResponse = authenticateWith(primaryAuthenticator, request);
        if (debugEnabled) {
            long authenticateElapsed = System.currentTimeMillis() - resolveStart;
            LOG.debug("authenticate: user={}, elapsed={}ms", userName, authenticateElapsed);
        }
        if (primaryResponse.isSuccess()) {
            return finishSuccessfulAuthentication(context, remoteIp, primaryResponse, false);
        }
        List<AuthenticationFailureSummary> failureSummaries = new ArrayList<>();
        addFailureSummary(failureSummaries, primaryResponse);

        AuthenticateResponse chainResponse = tryAuthenticationChainFallback(context, userName, remoteIp,
                channel, serializer, authPacket, handshakePacket, request);
        if (chainResponse != null && chainResponse.isSuccess()) {
            return finishSuccessfulAuthentication(context, remoteIp, chainResponse, true);
        }
        addFailureSummary(failureSummaries, chainResponse);

        return reportAuthenticationFailure(context, userName, remoteIp, request.getPassword(), failureSummaries);
    }

    Authenticator chooseAuthenticator(String userName, String remoteIp) {
        Authenticator primaryAuthenticator = authTypeAuthenticator;
        return primaryAuthenticator.canDeal(userName) ? primaryAuthenticator : defaultAuthenticator;
    }

    Authenticator getAuthenticationChainAuthenticator() {
        return new AuthenticationIntegrationAuthenticator(Config.authentication_chain, "authentication_chain");
    }

    private void applyAuthenticateResponse(ConnectContext context, String remoteIp, AuthenticateResponse response) {
        context.setCurrentUserIdentity(response.getUserIdentity());
        context.setRemoteIP(remoteIp);
        context.setIsTempUser(response.isTemp());
        context.setAuthenticatedPrincipal(response.getPrincipal());
        context.setAuthenticatedRoles(response.getAuthenticatedRoles());
    }

    private Optional<AuthenticateRequest> resolveAuthenticateRequest(Authenticator authenticator,
            String userName,
            ConnectContext context,
            MysqlChannel channel,
            MysqlSerializer serializer,
            MysqlAuthPacket authPacket,
            MysqlHandshakePacket handshakePacket) throws IOException {
        return authenticator.getPasswordResolver().resolveAuthenticateRequest(userName, context, channel, serializer,
                authPacket, handshakePacket);
    }

    private AuthenticateResponse authenticateWith(Authenticator authenticator,
            AuthenticateRequest request) throws IOException {
        return authenticator.authenticate(request);
    }

    private boolean finishSuccessfulAuthentication(ConnectContext context, String remoteIp,
            AuthenticateResponse response, boolean setOkState) {
        if (setOkState) {
            context.getState().setOk();
        }
        applyAuthenticateResponse(context, remoteIp, response);
        return true;
    }

    private AuthenticateResponse tryAuthenticationChainFallback(ConnectContext context,
            String userName,
            String remoteIp,
            MysqlChannel channel,
            MysqlSerializer serializer,
            MysqlAuthPacket authPacket,
            MysqlHandshakePacket handshakePacket,
            AuthenticateRequest primaryRequest) throws IOException {
        if (!hasAuthenticationChain()) {
            return null;
        }

        Authenticator chainAuthenticator;
        try {
            chainAuthenticator = getAuthenticationChainAuthenticator();
        } catch (RuntimeException e) {
            LOG.warn("Failed to initialize authentication_chain fallback authenticator: {}", e.getMessage(), e);
            return AuthenticateResponse.failed(AuthenticationFailureSummary.forFailureType(
                    AuthenticationFailureType.MISCONFIGURED,
                    "Failed to initialize authentication_chain fallback authenticator: " + e.getMessage()));
        }
        if (!chainAuthenticator.canDeal(userName)) {
            return null;
        }

        AuthenticateRequest chainRequest = normalizeAuthenticationChainRequest(userName, channel, authPacket,
                primaryRequest);
        if (!canReuseRequestForAuthenticationChain(chainRequest)) {
            Optional<AuthenticateRequest> fallbackRequest = resolveAuthenticateRequest(chainAuthenticator, userName,
                    context, channel, serializer, authPacket, handshakePacket);
            if (!fallbackRequest.isPresent()) {
                return null;
            }
            chainRequest = fallbackRequest.get();
        }

        if (isOidcAuthenticationWithoutSsl(authPacket, chainRequest)) {
            setInsecureOidcTransportError(context);
            return AuthenticateResponse.failedResponse;
        }
        LOG.info("Try authentication_chain fallback for user '{}'", userName);
        return authenticateWith(chainAuthenticator, chainRequest);
    }

    private boolean hasAuthenticationChain() {
        return !AuthenticationIntegrationAuthenticator.parseAuthenticationChain(Config.authentication_chain).isEmpty();
    }

    private AuthenticateResponse tryMtlsChainAuth(ConnectContext context,
            String remoteIp, X509Certificate clientCert, String userName) {
        try {
            byte[] certDer = clientCert.getEncoded();
            // Pass the MySQL-supplied userName so mapSuccessfulAuthentication() can call
            // getUserIdentityForExternalAuth(userName, ...) and find the pre-created Doris user.
            // The mTLS plugin ignores the username field (it derives identity from the cert SAN/OU)
            // but the post-auth identity lookup in the integration layer needs it to resolve roles.
            AuthenticateRequest mtlsRequest = AuthenticateRequest.builder()
                    .userName(userName)
                    .credentialType(CredentialType.X509_CERTIFICATE)
                    .credential(certDer)
                    .remoteHost(remoteIp)
                    .clientType("mysql")
                    .build();

            AuthenticateResponse response = authenticateWith(getAuthenticationChainAuthenticator(), mtlsRequest);
            if (response != null && response.isSuccess()) {
                Principal principal = response.getPrincipal();
                LOG.info("mTLS MySQL auth OK for user={} groups={}",
                        principal != null ? principal.getName() : "?",
                        response.getAuthenticatedRoles().size());
            }
            return response;
        } catch (RuntimeException e) {
            // RuntimeException means the mTLS plugin EXISTS but is misconfigured (e.g. wrong cert path).
            // Return a hard failure — do NOT fall through to password auth, which would silently
            // downgrade security when mTLS was explicitly configured but broken.
            LOG.error("mTLS chain auth threw unexpected exception — denying connection. "
                    + "Check mTLS integration configuration: {}", e.getMessage(), e);
            return AuthenticateResponse.failedResponse;
        } catch (Exception e) {
            // IOException / checked exceptions from cert encoding — transient or protocol errors.
            // Fall through to password/OIDC since mTLS was not positively configured for this cert.
            LOG.warn("mTLS chain auth failed with checked exception, falling through: {}", e.getMessage());
            return null;
        }
    }

    private boolean isOidcAuthenticationWithoutSsl(MysqlAuthPacket authPacket, AuthenticateRequest request) {
        return isOidcAuthenticateRequest(request) && !authPacket.getCapability().isClientUseSsl();
    }

    private boolean isOidcAuthenticateRequest(AuthenticateRequest request) {
        if (CredentialType.OIDC_ID_TOKEN.equals(request.getCredentialType())
                || CredentialType.OAUTH_TOKEN.equals(request.getCredentialType())
                || CredentialType.JWT_TOKEN.equals(request.getCredentialType())) {
            return true;
        }
        if (!(request.getPassword() instanceof ClearPassword)) {
            return false;
        }
        String clearPassword = ((ClearPassword) request.getPassword()).getPassword();
        if (Strings.isNullOrEmpty(clearPassword)) {
            return false;
        }
        return looksLikeJwt(clearPassword);
    }

    private boolean looksLikeJwt(String token) {
        if (!token.startsWith("eyJ")) {
            return false;
        }

        int segmentSeparatorCount = 0;
        for (int i = 0; i < token.length(); i++) {
            if (token.charAt(i) == '.') {
                segmentSeparatorCount++;
            }
        }
        return segmentSeparatorCount == 2;
    }

    private void setInsecureOidcTransportError(ConnectContext context) {
        context.getState().setError(ErrorCode.ERR_SECURE_TRANSPORT_REQUIRED,
                "OIDC authentication requires TLS/SSL; reconnect with sslMode=REQUIRED");
    }

    private AuthenticateRequest normalizeAuthenticationChainRequest(String userName, MysqlChannel channel,
            MysqlAuthPacket authPacket, AuthenticateRequest primaryRequest) {
        Optional<AuthenticateRequest> authPacketRequest =
                authPacketCredentialExtractor.extractAuthenticateRequest(userName, channel, authPacket);
        if (authPacketRequest.isPresent()) {
            return authPacketRequest.get();
        }
        if (!OIDC_CLIENT_PLUGIN_NAME.equals(authPacket.getPluginName())
                || !(primaryRequest.getPassword() instanceof ClearPassword)) {
            return primaryRequest;
        }
        ClearPassword clearPassword = (ClearPassword) primaryRequest.getPassword();
        return AuthenticateRequest.builder()
                .userName(primaryRequest.getUserName())
                .password(clearPassword)
                .remoteHost(primaryRequest.getRemoteHost())
                .remotePort(primaryRequest.getRemotePort())
                .clientType(primaryRequest.getClientType())
                .credentialType(CredentialType.OAUTH_TOKEN)
                .credential(clearPassword.getPassword().getBytes(StandardCharsets.UTF_8))
                .properties(primaryRequest.getProperties())
                .build();
    }

    private boolean canReuseRequestForAuthenticationChain(AuthenticateRequest request) {
        if (CredentialType.CLEAR_TEXT_PASSWORD.equals(request.getCredentialType())) {
            return true;
        }
        return request.getPassword() instanceof ClearPassword;
    }

    private boolean reportAuthenticationFailure(ConnectContext context, String userName, String remoteIp,
            Password password, List<AuthenticationFailureSummary> failureSummaries) throws IOException {
        logAuthenticationFailureSummary(userName, remoteIp, failureSummaries);
        ensureAuthenticationErrorReported(context, userName, remoteIp, password, failureSummaries);
        if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            MysqlProto.sendResponsePacket(context);
        }
        return false;
    }

    private void ensureAuthenticationErrorReported(ConnectContext context, String userName, String remoteIp,
            Password password, List<AuthenticationFailureSummary> failureSummaries) {
        Optional<String> clientVisibleFailureMessage = findClientVisibleFailureMessage(failureSummaries);
        if (clientVisibleFailureMessage.isPresent()
                && shouldExposeClientVisibleFailureMessage(context, failureSummaries)) {
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, clientVisibleFailureMessage.get());
            return;
        }
        if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            return;
        }
        if (containsOnlyOperationalFailures(failureSummaries)) {
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, OPERATIONAL_AUTHENTICATION_FAILURE_MESSAGE);
            return;
        }
        context.getState().setError(ErrorCode.ERR_ACCESS_DENIED_ERROR,
                ErrorCode.ERR_ACCESS_DENIED_ERROR.formatErrorMsg(userName, remoteIp,
                        hasPassword(password) ? "YES" : "NO"));
    }

    private void addFailureSummary(List<AuthenticationFailureSummary> failureSummaries, AuthenticateResponse response) {
        if (response == null || response.getFailureSummary() == null) {
            return;
        }
        failureSummaries.add(response.getFailureSummary());
    }

    private Optional<String> findClientVisibleFailureMessage(List<AuthenticationFailureSummary> failureSummaries) {
        for (AuthenticationFailureSummary failureSummary : failureSummaries) {
            if (failureSummary.hasClientVisibleMessage()) {
                return Optional.of(failureSummary.getClientVisibleMessage());
            }
        }
        return Optional.empty();
    }

    private boolean containsSensitiveFailure(List<AuthenticationFailureSummary> failureSummaries) {
        for (AuthenticationFailureSummary failureSummary : failureSummaries) {
            if (failureSummary.isSensitiveToClient()) {
                return true;
            }
        }
        return false;
    }

    private boolean shouldExposeClientVisibleFailureMessage(ConnectContext context,
            List<AuthenticationFailureSummary> failureSummaries) {
        if (containsSensitiveFailure(failureSummaries)) {
            return false;
        }
        if (context.getState().getStateType() != QueryState.MysqlStateType.ERR) {
            return true;
        }
        return ErrorCode.ERR_ACCESS_DENIED_ERROR.equals(context.getState().getErrorCode());
    }

    private boolean containsOnlyOperationalFailures(List<AuthenticationFailureSummary> failureSummaries) {
        if (failureSummaries.isEmpty()) {
            return false;
        }
        for (AuthenticationFailureSummary failureSummary : failureSummaries) {
            if (failureSummary.isSensitiveToClient() || !failureSummary.isOperationalFailure()) {
                return false;
            }
        }
        return true;
    }

    private void logAuthenticationFailureSummary(String userName, String remoteIp,
            List<AuthenticationFailureSummary> failureSummaries) {
        if (failureSummaries.isEmpty()) {
            return;
        }
        LOG.warn("Authentication failed for user '{}' from '{}'. Failure summary: {}",
                userName, remoteIp, failureSummaries);
    }

    private boolean hasPassword(Password password) {
        if (password == null) {
            return false;
        }
        if (password instanceof ClearPassword) {
            return !Strings.isNullOrEmpty(((ClearPassword) password).getPassword());
        }
        return true;
    }
}
