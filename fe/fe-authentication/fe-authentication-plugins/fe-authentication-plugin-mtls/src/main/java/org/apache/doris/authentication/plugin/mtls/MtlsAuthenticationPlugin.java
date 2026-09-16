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

package org.apache.doris.authentication.plugin.mtls;

import org.apache.doris.authentication.AuthenticationException;
import org.apache.doris.authentication.AuthenticationFailureType;
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.BasicPrincipal;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.authentication.Principal;
import org.apache.doris.authentication.spi.AuthenticationPlugin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateFactory;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * mTLS authentication plugin — authenticates clients via X.509 client certificates.
 *
 * <p>Designed for Apple Corporate PKI group certificates where:
 * <ul>
 *   <li>CN is an opaque public-key hash (not a username)</li>
 *   <li>OU contains {@code management:idms.group.<id>}</li>
 *   <li>SAN URI contains {@code aprn:apple:certmgr:::group-v2:/ig/<id>/uv/}</li>
 * </ul>
 *
 * <p>Identity extraction priority:
 * <ol>
 *   <li>SAN email — used as username when present (user certificates)</li>
 *   <li>SAN URI group ID — used to derive service account name (group certificates)</li>
 *   <li>OU group ID — fallback group extraction</li>
 * </ol>
 *
 * <p>Usage:
 * <pre>{@code
 * CREATE AUTHENTICATION INTEGRATION apple_mtls
 *   TYPE = 'mtls'
 *   WITH (
 *     'trusted_ca_pem'       = '-----BEGIN CERTIFICATE-----\n...',
 *     'san_uri_pattern'      = '.*?/ig/(\d+)/.*',
 *     'group_ou_pattern'     = 'management:idms\.group\.(\d+)',
 *     'service_account_prefix' = 'svc-group-',
 *     'require_role_match'   = 'true'
 *   );
 *
 * -- Bind to authentication_chain:
 * SET GLOBAL authentication_chain = 'apple_mtls';
 *
 * -- Map group to role:
 * GRANT data_pipeline_role TO GROUP '13226496';
 * }</pre>
 */
public class MtlsAuthenticationPlugin implements AuthenticationPlugin {

    private static final Logger LOG = LogManager.getLogger(MtlsAuthenticationPlugin.class);

    public static final String PLUGIN_NAME = "mtls";

    static final String PROP_TRUSTED_CA_PEM           = "trusted_ca_pem";
    static final String PROP_TRUSTED_CA_PATH          = "trusted_ca_path";
    static final String PROP_SAN_URI_GROUP_PATTERN    = "san_uri_pattern";
    static final String PROP_SAN_URI_PERSON_PATTERN   = "san_uri_person_pattern";
    static final String PROP_GROUP_OU_PATTERN         = "group_ou_pattern";
    static final String PROP_PERSON_UID_PATTERN       = "person_uid_pattern";
    static final String PROP_GROUP_USERNAME_PREFIX    = "group_username_prefix";
    static final String PROP_PERSON_USERNAME_PREFIX   = "person_username_prefix";
    static final String PROP_REQUIRE_ROLE_MATCH       = "require_role_match";

    static final String DEFAULT_SAN_URI_GROUP_PATTERN  = ".*?/ig/(\\d+)/.*";
    static final String DEFAULT_SAN_URI_PERSON_PATTERN = ".*?/pid/(\\d+)/.*";
    static final String DEFAULT_GROUP_OU_PATTERN       = "management:idms\\.group\\.(\\d+)";
    static final String DEFAULT_PERSON_UID_PATTERN     = "identity:idms\\.person\\.(\\d+)";
    static final String DEFAULT_GROUP_USERNAME_PREFIX  = "g_";
    static final String DEFAULT_PERSON_USERNAME_PREFIX = "p_";

    // Compiled once per integration instance in initialize() — patterns are config-driven
    // and must not be recompiled on every authentication request (hot path).
    private volatile Pattern compiledSanUriGroupPattern;
    private volatile Pattern compiledSanUriPersonPattern;
    private volatile Pattern compiledGroupOuPattern;
    private volatile Pattern compiledPersonUidPattern;
    private volatile String groupPrefix  = DEFAULT_GROUP_USERNAME_PREFIX;
    private volatile String personPrefix = DEFAULT_PERSON_USERNAME_PREFIX;
    // Trusted CAs loaded once from trusted_ca_pem (inline PEM) or trusted_ca_path (file).
    // Null means issuer validation is skipped — the TLS layer is trusted to enforce CA.
    private volatile List<X509Certificate> trustedCAs;

    @Override
    public String name() {
        return PLUGIN_NAME;
    }

    @Override
    public String description() {
        return "mTLS authentication plugin — authenticates via X.509 client certificates";
    }

    @Override
    public boolean requiresClearPassword() {
        return false;
    }

    @Override
    public boolean supports(AuthenticationRequest request) {
        return CredentialType.X509_CERTIFICATE.equalsIgnoreCase(request.getCredentialType());
    }

    @Override
    public AuthenticationResult authenticate(AuthenticationRequest request,
            AuthenticationIntegration integration) throws AuthenticationException {

        byte[] certDer = request.getCredential();
        if (certDer == null || certDer.length == 0) {
            return AuthenticationResult.failure(AuthenticationFailureType.BAD_CREDENTIAL,
                    "No client certificate provided");
        }

        // ── 1. Decode certificate ─────────────────────────────────────────────
        X509Certificate cert;
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            cert = (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(certDer));
        } catch (Exception e) {
            return AuthenticationResult.failure(AuthenticationFailureType.BAD_CREDENTIAL,
                    "Failed to parse client certificate: " + e.getMessage());
        }

        // ── 2. Check validity window ─────────────────────────────────────────
        try {
            cert.checkValidity(new Date());
        } catch (CertificateExpiredException e) {
            return AuthenticationResult.failure(AuthenticationFailureType.BAD_CREDENTIAL,
                    "Client certificate has expired");
        } catch (CertificateNotYetValidException e) {
            return AuthenticationResult.failure(AuthenticationFailureType.BAD_CREDENTIAL,
                    "Client certificate is not yet valid");
        }

        // ── 3. Verify issuer against trusted CAs loaded in initialize() ──────
        if (trustedCAs != null) {
            if (!isTrustedIssuer(cert, trustedCAs)) {
                LOG.warn("mTLS cert issuer not trusted: issuer={}",
                        cert.getIssuerX500Principal().getName());
                return AuthenticationResult.failure(AuthenticationFailureType.BAD_CREDENTIAL,
                        "Client certificate issuer is not trusted");
            }
        } else {
            LOG.warn("mTLS integration '{}' has no trusted_ca_pem/trusted_ca_path configured — "
                    + "issuer validation skipped. Ensure the TLS terminator enforces CA validation.",
                    integration.getName());
        }

        // ── 4. Extract identity using patterns compiled in initialize() ───────
        CertIdentity identity = extractIdentity(cert,
                compiledSanUriGroupPattern, compiledSanUriPersonPattern,
                compiledGroupOuPattern, compiledPersonUidPattern,
                groupPrefix, personPrefix);

        if (identity.username == null || identity.username.isEmpty()) {
            return AuthenticationResult.failure(AuthenticationFailureType.BAD_CREDENTIAL,
                    "Could not derive username from certificate");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("mTLS auth OK — user={} groups={} subject={}",
                    identity.username, identity.groups,
                    cert.getSubjectX500Principal().getName());
        }

        // ── 5. Build Principal — RBAC role mapping handled by fe-core ────────
        Principal principal = BasicPrincipal.builder()
                .name(identity.username)
                .authenticator(PLUGIN_NAME)
                .externalGroups(identity.groups)
                .build();

        return AuthenticationResult.success(principal);
    }

    @Override
    public void validate(AuthenticationIntegration integration) throws AuthenticationException {
        // If trusted_ca_path is provided, verify the file is readable at creation time
        // so operators get a clear error immediately rather than on first auth attempt.
        String caPath = integration.getProperty(PROP_TRUSTED_CA_PATH, "").trim();
        if (!caPath.isEmpty()) {
            java.io.File f = new java.io.File(caPath);
            if (!f.isFile() || !f.canRead()) {
                throw new AuthenticationException(
                        "trusted_ca_path '" + caPath + "' is not a readable file",
                        AuthenticationFailureType.MISCONFIGURED);
            }
        }
    }

    @Override
    public void initialize(AuthenticationIntegration integration) throws AuthenticationException {
        // Load trusted CAs once — supports both inline PEM and file path.
        // trusted_ca_path takes precedence over trusted_ca_pem when both are set.
        String caPath = integration.getProperty(PROP_TRUSTED_CA_PATH, "").trim();
        String caPem  = integration.getProperty(PROP_TRUSTED_CA_PEM,  "").trim();
        if (!caPath.isEmpty()) {
            try {
                byte[] bytes = java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(caPath));
                trustedCAs = parseCaPem(new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
            } catch (Exception e) {
                throw new AuthenticationException(
                        "Failed to read trusted_ca_path '" + caPath + "': " + e.getMessage(),
                        AuthenticationFailureType.MISCONFIGURED);
            }
        } else if (!caPem.isEmpty()) {
            trustedCAs = parseCaPem(caPem);
        } else {
            trustedCAs = null;
        }

        // Compile patterns once here rather than on every authentication request.
        compiledSanUriGroupPattern  = Pattern.compile(integration.getProperty(
                PROP_SAN_URI_GROUP_PATTERN, DEFAULT_SAN_URI_GROUP_PATTERN));
        compiledSanUriPersonPattern = Pattern.compile(integration.getProperty(
                PROP_SAN_URI_PERSON_PATTERN, DEFAULT_SAN_URI_PERSON_PATTERN));
        compiledGroupOuPattern      = Pattern.compile(integration.getProperty(
                PROP_GROUP_OU_PATTERN, DEFAULT_GROUP_OU_PATTERN));
        compiledPersonUidPattern    = Pattern.compile(integration.getProperty(
                PROP_PERSON_UID_PATTERN, DEFAULT_PERSON_UID_PATTERN));
        groupPrefix  = integration.getProperty(PROP_GROUP_USERNAME_PREFIX,  DEFAULT_GROUP_USERNAME_PREFIX);
        personPrefix = integration.getProperty(PROP_PERSON_USERNAME_PREFIX, DEFAULT_PERSON_USERNAME_PREFIX);
    }

    // ── Package-private for testing ───────────────────────────────────────────

    static final class CertIdentity {
        final String username;
        final Set<String> groups;

        CertIdentity(String username, Set<String> groups) {
            this.username = username;
            this.groups = Collections.unmodifiableSet(groups);
        }
    }

    /**
     * Extracts username and group set from a certificate.
     *
     * <p>Handles two Apple PKI cert types:
     *
     * <p><b>Group cert</b> (service account / pipeline):
     * <ul>
     *   <li>SAN URI: {@code aprn:apple:certmgr:::group-v2:/ig/13226496/uv/}</li>
     *   <li>OU: {@code management:idms.group.13226496}</li>
     *   <li>Username derived: {@code svc-group-13226496}</li>
     *   <li>Groups: extracted group IDs → used for auto_match_groups_to_roles</li>
     * </ul>
     *
     * <p><b>Person cert</b> (individual user):
     * <ul>
     *   <li>SAN URI: {@code aprn:apple:certmgr:::person-v2:/pid/2304357084/uv/}</li>
     *   <li>UID: {@code identity:idms.person.2304357084}</li>
     *   <li>Username: DSID string (aligns with OIDC {@code sub} claim)</li>
     *   <li>Groups: empty — use static role grants in Doris</li>
     * </ul>
     */
    static CertIdentity extractIdentity(X509Certificate cert,
            String sanUriGroupPatternStr, String sanUriPersonPatternStr,
            String groupOuPatternStr, String personUidPatternStr,
            String groupPrefix, String personPrefix) {
        return extractIdentity(cert,
                Pattern.compile(sanUriGroupPatternStr), Pattern.compile(sanUriPersonPatternStr),
                Pattern.compile(groupOuPatternStr), Pattern.compile(personUidPatternStr),
                groupPrefix, personPrefix);
    }

    static CertIdentity extractIdentity(X509Certificate cert,
            Pattern groupUriPattern, Pattern personUriPattern,
            Pattern ouGroupPattern, Pattern uidPersonPattern,
            String groupPrefix, String personPrefix) {
        Set<String> groups = new HashSet<>();
        String emailUsername = null;
        String personDsid = null;
        boolean isPersonCert = false;

        // ── Extract from SAN entries ──────────────────────────────────────────
        try {
            Collection<List<?>> sans = cert.getSubjectAlternativeNames();
            if (sans != null) {
                for (List<?> san : sans) {
                    Integer type = (Integer) san.get(0);
                    String value = String.valueOf(san.get(1));
                    if (type == 1) {
                        // rfc822Name (email) — user cert with explicit email
                        if (emailUsername == null) {
                            emailUsername = value;
                        }
                    } else if (type == 6) {
                        // uniformResourceIdentifier (URI)
                        Matcher gm = groupUriPattern.matcher(value);
                        if (gm.matches() && gm.groupCount() >= 1) {
                            groups.add(gm.group(1));
                        } else {
                            Matcher pm = personUriPattern.matcher(value);
                            if (pm.matches() && pm.groupCount() >= 1) {
                                personDsid = pm.group(1);
                                isPersonCert = true;
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse SAN entries from certificate: {}", e.getMessage());
        }

        // Split on unescaped commas only — RFC 4514 allows escaped commas in values (e.g. CN=Smith\, John)
        String dn = cert.getSubjectX500Principal().getName();
        for (String part : dn.split("(?<!\\\\),")) {
            String trimmed = part.trim();
            if (trimmed.startsWith("OU=")) {
                String ouValue = trimmed.substring(3);
                Matcher m = ouGroupPattern.matcher(ouValue);
                if (m.find() && m.groupCount() >= 1) {
                    groups.add(m.group(1));
                }
            } else if (trimmed.startsWith("UID=")) {
                String uidValue = trimmed.substring(4);
                Matcher m = uidPersonPattern.matcher(uidValue);
                if (m.find() && m.groupCount() >= 1) {
                    personDsid = m.group(1);
                    isPersonCert = true;
                }
            }
        }

        // ── Derive username and normalise groups ──────────────────────────────
        String username;
        if (emailUsername != null) {
            // Explicit email SAN — use as-is (rare, non-Apple PKI path)
            username = emailUsername;
        } else if (isPersonCert && personDsid != null) {
            // Person cert: p_<dsid> — valid SQL identifier, no backticks needed.
            // Add DSID as an external group so auto_match_groups_to_roles can fire:
            //   CREATE ROLE `2304357084`; GRANT ... TO ROLE `2304357084`;
            username = personPrefix + personDsid;
            groups.add(personDsid);
        } else if (!groups.isEmpty()) {
            // Group cert: g_<groupid>. Use numeric sort for numeric IDs (most Apple group IDs);
            // fall back to natural string order for non-numeric IDs. Smallest ID wins for determinism.
            String groupId = groups.stream()
                    .min(java.util.Comparator.comparingLong(s -> {
                        try { return Long.parseLong(s); } catch (NumberFormatException e) { return Long.MAX_VALUE; }
                    }).thenComparing(java.util.Comparator.naturalOrder()))
                    .get();
            username = groupPrefix + groupId;
        } else {
            // Last resort: short CN (< 48 chars means it's not an opaque hash)
            String cn = extractCn(dn);
            username = (cn != null && cn.length() < 48) ? cn : null;
        }

        return new CertIdentity(username, groups);
    }

    /**
     * Parses one or more PEM-encoded CA certificates from a string.
     * Uses CertificateFactory.generateCertificates() which accepts PEM input directly,
     * handles concatenated multi-cert PEM, and supports all standard header variants
     * (including CRLF line endings and TRUSTED CERTIFICATE headers).
     */
    static List<X509Certificate> parseCaPem(String pem) {
        List<X509Certificate> certs = new ArrayList<>();
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            byte[] pemBytes = pem.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
            for (java.security.cert.Certificate c :
                    cf.generateCertificates(new ByteArrayInputStream(pemBytes))) {
                certs.add((X509Certificate) c);
            }
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse trusted CA PEM: {}", e.getMessage());
        }
        return certs;
    }

    /**
     * Checks that the cert's issuer matches one of the trusted CA subjects using
     * encoded-byte comparison (same as Java's PKIX path builder) rather than getName()
     * string comparison, which is fragile under RFC 4514 encoding differences.
     *
     * <p><b>Deployment requirement:</b> This method verifies issuer identity only —
     * it does NOT verify the cert's cryptographic signature against the CA public key.
     * The TLS terminator MUST perform full chain validation before this code is reached.
     * If the TLS layer is a passthrough proxy that does not enforce CA validation,
     * {@code trusted_ca_pem} alone provides no security: an attacker who knows the CA's DN
     * could craft a self-signed cert that passes this check. Optionally call
     * {@code cert.verify(ca.getPublicKey())} when defense-in-depth is required.
     */
    static boolean isTrustedIssuer(X509Certificate cert, List<X509Certificate> trustedCAs) {
        byte[] issuerEncoded = cert.getIssuerX500Principal().getEncoded();
        for (X509Certificate ca : trustedCAs) {
            if (java.util.Arrays.equals(issuerEncoded, ca.getSubjectX500Principal().getEncoded())) {
                return true;
            }
        }
        return false;
    }

    private static String extractCn(String dn) {
        for (String part : dn.split("(?<!\\\\),")) {
            String trimmed = part.trim();
            if (trimmed.startsWith("CN=")) {
                return trimmed.substring(3);
            }
        }
        return null;
    }
}
