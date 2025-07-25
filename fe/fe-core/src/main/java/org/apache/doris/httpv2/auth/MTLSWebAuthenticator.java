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

package org.apache.doris.httpv2.auth;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.controller.BaseController.ActionAuthorizationInfo;
import org.apache.doris.mysql.authenticate.mtls.MTLSUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.cert.X509Certificate;
import javax.servlet.http.HttpServletRequest;

/**
 * Authenticator for MTLS authentication in the web UI
 */
public class MTLSWebAuthenticator {
    private static final Logger LOG = LogManager.getLogger(MTLSWebAuthenticator.class);
    private static final String CERT_ATTRIBUTE = "javax.servlet.request.X509Certificate";

    /**
     * Authenticate a user using client certificate
     * @param request The HTTP request containing the client certificate
     * @return ActionAuthorizationInfo if authentication succeeds, null otherwise
     */
    public static ActionAuthorizationInfo authenticate(HttpServletRequest request) {
        if (!"mtls".equalsIgnoreCase(Config.authentication_type)) {
            return null;
        }

        try {
            // Get client certificate from request
            X509Certificate[] certs = (X509Certificate[]) request.getAttribute(CERT_ATTRIBUTE);
            if (certs == null || certs.length == 0) {
                LOG.warn("No client certificate found for MTLS web authentication");
                return null;
            }

            X509Certificate clientCert = certs[0];

            // Generate username from certificate serial number
            String username = MTLSUtils.getUsernameFromCertificate(clientCert);
            String serialNumber = MTLSUtils.getSerialNumberHex(clientCert);

            LOG.info("Web UI MTLS: Generated username '{}' for certificate with serial number '{}'",
                    username, serialNumber);

            // Look up Doris user by generated username
            UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp(username, "%");
            if (!Env.getCurrentEnv().getAuth().doesUserExist(userIdentity)) {
                LOG.warn("Web UI MTLS: No Doris user found for username: {} (certificate serial: {})",
                        username, serialNumber);
                return null;
            }

            // Create authorization info
            ActionAuthorizationInfo authInfo = new ActionAuthorizationInfo();
            authInfo.fullUserName = userIdentity.getQualifiedUser();
            authInfo.remoteIp = request.getRemoteAddr();
            // Password is not used in MTLS authentication
            authInfo.password = "";

            LOG.info("Web UI MTLS authentication successful for user: {}", username);
            return authInfo;
        } catch (AnalysisException e) {
            LOG.error("Failed to authenticate with MTLS for web UI: {}", e.getMessage());
            return null;
        } catch (Exception e) {
            LOG.error("Exception during MTLS authentication for web UI", e);
            return null;
        }
    }
}
