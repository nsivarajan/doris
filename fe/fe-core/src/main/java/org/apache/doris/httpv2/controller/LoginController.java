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

package org.apache.doris.httpv2.controller;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.HttpAuthManager;
import org.apache.doris.mysql.authenticate.mtls.MTLSUtils;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
@RequestMapping("/rest/v1")
public class LoginController extends BaseController {
    private static final Logger LOG = LogManager.getLogger(LoginController.class);

    @RequestMapping(path = "/login", method = RequestMethod.POST)
    public Object login(HttpServletRequest request, HttpServletResponse response) {
        Map<String, Object> msg = new HashMap<>();
        
        // Check if we're in MTLS mode
        if ("mtls".equalsIgnoreCase(Config.authentication_type)) {
            try {
                // For MTLS, authenticate using client certificate
                boolean success = handleMTLSAuthentication(request, response);
                if (success) {
                    msg.put("code", 200);
                    msg.put("msg", "MTLS authentication successful");
                    return msg;
                } else {
                    msg.put("code", 401);
                    msg.put("msg", "MTLS authentication failed");
                    return msg;
                }
            } catch (Exception e) {
                LOG.error("Error during MTLS authentication", e);
                msg.put("code", 500);
                msg.put("msg", "Internal server error during authentication");
                return msg;
            }
        } else {
            // For non-MTLS mode, use standard cookie check
            checkAuthWithCookie(request, response);
            msg.put("code", 200);
            msg.put("msg", "Login success!");
            return msg;
        }
    }
    
    /**
     * Handle MTLS authentication by checking client certificate
     *
     * @param request HTTP request containing client certificate
     * @param response HTTP response
     * @return true if authentication succeeded, false otherwise
     */
    private boolean handleMTLSAuthentication(HttpServletRequest request, HttpServletResponse response) {
        X509Certificate[] certs = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");
        if (certs == null || certs.length == 0) {
            LOG.warn("No client certificate presented for mTLS HTTP authentication");
            return false;
        }
        
        X509Certificate clientCert = certs[0];
        
        try {
            // Generate username from certificate serial number
            String username = MTLSUtils.getUsernameFromCertificate(clientCert);
            String serialNumber = MTLSUtils.getSerialNumberHex(clientCert);
            
            LOG.info("Login: Generated username '{}' for certificate with serial number '{}'", username, serialNumber);
            
            // Check if user exists with generated username
            UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp(username, "%");
            if (!Env.getCurrentEnv().getAuth().doesUserExist(userIdentity)) {
                LOG.warn("Login: No Doris user found for username: {} (certificate serial: {})", username, serialNumber);
                return false;
            }
            
            // Set up ConnectContext for this user
            ConnectContext ctx = new ConnectContext();
            ctx.setQualifiedUser(username);
            ctx.setRemoteIP(request.getRemoteAddr());
            ctx.setCurrentUserIdentity(userIdentity);
            ctx.setEnv(Env.getCurrentEnv());
            ctx.setThreadLocalInfo();
            
            // Create a session for this user
            HttpAuthManager.SessionValue value = new HttpAuthManager.SessionValue();
            value.currentUser = userIdentity;
            value.password = ""; // No password for MTLS
            addSession(request, response, value);
            
            LOG.info("MTLS HTTP authentication succeeded for username: {}", username);
            return true;
        } catch (AnalysisException e) {
            LOG.error("Failed to authenticate with mTLS: {}", e.getMessage());
            return false;
        } catch (Exception e) {
            LOG.error("Exception during mTLS authentication", e);
            return false;
        }
    }
}
