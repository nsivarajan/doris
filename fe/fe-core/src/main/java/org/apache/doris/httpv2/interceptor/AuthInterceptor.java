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

package org.apache.doris.httpv2.interceptor;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.HttpAuthManager.SessionValue;
import org.apache.doris.httpv2.controller.BaseController;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.mysql.authenticate.mtls.MTLSUtils;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import java.security.cert.X509Certificate;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class AuthInterceptor extends BaseController implements HandlerInterceptor {
    private static final Logger LOG = LogManager.getLogger(AuthInterceptor.class);

    /**
     * Handle authentication for all requests.
     */
    @Override
    public boolean preHandle(HttpServletRequest request,
                             HttpServletResponse response, Object handler) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("AuthInterceptor preHandle: method={}, uri={}, remoteAddr={}",
                    request.getMethod(), request.getRequestURI(), request.getRemoteAddr());
        }

        // Check if this is a login/logout path that should be excluded
        String uri = request.getRequestURI();
        if (uri.equals("/rest/v1/login") || uri.equals("/rest/v1/logout")) {
            return true;
        }

        String method = request.getMethod();
        if (method.equalsIgnoreCase(RequestMethod.OPTIONS.toString())) {
            response.setStatus(HttpStatus.NO_CONTENT.value());
            return true;
        }

        // Choose authentication method based on configuration
        if ("mtls".equalsIgnoreCase(Config.authentication_type)) {
            return handleMTLSAuthentication(request, response);
        } else {
            // Default/LDAP: use existing logic
            checkAuthWithCookie(request, response);
            return true;
        }
    }

    /**
     * Handle MTLS authentication by first checking for a valid session cookie,
     * and falling back to certificate authentication if needed.
     */
    private boolean handleMTLSAuthentication(HttpServletRequest request, HttpServletResponse response)
            throws Exception {
        try {
            // First try to validate the session cookie
            if (LOG.isDebugEnabled()) {
                LOG.debug("MTLS mode: trying to validate session cookie first");
            }
            checkAuthWithCookie(request, response);
            if (LOG.isDebugEnabled()) {
                LOG.debug("MTLS mode: session cookie validation successful");
            }
            return true;
        } catch (UnauthorizedException e) {
            // If cookie validation fails, try certificate authentication
            if (LOG.isDebugEnabled()) {
                LOG.debug("MTLS mode: session cookie validation failed, trying certificate: {}", e.getMessage());
            }
            return authenticateWithCertificate(request, response);
        }
    }

    /**
     * Authenticate using client certificate.
     */
    private boolean authenticateWithCertificate(HttpServletRequest request, HttpServletResponse response)
            throws Exception {
        X509Certificate[] certs = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");
        if (certs == null || certs.length == 0) {
            LOG.warn("No client certificate presented for mTLS HTTP authentication");
            response.sendError(HttpStatus.UNAUTHORIZED.value(), "Client certificate required");
            return false;
        }
        X509Certificate clientCert = certs[0];

        try {
            // Generate username from certificate serial number
            String username = MTLSUtils.getUsernameFromCertificate(clientCert);
            String serialNumber = MTLSUtils.getSerialNumberHex(clientCert);

            // Check if user exists with generated username
            UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp(username, "%");
            if (!Env.getCurrentEnv().getAuth().doesUserExist(userIdentity)) {
                LOG.warn("No Doris user found for username: {} (certificate serial: {})", username, serialNumber);
                response.sendError(HttpStatus.UNAUTHORIZED.value(), "User not found for certificate");
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
            SessionValue value = new SessionValue();
            value.currentUser = userIdentity;
            value.password = ""; // No password for MTLS
            addSession(request, response, value);

            if (LOG.isDebugEnabled()) {
                LOG.debug("MTLS certificate authentication succeeded for username: {}", username);
            }
            return true;
        } catch (AnalysisException ae) {
            LOG.error("Failed to authenticate with mTLS: {}", ae.getMessage());
            response.sendError(HttpStatus.UNAUTHORIZED.value(),
                    "Certificate authentication failed: " + ae.getMessage());
            return false;
        } catch (Exception ex) {
            LOG.error("Exception during mTLS authentication", ex);
            response.sendError(HttpStatus.UNAUTHORIZED.value(), "Certificate authentication failed");
            return false;
        }
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response,
            Object handler, ModelAndView modelAndView) throws Exception {
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response,
            Object handler, Exception ex) throws Exception {
    }
}
