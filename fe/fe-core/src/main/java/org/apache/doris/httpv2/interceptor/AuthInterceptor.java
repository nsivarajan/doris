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
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.controller.BaseController;
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

    @Override
    public boolean preHandle(HttpServletRequest request,
                             HttpServletResponse response, Object handler) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("get prehandle. thread: {}", Thread.currentThread().getId());
        }
        String method = request.getMethod();
        if (method.equalsIgnoreCase(RequestMethod.OPTIONS.toString())) {
            response.setStatus(HttpStatus.NO_CONTENT.value());
            return true;
        }
        // mTLS mode: extract UID from client certificate
        if ("mtls".equalsIgnoreCase(Config.authentication_type)) {
            X509Certificate[] certs = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");
            if (certs == null || certs.length == 0) {
                LOG.warn("No client certificate presented for mTLS HTTP authentication");
                response.sendError(HttpStatus.UNAUTHORIZED.value(), "Client certificate required");
                return false;
            }
            X509Certificate clientCert = certs[0];
            String uid = null;
            try {
                String dn = clientCert.getSubjectX500Principal().getName();
                String[] dnParts = dn.split(",");
                for (String part : dnParts) {
                    part = part.trim();
                    if (part.startsWith("UID=")) {
                        uid = part.substring(4);
                        break;
                    }
                }
            } catch (Exception e) {
                LOG.warn("Failed to parse UID from client certificate", e);
            }
            if (uid == null || uid.isEmpty()) {
                LOG.warn("No UID found in client certificate subject: {}", clientCert.getSubjectX500Principal());
                response.sendError(HttpStatus.UNAUTHORIZED.value(), "UID not found in client certificate");
                return false;
            }
            // Check if user exists
            UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp(uid, "%");
            if (!Env.getCurrentEnv().getAuth().doesUserExist(userIdentity)) {
                LOG.warn("No Doris user found for UID: {}", uid);
                response.sendError(HttpStatus.UNAUTHORIZED.value(), "User not found for UID");
                return false;
            }
            // Set up ConnectContext for this user
            ConnectContext ctx = new ConnectContext();
            ctx.setQualifiedUser(uid);
            ctx.setRemoteIP(request.getRemoteAddr());
            ctx.setCurrentUserIdentity(userIdentity);
            ctx.setEnv(Env.getCurrentEnv());
            ctx.setThreadLocalInfo();
            if (LOG.isDebugEnabled()) {
                LOG.debug("mTLS HTTP authentication succeeded for UID: {}", uid);
            }
            return true;
        } else {
            // Default/LDAP: use existing logic
            checkAuthWithCookie(request, response);
            return true;
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
