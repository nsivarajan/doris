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

import org.apache.doris.common.Config;

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

    @RequestMapping(path = "/login", method = {RequestMethod.POST, RequestMethod.GET})
    public Object login(HttpServletRequest request, HttpServletResponse response) {
        Map<String, Object> msg = new HashMap<>();

        try {
            LOG.info("Login request received: method={}, uri={}, auth={}",
                    request.getMethod(), request.getRequestURI(), request.getHeader("Authorization"));

            // Check if we're in MTLS mode
            if ("mtls".equalsIgnoreCase(Config.authentication_type)) {
                LOG.info("MTLS mode detected");

                // For MTLS, check if client certificate is present
                X509Certificate[] certs = (X509Certificate[]) request.getAttribute(
                        "javax.servlet.request.X509Certificate");
                if (certs != null && certs.length > 0) {
                    LOG.info("MTLS certificate detected in login request");

                    // Set no-cache headers to prevent caching issues
                    response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
                    response.setHeader("Pragma", "no-cache");
                    response.setHeader("Expires", "0");

                    // Return standard format expected by frontend
                    msg.put("msg", "success");
                    msg.put("code", 0);
                    msg.put("data", "");
                    msg.put("count", 0);

                    LOG.info("Returning success response for MTLS login");
                    return msg;
                } else {
                    LOG.warn("No client certificate found in MTLS mode");
                    response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                    msg.put("msg", "Client certificate required");
                    msg.put("code", -1);
                    return msg;
                }
            } else {
                LOG.info("Standard authentication mode");
                // For non-MTLS mode, use standard cookie check
                checkAuthWithCookie(request, response);

                // Return standard format expected by frontend
                msg.put("msg", "success");
                msg.put("code", 0);
                msg.put("data", "");
                msg.put("count", 0);
                return msg;
            }
        } catch (Exception e) {
            LOG.error("Error during authentication", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            msg.put("msg", "Authentication failed: " + e.getMessage());
            msg.put("code", -1);
            return msg;
        }
    }
}
