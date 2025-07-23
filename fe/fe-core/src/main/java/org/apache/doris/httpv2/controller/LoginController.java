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
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.HttpAuthManager.SessionValue;
import org.apache.doris.httpv2.auth.MTLSWebAuthenticator;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
@RequestMapping("/rest/v1")
public class LoginController extends BaseController {

    @RequestMapping(path = "/login", method = RequestMethod.POST)
    public Object login(HttpServletRequest request, HttpServletResponse response) {
        checkAuthWithCookie(request, response);
        Map<String, Object> msg = new HashMap<>();
        msg.put("code", 200);
        msg.put("msg", "Login success!");

        // If this is an mTLS authentication, include the actual username from certificate
        if ("mtls".equalsIgnoreCase(Config.authentication_type)) {
            ActionAuthorizationInfo authInfo = MTLSWebAuthenticator.authenticate(request);
            if (authInfo != null) {
                msg.put("mtlsUsername", authInfo.fullUserName);
            }
        }

        return msg;
    }

    @RequestMapping(path = "/auth_info", method = RequestMethod.GET)
    public Object getAuthInfo(HttpServletRequest request, HttpServletResponse response) {
        Map<String, Object> info = new HashMap<>();
        info.put("authType", Config.authentication_type);
        info.put("code", 200);

        // If this is an mTLS authentication, check if we have a valid certificate
        if ("mtls".equalsIgnoreCase(Config.authentication_type)) {
            ActionAuthorizationInfo authInfo = MTLSWebAuthenticator.authenticate(request);
            if (authInfo != null) {
                // MTLS authentication successful
                // Create a session for the authenticated user
                SessionValue value = new SessionValue();
                value.currentUser = UserIdentity.createAnalyzedUserIdentWithIp(authInfo.fullUserName, "%");
                value.password = authInfo.password;
                addSession(request, response, value);

                info.put("authenticated", true);
                info.put("username", authInfo.fullUserName);
            } else {
                info.put("authenticated", false);
            }
        } else {
            info.put("authenticated", false);
        }

        return info;
    }
}
