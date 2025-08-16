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

package org.apache.doris.service.arrowflight.auth2;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.mysql.authenticate.ldap.LdapManager;
import org.apache.doris.service.arrowflight.tokens.FlightTokenManager;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.arrow.flight.CallStatus;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * A collection of common Flight server authentication methods.
 */
public final class FlightAuthUtils {
    private FlightAuthUtils() {
    }

    /**
     * Authenticate against with the provided credentials.
     *
     * @param username username.
     * @param password password.
     * @param logger the slf4j logger for logging.
     * @throws org.apache.arrow.flight.FlightRuntimeException if unable to authenticate against
     *         with the provided credentials.
     */
    public static FlightAuthResult authenticateCredentials(String username, String password, String remoteIp,
            Logger logger) {
        try {
            List<UserIdentity> currentUserIdentity = Lists.newArrayList();
            // If the password is empty, DBeaver will pass "null" string for authentication.
            // This behavior of DBeaver is strange, but we have to be compatible with it, of course,
            // it may be a problem with Arrow Flight Jdbc driver.
            // Here, "null" is converted to null, if user's password is really the string "null",
            // authentication will fail. Usually, the user's password will not be "null", let's hope so.
            password = (password.equals("null")) ? null : password;
            
            // Check if LDAP authentication should be used for Arrow Flight
            if (LdapManager.isLdapAuthEnabledForArrowFlight()) {
                // Try LDAP authentication first
                LdapManager ldapManager = new LdapManager();
                if (ldapManager.doesUserExist(username)) {
                    // User exists in LDAP, try LDAP authentication
                    if (ldapManager.checkUserPasswd(username, password, remoteIp, currentUserIdentity)) {
                        // LDAP authentication successful
                        Preconditions.checkState(currentUserIdentity.size() == 1);
                        return FlightAuthResult.of(username, currentUserIdentity.get(0), remoteIp);
                    } else {
                        // LDAP authentication failed
                        logger.error("LDAP authentication failed for user {}", username);
                        throw new AuthenticationException(ErrorCode.ERR_ACCESS_DENIED_ERROR, username + "@" + remoteIp,
                                Strings.isNullOrEmpty(password) ? "NO" : "YES");
                    }
                }
            }
            
            // If LDAP authentication is not enabled or user doesn't exist in LDAP,
            // fall back to default password authentication
            Env.getCurrentEnv().getAuth().checkPlainPassword(username, remoteIp, password, currentUserIdentity);
            Preconditions.checkState(currentUserIdentity.size() == 1);
            return FlightAuthResult.of(username, currentUserIdentity.get(0), remoteIp);
        } catch (AuthenticationException e) {
            logger.error("Unable to authenticate user {}", username, e);
            final String errMsg = "Unable to authenticate user " + username + ", exception: " + e.getMessage();
            throw CallStatus.UNAUTHENTICATED.withCause(e).withDescription(errMsg).toRuntimeException();
        }
    }

    /**
     * Creates a new Bearer Token. Returns the bearer token associated with the User.
     *
     * @param flightTokenManager the TokenManager.
     * @param username the user to create a Flight server session for.
     * @param flightAuthResult the FlightAuthResult.
     * @return the token associated with the FlightTokenDetails created.
     */
    public static String createToken(FlightTokenManager flightTokenManager, String username,
            FlightAuthResult flightAuthResult) {
        return flightTokenManager.createToken(username, flightAuthResult).getToken();
    }
}
