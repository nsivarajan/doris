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

package org.apache.doris.mysql.authenticate.mtls;

import java.io.IOException;
import java.security.cert.X509Certificate;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.authenticate.Authenticator;
import org.apache.doris.mysql.authenticate.AuthenticateRequest;
import org.apache.doris.mysql.authenticate.AuthenticateResponse;
import org.apache.doris.mysql.authenticate.password.NativePasswordResolver;
import org.apache.doris.mysql.authenticate.password.PasswordResolver;

public class MTLSAuthenticator implements Authenticator {
    private static final Logger LOG = LogManager.getLogger(MTLSAuthenticator.class);
    private PasswordResolver passwordResolver;

    public MTLSAuthenticator() {
        this.passwordResolver = new NativePasswordResolver(); // Not used, but required by interface
    }

    @Override
    public AuthenticateResponse authenticate(AuthenticateRequest request) throws IOException {
        // Get the channel from the context (must be set in the context)
        MysqlChannel channel = request.getChannel();
        if (channel == null) {
            LOG.warn("No MysqlChannel available for mTLS authentication");
            return AuthenticateResponse.failedResponse;
        }
        SSLEngine sslEngine = channel.getSslEngine();
        if (sslEngine == null) {
            LOG.warn("No SSLEngine available for mTLS authentication");
            return AuthenticateResponse.failedResponse;
        }
        try {
            SSLSession session = sslEngine.getSession();
            X509Certificate[] certs = (X509Certificate[]) session.getPeerCertificates();
            X509Certificate clientCert = certs[0];
            String subjectDN = clientCert.getSubjectX500Principal().getName();
            String uid = null;
            LdapName ldapDN = new LdapName(subjectDN);
            for (Rdn rdn : ldapDN.getRdns()) {
                if ("UID".equalsIgnoreCase(rdn.getType())) {
                    uid = (String) rdn.getValue();
                    break;
                }
            }
            if (uid == null) {
                LOG.warn("No UID found in client certificate subject: {}", subjectDN);
                return AuthenticateResponse.failedResponse;
            }
            // Look up Doris user by UID
            UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp(uid, "%");
            if (!Env.getCurrentEnv().getAuth().doesUserExist(userIdentity)) {
                LOG.warn("No Doris user found for UID: {}", uid);
                return AuthenticateResponse.failedResponse;
            }
            return new AuthenticateResponse(true, userIdentity);
        } catch (Exception e) {
            LOG.warn("Exception during mTLS authentication", e);
            return AuthenticateResponse.failedResponse;
        }
    }

    @Override
    public boolean canDeal(String qualifiedUser) {
        return true;
    }

    @Override
    public PasswordResolver getPasswordResolver() {
        return passwordResolver;
    }
}
