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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.authenticate.AuthenticateRequest;
import org.apache.doris.mysql.authenticate.AuthenticateResponse;
import org.apache.doris.mysql.privilege.Auth;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.security.cert.X509Certificate;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

public class MTLSAuthenticatorTest {
    private static final String UID = "testuser123";
    private static final String SUBJECT_DN = "UID=testuser123, CN=Alice Smith, O=AcmeCorp, C=US";

    @Mocked
    private Auth auth;
    @Mocked
    private Env env;
    @Mocked
    private MysqlChannel channel;
    @Mocked
    private SSLEngine sslEngine;
    @Mocked
    private SSLSession sslSession;
    @Mocked
    private X509Certificate certificate;

    private MTLSAuthenticator authenticator = new MTLSAuthenticator();

    @Before
    public void setUp() throws Exception {
        new Expectations(Env.class) {
            {
                Env.getCurrentEnv();
                result = env;
                env.getAuth();
                result = auth;
            }
        };
        new Expectations() {
            {
                channel.getSslEngine();
                result = sslEngine;
                sslEngine.getSession();
                result = sslSession;
                sslSession.getPeerCertificates();
                result = new X509Certificate[] { certificate };
                certificate.getSubjectX500Principal().getName();
                result = SUBJECT_DN;
            }
        };
    }

    @Test
    public void testAuthenticateSuccess() throws Exception {
        new Expectations() {
            {
                auth.doesUserExist((UserIdentity) any);
                result = true;
            }
        };
        AuthenticateRequest request = new AuthenticateRequest(null, null, null, channel);
        AuthenticateResponse response = authenticator.authenticate(request);
        Assert.assertTrue(response.isSuccess());
        Assert.assertEquals(UID, response.getUserIdentity().getQualifiedUser());
    }

    @Test
    public void testAuthenticateFailNoUser() throws Exception {
        new Expectations() {
            {
                auth.doesUserExist((UserIdentity) any);
                result = false;
            }
        };
        AuthenticateRequest request = new AuthenticateRequest(null, null, null, channel);
        AuthenticateResponse response = authenticator.authenticate(request);
        Assert.assertFalse(response.isSuccess());
    }

    @Test
    public void testAuthenticateFailNoUID() throws Exception {
        new Expectations() {
            {
                certificate.getSubjectX500Principal().getName();
                result = "CN=Test, O=Example, C=US";
            }
        };
        AuthenticateRequest request = new AuthenticateRequest(null, null, null, channel);
        AuthenticateResponse response = authenticator.authenticate(request);
        Assert.assertFalse(response.isSuccess());
    }

    @Test
    public void testTrustStoreConfigUsed() throws Exception {
        // Set custom CA certificate config
        org.apache.doris.common.Config.mysql_ssl_default_ca_certificate = "/tmp/fake-truststore-mysql.jks";
        org.apache.doris.common.Config.mysql_ssl_default_ca_certificate_password = "mysqlpass";
        org.apache.doris.common.Config.ssl_trust_store_type = "PKCS12";
        Assert.assertEquals("/tmp/fake-truststore-mysql.jks", org.apache.doris.common.Config.mysql_ssl_default_ca_certificate);
        Assert.assertEquals("mysqlpass", org.apache.doris.common.Config.mysql_ssl_default_ca_certificate_password);
        Assert.assertEquals("PKCS12", org.apache.doris.common.Config.ssl_trust_store_type);
    }
}
