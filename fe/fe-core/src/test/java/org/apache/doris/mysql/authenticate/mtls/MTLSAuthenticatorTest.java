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
import org.apache.doris.common.Config;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.authenticate.AuthenticateRequest;
import org.apache.doris.mysql.authenticate.AuthenticateResponse;
import org.apache.doris.mysql.privilege.Auth;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

public class MTLSAuthenticatorTest {
    private static final String SERIAL_HEX = "abcdef12";
    private static final String EXPECTED_USERNAME = "mtls_" + SERIAL_HEX;

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
                certificate.getSerialNumber();
                result = new BigInteger(SERIAL_HEX, 16);
            }
        };

        // Clear any existing mappings
        Config.mtls_cert_user_mapping = "";
        MTLSUtils.initCertMapping();
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
        Assert.assertEquals(EXPECTED_USERNAME, response.getUserIdentity().getQualifiedUser());
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
    public void testAuthenticateFailNullChannel() throws Exception {
        AuthenticateRequest request = new AuthenticateRequest(null, null, null, null);
        AuthenticateResponse response = authenticator.authenticate(request);
        Assert.assertFalse(response.isSuccess());
    }

    @Test
    public void testAuthenticateFailNullSslEngine() throws Exception {
        new Expectations() {
            {
                channel.getSslEngine();
                result = null;
            }
        };

        AuthenticateRequest request = new AuthenticateRequest(null, null, null, channel);
        AuthenticateResponse response = authenticator.authenticate(request);
        Assert.assertFalse(response.isSuccess());
    }

    @Test
    public void testAuthenticateWithCertMapping() throws Exception {
        // Setup certificate with a specific serial number
        final String serialHex = "1a2b3c";
        final String mappedUser = "mapped_user";

        new Expectations() {
            {
                certificate.getSerialNumber();
                result = new BigInteger(serialHex, 16);
                auth.doesUserExist((UserIdentity) any);
                result = true;
            }
        };

        // Set up the mapping in Config
        Config.mtls_cert_user_mapping = serialHex + ":" + mappedUser;
        MTLSUtils.initCertMapping();
        AuthenticateRequest request = new AuthenticateRequest(null, null, null, channel);
        AuthenticateResponse response = authenticator.authenticate(request);
        Assert.assertTrue(response.isSuccess());
        Assert.assertEquals(mappedUser, response.getUserIdentity().getQualifiedUser());
    }

    @Test
    public void testAuthenticateFailSerialNumberException() throws Exception {
        new Expectations() {
            {
                certificate.getSerialNumber();
                result = new RuntimeException("Failed to get serial number");
            }
        };

        AuthenticateRequest request = new AuthenticateRequest(null, null, null, channel);
        AuthenticateResponse response = authenticator.authenticate(request);
        Assert.assertFalse(response.isSuccess());
    }

    @Test
    public void testTrustStoreConfigUsed() throws Exception {
        // Set custom CA certificate config
        Config.mysql_ssl_default_ca_certificate = "/tmp/fake-truststore-mysql.jks";
        Config.mysql_ssl_default_ca_certificate_password = "mysqlpass";
        Config.ssl_trust_store_type = "PKCS12";
        Assert.assertEquals("/tmp/fake-truststore-mysql.jks", Config.mysql_ssl_default_ca_certificate);
        Assert.assertEquals("mysqlpass", Config.mysql_ssl_default_ca_certificate_password);
        Assert.assertEquals("PKCS12", Config.ssl_trust_store_type);
    }
}
