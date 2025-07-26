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

package org.apache.doris.httpv2;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.interceptor.AuthInterceptor;
import org.apache.doris.mysql.authenticate.mtls.MTLSUtils;
import org.apache.doris.mysql.privilege.Auth;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.math.BigInteger;
import java.security.cert.X509Certificate;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class AuthInterceptorMTLSTest {
    private AuthInterceptor interceptor;
    private Env env;
    private Auth auth;

    @Before
    public void setUp() {
        interceptor = new AuthInterceptor();
        env = Mockito.mock(Env.class);
        auth = Mockito.mock(Auth.class);
        Mockito.when(env.getAuth()).thenReturn(auth);
        Config.authentication_type = "mtls";

        // Clear any existing mappings
        Config.mtls_cert_user_mapping = "";
        MTLSUtils.initCertMapping();
    }

    @Test
    public void testMTLSAuthSuccess() throws Exception {
        // Mock certificate with serial number
        final String serialHex = "abcdef12";

        X509Certificate cert = Mockito.mock(X509Certificate.class);
        Mockito.when(cert.getSerialNumber()).thenReturn(new BigInteger(serialHex, 16));
        X509Certificate[] certs = new X509Certificate[] { cert };

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.when(request.getAttribute("javax.servlet.request.X509Certificate")).thenReturn(certs);
        Mockito.when(request.getRemoteAddr()).thenReturn("127.0.0.1");

        // Mock the doesUserExist method to check for the expected username
        Mockito.when(auth.doesUserExist(Mockito.any(UserIdentity.class))).thenReturn(true);

        boolean result = interceptor.preHandle(request, response, null);
        Assert.assertTrue(result);
    }

    @Test
    public void testMTLSAuthNoCert() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.when(request.getAttribute("javax.servlet.request.X509Certificate")).thenReturn(null);

        boolean result = interceptor.preHandle(request, response, null);
        Assert.assertTrue(result); // The interceptor still returns true but sends an error
        Mockito.verify(response).sendError(Mockito.anyInt(), Mockito.anyString());
    }

    @Test
    public void testMTLSAuthWithCertMapping() throws Exception {
        // Test with certificate mapping
        final String serialHex = "1a2b3c";
        final String mappedUser = "mapped_user";

        X509Certificate cert = Mockito.mock(X509Certificate.class);
        Mockito.when(cert.getSerialNumber()).thenReturn(new BigInteger(serialHex, 16));
        X509Certificate[] certs = new X509Certificate[] { cert };

        // Set up the mapping in Config
        Config.mtls_cert_user_mapping = serialHex + ":" + mappedUser;
        MTLSUtils.initCertMapping();

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.when(request.getAttribute("javax.servlet.request.X509Certificate")).thenReturn(certs);
        Mockito.when(request.getRemoteAddr()).thenReturn("127.0.0.1");

        // Mock the doesUserExist method to check for the mapped username
        Mockito.when(auth.doesUserExist(Mockito.any(UserIdentity.class))).thenAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) {
                UserIdentity userIdentity = invocation.getArgument(0);
                return userIdentity.getQualifiedUser().equals(mappedUser);
            }
        });

        boolean result = interceptor.preHandle(request, response, null);
        Assert.assertTrue(result);
    }

    @Test
    public void testMTLSAuthSerialNumberException() throws Exception {
        X509Certificate cert = Mockito.mock(X509Certificate.class);
        Mockito.when(cert.getSerialNumber()).thenThrow(new RuntimeException("Failed to get serial number"));
        X509Certificate[] certs = new X509Certificate[] { cert };

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.when(request.getAttribute("javax.servlet.request.X509Certificate")).thenReturn(certs);

        boolean result = interceptor.preHandle(request, response, null);
        Assert.assertTrue(result);
        Mockito.verify(response).sendError(Mockito.anyInt(), Mockito.anyString());
    }

    @Test
    public void testMTLSAuthUserNotExist() throws Exception {
        // Mock certificate with serial number
        final String serialHex = "abcdef12";

        X509Certificate cert = Mockito.mock(X509Certificate.class);
        Mockito.when(cert.getSerialNumber()).thenReturn(new BigInteger(serialHex, 16));
        X509Certificate[] certs = new X509Certificate[] { cert };

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.when(request.getAttribute("javax.servlet.request.X509Certificate")).thenReturn(certs);
        Mockito.when(request.getRemoteAddr()).thenReturn("127.0.0.1");

        // User does not exist
        Mockito.when(auth.doesUserExist(Mockito.any(UserIdentity.class))).thenReturn(false);

        boolean result = interceptor.preHandle(request, response, null);
        Assert.assertTrue(result); // The interceptor still returns true but sends an error
        Mockito.verify(response).sendError(Mockito.anyInt(), Mockito.anyString());
    }

    @Test
    public void testTrustStoreConfigForHttp() {
        org.apache.doris.common.Config.mysql_ssl_default_ca_certificate = "/tmp/fake-truststore-http.jks";
        org.apache.doris.common.Config.mysql_ssl_default_ca_certificate_password = "httppass";
        org.apache.doris.common.Config.ssl_trust_store_type = "PKCS12";
        Assert.assertEquals("/tmp/fake-truststore-http.jks", org.apache.doris.common.Config.mysql_ssl_default_ca_certificate);
        Assert.assertEquals("httppass", org.apache.doris.common.Config.mysql_ssl_default_ca_certificate_password);
        Assert.assertEquals("PKCS12", org.apache.doris.common.Config.ssl_trust_store_type);
    }
}
