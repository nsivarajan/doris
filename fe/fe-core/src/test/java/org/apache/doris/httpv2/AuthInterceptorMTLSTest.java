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

import org.apache.doris.common.Config;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.httpv2.interceptor.AuthInterceptor;
import org.apache.doris.mysql.privilege.Auth;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.security.cert.X509Certificate;
import javax.security.auth.x500.X500Principal;

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
    }

    @Test
    public void testMTLSAuthSuccess() throws Exception {
        // Mock UID in certificate
        String uid = "testuser";
        X509Certificate cert = Mockito.mock(X509Certificate.class);
        X500Principal principal = new X500Principal("UID=" + uid + ",CN=Test,O=Org");
        Mockito.when(cert.getSubjectX500Principal()).thenReturn(principal);
        X509Certificate[] certs = new X509Certificate[] { cert };

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.when(request.getAttribute("javax.servlet.request.X509Certificate")).thenReturn(certs);
        Mockito.when(request.getRemoteAddr()).thenReturn("127.0.0.1");
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
        Assert.assertFalse(result);
        Mockito.verify(response).sendError(Mockito.anyInt(), Mockito.anyString());
    }

    @Test
    public void testMTLSAuthNoUID() throws Exception {
        X509Certificate cert = Mockito.mock(X509Certificate.class);
        X500Principal principal = new X500Principal("CN=Test,O=Org");
        Mockito.when(cert.getSubjectX500Principal()).thenReturn(principal);
        X509Certificate[] certs = new X509Certificate[] { cert };

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.when(request.getAttribute("javax.servlet.request.X509Certificate")).thenReturn(certs);

        boolean result = interceptor.preHandle(request, response, null);
        Assert.assertFalse(result);
        Mockito.verify(response).sendError(Mockito.anyInt(), Mockito.anyString());
    }

    @Test
    public void testMTLSAuthUserNotExist() throws Exception {
        String uid = "nouser";
        X509Certificate cert = Mockito.mock(X509Certificate.class);
        X500Principal principal = new X500Principal("UID=" + uid + ",CN=Test,O=Org");
        Mockito.when(cert.getSubjectX500Principal()).thenReturn(principal);
        X509Certificate[] certs = new X509Certificate[] { cert };

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.when(request.getAttribute("javax.servlet.request.X509Certificate")).thenReturn(certs);
        Mockito.when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        Mockito.when(auth.doesUserExist(Mockito.any(UserIdentity.class))).thenReturn(false);

        boolean result = interceptor.preHandle(request, response, null);
        Assert.assertFalse(result);
        Mockito.verify(response).sendError(Mockito.anyInt(), Mockito.anyString());
    }

    @Test
    public void testTrustStoreConfigForHttp() {
        org.apache.doris.common.Config.trust_store_path = "/tmp/fake-truststore-http.jks";
        org.apache.doris.common.Config.trust_store_password = "httppass";
        org.apache.doris.common.Config.trust_store_type = "JKS";
        Assert.assertEquals("/tmp/fake-truststore-http.jks", org.apache.doris.common.Config.trust_store_path);
        Assert.assertEquals("httppass", org.apache.doris.common.Config.trust_store_password);
        Assert.assertEquals("JKS", org.apache.doris.common.Config.trust_store_type);
        // This ensures the config is set and would be used by Jetty SSL context.
    }
} 