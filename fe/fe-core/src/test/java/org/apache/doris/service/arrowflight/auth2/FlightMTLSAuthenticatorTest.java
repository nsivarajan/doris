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
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.mysql.authenticate.mtls.MTLSUtils;
import org.apache.doris.service.arrowflight.tokens.FlightTokenDetails;
import org.apache.doris.service.arrowflight.tokens.FlightTokenManager;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator.AuthResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import io.grpc.Context;

import java.math.BigInteger;
import java.security.cert.X509Certificate;

@RunWith(MockitoJUnitRunner.class)
public class FlightMTLSAuthenticatorTest {

    @Mock
    private FlightTokenManager mockTokenManager;

    @Mock
    private X509Certificate mockCertificate;

    @Mock
    private CallHeaders mockHeaders;

    @Mock
    private Env mockEnv;

    private FlightMTLSAuthenticator authenticator;
    private static final Context.Key<X509Certificate[]> CLIENT_CERTIFICATES = Context.key("client-certificates");

    @Before
    public void setUp() {
        FeConstants.runningUnitTest = true;
        authenticator = new FlightMTLSAuthenticator(mockTokenManager);

        // Mock certificate serial number
        Mockito.when(mockCertificate.getSerialNumber()).thenReturn(new BigInteger("123456789", 16));

        // Set up certificate mapping
        Config.mtls_cert_user_mapping = "123456789:test_user";
        MTLSUtils.initCertMapping();

        // Mock Env
        Env.setCurrentEnv(mockEnv);
        Mockito.when(mockEnv.getAuth()).thenReturn(Mockito.mock(org.apache.doris.catalog.Auth.class));
        Mockito.when(mockEnv.getAuth().doesUserExist(Mockito.any(UserIdentity.class))).thenReturn(true);

        // Mock token manager
        FlightTokenDetails mockTokenDetails = new FlightTokenDetails("test_token", "test_user", 
                System.currentTimeMillis() + 3600000);
        Mockito.when(mockTokenManager.createToken(Mockito.anyString(), Mockito.any(FlightAuthResult.class)))
                .thenReturn(mockTokenDetails);

        // Set up certificates in context
        X509Certificate[] certs = new X509Certificate[] { mockCertificate };
        Context context = Context.current().withValue(CLIENT_CERTIFICATES, certs);
        context.attach();
    }

    @Test
    public void testAuthenticate() {
        // Test successful authentication
        AuthResult result = authenticator.authenticate(mockHeaders);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_token", result.getPeerIdentity());

        // Verify token was created
        Mockito.verify(mockTokenManager).createToken(Mockito.eq("test_user"), Mockito.any(FlightAuthResult.class));
    }

    @Test(expected = CallStatus.FlightRuntimeException.class)
    public void testAuthenticateWithNoCertificate() {
        // Set up empty certificates in context
        Context context = Context.current().withValue(CLIENT_CERTIFICATES, null);
        context.attach();

        // Should throw exception
        authenticator.authenticate(mockHeaders);
    }

    @Test(expected = CallStatus.FlightRuntimeException.class)
    public void testAuthenticateWithNonExistentUser() {
        // Mock user does not exist
        Mockito.when(mockEnv.getAuth().doesUserExist(Mockito.any(UserIdentity.class))).thenReturn(false);

        // Should throw exception
        authenticator.authenticate(mockHeaders);
    }
}
