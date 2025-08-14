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

import org.apache.arrow.flight.CallStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigInteger;
import java.security.cert.X509Certificate;

@RunWith(MockitoJUnitRunner.class)
public class MTLSFlightUtilsTest {

    @Mock
    private X509Certificate mockCertificate;

    @Mock
    private Env mockEnv;

    @Before
    public void setUp() {
        FeConstants.runningUnitTest = true;

        // Mock certificate serial number
        Mockito.when(mockCertificate.getSerialNumber()).thenReturn(new BigInteger("123456789", 16));

        // Set up certificate mapping
        Config.mtls_cert_user_mapping = "123456789:test_user";
        MTLSUtils.initCertMapping();

        // Mock Env
        Env.setCurrentEnv(mockEnv);
        Mockito.when(mockEnv.getAuth()).thenReturn(Mockito.mock(org.apache.doris.catalog.Auth.class));
        Mockito.when(mockEnv.getAuth().doesUserExist(Mockito.any(UserIdentity.class))).thenReturn(true);
    }

    @Test
    public void testValidateCertificateAndGetUsername() {
        // Test successful validation
        String username = MTLSFlightUtils.validateCertificateAndGetUsername(mockCertificate);
        Assert.assertEquals("test_user", username);
    }

    @Test(expected = CallStatus.FlightRuntimeException.class)
    public void testValidateCertificateAndGetUsernameWithNullCertificate() {
        // Should throw exception
        MTLSFlightUtils.validateCertificateAndGetUsername(null);
    }

    @Test(expected = CallStatus.FlightRuntimeException.class)
    public void testValidateCertificateAndGetUsernameWithNonExistentUser() {
        // Mock user does not exist
        Mockito.when(mockEnv.getAuth().doesUserExist(Mockito.any(UserIdentity.class))).thenReturn(false);

        // Should throw exception
        MTLSFlightUtils.validateCertificateAndGetUsername(mockCertificate);
    }

    @Test
    public void testValidateCertificateAndGetAuthResult() {
        // Test successful validation
        FlightAuthResult result = MTLSFlightUtils.validateCertificateAndGetAuthResult(mockCertificate, "127.0.0.1");
        Assert.assertNotNull(result);
        Assert.assertEquals("test_user", result.getUserName());
        Assert.assertEquals("127.0.0.1", result.getRemoteIp());
    }

    @Test(expected = CallStatus.FlightRuntimeException.class)
    public void testValidateCertificateAndGetAuthResultWithNullCertificate() {
        // Should throw exception
        MTLSFlightUtils.validateCertificateAndGetAuthResult(null, "127.0.0.1");
    }
}
