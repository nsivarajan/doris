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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;
import java.security.cert.X509Certificate;

public class MTLSUtilsTest {
    
    @Mocked
    private X509Certificate certificate;
    
    @Before
    public void setUp() {
        // Clear any existing mappings before each test
        Config.mtls_cert_user_mapping = "";
        MTLSUtils.initCertMapping();
    }
    
    @Test
    public void testCertificateMapping() throws AnalysisException {
        // Test certificate-to-username mapping
        final String serialHex = "1a2b3c";
        final String mappedUser = "mapped_user";
        
        new Expectations() {{
            certificate.getSerialNumber();
            result = new BigInteger(serialHex, 16);
        }};
        
        // Set up the mapping in Config
        Config.mtls_cert_user_mapping = serialHex + ":" + mappedUser;
        MTLSUtils.initCertMapping();
        
        String username = MTLSUtils.getUsernameFromCertificate(certificate);
        Assert.assertEquals(mappedUser, username);
    }
    
    @Test
    public void testMultipleCertificateMappings() throws AnalysisException {
        // Test multiple certificate-to-username mappings
        final String serialHex1 = "1a2b3c";
        final String mappedUser1 = "user1";
        final String serialHex2 = "4d5e6f";
        final String mappedUser2 = "user2";
        
        new Expectations() {{
            certificate.getSerialNumber();
            result = new BigInteger(serialHex2, 16);
        }};
        
        // Set up multiple mappings in Config
        Config.mtls_cert_user_mapping = serialHex1 + ":" + mappedUser1 + ";" + serialHex2 + ":" + mappedUser2;
        MTLSUtils.initCertMapping();
        
        String username = MTLSUtils.getUsernameFromCertificate(certificate);
        Assert.assertEquals(mappedUser2, username);
    }
    
    @Test
    public void testFallbackUsernameGeneration() throws AnalysisException {
        // Test fallback username generation when no mapping exists
        final String serialHex = "abcdef12";
        final String expectedUsername = "mtls_abcdef12";
        
        new Expectations() {{
            certificate.getSerialNumber();
            result = new BigInteger(serialHex, 16);
        }};
        
        // Clear any existing mappings
        Config.mtls_cert_user_mapping = "";
        MTLSUtils.initCertMapping();
        
        String username = MTLSUtils.getUsernameFromCertificate(certificate);
        Assert.assertEquals(expectedUsername, username);
    }
    
    @Test
    public void testLongSerialNumberTruncation() throws AnalysisException {
        // Test that long serial numbers are truncated appropriately
        final String longSerial = "abcdef1234567890abcdef1234567890abcdef1234567890";
        final int maxSerialLength = 26; // From MTLSUtils.java
        
        new Expectations() {{
            certificate.getSerialNumber();
            result = new BigInteger(longSerial, 16);
        }};
        
        // Clear any existing mappings
        Config.mtls_cert_user_mapping = "";
        MTLSUtils.initCertMapping();
        
        String username = MTLSUtils.getUsernameFromCertificate(certificate);
        Assert.assertTrue(username.length() <= 32); // MySQL's username limit
        Assert.assertTrue(username.startsWith("mtls_"));
        Assert.assertEquals("mtls_" + longSerial.substring(0, maxSerialLength), username);
    }
    
    @Test(expected = AnalysisException.class)
    public void testNullCertificate() throws AnalysisException {
        MTLSUtils.getUsernameFromCertificate(null);
    }
    
    @Test
    public void testInvalidMappingFormat() {
        // Test handling of invalid mapping format
        Config.mtls_cert_user_mapping = "invalid_format";
        MTLSUtils.initCertMapping();
        // Should not throw exception, just log warning
        
        // Verify no mappings were added
        try {
            new Expectations() {{
                certificate.getSerialNumber();
                result = new BigInteger("1a2b3c", 16);
            }};
            
            String username = MTLSUtils.getUsernameFromCertificate(certificate);
            Assert.assertEquals("mtls_1a2b3c", username);
        } catch (AnalysisException e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }
    
    @Test
    public void testGetSerialNumberHex() throws AnalysisException {
        // Test getSerialNumberHex method
        final String serialHex = "1a2b3c";
        
        new Expectations() {{
            certificate.getSerialNumber();
            result = new BigInteger(serialHex, 16);
        }};
        
        String result = MTLSUtils.getSerialNumberHex(certificate);
        Assert.assertEquals(serialHex, result);
    }
    
    @Test(expected = AnalysisException.class)
    public void testGetSerialNumberHexNullCertificate() throws AnalysisException {
        MTLSUtils.getSerialNumberHex(null);
    }
    
    @Test(expected = AnalysisException.class)
    public void testGetSerialNumberHexException() throws AnalysisException {
        new Expectations() {{
            certificate.getSerialNumber();
            result = new RuntimeException("Failed to get serial number");
        }};
        
        MTLSUtils.getSerialNumberHex(certificate);
    }
    
    @Test(expected = AnalysisException.class)
    public void testGetUsernameFromCertificateException() throws AnalysisException {
        new Expectations() {{
            certificate.getSerialNumber();
            result = new RuntimeException("Failed to get serial number");
        }};
        
        MTLSUtils.getUsernameFromCertificate(certificate);
    }
}