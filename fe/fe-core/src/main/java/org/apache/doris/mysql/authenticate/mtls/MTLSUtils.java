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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for MTLS authentication
 */
public class MTLSUtils {
    private static final Logger LOG = LogManager.getLogger(MTLSUtils.class);
    
    // Certificate serial number to username mapping
    // Format: serialNumber1:username1;serialNumber2:username2
    // Example: "1a2b3c:root;4d5e6f:admin"
    private static Map<String, String> certSerialToUserMap = new HashMap<>();
    
    // Initialize the certificate mapping from Config
    static {
        initCertMapping();
    }
    
    /**
     * Initialize the certificate-to-user mapping from configuration
     */
    public static void initCertMapping() {
        // Read from Config.mtls_cert_user_mapping in fe.conf
        // Format: serialNumber1:username1;serialNumber2:username2
        // Example: 1a2b3c:root;4d5e6f:admin
        
        String mappingStr = Config.mtls_cert_user_mapping;
        
        if (mappingStr != null && !mappingStr.isEmpty()) {
            String[] mappings = mappingStr.split(";");
            for (String mapping : mappings) {
                String[] parts = mapping.split(":");
                if (parts.length == 2) {
                    certSerialToUserMap.put(parts[0].toLowerCase(), parts[1]);
                    LOG.info("Added certificate mapping: {} -> {}", parts[0], parts[1]);
                } else {
                    LOG.warn("Invalid certificate mapping format: {}. Expected format: serialNumber:username", mapping);
                }
            }
            LOG.info("Loaded {} certificate-to-user mappings from configuration", certSerialToUserMap.size());
        } else {
            LOG.info("No certificate-to-user mappings configured");
        }
    }

    /**
     * Generate a username from a certificate for MTLS authentication
     * This method first checks if there's a mapping for the certificate's serial number
     * If not, it falls back to using the certificate's serial number with "mtls_" prefix
     *
     * @param certificate The client certificate
     * @return A username valid for Doris that can be determined in advance
     * @throws AnalysisException if the certificate is invalid or the username cannot be generated
     */
    public static String getUsernameFromCertificate(X509Certificate certificate) throws AnalysisException {
        if (certificate == null) {
            throw new AnalysisException("Certificate cannot be null");
        }

        try {
            // Get certificate serial number (guaranteed to be unique per CA)
            BigInteger serialNumber = certificate.getSerialNumber();
            String serialHex = serialNumber.toString(16).toLowerCase();
            
            // Check if we have a mapping for this certificate
            if (certSerialToUserMap.containsKey(serialHex)) {
                String mappedUser = certSerialToUserMap.get(serialHex);
                LOG.info("Using mapped username '{}' for certificate serial number {}",
                        mappedUser, serialHex);
                return mappedUser;
            }
            
            // Fall back to default behavior - generate username from serial number
            // Start with prefix
            StringBuilder username = new StringBuilder("mtls_");

            // Add the serial number (in hex format)
            // Limit to a reasonable length to stay within MySQL's 32-character limit
            int maxSerialLength = 26; // 32 - "mtls_" = 27, leave 1 for safety
            username.append(serialHex.length() <= maxSerialLength
                    ? serialHex : serialHex.substring(0, maxSerialLength));

            LOG.debug("Generated username '{}' from certificate serial number {}",
                    username.toString(), serialNumber);

            return username.toString();
        } catch (Exception e) {
            LOG.error("Error generating username from certificate", e);
            throw new AnalysisException("Failed to generate username from certificate: " + e.getMessage());
        }
    }

    /**
     * Get the serial number of a certificate as a hex string
     * This is used for logging purposes
     *
     * @param certificate The client certificate
     * @return The serial number as a hex string
     * @throws AnalysisException if the certificate is invalid or the serial number cannot be extracted
     */
    public static String getSerialNumberHex(X509Certificate certificate) throws AnalysisException {
        if (certificate == null) {
            throw new AnalysisException("Certificate cannot be null");
        }

        try {
            return certificate.getSerialNumber().toString(16).toLowerCase();
        } catch (Exception e) {
            LOG.error("Error extracting serial number from certificate", e);
            throw new AnalysisException("Failed to extract serial number from certificate: " + e.getMessage());
        }
    }
}
