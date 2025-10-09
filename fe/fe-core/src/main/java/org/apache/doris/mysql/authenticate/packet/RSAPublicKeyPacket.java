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

package org.apache.doris.mysql.authenticate.packet;

import org.apache.doris.mysql.MysqlPacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.interfaces.RSAPublicKey;

/**
 * MySQL RSA public key packet for caching_sha2_password authentication.
 * 
 * This packet is sent by the server to provide the RSA public key to the client
 * for encrypting the password when SSL is not available. The client uses this
 * public key to encrypt the password before sending it back to the server.
 * 
 * Packet format:
 * - Variable length: PEM-formatted RSA public key string (null-terminated)
 */
public class RSAPublicKeyPacket extends MysqlPacket {
    private static final Logger LOG = LogManager.getLogger(RSAPublicKeyPacket.class);
    
    private final String pemPublicKey;
    private final RSAPublicKey publicKey;
    
    /**
     * Constructor with PEM-formatted public key string
     */
    public RSAPublicKeyPacket(String pemPublicKey) {
        this.pemPublicKey = pemPublicKey;
        this.publicKey = null;
    }
    
    /**
     * Constructor with RSAPublicKey object (will be converted to PEM format)
     */
    public RSAPublicKeyPacket(RSAPublicKey publicKey, String pemPublicKey) {
        this.publicKey = publicKey;
        this.pemPublicKey = pemPublicKey;
    }
    
    /**
     * Get the PEM-formatted public key
     */
    public String getPemPublicKey() {
        return pemPublicKey;
    }
    
    /**
     * Get the RSA public key object
     */
    public RSAPublicKey getPublicKey() {
        return publicKey;
    }
    
    /**
     * Get the key size in bits
     */
    public int getKeySize() {
        if (publicKey != null) {
            return publicKey.getModulus().bitLength();
        }
        return -1; // Unknown key size
    }
    
    @Override
    public void writeTo(MysqlSerializer serializer) {
        if (pemPublicKey == null || pemPublicKey.isEmpty()) {
            LOG.error("Cannot write RSA public key packet: PEM key is null or empty");
            return;
        }
        
        // Write PEM-formatted public key as null-terminated string
        serializer.writeEofString(pemPublicKey);
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("RSA public key packet written: keySize={} bits, pemLength={} chars", 
                    getKeySize(), pemPublicKey.length());
        }
    }
    
    /**
     * Validate the PEM public key format
     */
    public boolean isValidPemFormat() {
        if (pemPublicKey == null || pemPublicKey.isEmpty()) {
            return false;
        }
        
        // Basic PEM format validation
        return pemPublicKey.contains("-----BEGIN PUBLIC KEY-----") && 
               pemPublicKey.contains("-----END PUBLIC KEY-----");
    }
    
    /**
     * Get key information for logging (without exposing the actual key)
     */
    public String getKeyInfo() {
        if (publicKey != null) {
            return String.format("RSA-%d", publicKey.getModulus().bitLength());
        } else if (pemPublicKey != null) {
            return String.format("PEM(%d chars)", pemPublicKey.length());
        } else {
            return "INVALID";
        }
    }
    
    @Override
    public String toString() {
        return String.format("RSAPublicKeyPacket[keyInfo=%s, valid=%s]",
                getKeyInfo(), isValidPemFormat());
    }
    
    /**
     * Factory method to create packet from RSA public key
     */
    public static RSAPublicKeyPacket fromRSAPublicKey(RSAPublicKey publicKey, String pemKey) {
        if (publicKey == null || pemKey == null) {
            throw new IllegalArgumentException("RSA public key and PEM string cannot be null");
        }
        
        return new RSAPublicKeyPacket(publicKey, pemKey);
    }
    
    /**
     * Factory method to create packet from PEM string
     */
    public static RSAPublicKeyPacket fromPemString(String pemKey) {
        if (pemKey == null || pemKey.isEmpty()) {
            throw new IllegalArgumentException("PEM public key string cannot be null or empty");
        }
        
        return new RSAPublicKeyPacket(pemKey);
    }
    
    /**
     * Get packet size estimation for buffer allocation
     */
    public int getEstimatedPacketSize() {
        if (pemPublicKey != null) {
            return pemPublicKey.length() + 1; // +1 for null terminator
        }
        return 1024; // Default estimation for typical RSA key
    }
}
