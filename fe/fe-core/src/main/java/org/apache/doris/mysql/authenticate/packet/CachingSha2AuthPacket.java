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

/**
 * MySQL caching_sha2_password authentication response packet.
 *
 * This packet is sent by the server during caching_sha2_password authentication
 * to indicate the authentication status and next steps in the multi-phase process.
 *
 * Packet format:
 * - 1 byte: Status code
 *   - 0x01: Full authentication required
 *   - 0x02: RSA public key request
 *   - 0x03: Fast authentication success
 *   - 0x04: Authentication complete
 */
public class CachingSha2AuthPacket extends MysqlPacket {
    private static final Logger LOG = LogManager.getLogger(CachingSha2AuthPacket.class);

    // Status codes for caching_sha2_password authentication
    public static final byte FULL_AUTH_REQUIRED = 0x01;
    public static final byte RSA_KEY_REQUEST = 0x02;
    public static final byte FAST_AUTH_SUCCESS = 0x03;
    public static final byte AUTH_COMPLETE = 0x04;

    private final byte statusCode;
    private final byte[] additionalData;

    /**
     * Constructor for status-only packet
     */
    public CachingSha2AuthPacket(byte statusCode) {
        this.statusCode = statusCode;
        this.additionalData = null;
    }

    /**
     * Constructor for packet with additional data
     */
    public CachingSha2AuthPacket(byte statusCode, byte[] additionalData) {
        this.statusCode = statusCode;
        this.additionalData = additionalData;
    }

    /**
     * Get the status code
     */
    public byte getStatusCode() {
        return statusCode;
    }

    /**
     * Get additional data (if any)
     */
    public byte[] getAdditionalData() {
        return additionalData;
    }

    /**
     * Check if this is a full authentication required packet
     */
    public boolean isFullAuthRequired() {
        return statusCode == FULL_AUTH_REQUIRED;
    }

    /**
     * Check if this is an RSA key request packet
     */
    public boolean isRSAKeyRequest() {
        return statusCode == RSA_KEY_REQUEST;
    }

    /**
     * Check if this is a fast authentication success packet
     */
    public boolean isFastAuthSuccess() {
        return statusCode == FAST_AUTH_SUCCESS;
    }

    /**
     * Check if this is an authentication complete packet
     */
    public boolean isAuthComplete() {
        return statusCode == AUTH_COMPLETE;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {
        // Write status code
        serializer.writeInt1(statusCode);

        // Write additional data if present
        if (additionalData != null && additionalData.length > 0) {
            serializer.writeBytes(additionalData);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("CachingSha2AuthPacket written: status=0x{}, dataLen={}",
                    Integer.toHexString(statusCode & 0xFF),
                    additionalData != null ? additionalData.length : 0);
        }
    }

    /**
     * Get status description for logging
     */
    public String getStatusDescription() {
        switch (statusCode) {
            case FULL_AUTH_REQUIRED:
                return "FULL_AUTH_REQUIRED";
            case RSA_KEY_REQUEST:
                return "RSA_KEY_REQUEST";
            case FAST_AUTH_SUCCESS:
                return "FAST_AUTH_SUCCESS";
            case AUTH_COMPLETE:
                return "AUTH_COMPLETE";
            default:
                return "UNKNOWN(0x" + Integer.toHexString(statusCode & 0xFF) + ")";
        }
    }

    @Override
    public String toString() {
        return String.format("CachingSha2AuthPacket[status=%s, dataLen=%d]",
                getStatusDescription(),
                additionalData != null ? additionalData.length : 0);
    }

    /**
     * Factory method to create a full authentication required packet
     */
    public static CachingSha2AuthPacket createFullAuthRequired() {
        return new CachingSha2AuthPacket(FULL_AUTH_REQUIRED);
    }

    /**
     * Factory method to create an RSA key request packet
     */
    public static CachingSha2AuthPacket createRSAKeyRequest() {
        return new CachingSha2AuthPacket(RSA_KEY_REQUEST);
    }

    /**
     * Factory method to create a fast authentication success packet
     */
    public static CachingSha2AuthPacket createFastAuthSuccess() {
        return new CachingSha2AuthPacket(FAST_AUTH_SUCCESS);
    }

    /**
     * Factory method to create an authentication complete packet
     */
    public static CachingSha2AuthPacket createAuthComplete() {
        return new CachingSha2AuthPacket(AUTH_COMPLETE);
    }
}
