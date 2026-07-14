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

package org.apache.doris.replication.credentials;

import java.time.Instant;

/**
 * Immutable snapshot of cloud credentials at a point in time.
 * securityToken is null for long-term (non-STS) credentials.
 * expiresAt is null for long-term credentials that do not expire.
 */
public final class ReplicationCredentials {

    public final String accessKey;
    public final String secretKey;
    // STS session token — null for long-term credentials
    public final String securityToken;
    // null for non-expiring credentials (instance profile long-lived tokens)
    public final Instant expiresAt;

    public ReplicationCredentials(String accessKey, String secretKey,
            String securityToken, Instant expiresAt) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.securityToken = securityToken;
        this.expiresAt = expiresAt;
    }

    /** Long-term credentials with no expiry (dev/test AK/SK only). */
    public static ReplicationCredentials longTerm(String accessKey, String secretKey) {
        return new ReplicationCredentials(accessKey, secretKey, null, null);
    }

    /** Returns true if credentials will expire within the given seconds. */
    public boolean isNearExpiry(int withinSeconds) {
        if (expiresAt == null) {
            return false;
        }
        return Instant.now().plusSeconds(withinSeconds).isAfter(expiresAt);
    }

    /** Returns true if credentials have already expired. */
    public boolean isExpired() {
        return expiresAt != null && Instant.now().isAfter(expiresAt);
    }
}
