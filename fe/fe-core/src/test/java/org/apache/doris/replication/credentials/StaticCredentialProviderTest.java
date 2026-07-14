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

import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;

public class StaticCredentialProviderTest {

    @Test
    public void testReturnsConfiguredCredentials() throws Exception {
        StaticCredentialProvider provider = new StaticCredentialProvider("MYAK", "MYSK");
        ReplicationCredentials creds = provider.getCredentials();

        Assert.assertEquals("MYAK", creds.accessKey);
        Assert.assertEquals("MYSK", creds.secretKey);
        Assert.assertNull(creds.securityToken);   // static creds have no session token
        Assert.assertNull(creds.expiresAt);       // static creds do not expire
    }

    @Test
    public void testDescribeContainsPartialAK() throws Exception {
        StaticCredentialProvider provider = new StaticCredentialProvider("ABCD1234", "secret");
        // describe should show partial AK for identification without exposing full key
        Assert.assertTrue(provider.describe().contains("ABCD"));
        Assert.assertFalse(provider.describe().contains("secret"));
    }

    @Test
    public void testLongTermCredentialsNeverExpire() {
        ReplicationCredentials creds = ReplicationCredentials.longTerm("ak", "sk");
        Assert.assertFalse(creds.isExpired());
        Assert.assertFalse(creds.isNearExpiry(3600));
    }

    @Test
    public void testIsExpiredReturnsTrueForPastExpiry() {
        ReplicationCredentials creds = new ReplicationCredentials(
                "ak", "sk", "token", Instant.now().minusSeconds(60));
        Assert.assertTrue(creds.isExpired());
    }

    @Test
    public void testIsNearExpiryReturnsTrueWithinWindow() {
        // expires in 3 minutes — within a 5-minute refresh window
        ReplicationCredentials creds = new ReplicationCredentials(
                "ak", "sk", "token", Instant.now().plusSeconds(180));
        Assert.assertTrue(creds.isNearExpiry(300));
        Assert.assertFalse(creds.isNearExpiry(60));
    }
}
