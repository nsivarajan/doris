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

package org.apache.doris.dr.credentials;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.auth.sts.AssumeRoleResponse;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.profile.DefaultProfile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;

/**
 * Calls Alibaba Cloud STS AssumeRole with the given RAM role ARN.
 * Uses the ECS instance profile as the base credential for the STS call —
 * no static AK/SK needed. Cached; refreshed 5 minutes before expiry.
 */
public class AssumeRoleCredentialProvider implements DRCredentialProvider {

    private static final Logger LOG = LogManager.getLogger(AssumeRoleCredentialProvider.class);

    private static final int REFRESH_BEFORE_EXPIRY_SECONDS = 300;
    private static final int MAX_RETRIES = 3;

    private final String roleArn;
    private final String roleSessionName;

    private volatile DRCredentials cached;
    private volatile Instant expiry;

    public AssumeRoleCredentialProvider(String roleArn, String roleSessionName) {
        this.roleArn = roleArn;
        this.roleSessionName = roleSessionName;
        LOG.info("[DR] AssumeRoleCredentialProvider created role_arn={} session={}",
                roleArn, roleSessionName);
    }

    @Override
    public synchronized DRCredentials getCredentials() {
        if (cached == null || isNearExpiry()) {
            cached = assumeRoleWithRetry();
        }
        return cached;
    }

    private boolean isNearExpiry() {
        return expiry != null
                && Instant.now().plusSeconds(REFRESH_BEFORE_EXPIRY_SECONDS).isAfter(expiry);
    }

    private DRCredentials assumeRoleWithRetry() {
        Exception lastEx = null;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                return doAssumeRole();
            } catch (Exception e) {
                lastEx = e;
                LOG.warn("[DR] STS AssumeRole attempt {}/{} failed: {}",
                        attempt, MAX_RETRIES, e.getMessage());
            }
        }
        throw new RuntimeException(
                "STS AssumeRole failed after " + MAX_RETRIES
                + " retries for role=" + roleArn, lastEx);
    }

    private DRCredentials doAssumeRole() throws Exception {
        // Use the default profile; ECS instance profile provides base credentials
        DefaultProfile profile = DefaultProfile.getProfile("cn-hangzhou");
        DefaultAcsClient client = new DefaultAcsClient(profile);

        com.aliyuncs.auth.sts.AssumeRoleRequest req =
                new com.aliyuncs.auth.sts.AssumeRoleRequest();
        req.setRoleArn(roleArn);
        req.setRoleSessionName(roleSessionName);
        req.setDurationSeconds(3600L);

        try {
            AssumeRoleResponse response = client.getAcsResponse(req);
            AssumeRoleResponse.Credentials c = response.getCredentials();
            expiry = Instant.parse(c.getExpiration());
            LOG.info("[DR] STS AssumeRole succeeded, expiry={}", expiry);
            return new DRCredentials(
                    c.getAccessKeyId(),
                    c.getAccessKeySecret(),
                    c.getSecurityToken());
        } catch (ClientException e) {
            throw new Exception("STS AssumeRole failed: " + e.getErrMsg(), e);
        }
    }
}
