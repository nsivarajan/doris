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

package org.apache.doris.fs.obj;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.sts.model.v20150401.AssumeRoleRequest;
import com.aliyuncs.sts.model.v20150401.AssumeRoleResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.time.Instant;

/**
 * Credentials provider using explicit credentials + STS AssumeRole.
 * Uses STS SDK for AssumeRole with external_id support.
 */
public class OSSAssumeRoleCredentialsProvider implements AwsCredentialsProvider {
    private static final Logger LOG = LogManager.getLogger(OSSAssumeRoleCredentialsProvider.class);
    private static final long REFRESH_BUFFER_SECONDS = 300;
    private static final long DURATION_SECONDS = 3600;

    private final String roleArn;
    private final String region;
    private final String accessKey;
    private final String secretKey;
    private final String sessionToken;
    private final String externalId;

    private AwsSessionCredentials cachedCredentials;
    private Instant expirationTime;

    public OSSAssumeRoleCredentialsProvider(String roleArn, String region,
                                           String accessKey, String secretKey,
                                           String sessionToken, String externalId) {
        this.roleArn = roleArn;
        this.region = region;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.sessionToken = sessionToken;
        this.externalId = externalId;
    }

    @Override
    public AwsCredentials resolveCredentials() {
        if (needsRefresh()) {
            synchronized (this) {
                if (needsRefresh()) {
                    refreshCredentials();
                }
            }
        }
        return cachedCredentials;
    }

    private boolean needsRefresh() {
        return cachedCredentials == null || expirationTime == null
                || Instant.now().isAfter(expirationTime.minusSeconds(REFRESH_BUFFER_SECONDS));
    }

    private void refreshCredentials() {
        try {
            // Create STS client with base credentials
            IAcsClient stsClient;
            if (StringUtils.isNotBlank(sessionToken)) {
                com.aliyuncs.auth.BasicSessionCredentials baseCreds =
                        new com.aliyuncs.auth.BasicSessionCredentials(accessKey, secretKey, sessionToken);
                stsClient = new DefaultAcsClient(DefaultProfile.getProfile(region), baseCreds);
            } else {
                stsClient = new DefaultAcsClient(DefaultProfile.getProfile(region, accessKey, secretKey));
            }

            // AssumeRole request
            AssumeRoleRequest request = new AssumeRoleRequest();
            request.setMethod(MethodType.POST);
            request.setRoleArn(roleArn);
            request.setRoleSessionName("doris-fe-" + System.currentTimeMillis());
            request.setDurationSeconds(DURATION_SECONDS);
            if (StringUtils.isNotBlank(externalId)) {
                request.setExternalId(externalId);
            }

            AssumeRoleResponse response = stsClient.getAcsResponse(request);
            AssumeRoleResponse.Credentials creds = response.getCredentials();

            cachedCredentials = AwsSessionCredentials.create(
                    creds.getAccessKeyId(),
                    creds.getAccessKeySecret(),
                    creds.getSecurityToken()
            );
            expirationTime = Instant.parse(creds.getExpiration());

            LOG.info("OSS AssumeRole credentials refreshed, expires: {}", expirationTime);
        } catch (ClientException e) {
            String msg = String.format("STS AssumeRole failed for role %s: %s", roleArn, e.getErrMsg());
            LOG.error(msg, e);
            throw new RuntimeException(msg, e);
        } catch (Exception e) {
            LOG.error("Failed to refresh AssumeRole credentials", e);
            throw new RuntimeException("Failed to refresh AssumeRole credentials", e);
        }
    }
}
