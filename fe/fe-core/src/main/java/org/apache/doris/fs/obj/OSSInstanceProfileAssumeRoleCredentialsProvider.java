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

import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.EcsRamRoleCredentialsProvider;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.http.HttpRequest;
import com.aliyuncs.http.HttpResponse;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.http.clients.CompatibleUrlConnClient;
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
 * Credentials provider using ECS RAM role + STS AssumeRole.
 * Uses OSS SDK for base credentials, STS SDK for AssumeRole.
 */
public class OSSInstanceProfileAssumeRoleCredentialsProvider implements AwsCredentialsProvider {
    private static final Logger LOG = LogManager.getLogger(OSSInstanceProfileAssumeRoleCredentialsProvider.class);
    private static final String METADATA_URL = "http://100.100.100.200/latest/meta-data/ram/security-credentials/";
    private static final long REFRESH_BUFFER_SECONDS = 300;
    private static final long DURATION_SECONDS = 3600;

    private final String roleArn;
    private final String region;
    private final String externalId;

    private EcsRamRoleCredentialsProvider baseProvider;
    private AwsSessionCredentials cachedCredentials;
    private Instant expirationTime;

    public OSSInstanceProfileAssumeRoleCredentialsProvider(String roleArn, String region, String externalId) {
        this.roleArn = roleArn;
        this.region = region;
        this.externalId = externalId;
    }

    @Override
    public AwsCredentials resolveCredentials() {
        if (baseProvider == null) {
            synchronized (this) {
                if (baseProvider == null) {
                    initializeBaseProvider();
                }
            }
        }

        if (needsRefresh()) {
            synchronized (this) {
                if (needsRefresh()) {
                    refreshCredentials();
                }
            }
        }
        return cachedCredentials;
    }

    private void initializeBaseProvider() {
        try {
            HttpRequest request = new HttpRequest(METADATA_URL);
            request.setMethod(MethodType.GET);
            request.setConnectTimeout(5000);
            request.setReadTimeout(5000);

            CompatibleUrlConnClient client = new CompatibleUrlConnClient();
            HttpResponse response = client.syncInvoke(request);
            String roleName = new String(response.getHttpContent(), "UTF-8").trim();

            baseProvider = new EcsRamRoleCredentialsProvider(roleName);
            LOG.info("OSS: Using instance profile + AssumeRole, role: {}, region: {}", roleArn, region);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize base credentials provider", e);
        }
    }

    private boolean needsRefresh() {
        return cachedCredentials == null || expirationTime == null
                || Instant.now().isAfter(expirationTime.minusSeconds(REFRESH_BUFFER_SECONDS));
    }

    private void refreshCredentials() {
        try {
            // Get base credentials from OSS SDK provider
            Credentials baseCreds = baseProvider.getCredentials();

            // Create STS client
            com.aliyuncs.auth.BasicSessionCredentials stsCreds =
                    new com.aliyuncs.auth.BasicSessionCredentials(
                            baseCreds.getAccessKeyId(),
                            baseCreds.getSecretAccessKey(),
                            baseCreds.getSecurityToken()
                    );
            IAcsClient stsClient = new DefaultAcsClient(DefaultProfile.getProfile(region), stsCreds);

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
