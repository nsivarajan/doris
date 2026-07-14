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

import org.apache.doris.replication.ReplicationConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// Alibaba Cloud STS — fully qualified at use-site to avoid name conflict with AWS
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.auth.sts.AssumeRoleResponse;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.profile.DefaultProfile;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
// AWS AssumeRoleResult — AssumeRoleRequest used via FQN below to avoid clash
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;

import java.time.Instant;

/**
 * Obtains temporary credentials by calling STS AssumeRole with the given role ARN.
 * Auto-refreshes 5 minutes before expiry using the configured refresh window.
 * Supports AWS IAM AssumeRole and Alibaba Cloud RAM STS AssumeRole.
 */
public class AssumeRoleCredentialProvider implements ReplicationCredentialProvider {

    private static final Logger LOG = LogManager.getLogger(AssumeRoleCredentialProvider.class);

    // max retries for STS calls before giving up
    private static final int MAX_RETRIES = 3;

    private final ReplicationConfig config;
    private volatile ReplicationCredentials cached;

    public AssumeRoleCredentialProvider(ReplicationConfig config) {
        this.config = config;
        LOG.info("[Replication] AssumeRoleCredentialProvider created "
                + "provider={} role_arn={} session={}",
                config.storageType, config.roleArn, config.roleSessionName);
    }

    @Override
    public ReplicationCredentials getCredentials() throws ReplicationCredentialException {
        // refresh if no cached credentials or near expiry
        if (cached == null || cached.isNearExpiry(config.credentialRefreshWindowSeconds)) {
            cached = assumeRoleWithRetry();
        }
        return cached;
    }

    @Override
    public String describe() {
        return "AssumeRole(provider=" + config.storageType
                + " arn=" + config.roleArn + ")";
    }

    private ReplicationCredentials assumeRoleWithRetry() throws ReplicationCredentialException {
        Exception lastException = null;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                ReplicationCredentials creds = doAssumeRole();
                LOG.info("[Replication] AssumeRole succeeded attempt={} expire={}",
                        attempt, creds.expiresAt);
                return creds;
            } catch (Exception e) {
                lastException = e;
                LOG.warn("[Replication] AssumeRole attempt {}/{} failed: {}",
                        attempt, MAX_RETRIES, e.getMessage());
            }
        }
        throw new ReplicationCredentialException(
                ReplicationCredentialException.ErrorCode.STS_ERROR,
                "STS AssumeRole failed after " + MAX_RETRIES + " retries for role="
                        + config.roleArn, lastException);
    }

    private ReplicationCredentials doAssumeRole() throws Exception {
        switch (config.storageType) {
            case S3:  return assumeRoleAWS();
            case OSS: return assumeRoleAlibaba();
            default:
                throw new ReplicationCredentialException(
                        ReplicationCredentialException.ErrorCode.CONFIGURATION_ERROR,
                        "AssumeRole not supported for storageType=" + config.storageType);
        }
    }

    /** AWS STS AssumeRole using aws-java-sdk-sts (already in fe-core pom). */
    private ReplicationCredentials assumeRoleAWS() throws Exception {
        // use instance profile as the base credential for STS call
        AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder
                .standard()
                .withCredentials(InstanceProfileCredentialsProvider.getInstance())
                .build();

        com.amazonaws.services.securitytoken.model.AssumeRoleRequest req =
                new com.amazonaws.services.securitytoken.model.AssumeRoleRequest()
                        .withRoleArn(config.roleArn)
                        .withRoleSessionName(config.roleSessionName)
                        .withDurationSeconds(3600);

        if (config.externalId != null && !config.externalId.isEmpty()) {
            req.withExternalId(config.externalId);
        }

        AssumeRoleResult result = stsClient.assumeRole(req);
        com.amazonaws.services.securitytoken.model.Credentials c = result.getCredentials();
        return new ReplicationCredentials(
                c.getAccessKeyId(),
                c.getSecretAccessKey(),
                c.getSessionToken(),
                c.getExpiration().toInstant());
    }

    /** Alibaba Cloud RAM STS AssumeRole using aliyuncs-java-sdk-sts. */
    private ReplicationCredentials assumeRoleAlibaba() throws Exception {
        // derive region from endpoint (e.g. oss-cn-beijing → cn-beijing)
        String region = deriveRegionFromEndpoint(config.endpoint);

        // use instance RAM role as the base credential for STS call
        DefaultProfile profile = DefaultProfile.getProfile(region);
        DefaultAcsClient client = new DefaultAcsClient(profile);

        com.aliyuncs.auth.sts.AssumeRoleRequest req =
                new com.aliyuncs.auth.sts.AssumeRoleRequest();
        req.setRoleArn(config.roleArn);
        req.setRoleSessionName(config.roleSessionName);
        req.setDurationSeconds(3600L);

        try {
            AssumeRoleResponse response = client.getAcsResponse(req);
            AssumeRoleResponse.Credentials c = response.getCredentials();
            Instant expiry = Instant.parse(c.getExpiration());
            return new ReplicationCredentials(
                    c.getAccessKeyId(),
                    c.getAccessKeySecret(),
                    c.getSecurityToken(),
                    expiry);
        } catch (ClientException e) {
            throw new Exception("Alibaba STS AssumeRole failed: " + e.getErrMsg(), e);
        }
    }

    /** Extracts region from OSS endpoint string for STS client setup. */
    private static String deriveRegionFromEndpoint(String endpoint) {
        // oss-cn-beijing-internal.aliyuncs.com → cn-beijing
        // oss-cn-shanghai.aliyuncs.com         → cn-shanghai
        if (endpoint == null || endpoint.isEmpty()) {
            return "cn-hangzhou";
        }
        String host = endpoint.replace("https://", "").replace("http://", "");
        if (host.startsWith("oss-")) {
            String withoutOss = host.substring(4); // cn-beijing-internal.aliyuncs.com
            int dot = withoutOss.indexOf('.');
            String regionPart = dot > 0 ? withoutOss.substring(0, dot) : withoutOss;
            return regionPart.replace("-internal", "");
        }
        return "cn-hangzhou";
    }
}
