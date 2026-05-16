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

package org.apache.doris.datasource.property.credentials.aliyun;

import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.auth.BasicSessionCredentials;
import com.aliyuncs.auth.StaticCredentialsProvider;
import com.aliyuncs.auth.sts.AssumeRoleRequest;
import com.aliyuncs.auth.sts.AssumeRoleResponse;
import com.aliyuncs.profile.DefaultProfile;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * Resolves temporary Alibaba Cloud credentials by chaining ECS instance profile
 * (IMDSv2) with STS AssumeRole. Used by {@link AliyunECSRestCredentialProvider}
 * and {@link org.apache.doris.datasource.property.metastore.AliyunDLFBaseProperties}.
 */
public class AliyunSTSCredentialResolver {

    private static final String IMDS_HOST = "http://100.100.100.200";
    private static final String IMDS_TOKEN_PATH = "/latest/api/token";
    private static final String IMDS_ROLE_PATH = "/latest/meta-data/ram/security-credentials/";
    private static final int IMDS_TIMEOUT_MS = 1000;
    private static final int IMDS_TOKEN_TTL_SECONDS = 21600;

    private AliyunSTSCredentialResolver() {}

    public static final class Credentials {
        public final String accessKeyId;
        public final String accessKeySecret;
        public final String securityToken;
        public final String expiration;    // ISO-8601, e.g. "2025-01-01T12:00:00Z"

        public Credentials(String accessKeyId, String accessKeySecret,
                String securityToken, String expiration) {
            this.accessKeyId = accessKeyId;
            this.accessKeySecret = accessKeySecret;
            this.securityToken = securityToken;
            this.expiration = expiration;
        }
    }

    public static Credentials resolve(String roleArn, String region, String stsEndpoint) {
        try {
            // IMDSv2: get a session token first; null means instance doesn't support IMDSv2,
            // in which case imdsGet falls back to IMDSv1 (no token header).
            String imdsToken = fetchImdsV2Token();

            String roleList = imdsGet(IMDS_HOST + IMDS_ROLE_PATH, imdsToken).trim();
            String roleName = roleList.contains("\n")
                    ? roleList.substring(0, roleList.indexOf('\n')).trim()
                    : roleList;
            if (roleName.isEmpty()) {
                throw new StoragePropertiesException("No RAM role attached to this ECS instance.");
            }

            JsonObject cred = JsonParser.parseString(
                    imdsGet(IMDS_HOST + IMDS_ROLE_PATH + roleName, imdsToken)).getAsJsonObject();
            if (!cred.has("Code") || !"Success".equals(cred.get("Code").getAsString())) {
                String msg = cred.has("Message") ? cred.get("Message").getAsString() : "unknown";
                throw new StoragePropertiesException("ECS metadata error: " + msg);
            }

            DefaultProfile profile = DefaultProfile.getProfile(region);
            DefaultAcsClient stsClient = new DefaultAcsClient(profile,
                    new StaticCredentialsProvider(new BasicSessionCredentials(
                            cred.get("AccessKeyId").getAsString(),
                            cred.get("AccessKeySecret").getAsString(),
                            cred.get("SecurityToken").getAsString())));

            AssumeRoleRequest request = new AssumeRoleRequest();
            request.setRoleArn(roleArn);
            request.setRoleSessionName("doris-dlf-" + System.currentTimeMillis());
            request.setDurationSeconds(3600L);
            request.setSysEndpoint(StringUtils.isNotBlank(stsEndpoint)
                    ? stsEndpoint : "sts." + region + ".aliyuncs.com");

            AssumeRoleResponse.Credentials assumed = stsClient.getAcsResponse(request).getCredentials();
            return new Credentials(
                    assumed.getAccessKeyId(),
                    assumed.getAccessKeySecret(),
                    assumed.getSecurityToken(),
                    assumed.getExpiration());
        } catch (StoragePropertiesException e) {
            throw e;
        } catch (Exception e) {
            throw new StoragePropertiesException(
                    "Failed to obtain STS credentials for role_arn=" + roleArn + ": " + e.getMessage());
        }
    }

    // Returns null on failure; callers pass null to imdsGet which omits the token header (IMDSv1 fallback).
    private static String fetchImdsV2Token() {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(IMDS_HOST + IMDS_TOKEN_PATH).openConnection();
            conn.setRequestMethod("PUT");
            conn.setRequestProperty("X-aliyun-ecs-metadata-token-ttl-seconds",
                    String.valueOf(IMDS_TOKEN_TTL_SECONDS));
            conn.setConnectTimeout(IMDS_TIMEOUT_MS);
            conn.setReadTimeout(IMDS_TIMEOUT_MS);
            return conn.getResponseCode() == 200 ? readResponse(conn) : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static String imdsGet(String url, String imdsToken) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("GET");
        if (imdsToken != null) {
            conn.setRequestProperty("X-aliyun-ecs-metadata-token", imdsToken);
        }
        conn.setConnectTimeout(IMDS_TIMEOUT_MS);
        conn.setReadTimeout(IMDS_TIMEOUT_MS);
        if (conn.getResponseCode() != 200) {
            throw new IOException("IMDS GET failed: HTTP " + conn.getResponseCode() + " url=" + url);
        }
        return readResponse(conn);
    }

    private static String readResponse(HttpURLConnection conn) throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (sb.length() > 0) {
                    sb.append('\n');
                }
                sb.append(line);
            }
            return sb.toString();
        }
    }
}
