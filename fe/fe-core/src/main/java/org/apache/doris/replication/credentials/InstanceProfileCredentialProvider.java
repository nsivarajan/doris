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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;

/**
 * Reads temporary credentials from the cloud instance metadata service.
 * No credential config needed — the ECS/EC2 instance role is used automatically.
 * Metadata endpoint URLs differ per cloud provider.
 */
public class InstanceProfileCredentialProvider implements ReplicationCredentialProvider {

    private static final Logger LOG = LogManager.getLogger(InstanceProfileCredentialProvider.class);

    // metadata endpoint URLs per provider
    private static final String AWS_METADATA_BASE
            = "http://169.254.169.254/latest/meta-data/iam/security-credentials/";
    private static final String ALIBABA_METADATA_BASE
            = "http://100.100.100.200/latest/meta-data/ram/security-credentials/";
    private static final String GCP_METADATA_URL
            = "http://metadata.google.internal/computeMetadata/v1/instance/"
            + "service-accounts/default/token";

    private static final int CONNECT_TIMEOUT_MS = 2000;
    private static final int READ_TIMEOUT_MS    = 5000;

    private final ReplicationConfig.StorageType storageType;

    // cached credentials; refreshed when near expiry
    private volatile ReplicationCredentials cached;

    public InstanceProfileCredentialProvider(ReplicationConfig.StorageType storageType) {
        this.storageType = storageType;
        LOG.info("[Replication] InstanceProfileCredentialProvider created for provider={}",
                storageType);
    }

    @Override
    public ReplicationCredentials getCredentials() throws ReplicationCredentialException {
        if (cached == null || cached.isNearExpiry(300)) {
            cached = fetchFromMetadata();
        }
        return cached;
    }

    @Override
    public String describe() {
        return "InstanceProfile(provider=" + storageType + ")";
    }

    private ReplicationCredentials fetchFromMetadata() throws ReplicationCredentialException {
        switch (storageType) {
            case S3:  return fetchAwsCredentials();
            case OSS: return fetchAlibabaCredentials();
            case GCS: return fetchGcpCredentials();
            default:
                throw new ReplicationCredentialException(
                        ReplicationCredentialException.ErrorCode.CONFIGURATION_ERROR,
                        "InstanceProfile not supported for storageType=" + storageType);
        }
    }

    /** Reads AWS instance profile credentials from IMDSv1 metadata endpoint. */
    private ReplicationCredentials fetchAwsCredentials() throws ReplicationCredentialException {
        try {
            // step 1: get the role name
            String roleName = httpGet(AWS_METADATA_BASE, null).trim();
            // step 2: get credentials for that role
            String json = httpGet(AWS_METADATA_BASE + roleName, null);
            JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
            Instant expiry = Instant.parse(obj.get("Expiration").getAsString());
            LOG.info("[Replication] AWS instance profile credentials fetched, expire={}",
                    expiry);
            return new ReplicationCredentials(
                    obj.get("AccessKeyId").getAsString(),
                    obj.get("SecretAccessKey").getAsString(),
                    obj.get("Token").getAsString(),
                    expiry);
        } catch (Exception e) {
            throw new ReplicationCredentialException(
                    ReplicationCredentialException.ErrorCode.METADATA_ENDPOINT_UNAVAILABLE,
                    "Failed to fetch AWS instance profile credentials: " + e.getMessage(), e);
        }
    }

    /** Reads Alibaba Cloud ECS RAM role credentials from metadata endpoint. */
    private ReplicationCredentials fetchAlibabaCredentials() throws ReplicationCredentialException {
        try {
            // step 1: get the RAM role name
            String roleName = httpGet(ALIBABA_METADATA_BASE, null).trim();
            // step 2: get credentials for that role
            String json = httpGet(ALIBABA_METADATA_BASE + roleName, null);
            JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
            Instant expiry = Instant.parse(obj.get("Expiration").getAsString());
            LOG.info("[Replication] Alibaba Cloud instance RAM role credentials fetched, expire={}",
                    expiry);
            return new ReplicationCredentials(
                    obj.get("AccessKeyId").getAsString(),
                    obj.get("AccessKeySecret").getAsString(),
                    obj.get("SecurityToken").getAsString(),
                    expiry);
        } catch (Exception e) {
            throw new ReplicationCredentialException(
                    ReplicationCredentialException.ErrorCode.METADATA_ENDPOINT_UNAVAILABLE,
                    "Failed to fetch Alibaba Cloud instance RAM credentials: " + e.getMessage(), e);
        }
    }

    /** Reads GCP service account token from metadata endpoint. */
    private ReplicationCredentials fetchGcpCredentials() throws ReplicationCredentialException {
        try {
            // GCP returns a bearer token — stored in accessKey, secretKey left empty
            String json = httpGet(GCP_METADATA_URL, "metadata-flavor:Google");
            JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
            long expiresInSeconds = obj.get("expires_in").getAsLong();
            Instant expiry = Instant.now().plusSeconds(expiresInSeconds);
            LOG.info("[Replication] GCP service account token fetched, expire={}", expiry);
            return new ReplicationCredentials(
                    obj.get("access_token").getAsString(),
                    "",
                    null,
                    expiry);
        } catch (Exception e) {
            throw new ReplicationCredentialException(
                    ReplicationCredentialException.ErrorCode.METADATA_ENDPOINT_UNAVAILABLE,
                    "Failed to fetch GCP workload identity token: " + e.getMessage(), e);
        }
    }

    /** Simple blocking HTTP GET — only used for metadata endpoints (tiny responses). */
    private String httpGet(String urlString, String extraHeader) throws Exception {
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
        conn.setReadTimeout(READ_TIMEOUT_MS);
        conn.setRequestMethod("GET");
        if (extraHeader != null) {
            String[] parts = extraHeader.split(":", 2);
            conn.setRequestProperty(parts[0].trim(), parts[1].trim());
        }
        int status = conn.getResponseCode();
        if (status != 200) {
            throw new RuntimeException("HTTP " + status + " from " + urlString);
        }
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(conn.getInputStream()))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        }
    }
}
