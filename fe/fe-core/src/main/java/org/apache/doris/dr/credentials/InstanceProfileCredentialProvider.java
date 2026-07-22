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
 * Fetches temporary credentials from the cloud instance metadata service.
 * Supports Alibaba Cloud ECS (100.100.100.200) and AWS EC2 (169.254.169.254).
 * Cached internally; refreshed 5 minutes before expiry.
 */
public class InstanceProfileCredentialProvider implements DRCredentialProvider {

    private static final Logger LOG = LogManager.getLogger(InstanceProfileCredentialProvider.class);

    // Alibaba Cloud ECS metadata endpoint
    private static final String ALIBABA_METADATA_BASE =
            "http://100.100.100.200/latest/meta-data/ram/security-credentials/";

    // AWS EC2 IMDS endpoint
    private static final String AWS_METADATA_BASE =
            "http://169.254.169.254/latest/meta-data/iam/security-credentials/";

    private static final int CONNECT_TIMEOUT_MS = 2000;
    private static final int READ_TIMEOUT_MS = 5000;
    // refresh 5 minutes before expiry to avoid mid-operation expiry
    private static final int REFRESH_BEFORE_EXPIRY_SECONDS = 300;

    private volatile DRCredentials cached;
    private volatile Instant expiry;

    @Override
    public synchronized DRCredentials getCredentials() {
        if (cached == null || isNearExpiry()) {
            cached = fetch();
        }
        return cached;
    }

    private boolean isNearExpiry() {
        return expiry != null
                && Instant.now().plusSeconds(REFRESH_BEFORE_EXPIRY_SECONDS).isAfter(expiry);
    }

    private DRCredentials fetch() {
        // Try Alibaba Cloud first, then AWS
        try {
            return fetchAlibaba();
        } catch (Exception alibaba) {
            LOG.debug("[DR] Alibaba metadata unavailable, trying AWS: {}", alibaba.getMessage());
            try {
                return fetchAws();
            } catch (Exception aws) {
                throw new RuntimeException(
                        "Failed to fetch instance profile credentials from both "
                        + "Alibaba (100.100.100.200) and AWS (169.254.169.254) metadata. "
                        + "Last error: " + aws.getMessage(), aws);
            }
        }
    }

    private DRCredentials fetchAlibaba() throws Exception {
        String roleName = httpGet(ALIBABA_METADATA_BASE).trim();
        String json = httpGet(ALIBABA_METADATA_BASE + roleName);
        JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
        expiry = Instant.parse(obj.get("Expiration").getAsString());
        LOG.info("[DR] Alibaba ECS RAM role credentials fetched, expiry={}", expiry);
        return new DRCredentials(
                obj.get("AccessKeyId").getAsString(),
                obj.get("AccessKeySecret").getAsString(),
                obj.get("SecurityToken").getAsString());
    }

    private DRCredentials fetchAws() throws Exception {
        String roleName = httpGet(AWS_METADATA_BASE).trim();
        String json = httpGet(AWS_METADATA_BASE + roleName);
        JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
        expiry = Instant.parse(obj.get("Expiration").getAsString());
        LOG.info("[DR] AWS EC2 instance profile credentials fetched, expiry={}", expiry);
        return new DRCredentials(
                obj.get("AccessKeyId").getAsString(),
                obj.get("SecretAccessKey").getAsString(),
                obj.get("Token").getAsString());
    }

    private String httpGet(String urlString) throws Exception {
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
        conn.setReadTimeout(READ_TIMEOUT_MS);
        conn.setRequestMethod("GET");
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
        } finally {
            conn.disconnect();
        }
    }
}
