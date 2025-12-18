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
import com.aliyuncs.http.HttpRequest;
import com.aliyuncs.http.HttpResponse;
import com.aliyuncs.http.clients.CompatibleUrlConnClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/**
 * Credentials provider using ECS RAM role (instance profile).
 * Wraps OSS SDK's EcsRamRoleCredentialsProvider.
 */
public class OSSInstanceProfileCredentialsProvider implements AwsCredentialsProvider {
    private static final Logger LOG = LogManager.getLogger(OSSInstanceProfileCredentialsProvider.class);
    private static final String METADATA_URL = "http://100.100.100.200/latest/meta-data/ram/security-credentials/";

    private EcsRamRoleCredentialsProvider ossProvider;

    @Override
    public AwsCredentials resolveCredentials() {
        if (ossProvider == null) {
            synchronized (this) {
                if (ossProvider == null) {
                    initialize();
                }
            }
        }

        Credentials creds = ossProvider.getCredentials();
        return AwsSessionCredentials.create(
                creds.getAccessKeyId(),
                creds.getSecretAccessKey(),
                creds.getSecurityToken()
        );
    }

    private void initialize() {
        try {
            // Fetch role name from metadata service
            HttpRequest request = new HttpRequest(METADATA_URL);
            request.setMethod(com.aliyuncs.http.MethodType.GET);
            request.setConnectTimeout(5000);
            request.setReadTimeout(5000);

            CompatibleUrlConnClient client = new CompatibleUrlConnClient();
            HttpResponse response = client.syncInvoke(request);
            String roleName = new String(response.getHttpContent(), "UTF-8").trim();

            // Use OSS SDK's provider (handles refresh automatically)
            ossProvider = new EcsRamRoleCredentialsProvider(roleName);
            LOG.info("OSS: Using ECS instance profile directly, role: {}", roleName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize ECS instance profile provider", e);
        }
    }
}
