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
import com.aliyuncs.auth.BasicCredentials;
import com.aliyuncs.auth.StaticCredentialsProvider;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.sts.model.v20150401.AssumeRoleWithOIDCRequest;
import com.aliyuncs.sts.model.v20150401.AssumeRoleWithOIDCResponse;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Resolves Alibaba Cloud credentials for ACK pods via RRSA:
 * reads an OIDC token from a file and calls sts:AssumeRoleWithOIDC.
 * AssumeRoleWithOIDC does not require long-lived credentials to sign the request;
 * the OIDC token itself authenticates the caller with the STS service.
 */
public class AliyunOIDCCredentialResolver {

    private AliyunOIDCCredentialResolver() {}

    public static AliyunSTSCredentialResolver.Credentials resolve(
            String roleArn, String oidcProviderArn, String oidcTokenFile,
            String region, String stsEndpoint) {
        try {
            String tokenFile = StringUtils.isBlank(oidcTokenFile)
                    ? System.getenv("ALIBABA_CLOUD_OIDC_TOKEN_FILE")
                    : oidcTokenFile;
            if (StringUtils.isBlank(tokenFile)) {
                throw new StoragePropertiesException(
                        "OIDC token file path not set. Provide iceberg.rest.oidc_token_file "
                                + "or set ALIBABA_CLOUD_OIDC_TOKEN_FILE env var.");
            }

            String oidcToken = new String(
                    Files.readAllBytes(Paths.get(tokenFile)), StandardCharsets.UTF_8).trim();

            DefaultProfile profile = DefaultProfile.getProfile(region);
            DefaultAcsClient client = new DefaultAcsClient(profile,
                    new StaticCredentialsProvider(new BasicCredentials("", "")));

            AssumeRoleWithOIDCRequest request = new AssumeRoleWithOIDCRequest();
            request.setRoleArn(roleArn);
            request.setOIDCProviderArn(oidcProviderArn);
            request.setOIDCToken(oidcToken);
            request.setRoleSessionName("doris-rrsa-" + System.currentTimeMillis());
            request.setDurationSeconds(3600L);
            request.setSysEndpoint(StringUtils.isNotBlank(stsEndpoint)
                    ? stsEndpoint : "sts." + region + ".aliyuncs.com");

            AssumeRoleWithOIDCResponse.Credentials assumed =
                    client.getAcsResponse(request).getCredentials();
            return new AliyunSTSCredentialResolver.Credentials(
                    assumed.getAccessKeyId(),
                    assumed.getAccessKeySecret(),
                    assumed.getSecurityToken(),
                    assumed.getExpiration());
        } catch (StoragePropertiesException e) {
            throw e;
        } catch (Exception e) {
            throw new StoragePropertiesException(
                    "Failed to obtain RRSA credentials for role_arn=" + roleArn + ": " + e.getMessage());
        }
    }
}
