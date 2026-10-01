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

package org.apache.doris.cloud.storage;

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.common.auth.EcsRamRoleCredentialsProvider;
import com.aliyun.oss.model.GeneratePresignedUrlRequest;
import com.aliyun.oss.model.HeadObjectRequest;
import com.aliyun.oss.model.ListObjectsV2Request;
import com.aliyun.oss.model.ListObjectsV2Result;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.auth.BasicCredentials;
import com.aliyuncs.auth.StaticCredentialsProvider;
import com.aliyuncs.auth.sts.AssumeRoleRequest;
import com.aliyuncs.auth.sts.AssumeRoleResponse;
import com.aliyuncs.profile.DefaultProfile;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class OssRemote extends DefaultRemote {
    private static final Logger LOG = LogManager.getLogger(OssRemote.class);
    private OSS ossClient;

    public OssRemote(ObjectInfo obj) {
        super(obj);
    }

    @Override
    public String getPresignedUrl(String fileName) {
        String bucketName = obj.getBucket();
        String objectName = normalizePrefix(fileName);
        initClient();
        try {
            GeneratePresignedUrlRequest request
                    = new GeneratePresignedUrlRequest(bucketName, objectName, HttpMethod.PUT);
            Date expiration = new Date(new Date().getTime() + SESSION_EXPIRE_SECOND * 1000);
            request.setExpiration(expiration);
            URL signedUrl = ossClient.generatePresignedUrl(request);
            return signedUrl.toString();
        } catch (OSSException oe) {
            LOG.warn("Caught an OSSException, which means your request made it to OSS, "
                    + "but was rejected with an error response for some reason. "
                    + "Error Message: {} , Error Code: {} Request ID: {} Host ID: {}",
                    oe.getErrorMessage(), oe.getErrorCode(), oe.getRequestId(), oe.getHostId());
        } catch (ClientException ce) {
            LOG.warn("Caught an ClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with OSS, "
                    + "such as not being able to access the network. Error Message: {}", ce.getMessage());
        } finally {
            close();
        }
        return "";
    }

    @Override
    public ListObjectsResult listObjects(String continuationToken) throws DdlException {
        return listObjectsInner(normalizePrefix(), continuationToken);
    }

    @Override
    public ListObjectsResult listObjects(String subPrefix, String continuationToken) throws DdlException {
        return listObjectsInner(normalizePrefix(subPrefix), continuationToken);
    }

    @Override
    public ListObjectsResult headObject(String subKey) throws DdlException {
        initClient();
        try {
            String key = normalizePrefix(subKey);
            HeadObjectRequest request = new HeadObjectRequest(obj.getBucket(), key);
            ObjectMetadata metadata = ossClient.headObject(request);
            ObjectFile objectFile = new ObjectFile(key, getRelativePath(key), formatEtag(metadata.getETag()),
                    metadata.getContentLength());
            return new ListObjectsResult(Lists.newArrayList(objectFile), false, null);
        } catch (OSSException e) {
            if (e.getErrorCode().equals(OSSErrorCode.NO_SUCH_KEY)) {
                LOG.warn("NoSuchKey when head object for OSS, subKey={}", subKey);
                return new ListObjectsResult(Lists.newArrayList(), false, null);
            }
            LOG.warn("Failed to head object for OSS, subKey={}", subKey, e);
            throw new DdlException(
                    "Failed to head object for OSS, subKey=" + subKey + ", Error code=" + e.getErrorCode()
                            + ", Error message=" + e.getErrorMessage());
        }
    }

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest();
        assumeRoleRequest.setRoleArn(obj.getArn());
        assumeRoleRequest.setRoleSessionName(getNewRoleSessionName());
        assumeRoleRequest.setDurationSeconds((long) getDurationSeconds());
        try {
            DefaultProfile profile = DefaultProfile.getProfile(obj.getRegion());
            if (Config.enable_sts_vpc) {
                profile.enableUsingVpcEndpoint();
            }
            BasicCredentials basicCredentials = new BasicCredentials(obj.getAk(), obj.getSk());
            DefaultAcsClient ramClient = new DefaultAcsClient(profile, new StaticCredentialsProvider(basicCredentials));
            AssumeRoleResponse response = ramClient.getAcsResponse(assumeRoleRequest);
            AssumeRoleResponse.Credentials credentials = response.getCredentials();
            return Triple.of(credentials.getAccessKeyId(), credentials.getAccessKeySecret(),
                    credentials.getSecurityToken());
        } catch (Exception e) {
            LOG.warn("Failed get oss sts token", e);
            throw new DdlException(e.getMessage());
        }
    }

    private ListObjectsResult listObjectsInner(String prefix, String continuationToken) throws DdlException {
        initClient();
        try {
            ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(obj.getBucket())
                    .withPrefix(prefix);
            if (!StringUtils.isEmpty(continuationToken)) {
                request.setContinuationToken(continuationToken);
            }
            ListObjectsV2Result result = ossClient.listObjectsV2(request);
            List<ObjectFile> objectFiles = new ArrayList<>();
            for (OSSObjectSummary s : result.getObjectSummaries()) {
                objectFiles.add(
                        new ObjectFile(s.getKey(), getRelativePath(s.getKey()), formatEtag(s.getETag()), s.getSize()));
            }
            return new ListObjectsResult(objectFiles, result.isTruncated(), result.getNextContinuationToken());
        } catch (OSSException e) {
            LOG.warn("Failed to list objects for OSS prefix {}", prefix, e);
            throw new DdlException("Failed to list objects for OSS, Error code=" + e.getErrorCode() + ", Error message="
                    + e.getErrorMessage());
        }
    }

    private void initClient() {
        if (ossClient != null) {
            return;
        }
        String endpoint = obj.getEndpoint();
        if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
            endpoint = "https://" + endpoint;
        }

        Cloud.CredProviderTypePB credType = obj.getCredProviderType() != null
                ? obj.getCredProviderType()
                : Cloud.CredProviderTypePB.INSTANCE_PROFILE;

        CredentialsProvider provider;

        if (StringUtils.isNotBlank(obj.getArn())) {
            // role_arn configured: fetch ECS RAM role credentials then call STS AssumeRole.
            // Mirrors OSSObjStorage.resolveEcsRoleThenAssumeRole() and BE OSSSTSCredentialProvider.
            Triple<String, String, String> stsCreds = assumeRoleViaEcs(obj.getArn(), obj.getRegion());
            provider = new DefaultCredentialProvider(
                    stsCreds.getLeft(), stsCreds.getMiddle(), stsCreds.getRight());
        } else if (credType == Cloud.CredProviderTypePB.INSTANCE_PROFILE
                || (StringUtils.isBlank(obj.getAk()) && StringUtils.isBlank(obj.getSk()))) {
            // No AK/SK and no role_arn: use ECS instance metadata directly.
            // Role name from roleName field, or fall back to ALIBABA_CLOUD_ECS_METADATA env var.
            String roleName = StringUtils.isNotBlank(obj.getRoleName())
                    ? obj.getRoleName()
                    : System.getenv("ALIBABA_CLOUD_ECS_METADATA");
            if (StringUtils.isBlank(roleName)) {
                throw new IllegalStateException(
                        "OssRemote: cred_provider_type=INSTANCE_PROFILE but no role_name set and "
                        + "ALIBABA_CLOUD_ECS_METADATA env var is not configured");
            }
            provider = new EcsRamRoleCredentialsProvider(roleName);
        } else if (StringUtils.isNotBlank(obj.getToken())) {
            // STS temporary credentials (ak + sk + session_token)
            provider = new DefaultCredentialProvider(obj.getAk(), obj.getSk(), obj.getToken());
        } else {
            // Static AK/SK
            provider = new DefaultCredentialProvider(obj.getAk(), obj.getSk());
        }

        ossClient = new OSSClientBuilder().build(endpoint, provider);
        LOG.info("OssRemote: client initialised, credType={}, hasRoleArn={}, hasAk={}",
                credType, StringUtils.isNotBlank(obj.getArn()), StringUtils.isNotBlank(obj.getAk()));
    }

    // Fetch ECS RAM role credentials from instance metadata (100.100.100.200),
    // then call STS AssumeRole to get short-lived credentials for the target role_arn.
    private Triple<String, String, String> assumeRoleViaEcs(String roleArn, String region) {
        try {
            // Step 1: discover the attached RAM role name from ECS metadata
            String roleName;
            try {
                URL metaUrl = new URL("http://100.100.100.200/latest/meta-data/ram/security-credentials/");
                try (java.io.BufferedReader reader = new java.io.BufferedReader(
                        new java.io.InputStreamReader(metaUrl.openStream()))) {
                    String line = reader.readLine();
                    if (line == null || line.trim().isEmpty()) {
                        throw new RuntimeException(
                                "ECS metadata returned empty role name — no RAM role attached to this instance");
                    }
                    roleName = line.trim();
                }
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Failed to fetch ECS RAM role name from metadata: " + e.getMessage(), e);
            }

            // Step 2: fetch temporary credentials for that role from ECS metadata
            URL credUrl = new URL(
                    "http://100.100.100.200/latest/meta-data/ram/security-credentials/" + roleName);
            String credJson;
            try (java.io.BufferedReader reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(credUrl.openStream()))) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
                credJson = sb.toString();
            }
            String ecsAk = extractJson(credJson, "AccessKeyId");
            String ecsSk = extractJson(credJson, "AccessKeySecret");
            String ecsSt = extractJson(credJson, "SecurityToken");

            // Step 3: call STS AssumeRole with the ECS credentials
            return callStsAssumeRole(ecsAk, ecsSk, ecsSt, roleArn, region);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("assumeRoleViaEcs failed: " + e.getMessage(), e);
        }
    }

    private Triple<String, String, String> callStsAssumeRole(
            String ak, String sk, String stsToken, String roleArn, String region) throws Exception {
        AssumeRoleRequest req = new AssumeRoleRequest();
        req.setRoleArn(roleArn);
        req.setRoleSessionName(getNewRoleSessionName());
        req.setDurationSeconds((long) getDurationSeconds());

        DefaultProfile profile = DefaultProfile.getProfile(
                StringUtils.isNotBlank(region) ? region : "cn-hangzhou");
        if (Config.enable_sts_vpc) {
            profile.enableUsingVpcEndpoint();
        }
        com.aliyuncs.auth.BasicSessionCredentials sessionCreds =
                new com.aliyuncs.auth.BasicSessionCredentials(ak, sk, stsToken);
        DefaultAcsClient stsClient = new DefaultAcsClient(profile,
                new StaticCredentialsProvider(sessionCreds));
        AssumeRoleResponse resp = stsClient.getAcsResponse(req);
        AssumeRoleResponse.Credentials creds = resp.getCredentials();
        return Triple.of(creds.getAccessKeyId(), creds.getAccessKeySecret(), creds.getSecurityToken());
    }

    // Minimal JSON field extractor — avoids pulling in a JSON dependency.
    private static String extractJson(String json, String field) {
        String key = "\"" + field + "\"";
        int idx = json.indexOf(key);
        if (idx < 0) {
            throw new IllegalArgumentException("Field '" + field + "' not found in JSON");
        }
        int colon = json.indexOf(':', idx + key.length());
        int start = json.indexOf('"', colon + 1) + 1;
        int end = json.indexOf('"', start);
        return json.substring(start, end);
    }

    @Override
    public void close() {
        super.close();
        if (ossClient != null) {
            ossClient.shutdown();
            ossClient = null;
        }
    }

    @Override
    public String toString() {
        return "OssRemote{obj=" + obj + '}';
    }
}
