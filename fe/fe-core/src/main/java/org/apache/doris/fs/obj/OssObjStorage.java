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

import org.apache.doris.backup.Status;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.S3URI;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.property.storage.OSSProperties;
import org.apache.doris.fs.remote.RemoteFile;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.common.auth.InstanceProfileCredentialsProvider;
import com.aliyun.oss.common.auth.STSAssumeRoleSessionCredentialsProvider;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ListObjectsV2Request;
import com.aliyun.oss.model.ListObjectsV2Result;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.sts.model.v20150401.AssumeRoleRequest;
import com.aliyuncs.sts.model.v20150401.AssumeRoleResponse;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class OssObjStorage implements ObjStorage<OSS> {
    private static final Logger LOG = LogManager.getLogger(OssObjStorage.class);
    private static final String ECS_METADATA_URL = "http://100.100.100.200/latest/meta-data/ram/security-credentials/";
    private static final int ECS_METADATA_TIMEOUT_MS = 5000;
    private static final Long STS_DURATION_SECONDS = 3600L;
    private static final Gson GSON = new Gson();

    protected OSSProperties ossProperties;
    private OSS client;
    private boolean isUsePathStyle;
    private boolean forceParsingByStandardUri;

    public OssObjStorage(OSSProperties ossProperties) {
        this.ossProperties = ossProperties;
        this.isUsePathStyle = Boolean.parseBoolean(ossProperties.getUsePathStyle());
        this.forceParsingByStandardUri = Boolean.parseBoolean(ossProperties.getForceParsingByStandardUrl());
    }

    @Override
    public OSS getClient() throws UserException {
        if (client == null) {
            client = createOssClient();
        }
        return client;
    }

    @Override
    public synchronized void close() throws Exception {
        if (client != null) {
            try {
                client.shutdown();
            } catch (Exception e) {
                LOG.warn("Failed to close OSS client: {}", e.getMessage(), e);
            }
            client = null;
        }
    }

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        return null;
    }

    @Override
    public Status headObject(String remotePath) {
        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            getClient().getObjectMetadata(uri.getBucket(), uri.getKey());
            return Status.OK;
        } catch (OSSException e) {
            if (e.getErrorCode().equals("NoSuchKey")) {
                return new Status(Status.ErrCode.NOT_FOUND, "remote path does not exist: " + remotePath);
            }
            return new Status(Status.ErrCode.COMMON_ERROR, "headObject failed: " + Util.getRootCauseMessage(e));
        } catch (UserException ue) {
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to OSS failed: " + Util.getRootCauseMessage(ue));
        }
    }

    @Override
    public Status getObject(String remoteFilePath, File localFile) {
        try {
            S3URI uri = S3URI.create(remoteFilePath, isUsePathStyle, forceParsingByStandardUri);
            getClient().getObject(new GetObjectRequest(uri.getBucket(), uri.getKey()), localFile);
            return Status.OK;
        } catch (OSSException e) {
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "get file from OSS error: " + e.getErrorMessage() + ". Root cause: " + Util.getRootCauseMessage(e));
        } catch (UserException ue) {
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to OSS failed: " + Util.getRootCauseMessage(ue));
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR, Util.getRootCauseMessage(e));
        }
    }

    @Override
    public Status putObject(String remotePath, @Nullable InputStream content, long contentLength) {
        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(contentLength);
            getClient().putObject(new PutObjectRequest(uri.getBucket(), uri.getKey(), content, metadata));
            return Status.OK;
        } catch (OSSException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "put object failed: " + Util.getRootCauseMessage(e));
        } catch (Exception ue) {
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to OSS failed: " + Util.getRootCauseMessage(ue));
        }
    }

    @Override
    public Status deleteObject(String remotePath) {
        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            getClient().deleteObject(uri.getBucket(), uri.getKey());
            return Status.OK;
        } catch (OSSException e) {
            if (e.getErrorCode().equals("NoSuchKey")) {
                return Status.OK;
            }
            return new Status(Status.ErrCode.COMMON_ERROR, "delete file failed: " + Util.getRootCauseMessage(e));
        } catch (UserException ue) {
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to OSS failed: " + Util.getRootCauseMessage(ue));
        }
    }

    @Override
    public Status deleteObjects(String absolutePath) {
        try {
            S3URI baseUri = S3URI.create(absolutePath, isUsePathStyle, forceParsingByStandardUri);
            String continuationToken = "";
            boolean isTruncated;
            do {
                RemoteObjects objects = listObjects(absolutePath, continuationToken);
                List<RemoteObject> objectList = objects.getObjectList();
                if (!objectList.isEmpty()) {
                    List<String> keysToDelete = objectList.stream()
                            .map(RemoteObject::getKey)
                            .collect(Collectors.toList());
                    getClient().deleteObjects(new DeleteObjectsRequest(baseUri.getBucket()).withKeys(keysToDelete));
                }
                isTruncated = objects.isTruncated();
                continuationToken = objects.getContinuationToken();
            } while (isTruncated);
            return Status.OK;
        } catch (DdlException e) {
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "list objects for delete failed: " + Util.getRootCauseMessage(e));
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "delete objects failed: " + Util.getRootCauseMessage(e));
        }
    }

    @Override
    public Status copyObject(String origFilePath, String destFilePath) {
        try {
            S3URI origUri = S3URI.create(origFilePath, isUsePathStyle, forceParsingByStandardUri);
            S3URI destUri = S3URI.create(destFilePath, isUsePathStyle, forceParsingByStandardUri);
            getClient().copyObject(new CopyObjectRequest(
                    origUri.getBucket(), origUri.getKey(), destUri.getBucket(), destUri.getKey()));
            return Status.OK;
        } catch (OSSException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "copy file failed: " + Util.getRootCauseMessage(e));
        } catch (UserException ue) {
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to OSS failed: " + Util.getRootCauseMessage(ue));
        }
    }

    @Override
    public RemoteObjects listObjects(String absolutePath, String continuationToken) throws DdlException {
        try {
            S3URI uri = S3URI.create(absolutePath, isUsePathStyle, forceParsingByStandardUri);
            ListObjectsV2Request request = new ListObjectsV2Request()
                    .withBucketName(uri.getBucket())
                    .withPrefix(normalizePrefix(uri.getKey()));
            if (!StringUtils.isEmpty(continuationToken)) {
                request.setContinuationToken(continuationToken);
            }
            ListObjectsV2Result result = getClient().listObjectsV2(request);
            List<RemoteObject> remoteObjects = new ArrayList<>();
            for (OSSObjectSummary obj : result.getObjectSummaries()) {
                String relativePath = getRelativePath(uri.getKey(), obj.getKey());
                remoteObjects.add(new RemoteObject(obj.getKey(), relativePath, obj.getETag(), obj.getSize()));
            }
            return new RemoteObjects(remoteObjects, result.isTruncated(), result.getNextContinuationToken());
        } catch (Exception e) {
            throw new DdlException("Failed to list objects for OSS: " + Util.getRootCauseMessage(e), e);
        }
    }

    public Status multipartUpload(String remotePath, @Nullable InputStream inputStream, long totalBytes) {
        String uploadId = null;
        S3URI uri = null;
        try {
            uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            InitiateMultipartUploadResult initiateResult = getClient().initiateMultipartUpload(
                    new InitiateMultipartUploadRequest(uri.getBucket(), uri.getKey()));
            uploadId = initiateResult.getUploadId();

            List<PartETag> partETags = new ArrayList<>();
            byte[] buffer = new byte[CHUNK_SIZE];
            int partNumber = 1;
            long uploadedBytes = 0;
            int bytesRead;

            while (uploadedBytes < totalBytes && (bytesRead = inputStream.read(buffer)) != -1) {
                uploadedBytes += bytesRead;
                UploadPartRequest uploadRequest = new UploadPartRequest();
                uploadRequest.setBucketName(uri.getBucket());
                uploadRequest.setKey(uri.getKey());
                uploadRequest.setUploadId(uploadId);
                uploadRequest.setPartNumber(partNumber++);
                uploadRequest.setPartSize(bytesRead);
                uploadRequest.setInputStream(new ByteArrayInputStream(buffer, 0, bytesRead));
                partETags.add(getClient().uploadPart(uploadRequest).getPartETag());
            }

            getClient().completeMultipartUpload(
                    new CompleteMultipartUploadRequest(uri.getBucket(), uri.getKey(), uploadId, partETags));
            return Status.OK;
        } catch (Exception e) {
            if (uri != null && uploadId != null) {
                try {
                    getClient().abortMultipartUpload(
                            new AbortMultipartUploadRequest(uri.getBucket(), uri.getKey(), uploadId));
                } catch (Exception e1) {
                    LOG.warn("Failed to abort multipartUpload {}", remotePath, e1);
                }
            }
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "Failed to multipartUpload " + remotePath + " reason: " + Util.getRootCauseMessage(e));
        }
    }

    public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            String bucket = uri.getBucket();
            String globPath = uri.getKey();
            PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + Paths.get(globPath));
            HashSet<String> directorySet = new HashSet<>();
            String listPrefix = getLongestPrefix(globPath);

            ListObjectsV2Request request = new ListObjectsV2Request()
                    .withBucketName(bucket)
                    .withPrefix(listPrefix);

            boolean isTruncated;
            do {
                ListObjectsV2Result response = getClient().listObjectsV2(request);
                for (OSSObjectSummary obj : response.getObjectSummaries()) {
                    java.nio.file.Path objPath = Paths.get(obj.getKey());
                    boolean isPrefix = false;
                    while (objPath != null && objPath.normalize().toString().startsWith(listPrefix)) {
                        if (!matcher.matches(objPath)) {
                            isPrefix = true;
                            objPath = objPath.getParent();
                            continue;
                        }
                        if (directorySet.contains(objPath.normalize().toString())) {
                            break;
                        }
                        if (isPrefix) {
                            directorySet.add(objPath.normalize().toString());
                        }
                        result.add(new RemoteFile(
                                fileNameOnly ? objPath.getFileName().toString() : "oss://" + bucket + "/" + objPath,
                                !isPrefix,
                                isPrefix ? -1 : obj.getSize(),
                                isPrefix ? -1 : obj.getSize(),
                                isPrefix ? 0 : obj.getLastModified().getTime()));
                        objPath = objPath.getParent();
                        isPrefix = true;
                    }
                }
                isTruncated = response.isTruncated();
                if (isTruncated) {
                    request.setContinuationToken(response.getNextContinuationToken());
                }
            } while (isTruncated);
            return Status.OK;
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "Errors while getting file status " + Util.getRootCauseMessage(e));
        }
    }

    private OSS createOssClient() throws UserException {
        String endpoint = ossProperties.getEndpoint();
        String accessKey = ossProperties.getAccessKey();
        String secretKey = ossProperties.getSecretKey();
        String sessionToken = ossProperties.getSessionToken();
        String roleArn = ossProperties.getOssRoleArn();
        String externalId = ossProperties.getOssExternalId();

        try {
            if (StringUtils.isNotBlank(roleArn)) {
                return createClientWithAssumeRole(endpoint, accessKey, secretKey, sessionToken, roleArn, externalId);
            } else if (StringUtils.isNotBlank(sessionToken)) {
                return createClientWithSessionToken(endpoint, accessKey, secretKey, sessionToken);
            } else if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
                return createClientWithPermanentCredentials(endpoint, accessKey, secretKey);
            } else {
                return createClientWithEcsInstanceProfile(endpoint);
            }
        } catch (ClientException e) {
            throw new UserException("Failed to create OSS client: " + e.getMessage());
        }
    }

    private OSS createClientWithEcsInstanceProfile(String endpoint) throws UserException {
        // Security gate: prevent unauthorized OSS access via instance profile
        if (!org.apache.doris.common.Config.oss_enable_instance_profile) {
            throw new UserException(
                    "OSS instance profile access is disabled for security. "
                    + "S3() TVF requires explicit credentials. Please provide one of:\n"
                    + "  1. 'oss.role_arn' for AssumeRole authentication (recommended for production)\n"
                    + "  2. 'oss.access_key' + 'oss.secret_key' for permanent credentials\n"
                    + "  3. 'oss.session_token' for temporary STS credentials\n"
                    + "If you are an administrator and want to enable instance profile access, "
                    + "set 'oss_enable_instance_profile=true' in fe.conf (not recommended).");
        }

        try {
            String roleName = fetchEcsMetadata(ECS_METADATA_URL);
            if (StringUtils.isBlank(roleName)) {
                throw new UserException("No RAM role attached to this ECS instance");
            }

            LOG.info("Using ECS instance profile for OSS access. Role: {} (oss_enable_instance_profile=true)",
                     roleName);

            InstanceProfileCredentialsProvider provider = new InstanceProfileCredentialsProvider(roleName);
            return new OSSClientBuilder().build(endpoint, provider);
        } catch (Exception e) {
            throw new UserException("Failed to create OSS client with ECS instance profile: " + e.getMessage());
        }
    }

    private OSS createClientWithAssumeRole(String endpoint, String accessKey, String secretKey,
            String sessionToken, String roleArn, String externalId) throws UserException, ClientException {
        if (StringUtils.isBlank(accessKey) || StringUtils.isBlank(secretKey)) {
            return createClientWithAssumeRoleUsingEcsProfile(endpoint, roleArn, externalId);
        }
        AssumeRoleCredentials creds = callAssumeRole(endpoint, accessKey, secretKey, sessionToken, roleArn, externalId);
        return new OSSClientBuilder().build(endpoint,
                new DefaultCredentialProvider(creds.accessKeyId, creds.accessKeySecret, creds.securityToken));
    }

    private OSS createClientWithAssumeRoleUsingEcsProfile(String endpoint, String roleArn, String externalId)
            throws UserException {
        try {
            String region = extractRegionFromEndpoint(endpoint);
            if (region == null) {
                region = ossProperties.getRegion();
            }
            if (StringUtils.isBlank(region)) {
                throw new UserException("Cannot determine region for STS AssumeRole");
            }

            if (StringUtils.isNotBlank(externalId)) {
                // External ID not supported by SDK provider, use manual implementation
                EcsCredentials ecsCreds = fetchEcsCredentials();
                AssumeRoleCredentials creds = callAssumeRole(endpoint, ecsCreds.accessKeyId,
                        ecsCreds.accessKeySecret, ecsCreds.securityToken, roleArn, externalId);
                return new OSSClientBuilder().build(endpoint,
                        new DefaultCredentialProvider(creds.accessKeyId, creds.accessKeySecret, creds.securityToken));
            }

            // Fetch ECS role name for the base credentials provider
            String roleName = fetchEcsMetadata(ECS_METADATA_URL);
            if (StringUtils.isBlank(roleName)) {
                throw new UserException("No RAM role attached to this ECS instance");
            }

            // Use STSAssumeRoleSessionCredentialsProvider with AlibabaCloud SDK credentials provider
            com.aliyuncs.auth.InstanceProfileCredentialsProvider baseCredsProvider =
                    new com.aliyuncs.auth.InstanceProfileCredentialsProvider(roleName);
            com.aliyuncs.profile.IClientProfile profile =
                    com.aliyuncs.profile.DefaultProfile.getProfile(region);

            STSAssumeRoleSessionCredentialsProvider stsProvider = new STSAssumeRoleSessionCredentialsProvider(
                    baseCredsProvider, roleArn, profile);
            stsProvider.withRoleSessionName("doris-fe-" + System.currentTimeMillis());
            return new OSSClientBuilder().build(endpoint, stsProvider);
        } catch (Exception e) {
            throw new UserException("Failed to create OSS client with AssumeRole using ECS profile: "
                    + e.getMessage());
        }
    }

    private OSS createClientWithSessionToken(String endpoint, String accessKey, String secretKey, String sessionToken) {
        return new OSSClientBuilder().build(endpoint,
                new DefaultCredentialProvider(accessKey, secretKey, sessionToken));
    }

    private OSS createClientWithPermanentCredentials(String endpoint, String accessKey, String secretKey) {
        return new OSSClientBuilder().build(endpoint, accessKey, secretKey);
    }

    private AssumeRoleCredentials callAssumeRole(String endpoint, String accessKey, String secretKey,
            String baseSessionToken, String roleArn, String externalId) throws ClientException {
        String region = extractRegionFromEndpoint(endpoint);
        if (region == null) {
            region = ossProperties.getRegion();
        }
        if (StringUtils.isBlank(region)) {
            throw new ClientException("Cannot determine region for STS AssumeRole");
        }

        DefaultAcsClient stsClient;
        if (StringUtils.isNotBlank(baseSessionToken)) {
            com.aliyuncs.auth.BasicSessionCredentials sessionCreds =
                    new com.aliyuncs.auth.BasicSessionCredentials(accessKey, secretKey, baseSessionToken);
            stsClient = new DefaultAcsClient(DefaultProfile.getProfile(region), sessionCreds);
        } else {
            stsClient = new DefaultAcsClient(DefaultProfile.getProfile(region, accessKey, secretKey));
        }

        AssumeRoleRequest request = new AssumeRoleRequest();
        request.setSysMethod(MethodType.POST);
        request.setRoleArn(roleArn);
        request.setRoleSessionName("doris-fe-" + System.currentTimeMillis());
        request.setDurationSeconds(STS_DURATION_SECONDS);
        if (StringUtils.isNotBlank(externalId)) {
            request.setExternalId(externalId);
        }

        AssumeRoleResponse.Credentials credentials = stsClient.getAcsResponse(request).getCredentials();
        return new AssumeRoleCredentials(
                credentials.getAccessKeyId(), credentials.getAccessKeySecret(), credentials.getSecurityToken());
    }

    private EcsCredentials fetchEcsCredentials() throws UserException {
        try {
            String roleName = fetchEcsMetadata(ECS_METADATA_URL);
            if (StringUtils.isBlank(roleName)) {
                throw new UserException("No RAM role attached to this ECS instance");
            }
            String credentialsJson = fetchEcsMetadata(ECS_METADATA_URL + roleName);

            JsonObject jsonObject = GSON.fromJson(credentialsJson, JsonObject.class);
            String accessKeyId = jsonObject.get("AccessKeyId").getAsString();
            String accessKeySecret = jsonObject.get("AccessKeySecret").getAsString();
            String securityToken = jsonObject.get("SecurityToken").getAsString();

            if (StringUtils.isBlank(accessKeyId) || StringUtils.isBlank(accessKeySecret)
                    || StringUtils.isBlank(securityToken)) {
                throw new UserException("Invalid credentials from ECS metadata service");
            }
            return new EcsCredentials(accessKeyId, accessKeySecret, securityToken);
        } catch (IOException e) {
            throw new UserException("Failed to connect to ECS metadata service: " + e.getMessage());
        } catch (Exception e) {
            throw new UserException("Failed to parse ECS metadata response: " + e.getMessage());
        }
    }

    private String fetchEcsMetadata(String url) throws IOException, UserException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setConnectTimeout(ECS_METADATA_TIMEOUT_MS);
        conn.setReadTimeout(ECS_METADATA_TIMEOUT_MS);
        conn.setRequestMethod("GET");
        if (conn.getResponseCode() != 200) {
            throw new UserException("Failed to fetch from ECS metadata service: " + url);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        StringBuilder response = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            response.append(line);
        }
        reader.close();
        return response.toString();
    }

    private String extractRegionFromEndpoint(String endpoint) {
        if (StringUtils.isBlank(endpoint)) {
            return null;
        }
        String clean = endpoint.replaceFirst("^https?://", "");
        if (clean.startsWith("oss-") && clean.contains(".aliyuncs.com")) {
            String regionPart = clean.substring(4);
            int dashIndex = regionPart.indexOf("-internal");
            if (dashIndex > 0) {
                return regionPart.substring(0, dashIndex);
            }
            int dotIndex = regionPart.indexOf(".");
            if (dotIndex > 0) {
                return regionPart.substring(0, dotIndex);
            }
        }
        return null;
    }

    private String getLongestPrefix(String globPath) {
        StringBuilder prefix = new StringBuilder();
        for (int i = 0; i < globPath.length(); i++) {
            char c = globPath.charAt(i);
            if (c == '*' || c == '?' || c == '[' || c == '{') {
                break;
            }
            prefix.append(c);
        }
        String result = prefix.toString();
        int lastSlash = result.lastIndexOf('/');
        return lastSlash >= 0 ? result.substring(0, lastSlash + 1) : "";
    }

    private static class AssumeRoleCredentials {
        final String accessKeyId;
        final String accessKeySecret;
        final String securityToken;

        AssumeRoleCredentials(String accessKeyId, String accessKeySecret, String securityToken) {
            this.accessKeyId = accessKeyId;
            this.accessKeySecret = accessKeySecret;
            this.securityToken = securityToken;
        }
    }

    private static class EcsCredentials {
        final String accessKeyId;
        final String accessKeySecret;
        final String securityToken;

        EcsCredentials(String accessKeyId, String accessKeySecret, String securityToken) {
            this.accessKeyId = accessKeyId;
            this.accessKeySecret = accessKeySecret;
            this.securityToken = securityToken;
        }
    }
}
