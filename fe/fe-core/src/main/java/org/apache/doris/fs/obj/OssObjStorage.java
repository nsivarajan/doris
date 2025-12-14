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
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
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
import com.aliyun.oss.model.UploadPartResult;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;
import com.aliyuncs.sts.model.v20150401.AssumeRoleRequest;
import com.aliyuncs.sts.model.v20150401.AssumeRoleResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class OssObjStorage implements ObjStorage<OSS> {
    private static final Logger LOG = LogManager.getLogger(OssObjStorage.class);

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
            String endpoint = ossProperties.getEndpoint();
            String accessKey = ossProperties.getAccessKey();
            String secretKey = ossProperties.getSecretKey();
            String sessionToken = ossProperties.getSessionToken();
            String roleArn = ossProperties.getOssRoleArn();
            String externalId = ossProperties.getOssExternalId();

            try {
                // Priority 1: If role ARN is specified, use AssumeRole to get temporary credentials
                if (StringUtils.isNotBlank(roleArn)) {
                    LOG.info("Using AssumeRole authentication with role ARN: {}", roleArn);
                    AssumeRoleCredentials assumedCreds = assumeRole(endpoint, accessKey, secretKey,
                                                                     roleArn, externalId);
                    CredentialsProvider credentialsProvider = new DefaultCredentialProvider(
                            assumedCreds.accessKeyId,
                            assumedCreds.accessKeySecret,
                            assumedCreds.securityToken);
                    client = new OSSClientBuilder().build(endpoint, credentialsProvider);
                    LOG.info("Created OSS client with AssumeRole credentials for endpoint: {}", endpoint);
                }
                // Priority 2: If session token is provided, use it directly (STS temporary credentials)
                else if (StringUtils.isNotBlank(sessionToken)) {
                    LOG.info("Using direct session token authentication");
                    CredentialsProvider credentialsProvider = new DefaultCredentialProvider(
                            accessKey, secretKey, sessionToken);
                    client = new OSSClientBuilder().build(endpoint, credentialsProvider);
                    LOG.info("Created OSS client with session token for endpoint: {}", endpoint);
                }
                // Priority 3: Use permanent credentials (access key + secret key)
                else if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
                    LOG.info("Using permanent credentials authentication");
                    client = new OSSClientBuilder().build(endpoint, accessKey, secretKey);
                    LOG.info("Created OSS client with permanent credentials for endpoint: {}", endpoint);
                }
                // Priority 4: No credentials provided - this will fail
                else {
                    throw new UserException("No valid OSS credentials provided. Please specify either: "
                            + "(1) role_arn for AssumeRole, "
                            + "(2) session_token for STS, or "
                            + "(3) access_key + secret_key for permanent credentials");
                }
            } catch (ClientException e) {
                LOG.error("Failed to assume role for OSS: {}", e.getMessage(), e);
                throw new UserException("Failed to assume role for OSS: " + e.getMessage());
            } catch (Exception e) {
                LOG.error("Failed to create OSS client for endpoint: {}", endpoint, e);
                throw new UserException("Failed to create OSS client: " + e.getMessage());
            }
        }
        return client;
    }

    /**
     * Assume a role using STS to get temporary credentials.
     * This enables cross-account access and least-privilege security.
     */
    private AssumeRoleCredentials assumeRole(String endpoint, String accessKey, String secretKey,
                                            String roleArn, String externalId) throws ClientException {
        // Extract region from endpoint (e.g., "oss-cn-beijing.aliyuncs.com" -> "cn-beijing")
        String region = extractRegionFromEndpoint(endpoint);
        if (region == null) {
            region = ossProperties.getRegion();
        }
        if (StringUtils.isBlank(region)) {
            throw new ClientException("Cannot determine region for STS AssumeRole. "
                    + "Please specify region explicitly via oss.region property");
        }

        // Create STS client
        IClientProfile profile = DefaultProfile.getProfile(region, accessKey, secretKey);
        DefaultAcsClient stsClient = new DefaultAcsClient(profile);

        // Create AssumeRole request
        AssumeRoleRequest request = new AssumeRoleRequest();
        request.setSysMethod(MethodType.POST);
        request.setRoleArn(roleArn);
        request.setRoleSessionName("doris-fe-" + System.currentTimeMillis());
        request.setDurationSeconds(3600L); // 1 hour

        if (StringUtils.isNotBlank(externalId)) {
            request.setExternalId(externalId);
            LOG.info("AssumeRole with external ID for enhanced security");
        }

        // Call AssumeRole API
        AssumeRoleResponse response = stsClient.getAcsResponse(request);
        AssumeRoleResponse.Credentials credentials = response.getCredentials();

        LOG.info("Successfully assumed role: {}, expiration: {}", roleArn, credentials.getExpiration());

        return new AssumeRoleCredentials(
                credentials.getAccessKeyId(),
                credentials.getAccessKeySecret(),
                credentials.getSecurityToken());
    }

    /**
     * Extract region from OSS endpoint.
     * Examples:
     *   oss-cn-beijing.aliyuncs.com -> cn-beijing
     *   oss-cn-shanghai-internal.aliyuncs.com -> cn-shanghai
     */
    private String extractRegionFromEndpoint(String endpoint) {
        if (StringUtils.isBlank(endpoint)) {
            return null;
        }

        // Remove protocol if present
        String cleanEndpoint = endpoint.replaceFirst("^https?://", "");

        // Pattern: oss-{region}[-internal].aliyuncs.com
        if (cleanEndpoint.startsWith("oss-") && cleanEndpoint.contains(".aliyuncs.com")) {
            String regionPart = cleanEndpoint.substring(4); // Remove "oss-"
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

    /**
     * Helper class to hold AssumeRole credentials
     */
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

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        return null;
    }

    @Override
    public Status headObject(String remotePath) {
        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            ObjectMetadata metadata = getClient().getObjectMetadata(uri.getBucket(), uri.getKey());
            if (LOG.isDebugEnabled()) {
                LOG.debug("headObject success: {}, metadata: {}", remotePath, metadata);
            }
            return Status.OK;
        } catch (OSSException e) {
            if (e.getErrorCode().equals("NoSuchKey")) {
                return new Status(Status.ErrCode.NOT_FOUND, "remote path does not exist: " + remotePath);
            } else {
                LOG.warn("headObject failed:", e);
                return new Status(Status.ErrCode.COMMON_ERROR, "headObject failed: " + Util.getRootCauseMessage(e));
            }
        } catch (UserException ue) {
            LOG.warn("connect to OSS failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to OSS failed: " + Util.getRootCauseMessage(ue));
        }
    }

    @Override
    public Status getObject(String remoteFilePath, File localFile) {
        try {
            S3URI uri = S3URI.create(remoteFilePath, isUsePathStyle, forceParsingByStandardUri);
            GetObjectRequest request = new GetObjectRequest(uri.getBucket(), uri.getKey());
            getClient().getObject(request, localFile);
            if (LOG.isDebugEnabled()) {
                LOG.debug("get file {} success", remoteFilePath);
            }
            return Status.OK;
        } catch (OSSException e) {
            LOG.warn("connect to OSS failed with OSS exception", e);
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "get file from OSS error: " + e.getErrorMessage()
                            + ". Root cause: " + Util.getRootCauseMessage(e));
        } catch (UserException ue) {
            LOG.warn("connect to OSS failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to OSS failed: " + Util.getRootCauseMessage(ue));
        } catch (Exception e) {
            LOG.warn("connect to OSS failed with unexpected exception", e);
            return new Status(Status.ErrCode.COMMON_ERROR, Util.getRootCauseMessage(e));
        }
    }

    @Override
    public Status putObject(String remotePath, @Nullable InputStream content, long contentLength) {
        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(contentLength);
            PutObjectRequest request = new PutObjectRequest(uri.getBucket(), uri.getKey(), content, metadata);
            getClient().putObject(request);
            if (LOG.isDebugEnabled()) {
                LOG.debug("put object success: {}", remotePath);
            }
            return Status.OK;
        } catch (OSSException e) {
            LOG.warn("put object failed: ", e);
            return new Status(Status.ErrCode.COMMON_ERROR, "put object failed: " + Util.getRootCauseMessage(e));
        } catch (Exception ue) {
            LOG.warn("connect to OSS failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to OSS failed: " + Util.getRootCauseMessage(ue));
        }
    }

    @Override
    public Status deleteObject(String remotePath) {
        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            getClient().deleteObject(uri.getBucket(), uri.getKey());
            if (LOG.isDebugEnabled()) {
                LOG.debug("delete file {} success", remotePath);
            }
            return Status.OK;
        } catch (OSSException e) {
            LOG.warn("delete file failed: ", e);
            if (e.getErrorCode().equals("NoSuchKey")) {
                return Status.OK;
            }
            return new Status(Status.ErrCode.COMMON_ERROR, "delete file failed: " + Util.getRootCauseMessage(e));
        } catch (UserException ue) {
            LOG.warn("connect to OSS failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to OSS failed: " + Util.getRootCauseMessage(ue));
        }
    }

    @Override
    public Status deleteObjects(String absolutePath) {
        try {
            S3URI baseUri = S3URI.create(absolutePath, isUsePathStyle, forceParsingByStandardUri);
            String continuationToken = "";
            boolean isTruncated = false;
            long totalObjects = 0;
            do {
                RemoteObjects objects = listObjects(absolutePath, continuationToken);
                List<RemoteObject> objectList = objects.getObjectList();
                if (!objectList.isEmpty()) {
                    List<String> keysToDelete = objectList.stream()
                            .map(RemoteObject::getKey)
                            .collect(Collectors.toList());

                    DeleteObjectsRequest request = new DeleteObjectsRequest(baseUri.getBucket())
                            .withKeys(keysToDelete);
                    DeleteObjectsResult result = getClient().deleteObjects(request);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{} of {} objects deleted for dir {}",
                                result.getDeletedObjects().size(), objectList.size(), absolutePath);
                        totalObjects += objectList.size();
                    }
                }

                isTruncated = objects.isTruncated();
                continuationToken = objects.getContinuationToken();
            } while (isTruncated);

            if (LOG.isDebugEnabled()) {
                LOG.debug("total delete {} objects for dir {}", totalObjects, absolutePath);
            }
            return Status.OK;
        } catch (DdlException e) {
            LOG.warn("deleteObjects:", e);
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "list objects for delete objects failed: " + Util.getRootCauseMessage(e));
        } catch (Exception e) {
            LOG.warn(String.format("delete objects %s failed", absolutePath), e);
            return new Status(Status.ErrCode.COMMON_ERROR, "delete objects failed: " + Util.getRootCauseMessage(e));
        }
    }

    @Override
    public Status copyObject(String origFilePath, String destFilePath) {
        try {
            S3URI origUri = S3URI.create(origFilePath, isUsePathStyle, forceParsingByStandardUri);
            S3URI destUri = S3URI.create(destFilePath, isUsePathStyle, forceParsingByStandardUri);
            CopyObjectRequest request = new CopyObjectRequest(
                    origUri.getBucket(), origUri.getKey(),
                    destUri.getBucket(), destUri.getKey());
            getClient().copyObject(request);
            if (LOG.isDebugEnabled()) {
                LOG.debug("copy file from {} to {} success", origFilePath, destFilePath);
            }
            return Status.OK;
        } catch (OSSException e) {
            LOG.warn("copy file failed: ", e);
            return new Status(Status.ErrCode.COMMON_ERROR, "copy file failed: " + Util.getRootCauseMessage(e));
        } catch (UserException ue) {
            LOG.warn("copy to OSS failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to OSS failed: " + Util.getRootCauseMessage(ue));
        }
    }

    @Override
    public RemoteObjects listObjects(String absolutePath, String continuationToken) throws DdlException {
        try {
            S3URI uri = S3URI.create(absolutePath, isUsePathStyle, forceParsingByStandardUri);
            String bucket = uri.getBucket();
            String prefix = uri.getKey();
            ListObjectsV2Request request = new ListObjectsV2Request()
                    .withBucketName(bucket)
                    .withPrefix(normalizePrefix(prefix));
            if (!StringUtils.isEmpty(continuationToken)) {
                request.setContinuationToken(continuationToken);
            }
            ListObjectsV2Result result = getClient().listObjectsV2(request);
            List<RemoteObject> remoteObjects = new ArrayList<>();
            for (OSSObjectSummary obj : result.getObjectSummaries()) {
                String relativePath = getRelativePath(prefix, obj.getKey());
                remoteObjects.add(new RemoteObject(obj.getKey(), relativePath, obj.getETag(), obj.getSize()));
            }
            return new RemoteObjects(remoteObjects, result.isTruncated(), result.getNextContinuationToken());
        } catch (Exception e) {
            LOG.warn(String.format("Failed to list objects for OSS: %s", absolutePath), e);
            throw new DdlException("Failed to list objects for OSS, Error message: " + Util.getRootCauseMessage(e), e);
        }
    }

    public Status multipartUpload(String remotePath, @Nullable InputStream inputStream, long totalBytes) {
        Status st = Status.OK;
        long uploadedBytes = 0;
        int bytesRead = 0;
        byte[] buffer = new byte[CHUNK_SIZE];
        int partNumber = 1;

        String uploadId = null;
        S3URI uri = null;
        List<PartETag> partETags = new ArrayList<>();

        try {
            uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(
                    uri.getBucket(), uri.getKey());
            InitiateMultipartUploadResult initiateResult = getClient().initiateMultipartUpload(initiateRequest);
            uploadId = initiateResult.getUploadId();

            while (uploadedBytes < totalBytes && (bytesRead = inputStream.read(buffer)) != -1) {
                uploadedBytes += bytesRead;
                UploadPartRequest uploadRequest = new UploadPartRequest();
                uploadRequest.setBucketName(uri.getBucket());
                uploadRequest.setKey(uri.getKey());
                uploadRequest.setUploadId(uploadId);
                uploadRequest.setPartNumber(partNumber);
                uploadRequest.setPartSize(bytesRead);
                uploadRequest.setInputStream(new ByteArrayInputStream(buffer, 0, bytesRead));

                UploadPartResult uploadResult = getClient().uploadPart(uploadRequest);
                partETags.add(uploadResult.getPartETag());
                partNumber++;
            }

            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                    uri.getBucket(), uri.getKey(), uploadId, partETags);
            getClient().completeMultipartUpload(completeRequest);
        } catch (Exception e) {
            LOG.warn("remotePath:{}, ", remotePath, e);
            st = new Status(Status.ErrCode.COMMON_ERROR, "Failed to multipartUpload " + remotePath
                    + " reason: " + Util.getRootCauseMessage(e));

            if (uri != null && uploadId != null) {
                try {
                    AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest(
                            uri.getBucket(), uri.getKey(), uploadId);
                    getClient().abortMultipartUpload(abortRequest);
                } catch (Exception e1) {
                    LOG.warn("Failed to abort multipartUpload {}", remotePath, e1);
                }
            }
        }
        return st;
    }

    /**
     * List all files under the given path with glob pattern.
     */
    public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        long roundCnt = 0;
        long elementCnt = 0;
        long matchCnt = 0;
        long startTime = System.nanoTime();
        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            String bucket = uri.getBucket();
            String globPath = uri.getKey();

            if (LOG.isDebugEnabled()) {
                LOG.debug("globList globPath:{}, remotePath:{}", globPath, remotePath);
            }
            java.nio.file.Path pathPattern = Paths.get(globPath);
            PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pathPattern);
            HashSet<String> directorySet = new HashSet<>();

            String listPrefix = getLongestPrefix(globPath);
            if (LOG.isDebugEnabled()) {
                LOG.debug("globList listPrefix: '{}' (from globPath: '{}')", listPrefix, globPath);
            }

            ListObjectsV2Request request = new ListObjectsV2Request()
                    .withBucketName(bucket)
                    .withPrefix(listPrefix);

            boolean isTruncated = false;
            do {
                roundCnt++;
                ListObjectsV2Result response = getClient().listObjectsV2(request);
                for (OSSObjectSummary obj : response.getObjectSummaries()) {
                    elementCnt++;
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

                        matchCnt++;
                        RemoteFile remoteFile = new RemoteFile(
                                fileNameOnly ? objPath.getFileName().toString() :
                                        "oss://" + bucket + "/" + objPath.toString(),
                                !isPrefix,
                                isPrefix ? -1 : obj.getSize(),
                                isPrefix ? -1 : obj.getSize(),
                                isPrefix ? 0 : obj.getLastModified().getTime()
                        );
                        result.add(remoteFile);
                        objPath = objPath.getParent();
                        isPrefix = true;
                    }
                }

                isTruncated = response.isTruncated();
                if (isTruncated) {
                    request.setContinuationToken(response.getNextContinuationToken());
                }
            } while (isTruncated);

            if (LOG.isDebugEnabled()) {
                LOG.debug("remotePath:{}, result:{}", remotePath, result);
            }
            return Status.OK;
        } catch (Exception e) {
            LOG.warn("Errors while getting file status", e);
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "Errors while getting file status " + Util.getRootCauseMessage(e));
        } finally {
            long endTime = System.nanoTime();
            long duration = endTime - startTime;
            if (LOG.isDebugEnabled()) {
                LOG.debug("process {} elements under prefix {} for {} round, match {} elements, take {} ms",
                        elementCnt, remotePath, roundCnt, matchCnt,
                        duration / 1000 / 1000);
            }
        }
    }

    /**
     * Get longest non-wildcard prefix from glob pattern.
     */
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
}
