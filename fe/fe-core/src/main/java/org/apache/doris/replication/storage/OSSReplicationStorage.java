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

package org.apache.doris.replication.storage;

import org.apache.doris.replication.ReplicationConfig;
import org.apache.doris.replication.credentials.ReplicationCredentialException;
import org.apache.doris.replication.credentials.ReplicationCredentialProvider;
import org.apache.doris.replication.credentials.ReplicationCredentials;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Alibaba Cloud OSS implementation of ReplicationStorageBackend.
 * Builds a fresh OSSClient per operation so that rotated credentials (STS) are always used.
 * Retries transient network errors up to MAX_RETRIES times.
 */
public class OSSReplicationStorage implements ReplicationStorageBackend {

    private static final Logger LOG = LogManager.getLogger(OSSReplicationStorage.class);
    private static final int MAX_RETRIES = 3;

    private final String bucket;
    private final String endpoint;
    private final ReplicationCredentialProvider credentialProvider;

    public OSSReplicationStorage(ReplicationConfig config,
            ReplicationCredentialProvider credentialProvider) {
        this.bucket = config.bucket;
        this.endpoint = config.endpoint;
        this.credentialProvider = credentialProvider;
        LOG.info("[Replication:OSS] created bucket={} endpoint={} credentials={}",
                bucket, endpoint, credentialProvider.describe());
    }

    @Override
    public void put(String key, byte[] data) throws ReplicationStorageException {
        withRetry("put", key, () -> {
            buildClient().putObject(bucket, key, new ByteArrayInputStream(data));
            LOG.debug("[Replication:OSS] put key={} bytes={}", key, data.length);
        });
    }

    @Override
    public byte[] get(String key) throws ReplicationStorageException {
        return withRetryResult("get", key, () -> {
            try {
                com.aliyun.oss.model.OSSObject obj = buildClient().getObject(bucket, key);
                byte[] result = obj.getObjectContent().readAllBytes();
                obj.getObjectContent().close();
                LOG.debug("[Replication:OSS] get key={} bytes={}", key, result.length);
                return result;
            } catch (OSSException e) {
                if ("NoSuchKey".equals(e.getErrorCode())) {
                    LOG.debug("[Replication:OSS] get key={} → not found", key);
                    return null;
                }
                throw e;
            }
        });
    }

    @Override
    public List<String> list(String prefix) throws ReplicationStorageException {
        return withRetryResult("list", prefix, () -> {
            ListObjectsRequest req = new ListObjectsRequest(bucket)
                    .withPrefix(prefix)
                    .withMaxKeys(1000);
            ObjectListing listing = buildClient().listObjects(req);
            List<String> keys = new ArrayList<>();
            for (OSSObjectSummary s : listing.getObjectSummaries()) {
                keys.add(s.getKey());
            }
            LOG.debug("[Replication:OSS] list prefix={} count={}", prefix, keys.size());
            return keys;
        });
    }

    @Override
    public boolean exists(String key) throws ReplicationStorageException {
        return withRetryResult("exists", key, () -> buildClient().doesObjectExist(bucket, key));
    }

    @Override
    public void delete(String key) throws ReplicationStorageException {
        withRetry("delete", key, () -> {
            buildClient().deleteObject(bucket, key);
            LOG.debug("[Replication:OSS] delete key={}", key);
        });
    }

    /** Builds a fresh OSS client with current credentials — picks up STS token rotation. */
    private OSS buildClient() throws ReplicationStorageException {
        ReplicationCredentials creds;
        try {
            creds = credentialProvider.getCredentials();
        } catch (ReplicationCredentialException e) {
            throw new ReplicationStorageException(
                    ReplicationStorageException.ErrorCode.PERMISSION_DENIED,
                    "Failed to get credentials: " + e.getMessage(), e);
        }
        DefaultCredentialProvider ossCredProvider;
        if (creds.securityToken != null && !creds.securityToken.isEmpty()) {
            ossCredProvider = new DefaultCredentialProvider(
                    creds.accessKey, creds.secretKey, creds.securityToken);
        } else {
            ossCredProvider = new DefaultCredentialProvider(
                    creds.accessKey, creds.secretKey);
        }
        // use OSSClientBuilder (correct API for aliyun-sdk-oss 3.x)
        return new OSSClientBuilder().build(endpoint, ossCredProvider, null);
    }

    /** Execute void operation with retry on transient errors. */
    private void withRetry(String op, String key, OSSOperation action)
            throws ReplicationStorageException {
        Exception last = null;
        for (int i = 1; i <= MAX_RETRIES; i++) {
            try {
                action.execute();
                return;
            } catch (OSSException e) {
                if (isTransient(e)) {
                    last = e;
                    LOG.warn("[Replication:OSS] {} attempt {}/{} transient error key={}: {}",
                            op, i, MAX_RETRIES, key, e.getMessage());
                } else {
                    throw toStorageException(e, op, key);
                }
            } catch (ReplicationStorageException e) {
                throw e;
            } catch (Exception e) {
                last = e;
                LOG.warn("[Replication:OSS] {} attempt {}/{} error key={}: {}",
                        op, i, MAX_RETRIES, key, e.getMessage());
            }
        }
        throw new ReplicationStorageException(ReplicationStorageException.ErrorCode.NETWORK_ERROR,
                "OSS " + op + " failed after " + MAX_RETRIES + " retries for key=" + key, last);
    }

    /** Execute result-returning operation with retry. */
    private <T> T withRetryResult(String op, String key, OSSResultOperation<T> action)
            throws ReplicationStorageException {
        Exception last = null;
        for (int i = 1; i <= MAX_RETRIES; i++) {
            try {
                return action.execute();
            } catch (OSSException e) {
                if (isTransient(e)) {
                    last = e;
                    LOG.warn("[Replication:OSS] {} attempt {}/{} transient error key={}: {}",
                            op, i, MAX_RETRIES, key, e.getMessage());
                } else {
                    throw toStorageException(e, op, key);
                }
            } catch (ReplicationStorageException e) {
                throw e;
            } catch (Exception e) {
                last = e;
                LOG.warn("[Replication:OSS] {} attempt {}/{} error key={}: {}",
                        op, i, MAX_RETRIES, key, e.getMessage());
            }
        }
        throw new ReplicationStorageException(ReplicationStorageException.ErrorCode.NETWORK_ERROR,
                "OSS " + op + " failed after " + MAX_RETRIES + " retries for key=" + key, last);
    }

    /** Returns true for errors that are safe to retry. */
    private boolean isTransient(OSSException e) {
        int statusCode = e.getStatusCode();
        return statusCode == 429 || statusCode == 500 || statusCode == 503;
    }

    /** Maps OSS exception error codes to ReplicationStorageException. */
    private ReplicationStorageException toStorageException(OSSException e, String op, String key) {
        String code = e.getErrorCode();
        if ("AccessDenied".equals(code) || "InvalidAccessKeyId".equals(code)) {
            return new ReplicationStorageException(
                    ReplicationStorageException.ErrorCode.PERMISSION_DENIED,
                    "OSS " + op + " permission denied key=" + key + ": " + e.getMessage(), e);
        }
        return new ReplicationStorageException(
                ReplicationStorageException.ErrorCode.UNKNOWN,
                "OSS " + op + " failed key=" + key + " errorCode=" + code, e);
    }

    @FunctionalInterface
    interface OSSOperation { void execute() throws Exception; }

    @FunctionalInterface
    interface OSSResultOperation<T> { T execute() throws Exception; }
}
