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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * AWS S3 implementation of ReplicationStorageBackend using AWS SDK v2 (already in fe-core pom).
 * Builds a fresh S3Client per operation to pick up rotated STS credentials.
 * Retries transient errors up to MAX_RETRIES times.
 */
public class S3ReplicationStorage implements ReplicationStorageBackend {

    private static final Logger LOG = LogManager.getLogger(S3ReplicationStorage.class);
    private static final int MAX_RETRIES = 3;

    private final String bucket;
    private final String endpoint;
    private final ReplicationCredentialProvider credentialProvider;

    public S3ReplicationStorage(ReplicationConfig config,
            ReplicationCredentialProvider credentialProvider) {
        this.bucket = config.bucket;
        this.endpoint = config.endpoint;
        this.credentialProvider = credentialProvider;
        LOG.info("[Replication:S3] created bucket={} endpoint={} credentials={}",
                bucket, endpoint, credentialProvider.describe());
    }

    @Override
    public void put(String key, byte[] data) throws ReplicationStorageException {
        withRetry("put", key, () -> {
            buildClient().putObject(
                    PutObjectRequest.builder().bucket(bucket).key(key).build(),
                    RequestBody.fromBytes(data));
            LOG.debug("[Replication:S3] put key={} bytes={}", key, data.length);
        });
    }

    @Override
    public byte[] get(String key) throws ReplicationStorageException {
        return withRetryResult("get", key, () -> {
            try {
                byte[] result = buildClient().getObjectAsBytes(
                        GetObjectRequest.builder().bucket(bucket).key(key).build()).asByteArray();
                LOG.debug("[Replication:S3] get key={} bytes={}", key, result.length);
                return result;
            } catch (NoSuchKeyException e) {
                LOG.debug("[Replication:S3] get key={} → not found", key);
                return null;
            }
        });
    }

    @Override
    public List<String> list(String prefix) throws ReplicationStorageException {
        return withRetryResult("list", prefix, () -> {
            List<String> keys = new ArrayList<>();
            ListObjectsV2Request req = ListObjectsV2Request.builder()
                    .bucket(bucket).prefix(prefix).build();
            ListObjectsV2Response resp = buildClient().listObjectsV2(req);
            resp.contents().forEach(o -> keys.add(o.key()));
            LOG.debug("[Replication:S3] list prefix={} count={}", prefix, keys.size());
            return keys;
        });
    }

    @Override
    public boolean exists(String key) throws ReplicationStorageException {
        return withRetryResult("exists", key, () -> {
            try {
                buildClient().headObject(
                        HeadObjectRequest.builder().bucket(bucket).key(key).build());
                return true;
            } catch (NoSuchKeyException e) {
                return false;
            }
        });
    }

    @Override
    public void delete(String key) throws ReplicationStorageException {
        withRetry("delete", key, () -> {
            buildClient().deleteObject(
                    DeleteObjectRequest.builder().bucket(bucket).key(key).build());
            LOG.debug("[Replication:S3] delete key={}", key);
        });
    }

    /** Builds a fresh S3Client with current credentials to pick up STS token rotation. */
    private S3Client buildClient() throws ReplicationStorageException {
        ReplicationCredentials creds;
        try {
            creds = credentialProvider.getCredentials();
        } catch (ReplicationCredentialException e) {
            throw new ReplicationStorageException(
                    ReplicationStorageException.ErrorCode.PERMISSION_DENIED,
                    "Failed to get credentials: " + e.getMessage(), e);
        }
        // use AwsSessionCredentials only when we have a session token (STS)
        // use AwsBasicCredentials for static AK/SK (no session token)
        software.amazon.awssdk.auth.credentials.AwsCredentials awsCreds =
                (creds.securityToken != null && !creds.securityToken.isEmpty())
                ? AwsSessionCredentials.create(
                        creds.accessKey, creds.secretKey, creds.securityToken)
                : AwsBasicCredentials.create(creds.accessKey, creds.secretKey);

        return S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .endpointOverride(URI.create(
                        endpoint.startsWith("http") ? endpoint : "https://" + endpoint))
                .region(Region.US_EAST_1) // overridden by endpointOverride for non-AWS endpoints
                .build();
    }

    private void withRetry(String op, String key, S3Operation action)
            throws ReplicationStorageException {
        Exception last = null;
        for (int i = 1; i <= MAX_RETRIES; i++) {
            try {
                action.execute();
                return;
            } catch (S3Exception e) {
                if (isTransient(e.statusCode())) {
                    last = e;
                    LOG.warn("[Replication:S3] {} attempt {}/{} transient error key={}: {}",
                            op, i, MAX_RETRIES, key, e.getMessage());
                } else {
                    throw toStorageException(e, op, key);
                }
            } catch (ReplicationStorageException e) {
                throw e;
            } catch (Exception e) {
                last = e;
                LOG.warn("[Replication:S3] {} attempt {}/{} error key={}: {}",
                        op, i, MAX_RETRIES, key, e.getMessage());
            }
        }
        throw new ReplicationStorageException(ReplicationStorageException.ErrorCode.NETWORK_ERROR,
                "S3 " + op + " failed after " + MAX_RETRIES + " retries for key=" + key, last);
    }

    private <T> T withRetryResult(String op, String key, S3ResultOperation<T> action)
            throws ReplicationStorageException {
        Exception last = null;
        for (int i = 1; i <= MAX_RETRIES; i++) {
            try {
                return action.execute();
            } catch (S3Exception e) {
                if (isTransient(e.statusCode())) {
                    last = e;
                    LOG.warn("[Replication:S3] {} attempt {}/{} transient error key={}: {}",
                            op, i, MAX_RETRIES, key, e.getMessage());
                } else {
                    throw toStorageException(e, op, key);
                }
            } catch (ReplicationStorageException e) {
                throw e;
            } catch (Exception e) {
                last = e;
                LOG.warn("[Replication:S3] {} attempt {}/{} error key={}: {}",
                        op, i, MAX_RETRIES, key, e.getMessage());
            }
        }
        throw new ReplicationStorageException(ReplicationStorageException.ErrorCode.NETWORK_ERROR,
                "S3 " + op + " failed after " + MAX_RETRIES + " retries for key=" + key, last);
    }

    private boolean isTransient(int statusCode) {
        return statusCode == 429 || statusCode == 500 || statusCode == 503;
    }

    private ReplicationStorageException toStorageException(S3Exception e, String op, String key) {
        if (e.statusCode() == 403) {
            return new ReplicationStorageException(
                    ReplicationStorageException.ErrorCode.PERMISSION_DENIED,
                    "S3 " + op + " permission denied key=" + key, e);
        }
        return new ReplicationStorageException(
                ReplicationStorageException.ErrorCode.UNKNOWN,
                "S3 " + op + " failed key=" + key + " status=" + e.statusCode(), e);
    }

    @FunctionalInterface
    interface S3Operation { void execute() throws Exception; }

    @FunctionalInterface
    interface S3ResultOperation<T> { T execute() throws Exception; }
}
