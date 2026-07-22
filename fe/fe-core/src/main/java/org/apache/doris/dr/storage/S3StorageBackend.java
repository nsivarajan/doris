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

package org.apache.doris.dr.storage;

import org.apache.doris.dr.credentials.DRCredentialProvider;
import org.apache.doris.dr.credentials.DRCredentialProvider.DRCredentials;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * AWS S3 implementation of DRStorageBackend using AWS SDK v2 (already in fe-core pom).
 * Builds a fresh S3Client per operation to pick up rotated STS credentials.
 * Retries transient errors up to MAX_RETRIES times.
 */
public class S3StorageBackend implements DRStorageBackend {

    private static final Logger LOG = LogManager.getLogger(S3StorageBackend.class);
    private static final int MAX_RETRIES = 3;

    private final String bucket;
    private final String endpoint;
    private final DRCredentialProvider credentialProvider;

    public S3StorageBackend(String bucket, String endpoint,
            DRCredentialProvider credentialProvider) {
        this.bucket = bucket;
        this.endpoint = endpoint;
        this.credentialProvider = credentialProvider;
        LOG.info("[DR:S3] storage created bucket={} endpoint={}", bucket, endpoint);
    }

    @Override
    public void put(String key, byte[] data) throws DRStorageException {
        withRetry("put", key, () -> {
            buildClient().putObject(
                    PutObjectRequest.builder().bucket(bucket).key(key).build(),
                    RequestBody.fromBytes(data));
            LOG.debug("[DR:S3] put key={} bytes={}", key, data.length);
        });
    }

    @Override
    public byte[] get(String key) throws DRStorageException {
        return withRetryResult("get", key, () -> {
            try {
                byte[] result = buildClient().getObjectAsBytes(
                        GetObjectRequest.builder().bucket(bucket).key(key).build())
                        .asByteArray();
                LOG.debug("[DR:S3] get key={} bytes={}", key, result.length);
                return result;
            } catch (NoSuchKeyException e) {
                return null;
            }
        });
    }

    @Override
    public List<String> list(String prefix) throws DRStorageException {
        return withRetryResult("list", prefix, () -> {
            List<String> keys = new ArrayList<>();
            buildClient().listObjectsV2(
                    ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build())
                    .contents().forEach(o -> keys.add(o.key()));
            LOG.debug("[DR:S3] list prefix={} count={}", prefix, keys.size());
            return keys;
        });
    }

    @Override
    public boolean exists(String key) throws DRStorageException {
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
    public void delete(String key) throws DRStorageException {
        withRetry("delete", key, () -> {
            buildClient().deleteObject(
                    DeleteObjectRequest.builder().bucket(bucket).key(key).build());
            LOG.debug("[DR:S3] delete key={}", key);
        });
    }

    // ── client builder ────────────────────────────────────────────────────

    private S3Client buildClient() {
        DRCredentials creds = credentialProvider.getCredentials();
        software.amazon.awssdk.auth.credentials.AwsCredentials awsCreds;
        if (creds.hasStsToken()) {
            awsCreds = AwsSessionCredentials.create(
                    creds.accessKey, creds.secretKey, creds.securityToken);
        } else {
            awsCreds = AwsBasicCredentials.create(creds.accessKey, creds.secretKey);
        }
        S3ClientBuilder builder = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds));
        if (endpoint != null && !endpoint.isEmpty()) {
            builder.endpointOverride(URI.create(endpoint));
        }
        return builder.build();
    }

    // ── retry helpers ─────────────────────────────────────────────────────

    private void withRetry(String op, String key, S3Operation action)
            throws DRStorageException {
        Exception last = null;
        for (int i = 1; i <= MAX_RETRIES; i++) {
            try {
                action.execute();
                return;
            } catch (S3Exception e) {
                if (isTransient(e)) {
                    last = e;
                    LOG.warn("[DR:S3] {} transient attempt {}/{} key={}: {}",
                            op, i, MAX_RETRIES, key, e.getMessage());
                } else {
                    throw new DRStorageException(
                            "S3 " + op + " failed key=" + key
                            + " statusCode=" + e.statusCode(), e);
                }
            } catch (DRStorageException e) {
                throw e;
            } catch (Exception e) {
                last = e;
                LOG.warn("[DR:S3] {} attempt {}/{} error key={}: {}",
                        op, i, MAX_RETRIES, key, e.getMessage());
            }
        }
        throw new DRStorageException(
                "S3 " + op + " failed after " + MAX_RETRIES + " retries key=" + key, last);
    }

    private <T> T withRetryResult(String op, String key, S3ResultOperation<T> action)
            throws DRStorageException {
        Exception last = null;
        for (int i = 1; i <= MAX_RETRIES; i++) {
            try {
                return action.execute();
            } catch (S3Exception e) {
                if (isTransient(e)) {
                    last = e;
                    LOG.warn("[DR:S3] {} transient attempt {}/{} key={}: {}",
                            op, i, MAX_RETRIES, key, e.getMessage());
                } else {
                    throw new DRStorageException(
                            "S3 " + op + " failed key=" + key
                            + " statusCode=" + e.statusCode(), e);
                }
            } catch (DRStorageException e) {
                throw e;
            } catch (Exception e) {
                last = e;
                LOG.warn("[DR:S3] {} attempt {}/{} error key={}: {}",
                        op, i, MAX_RETRIES, key, e.getMessage());
            }
        }
        throw new DRStorageException(
                "S3 " + op + " failed after " + MAX_RETRIES + " retries key=" + key, last);
    }

    private boolean isTransient(S3Exception e) {
        int code = e.statusCode();
        // 429 Too Many Requests, 500/503 server errors
        return code == 429 || code == 500 || code == 503;
    }

    @FunctionalInterface
    interface S3Operation { void execute() throws Exception; }

    @FunctionalInterface
    interface S3ResultOperation<T> { T execute() throws Exception; }
}
