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

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Alibaba Cloud OSS implementation of DRStorageBackend.
 * Builds a fresh OSSClient per operation so STS-rotated credentials are always current.
 * Retries transient errors (throttle, server-side transients) up to MAX_RETRIES times.
 */
public class OSSStorageBackend implements DRStorageBackend {

    private static final Logger LOG = LogManager.getLogger(OSSStorageBackend.class);
    private static final int MAX_RETRIES = 3;

    private final String bucket;
    private final String endpoint;
    private final DRCredentialProvider credentialProvider;

    public OSSStorageBackend(String bucket, String endpoint,
            DRCredentialProvider credentialProvider) {
        this.bucket = bucket;
        this.endpoint = endpoint;
        this.credentialProvider = credentialProvider;
        LOG.info("[DR:OSS] storage created bucket={} endpoint={}", bucket, endpoint);
    }

    @Override
    public void put(String key, byte[] data) throws DRStorageException {
        withRetry("put", key, () -> {
            buildClient().putObject(bucket, key, new ByteArrayInputStream(data));
            LOG.debug("[DR:OSS] put key={} bytes={}", key, data.length);
        });
    }

    @Override
    public byte[] get(String key) throws DRStorageException {
        return withRetryResult("get", key, () -> {
            try {
                com.aliyun.oss.model.OSSObject obj = buildClient().getObject(bucket, key);
                byte[] result = obj.getObjectContent().readAllBytes();
                obj.getObjectContent().close();
                LOG.debug("[DR:OSS] get key={} bytes={}", key, result.length);
                return result;
            } catch (OSSException e) {
                if ("NoSuchKey".equals(e.getErrorCode())) {
                    return null;
                }
                throw e;
            }
        });
    }

    @Override
    public List<String> list(String prefix) throws DRStorageException {
        return withRetryResult("list", prefix, () -> {
            ListObjectsRequest req = new ListObjectsRequest(bucket)
                    .withPrefix(prefix)
                    .withMaxKeys(1000);
            ObjectListing listing = buildClient().listObjects(req);
            List<String> keys = new ArrayList<>();
            for (OSSObjectSummary s : listing.getObjectSummaries()) {
                keys.add(s.getKey());
            }
            LOG.debug("[DR:OSS] list prefix={} count={}", prefix, keys.size());
            return keys;
        });
    }

    @Override
    public boolean exists(String key) throws DRStorageException {
        return withRetryResult("exists", key,
                () -> buildClient().doesObjectExist(bucket, key));
    }

    @Override
    public void delete(String key) throws DRStorageException {
        withRetry("delete", key, () -> {
            buildClient().deleteObject(bucket, key);
            LOG.debug("[DR:OSS] delete key={}", key);
        });
    }

    // ── client builder ────────────────────────────────────────────────────

    /** Builds a fresh OSSClient with current credentials (picks up STS token rotation). */
    private OSS buildClient() throws DRStorageException {
        DRCredentials creds = credentialProvider.getCredentials();
        DefaultCredentialProvider ossCredProvider;
        if (creds.hasStsToken()) {
            ossCredProvider = new DefaultCredentialProvider(
                    creds.accessKey, creds.secretKey, creds.securityToken);
        } else {
            ossCredProvider = new DefaultCredentialProvider(
                    creds.accessKey, creds.secretKey);
        }
        return new OSSClientBuilder().build(endpoint, ossCredProvider, null);
    }

    // ── retry helpers ─────────────────────────────────────────────────────

    private void withRetry(String op, String key, OSSOperation action)
            throws DRStorageException {
        Exception last = null;
        for (int i = 1; i <= MAX_RETRIES; i++) {
            try {
                action.execute();
                return;
            } catch (OSSException e) {
                if (isTransient(e)) {
                    last = e;
                    LOG.warn("[DR:OSS] {} transient attempt {}/{} key={}: {}",
                            op, i, MAX_RETRIES, key, e.getMessage());
                } else {
                    throw new DRStorageException(
                            "OSS " + op + " failed key=" + key
                            + " errorCode=" + e.getErrorCode(), e);
                }
            } catch (DRStorageException e) {
                throw e;
            } catch (Exception e) {
                last = e;
                LOG.warn("[DR:OSS] {} attempt {}/{} error key={}: {}",
                        op, i, MAX_RETRIES, key, e.getMessage());
            }
        }
        throw new DRStorageException(
                "OSS " + op + " failed after " + MAX_RETRIES + " retries key=" + key, last);
    }

    private <T> T withRetryResult(String op, String key, OSSResultOperation<T> action)
            throws DRStorageException {
        Exception last = null;
        for (int i = 1; i <= MAX_RETRIES; i++) {
            try {
                return action.execute();
            } catch (OSSException e) {
                if (isTransient(e)) {
                    last = e;
                    LOG.warn("[DR:OSS] {} transient attempt {}/{} key={}: {}",
                            op, i, MAX_RETRIES, key, e.getMessage());
                } else {
                    throw new DRStorageException(
                            "OSS " + op + " failed key=" + key
                            + " errorCode=" + e.getErrorCode(), e);
                }
            } catch (DRStorageException e) {
                throw e;
            } catch (Exception e) {
                last = e;
                LOG.warn("[DR:OSS] {} attempt {}/{} error key={}: {}",
                        op, i, MAX_RETRIES, key, e.getMessage());
            }
        }
        throw new DRStorageException(
                "OSS " + op + " failed after " + MAX_RETRIES + " retries key=" + key, last);
    }

    private boolean isTransient(OSSException e) {
        String code = e.getErrorCode();
        return "RequestTimeout".equals(code)
                || "SlowDown".equals(code)
                || "ServiceUnavailable".equals(code)
                || "InternalError".equals(code);
    }

    @FunctionalInterface
    interface OSSOperation { void execute() throws Exception; }

    @FunctionalInterface
    interface OSSResultOperation<T> { T execute() throws Exception; }
}
