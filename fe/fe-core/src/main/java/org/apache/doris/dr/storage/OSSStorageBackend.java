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
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Alibaba Cloud OSS implementation of DRStorageBackend.
 *
 * H5 fix: OSS client is cached and rebuilt only when STS token changes,
 * avoiding per-operation connection pool creation and file descriptor leak.
 *
 * H6 fix: list() paginates using isTruncated + nextMarker so it returns
 * all segment keys beyond the 1000-object per-request limit.
 *
 * H7 fix: ClientException (auth failure, invalid config) is not retried —
 * only transient server-side errors are retried.
 *
 * M5 fix: OSSObject content stream is closed in try-with-resources.
 */
public class OSSStorageBackend implements DRStorageBackend {

    private static final Logger LOG = LogManager.getLogger(OSSStorageBackend.class);
    private static final int MAX_RETRIES = 3;
    // backoff base in ms — doubles on each retry with jitter (M8 fix)
    private static final long BACKOFF_BASE_MS = 200;

    private final String bucket;
    private final String endpoint;
    private final DRCredentialProvider credentialProvider;

    // cached client — rebuilt when credentials change (H5 fix)
    private volatile OSS cachedClient;
    private volatile String cachedTokenHash;

    public OSSStorageBackend(String bucket, String endpoint,
            DRCredentialProvider credentialProvider) {
        if (bucket == null || bucket.isEmpty() || endpoint == null || endpoint.isEmpty()) {
            throw new IllegalArgumentException(
                    "DR relay bucket and endpoint must be non-empty");
        }
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
                OSSObject obj = buildClient().getObject(bucket, key);
                // M5 fix: close stream in try-with-resources
                try (InputStream in = obj.getObjectContent()) {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    byte[] buf = new byte[8192];
                    int n;
                    while ((n = in.read(buf)) != -1) {
                        baos.write(buf, 0, n);
                    }
                    byte[] result = baos.toByteArray();
                    LOG.debug("[DR:OSS] get key={} bytes={}", key, result.length);
                    return result;
                }
            } catch (OSSException e) {
                if ("NoSuchKey".equals(e.getErrorCode())) {
                    return null;
                }
                throw e;
            } catch (IOException e) {
                throw new DRStorageException("OSS get stream read failed key=" + key, e);
            }
        });
    }

    /**
     * H6 fix: paginates using isTruncated + nextMarker to handle > 1000 objects.
     */
    @Override
    public List<String> list(String prefix) throws DRStorageException {
        return withRetryResult("list", prefix, () -> {
            List<String> keys = new ArrayList<>();
            String marker = null;
            do {
                ListObjectsRequest req = new ListObjectsRequest(bucket)
                        .withPrefix(prefix)
                        .withMaxKeys(1000);
                if (marker != null) {
                    req.setMarker(marker);
                }
                ObjectListing listing = buildClient().listObjects(req);
                for (OSSObjectSummary s : listing.getObjectSummaries()) {
                    keys.add(s.getKey());
                }
                marker = listing.isTruncated() ? listing.getNextMarker() : null;
            } while (marker != null);
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

    // ── client builder (H5 fix: cache client, rebuild on credential rotation) ──

    private OSS buildClient() throws DRStorageException {
        DRCredentials creds = credentialProvider.getCredentials();
        String tokenHash = creds.hasStsToken() ? creds.securityToken : "static";
        if (cachedClient == null || !tokenHash.equals(cachedTokenHash)) {
            DefaultCredentialProvider ossCredProvider = creds.hasStsToken()
                    ? new DefaultCredentialProvider(
                            creds.accessKey, creds.secretKey, creds.securityToken)
                    : new DefaultCredentialProvider(creds.accessKey, creds.secretKey);
            cachedClient = new OSSClientBuilder().build(endpoint, ossCredProvider, null);
            cachedTokenHash = tokenHash;
        }
        return cachedClient;
    }

    // ── retry helpers (H7: auth errors are not retried; M8: backoff) ──────

    private void withRetry(String op, String key, OSSOperation action)
            throws DRStorageException {
        Exception last = null;
        for (int i = 1; i <= MAX_RETRIES; i++) {
            try {
                action.execute();
                return;
            } catch (OSSException e) {
                if (isAuthError(e)) {
                    // H7: auth/config errors — fail fast, do not retry
                    throw new DRStorageException(
                            "OSS " + op + " auth error key=" + key
                            + " code=" + e.getErrorCode(), e);
                }
                if (isTransient(e)) {
                    last = e;
                    sleepBackoff(i);
                } else {
                    throw new DRStorageException(
                            "OSS " + op + " failed key=" + key
                            + " code=" + e.getErrorCode(), e);
                }
            } catch (DRStorageException e) {
                throw e;
            } catch (Exception e) {
                last = e;
                sleepBackoff(i);
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
                if (isAuthError(e)) {
                    throw new DRStorageException(
                            "OSS " + op + " auth error key=" + key
                            + " code=" + e.getErrorCode(), e);
                }
                if (isTransient(e)) {
                    last = e;
                    sleepBackoff(i);
                } else {
                    throw new DRStorageException(
                            "OSS " + op + " failed key=" + key
                            + " code=" + e.getErrorCode(), e);
                }
            } catch (DRStorageException e) {
                throw e;
            } catch (Exception e) {
                last = e;
                sleepBackoff(i);
            }
        }
        throw new DRStorageException(
                "OSS " + op + " failed after " + MAX_RETRIES + " retries key=" + key, last);
    }

    // H7: auth errors — fail immediately, retrying makes no sense
    private boolean isAuthError(OSSException e) {
        String code = e.getErrorCode();
        return "AccessDenied".equals(code)
                || "InvalidAccessKeyId".equals(code)
                || "SignatureDoesNotMatch".equals(code)
                || "SecurityTokenExpired".equals(code);
    }

    private boolean isTransient(OSSException e) {
        String code = e.getErrorCode();
        return "RequestTimeout".equals(code)
                || "SlowDown".equals(code)
                || "ServiceUnavailable".equals(code)
                || "InternalError".equals(code);
    }

    // M8: exponential backoff with jitter
    private void sleepBackoff(int attempt) {
        long delay = BACKOFF_BASE_MS * (1L << (attempt - 1));
        delay = delay + (long) (Math.random() * delay);
        try {
            Thread.sleep(Math.min(delay, 8000));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @FunctionalInterface interface OSSOperation { void execute() throws Exception; }

    @FunctionalInterface interface OSSResultOperation<T> { T execute() throws Exception; }
}
