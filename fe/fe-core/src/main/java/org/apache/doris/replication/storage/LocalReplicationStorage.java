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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * In-memory storage backend backed by a sorted map.
 * Used in unit tests — no cloud SDK dependency, no network calls.
 * Thread-safe. Not for production use.
 */
public class LocalReplicationStorage implements ReplicationStorageBackend {

    private static final Logger LOG = LogManager.getLogger(LocalReplicationStorage.class);

    // TreeMap keeps keys sorted lexicographically, matching cloud provider list() behaviour
    private final ConcurrentSkipListMap<String, byte[]> store = new ConcurrentSkipListMap<>();

    // optional: inject failures for error-path testing
    private volatile int failNextNPuts = 0;

    @Override
    public void put(String key, byte[] data) throws ReplicationStorageException {
        if (failNextNPuts > 0) {
            failNextNPuts--;
            throw new ReplicationStorageException(
                    ReplicationStorageException.ErrorCode.NETWORK_ERROR,
                    "Injected failure for testing");
        }
        store.put(key, Arrays.copyOf(data, data.length));
        LOG.debug("[Replication:Local] put key={} bytes={}", key, data.length);
    }

    @Override
    public byte[] get(String key) throws ReplicationStorageException {
        byte[] val = store.get(key);
        if (val == null) {
            LOG.debug("[Replication:Local] get key={} → not found", key);
            return null;
        }
        LOG.debug("[Replication:Local] get key={} bytes={}", key, val.length);
        return Arrays.copyOf(val, val.length);
    }

    @Override
    public List<String> list(String prefix) throws ReplicationStorageException {
        // tailMap from prefix, takeWhile key starts with prefix
        List<String> result = new ArrayList<>();
        for (Map.Entry<String, byte[]> entry : store.tailMap(prefix).entrySet()) {
            if (!entry.getKey().startsWith(prefix)) {
                break;
            }
            result.add(entry.getKey());
        }
        LOG.debug("[Replication:Local] list prefix={} count={}", prefix, result.size());
        return result;
    }

    @Override
    public boolean exists(String key) throws ReplicationStorageException {
        return store.containsKey(key);
    }

    @Override
    public void delete(String key) throws ReplicationStorageException {
        store.remove(key);
        LOG.debug("[Replication:Local] delete key={}", key);
    }

    /** Inject failures for the next N put() calls — for error-path unit tests. */
    public void injectPutFailures(int count) {
        this.failNextNPuts = count;
    }

    /** Clear all stored data — call between test cases. */
    public void clear() {
        store.clear();
    }

    /** Returns number of keys currently stored — for test assertions. */
    public int size() {
        return store.size();
    }
}
