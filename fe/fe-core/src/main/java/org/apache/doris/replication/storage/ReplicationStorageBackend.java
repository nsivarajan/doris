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

import java.util.List;

/**
 * Provider-agnostic interface for replication bucket I/O.
 * All implementations (S3, OSS, GCS, Local) must be idempotent on put
 * and return null (not throw) for missing keys on get.
 */
public interface ReplicationStorageBackend {

    /** Write bytes at key. Overwrites if key already exists. Idempotent. */
    void put(String key, byte[] data) throws ReplicationStorageException;

    /** Read bytes at key. Returns null if key does not exist. */
    byte[] get(String key) throws ReplicationStorageException;

    /** List all keys with the given prefix, sorted lexicographically. */
    List<String> list(String prefix) throws ReplicationStorageException;

    /** Returns true if key exists, false otherwise. */
    boolean exists(String key) throws ReplicationStorageException;

    /** Delete key. No-op if key does not exist. */
    void delete(String key) throws ReplicationStorageException;
}
