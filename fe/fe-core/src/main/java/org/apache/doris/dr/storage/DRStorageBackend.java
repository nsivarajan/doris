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

import java.util.List;

/**
 * Provider-agnostic interface for DR relay bucket I/O.
 * All put operations are idempotent (overwrite if key exists).
 * get() returns null for missing keys rather than throwing.
 */
public interface DRStorageBackend {

    /** Write bytes at key. Overwrites if key already exists. */
    void put(String key, byte[] data) throws DRStorageException;

    /** Read bytes at key. Returns null if key does not exist. */
    byte[] get(String key) throws DRStorageException;

    /** List all keys with the given prefix, sorted lexicographically. */
    List<String> list(String prefix) throws DRStorageException;

    /** Returns true if key exists. */
    boolean exists(String key) throws DRStorageException;

    /** Delete key. No-op if key does not exist. */
    void delete(String key) throws DRStorageException;
}
