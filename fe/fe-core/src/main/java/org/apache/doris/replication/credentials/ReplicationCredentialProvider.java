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

package org.apache.doris.replication.credentials;

/**
 * Provides credentials for accessing the replication storage bucket.
 * Implementations handle credential lifecycle (caching, refresh before expiry).
 * Callers must not cache the returned credentials — always call getCredentials()
 * to allow the provider to refresh transparently.
 */
public interface ReplicationCredentialProvider {

    /**
     * Returns current valid credentials.
     * May return a cached value if credentials are still fresh.
     * Blocks briefly if a refresh is in progress.
     *
     * @throws ReplicationCredentialException if credentials cannot be obtained
     */
    ReplicationCredentials getCredentials() throws ReplicationCredentialException;

    /**
     * Returns a human-readable description of this provider for logging.
     * Example: "InstanceProfile(role=doris-beijing-role)"
     */
    String describe();
}
