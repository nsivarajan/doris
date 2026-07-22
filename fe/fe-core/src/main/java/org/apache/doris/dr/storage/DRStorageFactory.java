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

import org.apache.doris.dr.DRConfig;
import org.apache.doris.dr.credentials.DRCredentialProvider;

/**
 * Creates the correct DRStorageBackend from the given config and credential provider.
 * Credential provider is built by DRManager and injected here.
 */
public class DRStorageFactory {

    private DRStorageFactory() {}

    public static DRStorageBackend create(DRConfig config, DRCredentialProvider creds) {
        // M6 fix: validate required fields before creating backend
        if (config.relayBucket == null || config.relayBucket.isEmpty()) {
            throw new IllegalArgumentException("dr.relay.bucket must be set");
        }
        if (config.relayEndpoint == null || config.relayEndpoint.isEmpty()) {
            throw new IllegalArgumentException("dr.relay.endpoint must be set");
        }
        switch (config.storageType) {
            case OSS:
                return new OSSStorageBackend(
                        config.relayBucket, config.relayEndpoint, creds);
            case S3:
                return new S3StorageBackend(
                        config.relayBucket, config.relayEndpoint, creds);
            default:
                throw new IllegalArgumentException(
                        "Unsupported DR relay storage type: " + config.storageType
                        + ". Supported: OSS, S3");
        }
    }
}
