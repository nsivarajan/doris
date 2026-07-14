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
import org.apache.doris.replication.credentials.AssumeRoleCredentialProvider;
import org.apache.doris.replication.credentials.InstanceProfileCredentialProvider;
import org.apache.doris.replication.credentials.ReplicationCredentialProvider;
import org.apache.doris.replication.credentials.StaticCredentialProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Creates the correct ReplicationStorageBackend + ReplicationCredentialProvider
 * pair from config. The credential provider is resolved first and injected
 * into the storage backend so credentials are always current.
 */
public class ReplicationStorageFactory {

    private static final Logger LOG = LogManager.getLogger(ReplicationStorageFactory.class);

    /** Creates a fully configured storage backend from the given config. */
    public static ReplicationStorageBackend create(ReplicationConfig config) {
        ReplicationCredentialProvider credProvider = buildCredentialProvider(config);
        ReplicationStorageBackend backend = buildStorageBackend(config, credProvider);
        LOG.info("[Replication] Storage backend created: type={} credentials={}",
                config.storageType, credProvider.describe());
        return backend;
    }

    /** Selects and builds the credential provider from config. */
    private static ReplicationCredentialProvider buildCredentialProvider(ReplicationConfig config) {
        switch (config.credentialType) {
            case INSTANCE_PROFILE:
                return new InstanceProfileCredentialProvider(config.storageType);
            case ASSUME_ROLE:
                if (config.roleArn == null || config.roleArn.isEmpty()) {
                    throw new IllegalArgumentException(
                            "replication_role_arn must be set when credential_type=assume_role");
                }
                return new AssumeRoleCredentialProvider(config);
            case AK_SK:
                // StaticCredentialProvider logs a WARNING — intentional for prod misuse detection
                return new StaticCredentialProvider(config.accessKey, config.secretKey);
            default:
                throw new IllegalArgumentException(
                        "Unsupported credential type: " + config.credentialType);
        }
    }

    /** Creates the storage backend for the configured cloud provider. */
    private static ReplicationStorageBackend buildStorageBackend(
            ReplicationConfig config, ReplicationCredentialProvider credProvider) {
        switch (config.storageType) {
            case OSS:
                return new OSSReplicationStorage(config, credProvider);
            case S3:
                return new S3ReplicationStorage(config, credProvider);
            default:
                throw new IllegalArgumentException(
                        "Unsupported storage type: " + config.storageType
                        + ". Supported: S3, OSS");
        }
    }

    private ReplicationStorageFactory() {}
}
