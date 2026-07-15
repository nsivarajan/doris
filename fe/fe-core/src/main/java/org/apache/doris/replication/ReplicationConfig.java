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

package org.apache.doris.replication;

/**
 * All configuration for one replication group, loaded from fe.conf.
 * Immutable after construction — built once at startup via fromDorisConfig().
 */
public class ReplicationConfig {

    public enum StorageType { S3, OSS, GCS }

    public enum CredentialType { INSTANCE_PROFILE, ASSUME_ROLE, WORKLOAD_IDENTITY, AK_SK }

    // ── identity ──────────────────────────────────────────────────────────
    public final String groupId;
    public final String siteName;

    // ── storage ───────────────────────────────────────────────────────────
    public final StorageType storageType;
    public final String bucket;
    public final String endpoint;

    // ── credentials ───────────────────────────────────────────────────────
    public final CredentialType credentialType;
    public final String accessKey;       // AK_SK only
    public final String secretKey;       // AK_SK only
    public final String roleArn;         // ASSUME_ROLE only
    public final String roleSessionName; // ASSUME_ROLE only
    public final String externalId;      // ASSUME_ROLE optional

    // ── tuning ────────────────────────────────────────────────────────────
    public final int exportIntervalMs;
    public final int exportBatchSize;
    public final int checkpointIntervalMs;
    public final int crrMaxLagMs;
    // early-refresh window: refresh credentials this many seconds before expiry
    public final int credentialRefreshWindowSeconds;

    private ReplicationConfig(Builder b) {
        this.groupId = b.groupId;
        this.siteName = b.siteName;
        this.storageType = b.storageType;
        this.bucket = b.bucket;
        this.endpoint = b.endpoint;
        this.credentialType = b.credentialType;
        this.accessKey = b.accessKey;
        this.secretKey = b.secretKey;
        this.roleArn = b.roleArn;
        this.roleSessionName = b.roleSessionName;
        this.externalId = b.externalId;
        this.exportIntervalMs = b.exportIntervalMs;
        this.exportBatchSize = b.exportBatchSize;
        this.checkpointIntervalMs = b.checkpointIntervalMs;
        this.crrMaxLagMs = b.crrMaxLagMs;
        this.credentialRefreshWindowSeconds = b.credentialRefreshWindowSeconds;
    }

    /** Build from Doris Config fields — called at FE startup. */
    public static ReplicationConfig fromDorisConfig() {
        return new Builder()
                .groupId(org.apache.doris.common.Config.replication_group_id)
                .siteName(org.apache.doris.common.Config.replication_site_name)
                .storageType(StorageType.valueOf(
                        org.apache.doris.common.Config.replication_storage_type.toUpperCase()))
                .bucket(org.apache.doris.common.Config.replication_bucket)
                .endpoint(org.apache.doris.common.Config.replication_endpoint)
                .credentialType(CredentialType.valueOf(
                        org.apache.doris.common.Config.replication_credential_type.toUpperCase()))
                .accessKey(org.apache.doris.common.Config.replication_access_key)
                .secretKey(org.apache.doris.common.Config.replication_secret_key)
                .roleArn(org.apache.doris.common.Config.replication_role_arn)
                .roleSessionName(org.apache.doris.common.Config.replication_role_session_name)
                .externalId(org.apache.doris.common.Config.replication_external_id)
                .exportIntervalMs(org.apache.doris.common.Config.replication_export_interval_ms)
                .exportBatchSize(org.apache.doris.common.Config.replication_export_batch_size)
                .checkpointIntervalMs(
                        org.apache.doris.common.Config.replication_checkpoint_interval_ms)
                .crrMaxLagMs(org.apache.doris.common.Config.replication_crr_max_lag_ms)
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String groupId = "default";
        private String siteName = "primary";
        private StorageType storageType = StorageType.OSS;
        private String bucket = "";
        private String endpoint = "";
        private CredentialType credentialType = CredentialType.INSTANCE_PROFILE;
        private String accessKey = "";
        private String secretKey = "";
        private String roleArn = "";
        private String roleSessionName = "doris-replication";
        private String externalId = "";
        private int exportIntervalMs = 5000;
        private int exportBatchSize = 500;
        private int checkpointIntervalMs = 30000;
        private int crrMaxLagMs = 300000;
        private int credentialRefreshWindowSeconds = 300; // refresh 5 min before expiry

        public Builder groupId(String v) {
            this.groupId = v;
            return this;
        }

        public Builder siteName(String v) {
            this.siteName = v;
            return this;
        }

        public Builder storageType(StorageType v) {
            this.storageType = v;
            return this;
        }

        public Builder bucket(String v) {
            this.bucket = v;
            return this;
        }

        public Builder endpoint(String v) {
            this.endpoint = v;
            return this;
        }

        public Builder credentialType(CredentialType v) {
            this.credentialType = v;
            return this;
        }

        public Builder accessKey(String v) {
            this.accessKey = v;
            return this;
        }

        public Builder secretKey(String v) {
            this.secretKey = v;
            return this;
        }

        public Builder roleArn(String v) {
            this.roleArn = v;
            return this;
        }

        public Builder roleSessionName(String v) {
            this.roleSessionName = v;
            return this;
        }

        public Builder externalId(String v) {
            this.externalId = v;
            return this;
        }

        public Builder exportIntervalMs(int v) {
            this.exportIntervalMs = v;
            return this;
        }

        public Builder exportBatchSize(int v) {
            this.exportBatchSize = v;
            return this;
        }

        public Builder checkpointIntervalMs(int v) {
            this.checkpointIntervalMs = v;
            return this;
        }

        public Builder crrMaxLagMs(int v) {
            this.crrMaxLagMs = v;
            return this;
        }

        public Builder credentialRefreshWindowSeconds(int v) {
            this.credentialRefreshWindowSeconds = v;
            return this;
        }

        public ReplicationConfig build() {
            return new ReplicationConfig(this);
        }
    }
}
