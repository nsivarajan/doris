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

package org.apache.doris.dr;

import org.apache.doris.common.Config;

import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.List;

/**
 * Immutable DR configuration loaded from fe.conf at startup.
 * All dr.* properties are read here — no other class reads Config directly
 * for DR settings, so the config surface is limited to this file.
 */
public class DRConfig {

    public enum StorageType { OSS, S3 }

    public enum CredentialType { AK_SK, INSTANCE_PROFILE, ASSUME_ROLE }

    // ── identity ──────────────────────────────────────────────────────────
    public final boolean enabled;
    public final DRState initialMode;   // ACTIVE or STANDBY
    public final String groupId;
    public final String siteName;

    // ── relay storage ─────────────────────────────────────────────────────
    public final StorageType storageType;
    public final String relayEndpoint;
    public final String relayBucket;
    public final String relayPrefix;    // key prefix inside bucket

    // ── credentials ───────────────────────────────────────────────────────
    public final CredentialType credentialType;
    public final String accessKey;
    public final String secretKey;
    public final String roleArn;
    public final String roleSessionName;

    // ── tuning ────────────────────────────────────────────────────────────
    public final int exportBatchSize;       // journal entries per segment file
    public final int exportIntervalMs;      // how often to flush a batch
    public final int checkpointIntervalMs;  // how often to write checkpoint.json
    public final int leaseTtlMs;            // primary.lease expiry window
    public final int crrMaxLagMs;           // assumed max OSS CRR lag
    public final int consumePollIntervalMs; // DR consumer poll frequency
    public final int lagAlertMs;            // alert threshold for BDBJE lag

    // ── FDB ───────────────────────────────────────────────────────────────
    public final String fdbClusterFile;     // path to fdb.cluster

    // ── vault mappings ────────────────────────────────────────────────────
    public final List<VaultMapping> vaultMappings;

    /** Per-vault endpoint+bucket remapping for the DR site. */
    public static class VaultMapping {
        public final String vaultName;
        public final String primaryEndpoint;
        public final String primaryBucket;
        public final String drEndpoint;
        public final String drBucket;

        public VaultMapping(String vaultName,
                String primaryEndpoint, String primaryBucket,
                String drEndpoint, String drBucket) {
            this.vaultName = vaultName;
            this.primaryEndpoint = primaryEndpoint;
            this.primaryBucket = primaryBucket;
            this.drEndpoint = drEndpoint;
            this.drBucket = drBucket;
        }
    }

    private DRConfig(Builder b) {
        this.enabled = b.enabled;
        this.initialMode = b.initialMode;
        this.groupId = b.groupId;
        this.siteName = b.siteName;
        this.storageType = b.storageType;
        this.relayEndpoint = b.relayEndpoint;
        this.relayBucket = b.relayBucket;
        this.relayPrefix = b.relayPrefix;
        this.credentialType = b.credentialType;
        this.accessKey = b.accessKey;
        this.secretKey = b.secretKey;
        this.roleArn = b.roleArn;
        this.roleSessionName = b.roleSessionName;
        this.exportBatchSize = b.exportBatchSize;
        this.exportIntervalMs = b.exportIntervalMs;
        this.checkpointIntervalMs = b.checkpointIntervalMs;
        this.leaseTtlMs = b.leaseTtlMs;
        this.crrMaxLagMs = b.crrMaxLagMs;
        this.consumePollIntervalMs = b.consumePollIntervalMs;
        this.lagAlertMs = b.lagAlertMs;
        this.fdbClusterFile = b.fdbClusterFile;
        this.vaultMappings = b.vaultMappings;
    }

    /**
     * Load DR configuration from fe.conf via Doris Config fields.
     * Returns a disabled config if dr.enabled=false.
     */
    public static DRConfig load() {
        if (!Config.dr_enabled) {
            return new Builder().enabled(false).build();
        }

        Builder b = new Builder()
                .enabled(true)
                .initialMode(parseMode(Config.dr_mode))
                .groupId(Config.dr_group_id)
                .siteName(Config.dr_site_name)
                .storageType(StorageType.valueOf(Config.dr_relay_type.trim().toUpperCase()))
                .relayEndpoint(Config.dr_relay_endpoint)
                .relayBucket(Config.dr_relay_bucket)
                .relayPrefix(Config.dr_relay_prefix)
                .credentialType(CredentialType.valueOf(
                        Config.dr_relay_credential_type.trim().toUpperCase()))
                .accessKey(Config.dr_relay_access_key)
                .secretKey(Config.dr_relay_secret_key)
                .roleArn(Config.dr_relay_role_arn)
                .roleSessionName(Config.dr_relay_role_session_name)
                .exportBatchSize(Config.dr_export_batch_size)
                .exportIntervalMs(Config.dr_export_interval_ms)
                .checkpointIntervalMs(Config.dr_checkpoint_interval_ms)
                .leaseTtlMs(Config.dr_lease_ttl_ms)
                .crrMaxLagMs(Config.dr_crr_max_lag_ms)
                .consumePollIntervalMs(Config.dr_consume_poll_interval_ms)
                .lagAlertMs(Config.dr_lag_alert_ms)
                .fdbClusterFile(Config.dr_fdb_cluster_file)
                .vaultMappings(parseVaultMappings(Config.dr_vault_mappings));

        DRConfig cfg = b.build();
        cfg.validate();
        return cfg;
    }

    /** Validates required fields are present when DR is enabled. */
    private void validate() {
        if (Strings.isNullOrEmpty(groupId)) {
            throw new IllegalArgumentException("dr.group_id must be set when dr.enabled=true");
        }
        if (Strings.isNullOrEmpty(siteName)) {
            throw new IllegalArgumentException("dr.site_name must be set when dr.enabled=true");
        }
        if (Strings.isNullOrEmpty(relayEndpoint)) {
            throw new IllegalArgumentException("dr.relay.endpoint must be set when dr.enabled=true");
        }
        if (Strings.isNullOrEmpty(relayBucket)) {
            throw new IllegalArgumentException("dr.relay.bucket must be set when dr.enabled=true");
        }
        if (Strings.isNullOrEmpty(fdbClusterFile)) {
            throw new IllegalArgumentException(
                    "dr.fdb.cluster_file must be set when dr.enabled=true. "
                    + "Example: /etc/foundationdb/fdb.cluster");
        }
    }

    private static DRState parseMode(String mode) {
        // H11 fix: trim() before toUpperCase() so trailing whitespace in fe.conf doesn't crash FE
        switch (mode.trim().toUpperCase()) {
            case "ACTIVE":  return DRState.ACTIVE;
            case "STANDBY": return DRState.STANDBY;
            default:
                throw new IllegalArgumentException(
                        "dr.mode must be ACTIVE or STANDBY, got: '" + mode + "'");
        }
    }

    /**
     * M4 fix: use pipe '|' as the inner delimiter between name, endpoints, and buckets
     * so that endpoint URLs containing ':' (e.g. https://host:port) are parsed correctly.
     *
     * Format: vaultName|primaryEndpoint|primaryBucket|drEndpoint|drBucket,...
     * Example: default_vault|oss-cn-hz.aliyuncs.com|prod-bucket|oss-cn-bj.aliyuncs.com|dr-bucket
     */
    private static List<VaultMapping> parseVaultMappings(String raw) {
        List<VaultMapping> result = new ArrayList<>();
        if (Strings.isNullOrEmpty(raw)) {
            return result;
        }
        for (String entry : raw.split(",")) {
            String[] parts = entry.trim().split("\\|", 5);
            if (parts.length != 5) {
                throw new IllegalArgumentException(
                        "Invalid dr.vault_mappings entry (expected 5 pipe-separated fields): '"
                                + entry + "'. Format: name|primaryEndpoint|primaryBucket|drEndpoint|drBucket");
            }
            result.add(new VaultMapping(
                    parts[0].trim(), parts[1].trim(), parts[2].trim(),
                    parts[3].trim(), parts[4].trim()));
        }
        return result;
    }

    // ── builder ───────────────────────────────────────────────────────────

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        boolean enabled = false;
        DRState initialMode = DRState.STANDBY;
        String groupId = "";
        String siteName = "";
        StorageType storageType = StorageType.OSS;
        String relayEndpoint = "";
        String relayBucket = "";
        String relayPrefix = "";
        CredentialType credentialType = CredentialType.INSTANCE_PROFILE;
        String accessKey = "";
        String secretKey = "";
        String roleArn = "";
        String roleSessionName = "doris-dr";
        int exportBatchSize = 500;
        int exportIntervalMs = 5000;
        int checkpointIntervalMs = 30000;
        int leaseTtlMs = 60000;
        int crrMaxLagMs = 900000;
        int consumePollIntervalMs = 1000;
        int lagAlertMs = 300000;
        String fdbClusterFile = "/etc/foundationdb/fdb.cluster";
        List<VaultMapping> vaultMappings = new ArrayList<>();

        public Builder enabled(boolean v) {
            this.enabled = v;
            return this;
        }

        public Builder initialMode(DRState v) {
            this.initialMode = v;
            return this;
        }

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

        public Builder relayEndpoint(String v) {
            this.relayEndpoint = v;
            return this;
        }

        public Builder relayBucket(String v) {
            this.relayBucket = v;
            return this;
        }

        public Builder relayPrefix(String v) {
            this.relayPrefix = v;
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

        public Builder exportBatchSize(int v) {
            this.exportBatchSize = v;
            return this;
        }

        public Builder exportIntervalMs(int v) {
            this.exportIntervalMs = v;
            return this;
        }

        public Builder checkpointIntervalMs(int v) {
            this.checkpointIntervalMs = v;
            return this;
        }

        public Builder leaseTtlMs(int v) {
            this.leaseTtlMs = v;
            return this;
        }

        public Builder crrMaxLagMs(int v) {
            this.crrMaxLagMs = v;
            return this;
        }

        public Builder consumePollIntervalMs(int v) {
            this.consumePollIntervalMs = v;
            return this;
        }

        public Builder lagAlertMs(int v) {
            this.lagAlertMs = v;
            return this;
        }

        public Builder fdbClusterFile(String v) {
            this.fdbClusterFile = v;
            return this;
        }

        public Builder vaultMappings(List<VaultMapping> v) {
            this.vaultMappings = v;
            return this;
        }

        public DRConfig build() {
            return new DRConfig(this);
        }
    }
}
