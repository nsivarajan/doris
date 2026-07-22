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

import org.apache.doris.catalog.Env;
import org.apache.doris.dr.credentials.AkSkCredentialProvider;
import org.apache.doris.dr.credentials.AssumeRoleCredentialProvider;
import org.apache.doris.dr.credentials.DRCredentialProvider;
import org.apache.doris.dr.credentials.InstanceProfileCredentialProvider;
import org.apache.doris.dr.storage.DRStorageBackend;
import org.apache.doris.dr.storage.DRStorageFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Central coordinator for the DR feature.
 *
 * Lifecycle:
 *   1. DorisFE.java calls onStartup() after FE is ready.
 *   2. Env.java calls register(env) once the catalog is initialized.
 *   3. If dr.enabled=false, the instance stays in INACTIVE state and
 *      all methods are no-ops — zero overhead for non-DR clusters.
 *
 * Thread safety: state transitions are serialized via synchronized methods.
 * The exporter and consumer threads are started/stopped here.
 */
public class DRManager {

    private static final Logger LOG = LogManager.getLogger(DRManager.class);

    // singleton — initialized once at startup, never replaced
    private static volatile DRManager instance;

    private final DRConfig config;
    private final AtomicReference<DRState> state;

    private Env env;                        // wired by register()
    private DRStorageBackend storage;       // relay OSS/S3 backend
    private DRExporter exporter;            // runs on ACTIVE FE
    private DRConsumer consumer;            // runs on STANDBY FE

    private DRManager(DRConfig config) {
        this.config = config;
        this.state = new AtomicReference<>(
                config.enabled ? config.initialMode : DRState.INACTIVE);
    }

    // ── singleton ─────────────────────────────────────────────────────────

    /** Returns the singleton instance. Always non-null after onStartup(). */
    public static DRManager get() {
        return instance;
    }

    // ── startup hooks (called from upstream files) ────────────────────────

    /**
     * Called from DorisFE.java after Env.getCurrentEnv().waitForReady().
     * Initializes the singleton, creates the storage backend, and starts
     * the appropriate thread based on dr.mode.
     */
    public static void onStartup() {
        DRConfig cfg = DRConfig.load();
        instance = new DRManager(cfg);

        if (!cfg.enabled) {
            LOG.info("[DR] dr.enabled=false — DR feature inactive");
            return;
        }

        LOG.info("[DR] starting group={} site={} mode={}",
                cfg.groupId, cfg.siteName, cfg.initialMode);

        DRCredentialProvider creds = buildCredentialProvider(cfg);
        instance.storage = DRStorageFactory.create(cfg, creds);

        // start the correct thread based on configured mode
        instance.applyState(cfg.initialMode);
    }

    /**
     * Called from Env.java at the end of initialize().
     * Wires the Env reference so the exporter/consumer can access the EditLog.
     */
    public static void register(Env env) {
        if (instance == null || !instance.config.enabled) {
            return;
        }
        instance.env = env;
        LOG.debug("[DR] Env registered");
    }

    // ── state management ──────────────────────────────────────────────────

    public DRState getState() {
        return state.get();
    }

    /**
     * Transitions to the new state, stopping/starting threads as needed.
     * Called by DrAction HTTP handlers (promote, demote, drill).
     */
    public synchronized void setState(DRState newState) {
        DRState current = state.get();
        if (current == newState) {
            return;
        }
        LOG.info("[DR] state transition {} → {}", current, newState);

        stopThreads();
        state.set(newState);
        applyState(newState);
    }

    // ── write guard (called from StmtExecutor) ────────────────────────────

    /**
     * Throws UserException if this cluster is not allowed to accept writes.
     * Called once per statement execution — must be fast.
     */
    public static void checkWrite(
            org.apache.doris.nereids.trees.plans.logical.LogicalPlan plan,
            org.apache.doris.qe.ConnectContext ctx)
            throws org.apache.doris.common.UserException {
        if (instance == null || instance.state.get().isWriteAllowed()) {
            return;
        }
        if (DRReadOnlyGuard.isReadOnlyStatement(plan)) {
            return;
        }
        throw new org.apache.doris.common.UserException(
                "This cluster is in STANDBY mode. "
                + "Connect to the primary cluster for write operations.");
    }

    // ── status (called by DrAction HTTP handler) ──────────────────────────

    public DRStatus getStatus() {
        DRStatus s = new DRStatus();
        s.site = config.siteName;
        s.groupId = config.groupId;
        s.state = state.get().name();
        s.drillMode = (state.get() == DRState.DRILL);

        if (exporter != null) {
            s.lastExportedJournalId = exporter.getLastExportedJournalId();
            s.primaryLeaseFreshMs = exporter.getLeaseFreshMs();
        }
        if (consumer != null) {
            s.lagMs = consumer.getLagMs();
            s.lagEntries = consumer.getLagEntries();
            s.lastAppliedJournalId = consumer.getLastAppliedJournalId();
        }
        return s;
    }

    // ── private helpers ───────────────────────────────────────────────────

    private synchronized void applyState(DRState newState) {
        if (newState.shouldExport() && env != null) {
            exporter = new DRExporter(env.getEditLog(), storage, config);
            Thread t = new Thread(exporter, "dr-exporter");
            t.setDaemon(true);
            t.start();
            LOG.info("[DR] exporter thread started");
        }

        if (newState.shouldConsume() && env != null) {
            consumer = new DRConsumer(env, storage, config);
            Thread t = new Thread(consumer, "dr-consumer");
            t.setDaemon(true);
            t.start();
            LOG.info("[DR] consumer thread started");
        }
    }

    private synchronized void stopThreads() {
        if (exporter != null) {
            exporter.stop();
            exporter = null;
        }
        if (consumer != null) {
            consumer.stop();
            consumer = null;
        }
    }

    private static DRCredentialProvider buildCredentialProvider(DRConfig cfg) {
        switch (cfg.credentialType) {
            case AK_SK:
                return new AkSkCredentialProvider(cfg.accessKey, cfg.secretKey);
            case INSTANCE_PROFILE:
                return new InstanceProfileCredentialProvider();
            case ASSUME_ROLE:
                return new AssumeRoleCredentialProvider(
                        cfg.roleArn, cfg.roleSessionName);
            default:
                throw new IllegalArgumentException(
                        "Unknown credential type: " + cfg.credentialType);
        }
    }

    // ── accessors for DR tool HTTP API ────────────────────────────────────

    public DRConfig getConfig() {
        return config;
    }

    public DRExporter getExporter() {
        return exporter;
    }

    public DRConsumer getConsumer() {
        return consumer;
    }
}
