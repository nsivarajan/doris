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
 * Lifecycle (H1 fix — env is wired before applyState):
 *   1. DorisFE.java calls onStartup() after waitForReady().
 *      onStartup() captures Env.getCurrentEnv() directly — EditLog is ready
 *      at that point because waitForReady() guarantees it.
 *   2. Env.java calls register(env) — kept for safety but threads already
 *      started in onStartup() with the captured env reference.
 *   3. If dr.enabled=false the instance stays INACTIVE — zero overhead.
 */
public class DRManager {

    private static final Logger LOG = LogManager.getLogger(DRManager.class);

    private static volatile DRManager instance;

    private final DRConfig config;
    private final AtomicReference<DRState> state;

    // volatile so register() and getStatus() see the latest value (M1 fix)
    private volatile Env env;
    private DRStorageBackend storage;
    private DRExporter exporter;
    private DRConsumer consumer;

    private DRManager(DRConfig config) {
        this.config = config;
        this.state = new AtomicReference<>(
                config.enabled ? config.initialMode : DRState.INACTIVE);
    }

    public static DRManager get() {
        return instance;
    }

    // ── startup hooks ─────────────────────────────────────────────────────

    /**
     * Called from DorisFE.java after waitForReady().
     * H1 fix: wire Env immediately before applyState() so threads can start.
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

        // wire Env before starting threads — EditLog is ready after waitForReady()
        instance.env = Env.getCurrentEnv();

        instance.applyState(cfg.initialMode);
    }

    /** Called from Env.java — secondary registration for post-init state changes. */
    public static void register(Env env) {
        if (instance == null || !instance.config.enabled) {
            return;
        }
        instance.env = env;
    }

    // ── state management ──────────────────────────────────────────────────

    public DRState getState() {
        return state.get();
    }

    public synchronized void setState(DRState newState) {
        DRState current = state.get();
        if (current == newState) {
            return;
        }
        LOG.info("[DR] state {} → {}", current, newState);
        stopThreads();
        state.set(newState);
        applyState(newState);
    }

    // ── write guard (called from StmtExecutor AFTER redirect check) ───────

    /**
     * H2/H3 fix: guard is intentionally allow-on-null (before onStartup).
     * Must be called AFTER redirect resolution in StmtExecutor so forwarded
     * writes are not rejected on STANDBY followers.
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

    // ── status ────────────────────────────────────────────────────────────

    public DRStatus getStatus() {
        DRStatus s = new DRStatus();
        s.site = config.siteName;
        s.groupId = config.groupId;
        s.state = state.get().name();
        s.drillMode = (state.get() == DRState.DRILL);

        // M1 fix: capture local references to avoid null race with stopThreads()
        DRExporter exp = exporter;
        DRConsumer con = consumer;
        if (exp != null) {
            s.lastExportedJournalId = exp.getLastExportedJournalId();
            s.primaryLeaseFreshMs = exp.getLeaseFreshMs();
        }
        if (con != null) {
            s.lagMs = con.getLagMs();
            s.lagEntries = con.getLagEntries();
            s.lastAppliedJournalId = con.getLastAppliedJournalId();
        }
        return s;
    }

    // ── private helpers ───────────────────────────────────────────────────

    private synchronized void applyState(DRState newState) {
        Env localEnv = this.env;
        if (newState.shouldExport() && localEnv != null) {
            exporter = new DRExporter(localEnv.getEditLog(), storage, config);
            Thread t = new Thread(exporter, "dr-exporter");
            t.setDaemon(true);
            t.start();
            LOG.info("[DR] exporter thread started");
        }
        if (newState.shouldConsume() && localEnv != null) {
            consumer = new DRConsumer(localEnv, storage, config);
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
                return new AssumeRoleCredentialProvider(cfg.roleArn, cfg.roleSessionName);
            default:
                throw new IllegalArgumentException(
                        "Unknown credential type: " + cfg.credentialType);
        }
    }

    public DRConfig getConfig() { return config; }
    public DRExporter getExporter() { return exporter; }
    public DRConsumer getConsumer() { return consumer; }
}
