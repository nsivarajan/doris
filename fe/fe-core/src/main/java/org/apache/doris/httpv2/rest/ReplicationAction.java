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

package org.apache.doris.httpv2.rest;

import org.apache.doris.common.Config;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.replication.EditLogS3Exporter;
import org.apache.doris.replication.ReplicationConfig;
import org.apache.doris.replication.storage.ReplicationStorageBackend;
import org.apache.doris.replication.storage.ReplicationStorageFactory;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * Read-only HTTP endpoints for the replication group feature.
 * Used for Prometheus metrics scraping and status monitoring.
 *
 * Mutation operations (failover, failback, mode changes) are performed
 * via SQL: ALTER SYSTEM REPLICATION ... / SHOW REPLICATION GROUP ...
 */
@RestController
public class ReplicationAction extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(ReplicationAction.class);

    private static volatile EditLogS3Exporter activeExporter = null;
    private static volatile Thread exporterThread = null;

    /** Called by Env.java after starting the exporter thread. */
    public static void registerExporter(EditLogS3Exporter exporter, Thread thread) {
        activeExporter = exporter;
        exporterThread = thread;
    }

    /** Returns true if the EditLogS3Exporter thread is currently alive. */
    public static boolean getExporterRunning() {
        return activeExporter != null && exporterThread != null && exporterThread.isAlive();
    }

    /** Returns the last journal_id successfully exported, or -1 if not running. */
    public static long getLastExportedJournalId() {
        return activeExporter != null ? activeExporter.getLastExportedJournalId() : -1L;
    }

    // ── Static methods called by SQL command handlers (ReplicationCommandHandler) ─────

    /** Called by ALTER SYSTEM REPLICATION PAUSE EXPORT */
    public static void doPauseExport() {
        if (activeExporter != null) {
            activeExporter.stop();
            LOG.info("[Replication] exporter paused");
        }
    }

    /** Called by ALTER SYSTEM REPLICATION PROMOTE MASTER */
    public static void doPromoteMaster() {
        Config.dr_read_only_mode = false;
        LOG.info("[Replication] dr_read_only_mode cleared — this FE is now primary");
        if (activeExporter == null || (exporterThread != null && !exporterThread.isAlive())) {
            try {
                ReplicationConfig replConfig = ReplicationConfig.fromDorisConfig();
                ReplicationStorageBackend storage = ReplicationStorageFactory.create(replConfig);
                EditLogS3Exporter exporter = new EditLogS3Exporter(
                        org.apache.doris.catalog.Env.getCurrentEnv().getEditLog().getJournal(),
                        storage, replConfig);
                Thread thread = new Thread(exporter, "edit-log-s3-exporter");
                thread.setDaemon(true);
                thread.start();
                registerExporter(exporter, thread);
                LOG.info("[Replication] exporter started on promoted master");
            } catch (Exception e) {
                LOG.error("[Replication] failed to start exporter after promote: {}", e.getMessage(), e);
            }
        }
    }

    /** Called by ALTER SYSTEM REPLICATION ENTER DR MODE */
    public static void doEnterDrMode() {
        if (activeExporter != null) {
            activeExporter.stop();
        }
        Config.dr_read_only_mode = true;
        LOG.info("[Replication] entered DR mode — writes rejected, exporter stopped");
    }

    /**
     * Called by ALTER SYSTEM REPLICATION ENTER/EXIT DRILL MODE.
     * enter=true lifts write guard without starting exporter (primary bucket safe).
     * enter=false restores write guard.
     */
    public static void doDrillMode(boolean enter) {
        Config.dr_read_only_mode = !enter;
        LOG.info("[Replication] drill mode {}: write guard {}",
                enter ? "activated" : "deactivated",
                enter ? "lifted" : "restored");
    }

    // ── Read-only HTTP endpoints (Prometheus scraping / monitoring) ─────────────

    /** Returns current replication state for monitoring. */
    @RequestMapping(path = "/api/replication/status", method = RequestMethod.GET)
    public Object status(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        Map<String, Object> result = new HashMap<>();
        result.put("enable_replication_group", Config.enable_replication_group);
        result.put("dr_read_only_mode", Config.dr_read_only_mode);
        result.put("site_name", Config.replication_site_name);
        result.put("group_id", Config.replication_group_id);
        result.put("exporter_running", getExporterRunning());
        result.put("last_exported_journal_id", getLastExportedJournalId());
        return ResponseEntityBuilder.ok(result);
    }

    /** Returns the current export cursor (journal_id + site). */
    @RequestMapping(path = "/api/replication/cursor", method = RequestMethod.GET)
    public Object cursor(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        return ResponseEntityBuilder.ok(Map.of(
                "last_exported_journal_id", getLastExportedJournalId(),
                "site_name", Config.replication_site_name));
    }

    /**
     * Returns replication metrics in Prometheus text format.
     * Scrape with: job="doris_replication", endpoint="/api/replication/metrics"
     */
    @RequestMapping(path = "/api/replication/metrics", method = RequestMethod.GET)
    public Object metrics(HttpServletRequest request, HttpServletResponse response) {
        String group = Config.replication_group_id.isEmpty() ? "default" : Config.replication_group_id;
        String site  = Config.replication_site_name.isEmpty() ? "unknown" : Config.replication_site_name;
        String labels = String.format("{group=\"%s\",site=\"%s\"}", group, site);

        StringBuilder sb = new StringBuilder();
        sb.append("# HELP doris_replication_feature_enabled Whether the replication group feature is enabled\n");
        sb.append("# TYPE doris_replication_feature_enabled gauge\n");
        sb.append("doris_replication_feature_enabled").append(labels)
          .append(" ").append(Config.enable_replication_group ? 1 : 0).append("\n");

        sb.append("# HELP doris_replication_exporter_running Whether the EditLogS3Exporter thread is alive\n");
        sb.append("# TYPE doris_replication_exporter_running gauge\n");
        sb.append("doris_replication_exporter_running").append(labels)
          .append(" ").append(getExporterRunning() ? 1 : 0).append("\n");

        sb.append("# HELP doris_replication_last_journal_id Last EditLog journal_id exported to bucket\n");
        sb.append("# TYPE doris_replication_last_journal_id gauge\n");
        sb.append("doris_replication_last_journal_id").append(labels)
          .append(" ").append(getLastExportedJournalId()).append("\n");

        sb.append("# HELP doris_replication_dr_read_only"
                + " Whether this FE is in DR read-only mode (write guard active)\n");
        sb.append("# TYPE doris_replication_dr_read_only gauge\n");
        sb.append("doris_replication_dr_read_only").append(labels)
          .append(" ").append(Config.dr_read_only_mode ? 1 : 0).append("\n");

        try {
            response.setContentType("text/plain; version=0.0.4; charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(sb.toString());
            response.getWriter().flush();
        } catch (Exception e) {
            LOG.warn("[Replication] failed to write metrics response: {}", e.getMessage());
        }
        return null;
    }
}
