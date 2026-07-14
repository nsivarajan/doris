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
 * HTTP endpoints for the replication group feature.
 * Used by the replication-manager CLI tool to query status
 * and orchestrate failover/failback operations.
 *
 * All endpoints return JSON and require admin authentication.
 * All endpoints are no-ops when enable_replication_group = false.
 */
@RestController
public class ReplicationAction extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(ReplicationAction.class);

    // shared exporter reference — set by Env.java when the thread is started
    private static volatile EditLogS3Exporter activeExporter = null;
    private static volatile Thread exporterThread = null;

    /** Called by Env.java after starting the exporter thread. */
    public static void registerExporter(EditLogS3Exporter exporter, Thread thread) {
        activeExporter = exporter;
        exporterThread = thread;
    }

    // ── GET /api/replication/status ──────────────────────────────────────────

    /** Returns current exporter state: last exported journal_id, running flag, site name. */
    @RequestMapping(path = "/api/replication/status", method = RequestMethod.GET)
    public Object status(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        Map<String, Object> result = new HashMap<>();
        result.put("enable_replication_group", Config.enable_replication_group);
        result.put("dr_read_only_mode", Config.dr_read_only_mode);
        result.put("site_name", Config.replication_site_name);
        result.put("group_id", Config.replication_group_id);
        if (activeExporter != null && exporterThread != null) {
            result.put("exporter_running", exporterThread.isAlive());
            result.put("last_exported_journal_id", activeExporter.getLastExportedJournalId());
        } else {
            result.put("exporter_running", false);
            result.put("last_exported_journal_id", -1);
        }
        return ResponseEntityBuilder.ok(result);
    }

    // ── GET /api/replication/cursor ──────────────────────────────────────────

    /** Returns the CURSOR value from the replication bucket — the DR FE's progress. */
    @RequestMapping(path = "/api/replication/cursor", method = RequestMethod.GET)
    public Object cursor(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        if (!Config.enable_replication_group) {
            return ResponseEntityBuilder.ok(Map.of("last_exported_journal_id", -1));
        }
        long journalId = activeExporter != null ? activeExporter.getLastExportedJournalId() : -1;
        return ResponseEntityBuilder.ok(Map.of(
                "last_exported_journal_id", journalId,
                "site_name", Config.replication_site_name));
    }

    // ── POST /api/replication/pause-export ───────────────────────────────────

    /** Pauses the EditLogS3Exporter thread — called before failover to prevent split-brain. */
    @RequestMapping(path = "/api/replication/pause-export", method = RequestMethod.POST)
    public Object pauseExport(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        if (activeExporter != null) {
            activeExporter.stop();
            LOG.info("[Replication] EditLogS3Exporter paused via HTTP request");
        }
        return ResponseEntityBuilder.ok(Map.of("status", "paused"));
    }

    // ── POST /api/replication/promote-master ─────────────────────────────────

    /**
     * Promotes this DR FE to master:
     *   1. Lifts dr_read_only_mode so writes are accepted
     *   2. Starts EditLogS3Exporter on the now-primary FE
     * The caller (failover command) must have already restored FDB and remapped vaults
     * before invoking this endpoint.
     */
    @RequestMapping(path = "/api/replication/promote-master", method = RequestMethod.POST)
    public Object promoteMaster(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        if (!Config.enable_replication_group) {
            return ResponseEntityBuilder.badRequest("enable_replication_group is false");
        }
        // lift write guard — this FE is now primary
        Config.dr_read_only_mode = false;
        LOG.info("[Replication] dr_read_only_mode cleared — this FE is now primary");

        // start exporter so this new primary exports its EditLog
        if (activeExporter == null || (exporterThread != null && !exporterThread.isAlive())) {
            try {
                ReplicationConfig replConfig = ReplicationConfig.fromDorisConfig();
                ReplicationStorageBackend storage = ReplicationStorageFactory.create(replConfig);
                EditLogS3Exporter exporter = new EditLogS3Exporter(
                        org.apache.doris.catalog.Env.getCurrentEnv().getEditLog(),
                        storage, replConfig);
                Thread thread = new Thread(exporter, "edit-log-s3-exporter");
                thread.setDaemon(true);
                thread.start();
                registerExporter(exporter, thread);
                LOG.info("[Replication] EditLogS3Exporter started on promoted master");
            } catch (Exception e) {
                LOG.error("[Replication] Failed to start exporter after promote: {}", e.getMessage(), e);
                return ResponseEntityBuilder.internalError(e.getMessage());
            }
        }
        return ResponseEntityBuilder.ok(Map.of("status", "promoted", "site", Config.replication_site_name));
    }

    // ── POST /api/replication/enter-dr-mode ──────────────────────────────────

    /**
     * Transitions this primary FE back to DR reader mode (used during failback).
     * Stops the exporter and sets dr_read_only_mode = true.
     */
    @RequestMapping(path = "/api/replication/enter-dr-mode", method = RequestMethod.POST)
    public Object enterDrMode(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        if (activeExporter != null) {
            activeExporter.stop();
        }
        Config.dr_read_only_mode = true;
        LOG.info("[Replication] Entered DR mode — writes rejected, exporter stopped");
        return ResponseEntityBuilder.ok(Map.of("status", "dr-mode"));
    }
}
