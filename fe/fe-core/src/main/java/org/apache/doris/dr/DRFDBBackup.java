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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Wraps the fdbbackup and fdbrestore CLI tools for DR use.
 *
 * FDB is not a managed service here — we drive it via subprocess calls.
 * All methods are synchronous and block until the CLI exits, except
 * waitForComplete() which polls the status until completion or timeout.
 *
 * The destUrl follows the fdbbackup blobstore URL format:
 *   blobstore://<endpoint>/<path>?bucket=<bucket>&region=<region>
 *
 * Example:
 *   blobstore://oss-cn-hangzhou.aliyuncs.com/dr-group/fdb-backup?bucket=doris-dr-relay
 */
public class DRFDBBackup {

    private static final Logger LOG = LogManager.getLogger(DRFDBBackup.class);

    private static final int PROC_TIMEOUT_SECONDS = 120;
    private static final int RESTORE_POLL_INTERVAL_MS = 5000;

    private final String clusterFile;   // path to fdb.cluster
    private final String destUrl;       // blobstore URL for the backup destination

    public DRFDBBackup(String clusterFile, String destUrl) {
        this.clusterFile = clusterFile;
        this.destUrl = destUrl;
    }

    // ── backup lifecycle ──────────────────────────────────────────────────

    /**
     * Starts continuous fdbbackup to the relay OSS bucket.
     * Called once at primary startup when dr.enabled=true.
     */
    public void startBackup() throws IOException, InterruptedException {
        LOG.info("[DR:FDB] starting continuous backup dest={}", destUrl);
        run("fdbbackup", "start",
                "--dest-url", destUrl,
                "--cluster-file", clusterFile);
        LOG.info("[DR:FDB] continuous backup started");
    }

    /**
     * Pauses the continuous backup and creates a consistent snapshot.
     * Returns the backup tag (timestamp-based) that identifies this snapshot
     * and is used as the fdb_versionstamp reference for DRCheckpoint.
     *
     * Called during planned switchover to get a deterministic cut-point.
     */
    public String pauseAndSnapshot() throws IOException, InterruptedException {
        LOG.info("[DR:FDB] pausing backup for consistent snapshot");
        run("fdbbackup", "pause",
                "--cluster-file", clusterFile);

        // read the status to get the backup tag / latest version
        String status = runAndCapture("fdbbackup", "status",
                "--cluster-file", clusterFile);
        String tag = parseBackupTag(status);
        LOG.info("[DR:FDB] snapshot created tag={}", tag);
        return tag;
    }

    /** Resumes continuous backup after a pauseAndSnapshot(). */
    public void resumeBackup() throws IOException, InterruptedException {
        LOG.info("[DR:FDB] resuming continuous backup");
        run("fdbbackup", "resume",
                "--cluster-file", clusterFile);
        LOG.info("[DR:FDB] backup resumed");
    }

    // ── restore ───────────────────────────────────────────────────────────

    /**
     * Restores the FDB cluster from the backup at the given versionstamp.
     * Blocks until fdbrestore start returns (restore runs asynchronously in FDB).
     * Use waitForRestoreComplete() to poll until done.
     *
     * @param versionstamp the FDB versionstamp from DRCheckpoint.fdbVersionstamp
     */
    public void restore(String versionstamp) throws IOException, InterruptedException {
        LOG.info("[DR:FDB] restoring cluster from backup versionstamp={}", versionstamp);
        run("fdbrestore", "start",
                "--source-url", destUrl,
                "--cluster-file", clusterFile,
                "--restore-version", versionstamp);
        LOG.info("[DR:FDB] restore initiated, polling for completion");
    }

    /**
     * Restores from the latest available backup (used for unplanned failover
     * where we don't have a specific versionstamp).
     */
    public void restoreLatest() throws IOException, InterruptedException {
        LOG.info("[DR:FDB] restoring cluster from latest backup");
        run("fdbrestore", "start",
                "--source-url", destUrl,
                "--cluster-file", clusterFile);
        LOG.info("[DR:FDB] restore from latest initiated");
    }

    /**
     * Polls fdbrestore status until the restore is complete or timeout expires.
     *
     * @param timeoutSeconds max time to wait
     * @throws IllegalStateException if restore fails or times out
     */
    public void waitForRestoreComplete(int timeoutSeconds)
            throws IOException, InterruptedException {
        LOG.info("[DR:FDB] waiting for restore to complete (timeout={}s)", timeoutSeconds);
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;

        while (System.currentTimeMillis() < deadline) {
            String status = runAndCapture("fdbrestore", "status",
                    "--cluster-file", clusterFile);
            if (status.contains("Completed")) {
                LOG.info("[DR:FDB] restore completed successfully");
                return;
            }
            if (status.contains("Error") || status.contains("Failed")) {
                throw new IllegalStateException(
                        "FDB restore failed. Status output: " + status);
            }
            LOG.debug("[DR:FDB] restore in progress...");
            Thread.sleep(RESTORE_POLL_INTERVAL_MS);
        }
        throw new IllegalStateException(
                "FDB restore did not complete within " + timeoutSeconds + " seconds");
    }

    // ── status ────────────────────────────────────────────────────────────

    /**
     * Returns true if a continuous backup is currently running.
     * Used by dr-tool.sh status to verify the backup is healthy.
     */
    public boolean isBackupRunning() {
        try {
            String status = runAndCapture("fdbbackup", "status",
                    "--cluster-file", clusterFile);
            return status.contains("Running") || status.contains("running");
        } catch (Exception e) {
            LOG.warn("[DR:FDB] could not get backup status: {}", e.getMessage());
            return false;
        }
    }

    // ── subprocess helpers ────────────────────────────────────────────────

    /**
     * Runs a CLI command, waits for it to exit, and throws if exit code != 0.
     */
    private void run(String... command) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        Process proc = pb.start();
        String output = readOutput(proc);
        boolean finished = proc.waitFor(PROC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (!finished) {
            proc.destroyForcibly();
            throw new IOException("Command timed out after " + PROC_TIMEOUT_SECONDS
                    + "s: " + String.join(" ", command));
        }
        int exitCode = proc.exitValue();
        if (exitCode != 0) {
            throw new IOException("Command failed (exit " + exitCode + "): "
                    + String.join(" ", command) + "\nOutput: " + output);
        }
        LOG.debug("[DR:FDB] command output: {}", output);
    }

    /**
     * Runs a CLI command and returns its stdout as a String.
     */
    private String runAndCapture(String... command) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        Process proc = pb.start();
        String output = readOutput(proc);
        proc.waitFor(PROC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return output;
    }

    private String readOutput(Process proc) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(proc.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append('\n');
            }
        }
        return sb.toString();
    }

    /**
     * Parses the backup tag from fdbbackup status output.
     * Returns a timestamp string used to identify the snapshot.
     */
    private String parseBackupTag(String statusOutput) {
        // fdbbackup status output contains lines like "BackupID: dr-2024-01-15T10:35:12"
        // or "Tag: default" — we use the timestamp portion as the reference
        for (String line : statusOutput.split("\n")) {
            String trimmed = line.trim();
            if (trimmed.startsWith("BackupID:") || trimmed.startsWith("Tag:")) {
                String[] parts = trimmed.split(":", 2);
                if (parts.length == 2) {
                    return parts[1].trim();
                }
            }
        }
        // fallback: use current timestamp as the tag
        return "backup-" + System.currentTimeMillis();
    }
}
