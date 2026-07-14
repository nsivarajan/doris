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
 * JSON-serializable snapshot of the replication consistent point.
 * Written to the bucket every checkpoint interval by EditLogS3Exporter.
 * Used by failover/failback to determine the safe restore timestamp.
 */
public class ReplicationCheckpoint {

    public String groupId;
    // FE EditLog journal ID at the checkpoint time
    public long feJournalId;
    // FDB versionstamp hex string from CloudMetaSyncPoint
    public String fdbVersionstamp;
    // wall-clock milliseconds when this checkpoint was written
    public long sampledAtMs;
    // OSS data is guaranteed consistent up to this timestamp:
    // sampledAtMs - crrMaxLagMs
    public long ossSafeBeforeMs;
    // which site is currently primary
    public String primarySite;
    public String createdAt;

    // default constructor for JSON deserialisation
    public ReplicationCheckpoint() {}

    public ReplicationCheckpoint(String groupId, long feJournalId,
            String fdbVersionstamp, long sampledAtMs, long ossSafeBeforeMs,
            String primarySite, String createdAt) {
        this.groupId = groupId;
        this.feJournalId = feJournalId;
        this.fdbVersionstamp = fdbVersionstamp;
        this.sampledAtMs = sampledAtMs;
        this.ossSafeBeforeMs = ossSafeBeforeMs;
        this.primarySite = primarySite;
        this.createdAt = createdAt;
    }
}
