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

/** Snapshot of DR state returned by GET /api/dr/status. */
public class DRStatus {
    public String site;
    public String groupId;
    public String state;
    public boolean drillMode;

    // ACTIVE side metrics
    public long lastExportedJournalId;
    public long primaryLeaseFreshMs;    // ms since lease was last renewed

    // STANDBY side metrics
    public long lagMs;                  // estimated BDBJE replication lag
    public long lagEntries;             // number of journal entries behind
    public long lastAppliedJournalId;
}
