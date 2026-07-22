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

/**
 * Lifecycle states of this cluster in the DR group.
 *
 * INACTIVE  — dr.enabled=false; feature is dormant, zero overhead.
 * ACTIVE    — this cluster is the primary; exporter thread running,
 *             primary.lease renewed, all writes accepted.
 * STANDBY   — this cluster is the DR replica; consumer thread running,
 *             all DML/DDL writes are rejected at StmtExecutor level.
 * SWITCHING — transitional state during planned switchover; writes
 *             are blocked on both sides until promotion completes.
 * DRILL     — isolated DR test; consumer paused, primary unaffected.
 *             State is discarded when drill ends (restore from backup).
 */
public enum DRState {
    INACTIVE,
    ACTIVE,
    STANDBY,
    SWITCHING,
    DRILL;

    /** Returns true if this cluster may accept write operations. */
    public boolean isWriteAllowed() {
        return this == ACTIVE;
    }

    /** Returns true if the exporter thread should be running. */
    public boolean shouldExport() {
        return this == ACTIVE;
    }

    /** Returns true if the consumer thread should be running. */
    public boolean shouldConsume() {
        return this == STANDBY;
    }
}
