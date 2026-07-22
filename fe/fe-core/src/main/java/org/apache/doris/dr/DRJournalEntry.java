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
 * One journal entry stored in a segment file: journal ID paired with
 * the raw bytes of the serialized JournalEntity.
 * Raw bytes avoid re-serialization and make the format independent
 * of BDB-JE at read time.
 */
public class DRJournalEntry {

    public final long journalId;
    // raw bytes from JournalEntity.write() — opCode (2 bytes) + data payload
    public final byte[] entityBytes;

    public DRJournalEntry(long journalId, byte[] entityBytes) {
        this.journalId = journalId;
        this.entityBytes = entityBytes;
    }
}
