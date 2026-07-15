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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Persisted replication group state written to BDB journal.
 * Survives FE restart — on startup Env.loadJournal() replays this to
 * restore Config.dr_read_only_mode and vault override state.
 */
public class ReplicationGroupInfo implements Writable {

    private static final Gson GSON = new GsonBuilder().create();

    public String groupId = "";
    public String primarySite = "";
    public boolean drReadOnly = false;
    // vault_name → {endpoint, bucket} — mirrors what was pushed to MS via apply_vault_override
    public Map<String, VaultOverride> vaultOverrides = new HashMap<>();
    public long lastUpdatedMs = 0L;

    public static class VaultOverride {
        public String endpoint;
        public String bucket;

        public VaultOverride() {}

        public VaultOverride(String endpoint, String bucket) {
            this.endpoint = endpoint;
            this.bucket = bucket;
        }
    }

    public ReplicationGroupInfo() {}

    @Override
    public void write(DataOutput out) throws IOException {
        // serialised as a single JSON string for simplicity and forward-compat
        Text.writeString(out, GSON.toJson(this));
    }

    public static ReplicationGroupInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GSON.fromJson(json, ReplicationGroupInfo.class);
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
