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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.Collections;
import java.util.Map;

/**
 * ALTER SYSTEM REPLICATION CREATE GROUP 'id'
 *   PRIMARY SITE 'beijing' SECONDARY SITE 'shanghai'
 *   PROPERTIES ('storage_type'='OSS', 'replication_bucket'='...', ...)
 *
 * One-time setup command. Configures group-level replication settings and
 * persists them to BDB so they survive FE restart.
 *
 * Required properties: storage_type, replication_bucket, replication_endpoint
 * Optional properties: credential_type, role_arn, role_session_name, external_id,
 *                      export_interval_ms, export_batch_size, checkpoint_interval_ms,
 *                      crr_max_lag_ms
 */
public class ReplicationCreateGroupOp extends AlterSystemOp {
    private final String groupId;
    private final String primarySite;
    private final String secondarySite;
    private final Map<String, String> properties;

    public ReplicationCreateGroupOp(String groupId, String primarySite,
            String secondarySite, Map<String, String> properties) {
        super(AlterOpType.ALTER_OTHER);
        this.groupId = groupId;
        this.primarySite = primarySite;
        this.secondarySite = secondarySite;
        this.properties = properties == null ? Collections.emptyMap() : properties;
    }

    @Override
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (!org.apache.doris.catalog.Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
            throw new AnalysisException("Access denied; requires ADMIN privilege");
        }
        if (groupId == null || groupId.isEmpty()) {
            throw new AnalysisException("REPLICATION CREATE GROUP: group name is required");
        }
        if (primarySite == null || primarySite.isEmpty()) {
            throw new AnalysisException("REPLICATION CREATE GROUP: PRIMARY SITE is required");
        }
        if (secondarySite == null || secondarySite.isEmpty()) {
            throw new AnalysisException("REPLICATION CREATE GROUP: SECONDARY SITE is required");
        }
        if (primarySite.equals(secondarySite)) {
            throw new AnalysisException("PRIMARY SITE and SECONDARY SITE must be different");
        }
        if (!properties.containsKey("replication_bucket")) {
            throw new AnalysisException("REPLICATION CREATE GROUP: property 'replication_bucket' is required");
        }
        if (!properties.containsKey("replication_endpoint")) {
            throw new AnalysisException("REPLICATION CREATE GROUP: property 'replication_endpoint' is required");
        }
    }

    @Override
    public String toSql() {
        return String.format(
                "ALTER SYSTEM REPLICATION CREATE GROUP '%s' PRIMARY SITE '%s' SECONDARY SITE '%s'",
                groupId, primarySite, secondarySite);
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getPrimarySite() {
        return primarySite;
    }

    public String getSecondarySite() {
        return secondarySite;
    }

    @Override
    public boolean allowOpMTMV() {
        return true;
    }

    @Override
    public boolean needChangeMTMVState() {
        return false;
    }
}
