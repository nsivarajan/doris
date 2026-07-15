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
 * ALTER SYSTEM REPLICATION ADD VAULT MAPPING &lt;vaultName&gt; PROPERTIES(...)
 *
 * Required properties: "secondary_endpoint", "secondary_bucket"
 */
public class ReplicationAddVaultMappingOp extends AlterSystemOp {
    private final String vaultName;
    private final Map<String, String> properties;

    public ReplicationAddVaultMappingOp(String vaultName, Map<String, String> properties) {
        super(AlterOpType.ALTER_OTHER);
        this.vaultName = vaultName;
        this.properties = properties == null ? Collections.emptyMap() : properties;
    }

    public String getVaultName() {
        return vaultName;
    }

    public String getSecondaryEndpoint() {
        return properties.get("secondary_endpoint");
    }

    public String getSecondaryBucket() {
        return properties.get("secondary_bucket");
    }

    @Override
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (!org.apache.doris.catalog.Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
            throw new AnalysisException("Access denied; requires ADMIN privilege");
        }
        if (vaultName == null || vaultName.isEmpty()) {
            throw new AnalysisException("ADD VAULT MAPPING requires a non-empty vault name");
        }
        if (!properties.containsKey("secondary_endpoint")) {
            throw new AnalysisException(
                    "ADD VAULT MAPPING requires property 'secondary_endpoint'");
        }
        if (!properties.containsKey("secondary_bucket")) {
            throw new AnalysisException(
                    "ADD VAULT MAPPING requires property 'secondary_bucket'");
        }
    }

    @Override
    public String toSql() {
        return "ALTER SYSTEM REPLICATION ADD VAULT MAPPING " + vaultName;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
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
