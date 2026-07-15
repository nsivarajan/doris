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
 * ALTER SYSTEM REPLICATION FAILOVER TO SITE '&lt;targetSite&gt;'
 */
public class ReplicationFailoverOp extends AlterSystemOp {
    private final String targetSite;

    public ReplicationFailoverOp(String targetSite) {
        super(AlterOpType.ALTER_OTHER);
        this.targetSite = targetSite;
    }

    public String getTargetSite() {
        return targetSite;
    }

    @Override
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (!org.apache.doris.catalog.Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
            throw new AnalysisException("Access denied; requires ADMIN privilege");
        }
        if (targetSite == null || targetSite.isEmpty()) {
            throw new AnalysisException("FAILOVER requires a non-empty target site name");
        }
    }

    @Override
    public String toSql() {
        return "ALTER SYSTEM REPLICATION FAILOVER TO SITE '" + targetSite + "'";
    }

    @Override
    public Map<String, String> getProperties() {
        return Collections.emptyMap();
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
