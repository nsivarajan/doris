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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.policy.DropPolicyLog;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * DropMaskPolicyCommand
 **/
public class DropMaskPolicyCommand extends DropCommand {
    private final boolean ifExists;
    private final String policyName;
    private final TableNameInfo tableNameInfo;
    private final String columnName;
    private final UserIdentity user;
    private final String roleName;

    /**
     * DropMaskPolicyCommand
     **/
    public DropMaskPolicyCommand(boolean ifExists,
                                String policyName,
                                TableNameInfo tableNameInfo,
                                String columnName,
                                UserIdentity user,
                                String roleName) {
        super(PlanType.DROP_MASKING_POLICY_COMMAND);
        this.ifExists = ifExists;
        this.policyName = policyName;
        this.tableNameInfo = tableNameInfo;
        this.columnName = columnName;
        this.user = user;
        this.roleName = roleName;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        DropPolicyLog dropPolicyLog = new DropPolicyLog(tableNameInfo.getCtl(), tableNameInfo.getDb(),
                tableNameInfo.getTbl(), columnName.toLowerCase(), PolicyTypeEnum.MASK, policyName, user, roleName);
        Env.getCurrentEnv().getPolicyMgr().dropPolicy(dropPolicyLog, ifExists);
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        tableNameInfo.analyze(ctx);
        if (user != null) {
            user.analyze();
        }
        // check auth
        if (!Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.GRANT.getPrivs().toString());
        }
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public String getPolicyName() {
        return policyName;
    }

    public TableNameInfo getTableNameInfo() {
        return tableNameInfo;
    }

    public String getColumnName() {
        return columnName;
    }

    public UserIdentity getUser() {
        return user;
    }

    public String getRoleName() {
        return roleName;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDropMaskPolicyCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DROP;
    }
}
