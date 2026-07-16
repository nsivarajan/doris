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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.policy.ColumnMaskPolicy;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.commons.lang3.StringUtils;

/**
 * CREATE MASKING POLICY: masks a single column's value, conditioned on arbitrary SQL over the
 * row, for a given user or role.
 */
public class CreateMaskPolicyCommand extends Command implements ForwardWithSync {

    private final String policyName;
    private final boolean ifNotExists;
    private final TableNameInfo tableNameInfo;
    private final String columnName;
    private final UserIdentity user;
    private final String roleName;
    private final Expression maskExpression;

    /**
     * ctor of this command.
     */
    public CreateMaskPolicyCommand(String policyName, boolean ifNotExists, TableNameInfo tableNameInfo,
            String columnName, UserIdentity user, String roleName, Expression maskExpression) {
        super(PlanType.CREATE_MASKING_POLICY_COMMAND);
        this.policyName = policyName;
        this.ifNotExists = ifNotExists;
        this.tableNameInfo = tableNameInfo;
        this.columnName = columnName;
        this.user = user;
        this.roleName = roleName;
        this.maskExpression = maskExpression;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateMaskPolicyCommand(this, context);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        ColumnMaskPolicy policy = createPolicy(executor);
        Env.getCurrentEnv().getPolicyMgr().createPolicy(policy, ifNotExists);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }

    private void validate(ConnectContext ctx) throws AnalysisException {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.GRANT.getPrivs().toString());
        }
        tableNameInfo.analyze(ctx);
        if (StringUtils.isEmpty(columnName) || "*".equals(columnName)) {
            throw new AnalysisException("masking policy column name can not be empty or wildcard");
        }
        if (user != null) {
            user.analyze();
            if (user.isRootUser() || user.isAdminUser()) {
                throw new AnalysisException("not allow add masking policy for system user");
            }
            if (!Env.getCurrentEnv().getAuth().doesUserExist(user)) {
                throw new AnalysisException("user not exist: " + user);
            }
        }
        if (!StringUtils.isEmpty(roleName)) {
            if (!Env.getCurrentEnv().getAuth().doesRoleExist(roleName)) {
                throw new AnalysisException("role not exist: " + roleName);
            }
        }
        // For wildcard scopes (db.*, catalog.*.*, *.*.*) there is no single table to validate
        // the column reference against; skip resolution and store as-is.
        if (!"*".equals(tableNameInfo.getTbl())
                && !"*".equals(tableNameInfo.getDb())
                && !"*".equals(tableNameInfo.getCtl())) {
            TableIf tableIf = Env.getCurrentEnv().getCatalogMgr()
                    .getCatalogOrAnalysisException(tableNameInfo.getCtl())
                    .getDbOrAnalysisException(tableNameInfo.getDb())
                    .getTableOrAnalysisException(tableNameInfo.getTbl());
            if (tableIf.getColumn(columnName) == null) {
                throw new AnalysisException("column not exist: " + columnName);
            }
            maskExpression.foreach(expr -> {
                if (expr instanceof UnboundSlot) {
                    UnboundSlot slot = (UnboundSlot) expr;
                    if (tableIf.getColumn(slot.getName()) == null) {
                        throw new org.apache.doris.nereids.exceptions.AnalysisException(
                                "column not exist: " + slot.getName());
                    }
                }
            });
        }
    }

    private ColumnMaskPolicy createPolicy(StmtExecutor executor) {
        long policyId = Env.getCurrentEnv().getNextId();
        return new ColumnMaskPolicy(policyId, policyName, tableNameInfo.getCtl(), tableNameInfo.getDb(),
                tableNameInfo.getTbl(), columnName.toLowerCase(), user, roleName, maskExpression.toSql(),
                executor.getOriginStmt().originStmt, executor.getOriginStmt().idx);
    }
}
