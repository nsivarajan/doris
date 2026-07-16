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

package org.apache.doris.policy;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.DataMaskPolicy;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Save policy for masking a column's value, conditioned on arbitrary SQL over the row.
 **/
@Data
public class ColumnMaskPolicy extends Policy implements DataMaskPolicy {

    public static final ShowResultSetMetaData MASK_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("PolicyName", ScalarType.createVarchar(100)))
                    .addColumn(new Column("CatalogName", ScalarType.createVarchar(100)))
                    .addColumn(new Column("DbName", ScalarType.createVarchar(100)))
                    .addColumn(new Column("TableName", ScalarType.createVarchar(100)))
                    .addColumn(new Column("ColumnName", ScalarType.createVarchar(100)))
                    .addColumn(new Column("MaskExpression", ScalarType.createVarchar(65535)))
                    .addColumn(new Column("User", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Role", ScalarType.createVarchar(20)))
                    .addColumn(new Column("OriginStmt", ScalarType.createVarchar(65535)))
                    .build();

    /**
     * Policy bind user.
     **/
    @SerializedName(value = "user")
    private UserIdentity user = null;

    @SerializedName(value = "roleName")
    private String roleName = null;

    @SerializedName(value = "ctlName")
    private String ctlName;
    @SerializedName(value = "dbName")
    private String dbName;
    @SerializedName(value = "tableName")
    private String tableName;
    /**
     * Always an exact column name, lower-cased; never a wildcard.
     **/
    @SerializedName(value = "columnName")
    private String columnName;

    /**
     * Raw SQL text of the mask expression (the USING (...) clause). Unlike RowPolicy's
     * wherePredicate, this is never parsed into an Expression here — DataMaskPolicy.getMaskTypeDef()
     * is a plain String contract, and the caller (LogicalCheckPolicy) re-parses it per query.
     **/
    @SerializedName(value = "maskExprSql")
    private String maskExprSql;

    /**
     * Use for Serialization/deserialization.
     **/
    @SerializedName(value = "originStmt")
    private String originStmt;
    @SerializedName(value = "stmtIdx")
    private int stmtIdx;

    public ColumnMaskPolicy() {
        super(PolicyTypeEnum.MASK);
    }

    /**
     * Policy for masking a single column.
     *
     * @param policyId policy id
     * @param policyName policy name
     * @param ctlName catalog name, may be "*" for a wildcard scope
     * @param dbName database name, may be "*" for a wildcard scope
     * @param tableName table name, may be "*" for a wildcard scope
     * @param columnName exact column name; never a wildcard
     * @param user username
     * @param roleName roleName
     * @param maskExprSql raw SQL text of the mask expression
     * @param originStmt origin stmt
     * @param stmtIdx origin stmt index
     */
    public ColumnMaskPolicy(long policyId, final String policyName, String ctlName, String dbName, String tableName,
            String columnName, UserIdentity user, String roleName, String maskExprSql,
            String originStmt, int stmtIdx) {
        super(policyId, PolicyTypeEnum.MASK, policyName);
        this.ctlName = ctlName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.user = user;
        this.roleName = roleName;
        this.maskExprSql = maskExprSql;
        this.originStmt = originStmt;
        this.stmtIdx = stmtIdx;
    }

    /**
     * Use for SHOW MASKING POLICY.
     **/
    public List<String> getShowInfo() throws AnalysisException {
        return Lists.newArrayList(this.policyName, ctlName, dbName, tableName, columnName, maskExprSql,
                this.user == null ? null : this.user.getQualifiedUser(), this.roleName, this.originStmt);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // No lazy field to reconstruct: maskExprSql is a plain String, unlike RowPolicy's
        // Expression wherePredicate, so there's nothing to re-parse after deserialization.
    }

    @Override
    public ColumnMaskPolicy clone() {
        return new ColumnMaskPolicy(this.id, this.policyName, this.ctlName, this.dbName, this.tableName,
                this.columnName, this.user, this.roleName, this.maskExprSql, this.originStmt, this.stmtIdx);
    }

    private boolean checkMatched(String ctlName, String dbName, String tableName, String columnName,
            PolicyTypeEnum type, String policyName, UserIdentity user, String roleName) {
        return super.checkMatched(type, policyName)
                && (StringUtils.isEmpty(ctlName) || StringUtils.equals(ctlName, this.ctlName))
                && (StringUtils.isEmpty(dbName) || StringUtils.equals(dbName, this.dbName))
                && (StringUtils.isEmpty(tableName) || StringUtils.equals(tableName, this.tableName))
                && (StringUtils.isEmpty(columnName) || StringUtils.equals(columnName, this.columnName))
                && (StringUtils.isEmpty(roleName) || StringUtils.equals(roleName, this.roleName))
                && (user == null || Objects.equals(user, this.user));
    }

    @Override
    public boolean matchPolicy(Policy checkedPolicyCondition) {
        if (!(checkedPolicyCondition instanceof ColumnMaskPolicy)) {
            return false;
        }
        ColumnMaskPolicy maskPolicy = (ColumnMaskPolicy) checkedPolicyCondition;
        return checkMatched(maskPolicy.getCtlName(), maskPolicy.getDbName(), maskPolicy.getTableName(),
                maskPolicy.getColumnName(), maskPolicy.getType(), maskPolicy.getPolicyName(),
                maskPolicy.getUser(), maskPolicy.getRoleName());
    }

    @Override
    public boolean matchPolicy(DropPolicyLog checkedDropPolicyLogCondition) {
        return checkMatched(checkedDropPolicyLogCondition.getCtlName(), checkedDropPolicyLogCondition.getDbName(),
                checkedDropPolicyLogCondition.getTableName(), checkedDropPolicyLogCondition.getColumnName(),
                checkedDropPolicyLogCondition.getType(), checkedDropPolicyLogCondition.getPolicyName(),
                checkedDropPolicyLogCondition.getUser(), checkedDropPolicyLogCondition.getRoleName());
    }

    @Override
    public boolean isInvalid() {
        return StringUtils.isEmpty(maskExprSql);
    }

    @Override
    public String getMaskTypeDef() {
        return maskExprSql;
    }

    @Override
    public String getPolicyIdent() {
        return getPolicyName();
    }

}
