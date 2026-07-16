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

import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateMaskPolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateRoleCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateUserCommand;
import org.apache.doris.nereids.trees.plans.commands.DropMaskPolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.GrantRoleCommand;
import org.apache.doris.nereids.trees.plans.commands.GrantTablePrivilegeCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateUserInfo;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

/**
 * Tests for ColumnMaskPolicy: creation, wildcard-scope lookup, and cross-tier precedence.
 **/
public class ColumnMaskPolicyMgrTest extends TestWithFeService {

    private static final String DB = "test_mask_policy_db";
    private static final String TBL1 = "orders";
    private static final String TBL2 = "customers";
    private static final String USER = "mask_policy_user";
    private static final String ROLE = "mask_policy_role";

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase(DB);
        useDatabase(DB);
        createTable("create table " + TBL1
                + " (id int, ssn varchar(20), gdpr_action_cd boolean)"
                + " distributed by hash(id) buckets 1 properties(\"replication_num\" = \"1\");");
        createTable("create table " + TBL2
                + " (id int, ssn varchar(20), gdpr_action_cd boolean)"
                + " distributed by hash(id) buckets 1 properties(\"replication_num\" = \"1\");");

        UserIdentity user = new UserIdentity(USER, "%");
        user.analyze();
        CreateUserCommand createUserCommand = new CreateUserCommand(new CreateUserInfo(new UserDesc(user)));
        createUserCommand.getInfo().validate();
        Env.getCurrentEnv().getAuth().createUser(createUserCommand.getInfo());

        List<AccessPrivilegeWithCols> privileges = Lists
                .newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.ADMIN_PRIV));
        TablePattern tablePattern = new TablePattern("*", "*", "*");
        tablePattern.analyze();
        GrantTablePrivilegeCommand grantCommand = new GrantTablePrivilegeCommand(
                privileges, tablePattern, Optional.of(user), Optional.empty());
        grantCommand.validate();
        Env.getCurrentEnv().getAuth().grantTablePrivilegeCommand(grantCommand);

        CreateRoleCommand createRoleCommand = new CreateRoleCommand(false, ROLE, "");
        createRoleCommand.run(connectContext, null);
        GrantRoleCommand grantRoleCommand = new GrantRoleCommand(user, Lists.newArrayList(ROLE));
        grantRoleCommand.validate();
        Env.getCurrentEnv().getAuth().grantRoleCommand(grantRoleCommand);

        useUser("root");
    }

    private void createMaskPolicy(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        CreateMaskPolicyCommand command = (CreateMaskPolicyCommand) nereidsParser.parseSingle(sql);
        command.run(connectContext, new StmtExecutor(connectContext, sql));
    }

    private void dropMaskPolicy(String policyName, TableNameInfo tableNameInfo, String columnName)
            throws Exception {
        DropMaskPolicyCommand command = new DropMaskPolicyCommand(
                false, policyName, tableNameInfo, columnName, null, ROLE);
        command.doRun(connectContext, null);
    }

    @Test
    public void testExactMatchCreateAndLookup() throws Exception {
        createMaskPolicy("CREATE MASKING POLICY exact_ssn_policy ON " + DB + "." + TBL1 + " (ssn)"
                + " TO ROLE " + ROLE + " USING (CASE WHEN gdpr_action_cd THEN NULL ELSE ssn END)");
        try {
            UserIdentity user = new UserIdentity(USER, "%");
            user.analyze();
            Optional<ColumnMaskPolicy> resolved = Env.getCurrentEnv().getPolicyMgr()
                    .getUserMaskPolicy("internal", DB, TBL1, "ssn", user);
            Assertions.assertTrue(resolved.isPresent(), "exact policy must resolve for the granted role");
            Assertions.assertEquals("exact_ssn_policy", resolved.get().getPolicyName());

            // A different table with the same column name must NOT match an exact-table policy.
            Optional<ColumnMaskPolicy> notResolved = Env.getCurrentEnv().getPolicyMgr()
                    .getUserMaskPolicy("internal", DB, TBL2, "ssn", user);
            Assertions.assertFalse(notResolved.isPresent(), "exact-table policy must not leak to another table");
        } finally {
            dropMaskPolicy("exact_ssn_policy", new TableNameInfo("internal", DB, TBL1), "ssn");
        }
    }

    @Test
    public void testTableWildcardCoversMultipleTables() throws Exception {
        createMaskPolicy("CREATE MASKING POLICY wc_ssn_policy ON " + DB + ".* (ssn)"
                + " TO ROLE " + ROLE + " USING (CASE WHEN gdpr_action_cd THEN NULL ELSE ssn END)");
        try {
            UserIdentity user = new UserIdentity(USER, "%");
            user.analyze();
            Optional<ColumnMaskPolicy> onTbl1 = Env.getCurrentEnv().getPolicyMgr()
                    .getUserMaskPolicy("internal", DB, TBL1, "ssn", user);
            Optional<ColumnMaskPolicy> onTbl2 = Env.getCurrentEnv().getPolicyMgr()
                    .getUserMaskPolicy("internal", DB, TBL2, "ssn", user);
            Assertions.assertTrue(onTbl1.isPresent(), "db-wildcard must cover table1");
            Assertions.assertTrue(onTbl2.isPresent(), "db-wildcard must cover table2");
            Assertions.assertEquals("wc_ssn_policy", onTbl1.get().getPolicyName());
            Assertions.assertEquals("wc_ssn_policy", onTbl2.get().getPolicyName());
        } finally {
            dropMaskPolicy("wc_ssn_policy", new TableNameInfo("internal", DB, "*"), "ssn");
        }
    }

    @Test
    public void testExactTablePrecedenceOverWildcard() throws Exception {
        createMaskPolicy("CREATE MASKING POLICY wc_precedence_policy ON " + DB + ".* (ssn)"
                + " TO ROLE " + ROLE + " USING (CASE WHEN gdpr_action_cd THEN NULL ELSE ssn END)");
        createMaskPolicy("CREATE MASKING POLICY exact_precedence_policy ON " + DB + "." + TBL1 + " (ssn)"
                + " TO ROLE " + ROLE + " USING (CONCAT('XXX-', ssn))");
        try {
            UserIdentity user = new UserIdentity(USER, "%");
            user.analyze();
            Optional<ColumnMaskPolicy> resolved = Env.getCurrentEnv().getPolicyMgr()
                    .getUserMaskPolicy("internal", DB, TBL1, "ssn", user);
            Assertions.assertTrue(resolved.isPresent());
            Assertions.assertEquals("exact_precedence_policy", resolved.get().getPolicyName(),
                    "exact-table policy must win over a simultaneously active db-wildcard policy");

            // table2 has no exact policy, so the wildcard must still apply there.
            Optional<ColumnMaskPolicy> onTbl2 = Env.getCurrentEnv().getPolicyMgr()
                    .getUserMaskPolicy("internal", DB, TBL2, "ssn", user);
            Assertions.assertTrue(onTbl2.isPresent());
            Assertions.assertEquals("wc_precedence_policy", onTbl2.get().getPolicyName());
        } finally {
            dropMaskPolicy("exact_precedence_policy", new TableNameInfo("internal", DB, TBL1), "ssn");
            dropMaskPolicy("wc_precedence_policy", new TableNameInfo("internal", DB, "*"), "ssn");
        }
    }

    @Test
    public void testNoPolicyMeansNoMasking() throws Exception {
        UserIdentity user = new UserIdentity(USER, "%");
        user.analyze();
        Optional<ColumnMaskPolicy> resolved = Env.getCurrentEnv().getPolicyMgr()
                .getUserMaskPolicy("internal", DB, TBL1, "ssn", user);
        Assertions.assertFalse(resolved.isPresent(), "no policy created — must resolve to nothing");
    }

    @Test
    public void testDropRemovesPolicy() throws Exception {
        createMaskPolicy("CREATE MASKING POLICY drop_test_policy ON " + DB + "." + TBL1 + " (ssn)"
                + " TO ROLE " + ROLE + " USING (CASE WHEN gdpr_action_cd THEN NULL ELSE ssn END)");
        UserIdentity user = new UserIdentity(USER, "%");
        user.analyze();
        Assertions.assertTrue(Env.getCurrentEnv().getPolicyMgr()
                .getUserMaskPolicy("internal", DB, TBL1, "ssn", user).isPresent());

        dropMaskPolicy("drop_test_policy", new TableNameInfo("internal", DB, TBL1), "ssn");

        Assertions.assertFalse(Env.getCurrentEnv().getPolicyMgr()
                .getUserMaskPolicy("internal", DB, TBL1, "ssn", user).isPresent(),
                "policy must no longer resolve after DROP");
    }

    @Test
    public void testShowMaskPolicy() throws Exception {
        createMaskPolicy("CREATE MASKING POLICY show_test_policy ON " + DB + "." + TBL1 + " (ssn)"
                + " TO ROLE " + ROLE + " USING (CASE WHEN gdpr_action_cd THEN NULL ELSE ssn END)");
        try {
            ShowResultSet result = Env.getCurrentEnv().getPolicyMgr()
                    .showMaskPolicy(new TableNameInfo("internal", DB, TBL1), null, ROLE);
            boolean found = result.getResultRows().stream()
                    .anyMatch(row -> row.get(0).equals("show_test_policy"));
            Assertions.assertTrue(found, "SHOW MASKING POLICY must list the created policy");
        } finally {
            dropMaskPolicy("show_test_policy", new TableNameInfo("internal", DB, TBL1), "ssn");
        }
    }
}
