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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.policy.DropPolicyLog;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for CreateMaskPolicyCommand.validate(): column-existence checks (skipped for wildcard
 * scopes), and rejection of a nonexistent column on a resolvable table.
 */
public class CreateMaskPolicyCommandTest extends TestWithFeService {

    private static final String CTL = "internal";
    private static final String DB = "test_create_mask_policy_db";
    private static final String TBL = "orders";

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase(DB);
        useDatabase(DB);
        createTable("create table " + TBL
                + " (id int, ssn varchar(20), gdpr_action_cd boolean)"
                + " distributed by hash(id) buckets 1 properties(\"replication_num\" = \"1\");");
        useUser("root");
    }

    private CreateMaskPolicyCommand parse(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        return (CreateMaskPolicyCommand) nereidsParser.parseSingle(sql);
    }

    @Test
    public void testValidColumnOnExactTable() throws Exception {
        CreateMaskPolicyCommand command = parse("CREATE MASKING POLICY p_exact ON " + DB + "." + TBL + " (ssn)"
                + " TO ROLE some_role USING (CASE WHEN gdpr_action_cd THEN NULL ELSE ssn END)");
        command.run(connectContext, new StmtExecutor(connectContext, ""));
        try {
            Assertions.assertTrue(Env.getCurrentEnv().getPolicyMgr()
                    .findPolicy("p_exact", PolicyTypeEnum.MASK).isPresent());
        } finally {
            Env.getCurrentEnv().getPolicyMgr().dropPolicy(
                    new DropPolicyLog(CTL, DB, TBL, "ssn", PolicyTypeEnum.MASK, "p_exact", null, "some_role"),
                    true);
        }
    }

    @Test
    public void testNonExistentColumnOnExactTableRejected() throws Exception {
        CreateMaskPolicyCommand command = parse("CREATE MASKING POLICY p_bad_col ON " + DB + "." + TBL
                + " (nonexistent_col) TO ROLE some_role USING (NULL)");
        ExceptionChecker.expectThrows(AnalysisException.class,
                () -> command.run(connectContext, new StmtExecutor(connectContext, "")));
    }

    @Test
    public void testWildcardTableSkipsColumnExistenceCheck() throws Exception {
        // "nonexistent_col" doesn't exist on any real table, but the table scope is wildcarded,
        // so there's no single table to validate against — creation must still succeed.
        CreateMaskPolicyCommand command = parse("CREATE MASKING POLICY p_wc ON " + DB + ".*"
                + " (nonexistent_col) TO ROLE some_role USING (NULL)");
        command.run(connectContext, new StmtExecutor(connectContext, ""));
        try {
            Assertions.assertTrue(Env.getCurrentEnv().getPolicyMgr()
                    .findPolicy("p_wc", PolicyTypeEnum.MASK).isPresent());
        } finally {
            Env.getCurrentEnv().getPolicyMgr().dropPolicy(
                    new DropPolicyLog(CTL, DB, "*", "nonexistent_col", PolicyTypeEnum.MASK, "p_wc", null,
                            "some_role"),
                    true);
        }
    }

    @Test
    public void testEmptyColumnNameRejected() {
        ExceptionChecker.expectThrows(Exception.class, () -> {
            CreateMaskPolicyCommand command = new CreateMaskPolicyCommand("p_empty_col", false,
                    new TableNameInfo(CTL, DB, TBL), "", null, "some_role",
                    org.apache.doris.nereids.trees.expressions.literal.NullLiteral.INSTANCE);
            command.run(connectContext, new StmtExecutor(connectContext, ""));
        });
    }
}
