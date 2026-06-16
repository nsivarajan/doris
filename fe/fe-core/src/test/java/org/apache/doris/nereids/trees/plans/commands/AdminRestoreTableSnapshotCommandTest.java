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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

public class AdminRestoreTableSnapshotCommandTest {
    private Env env;
    private ConnectContext connectContext;
    private AccessControllerManager accessControllerManager;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> ctxMockedStatic;
    private String originalMinPrivilege;
    private String originalDeployMode;

    @BeforeEach
    public void setUp() {
        originalMinPrivilege = Config.cluster_snapshot_min_privilege;
        originalDeployMode = Config.deploy_mode;

        env = Mockito.mock(Env.class);
        connectContext = Mockito.mock(ConnectContext.class);
        accessControllerManager = Mockito.mock(AccessControllerManager.class);

        envMockedStatic = Mockito.mockStatic(Env.class);
        ctxMockedStatic = Mockito.mockStatic(ConnectContext.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        ctxMockedStatic.when(ConnectContext::get).thenReturn(connectContext);

        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        Mockito.when(connectContext.getState()).thenReturn(new QueryState());
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
    }

    @AfterEach
    public void tearDown() {
        Config.cluster_snapshot_min_privilege = originalMinPrivilege;
        Config.deploy_mode = originalDeployMode;
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
        if (ctxMockedStatic != null) {
            ctxMockedStatic.close();
        }
    }

    private AdminRestoreTableSnapshotCommand cmd(String snapshotId, String db, String tbl) {
        return new AdminRestoreTableSnapshotCommand(snapshotId, db, tbl, Collections.emptyList());
    }

    @Test
    public void testDiskModeRejected() {
        Config.deploy_mode = "";
        Assertions.assertThrows(AnalysisException.class,
                () -> cmd("00000000abc", "orders", "items").validate(connectContext));
    }

    @Test
    public void testEmptySnapshotIdRejected() {
        Config.deploy_mode = "cloud";
        Assertions.assertThrows(AnalysisException.class,
                () -> cmd(null, "orders", "items").validate(connectContext));
        Assertions.assertThrows(AnalysisException.class,
                () -> cmd("", "orders", "items").validate(connectContext));
    }

    @Test
    public void testEmptyDbNameRejected() {
        Config.deploy_mode = "cloud";
        Assertions.assertThrows(AnalysisException.class,
                () -> cmd("00000000abc", null, "items").validate(connectContext));
        Assertions.assertThrows(AnalysisException.class,
                () -> cmd("00000000abc", "", "items").validate(connectContext));
    }

    @Test
    public void testEmptyTableNameRejected() {
        Config.deploy_mode = "cloud";
        Assertions.assertThrows(AnalysisException.class,
                () -> cmd("00000000abc", "orders", null).validate(connectContext));
        Assertions.assertThrows(AnalysisException.class,
                () -> cmd("00000000abc", "orders", "").validate(connectContext));
    }

    @Test
    public void testValidInputAllPartitions() throws Exception {
        Config.deploy_mode = "cloud";
        Assertions.assertDoesNotThrow(
                () -> cmd("00000000abc", "orders", "items").validate(connectContext));
    }

    @Test
    public void testValidInputSpecificPartitions() throws Exception {
        Config.deploy_mode = "cloud";
        Assertions.assertDoesNotThrow(() ->
                new AdminRestoreTableSnapshotCommand("00000000abc", "orders", "items",
                        Arrays.asList("p2024", "p2025")).validate(connectContext));
    }

    @Test
    public void testAsClauseTargetNamePropagated() {
        // AS clause: targetDbName and targetTableName are passed through.
        AdminRestoreTableSnapshotCommand withAs = new AdminRestoreTableSnapshotCommand(
                "00000000abc", "orders", "items", Collections.emptyList(),
                "orders", "items_restored");
        // validate() doesn't check targetTableName — it is applied in restoreTableFromSnapshot.
        Config.deploy_mode = "cloud";
        Assertions.assertDoesNotThrow(() -> withAs.validate(connectContext));
    }

    @Test
    public void testDefaultTargetNameFromBackwardCompatCtor() {
        // 4-arg constructor: targetTableName=null, targetDbName=null.
        // restoreTableFromSnapshot will default to sourceTable + "_restored".
        AdminRestoreTableSnapshotCommand cmd4 = new AdminRestoreTableSnapshotCommand(
                "00000000abc", "orders", "items", Collections.emptyList());
        Config.deploy_mode = "cloud";
        Assertions.assertDoesNotThrow(() -> cmd4.validate(connectContext));
    }

    @Test
    public void testRootModeNonRootDenied() {
        Config.deploy_mode = "cloud";
        Config.cluster_snapshot_min_privilege = "root";
        UserIdentity nonRoot = new UserIdentity("admin_user", "%");
        nonRoot.setIsAnalyzed();
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(nonRoot);
        Assertions.assertThrows(AnalysisException.class,
                () -> cmd("00000000abc", "orders", "items").validate(connectContext));
    }

    @Test
    public void testAdminModeAdminAllowed() {
        Config.deploy_mode = "cloud";
        Config.cluster_snapshot_min_privilege = "admin";
        Mockito.when(accessControllerManager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class),
                Mockito.eq(PrivPredicate.ADMIN))).thenReturn(true);
        Assertions.assertDoesNotThrow(
                () -> cmd("00000000abc", "orders", "items").validate(connectContext));
    }

    @Test
    public void testAdminModeNonAdminDenied() {
        Config.deploy_mode = "cloud";
        Config.cluster_snapshot_min_privilege = "admin";
        UserIdentity normalUser = new UserIdentity("normal_user", "%");
        normalUser.setIsAnalyzed();
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(normalUser);
        Mockito.when(accessControllerManager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class),
                Mockito.eq(PrivPredicate.ADMIN))).thenReturn(false);
        Assertions.assertThrows(AnalysisException.class,
                () -> cmd("00000000abc", "orders", "items").validate(connectContext));
    }
}
