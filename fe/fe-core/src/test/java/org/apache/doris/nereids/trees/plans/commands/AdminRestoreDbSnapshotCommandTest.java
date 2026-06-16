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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class AdminRestoreDbSnapshotCommandTest {

    private Env env;
    private ConnectContext connectContext;
    private AccessControllerManager accessControllerManager;
    private MockedStatic<Env> envMockedStatic;
    private String originalDeployMode;
    private String originalMinPrivilege;

    @BeforeEach
    public void setUp() {
        originalDeployMode = Config.deploy_mode;
        originalMinPrivilege = Config.cluster_snapshot_min_privilege;

        env = Mockito.mock(Env.class);
        connectContext = Mockito.mock(ConnectContext.class);
        accessControllerManager = Mockito.mock(AccessControllerManager.class);

        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);

        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
    }

    @AfterEach
    public void tearDown() {
        Config.deploy_mode = originalDeployMode;
        Config.cluster_snapshot_min_privilege = originalMinPrivilege;
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
    }

    @Test
    public void testDiskModeRejected() {
        Config.deploy_mode = "";
        Assertions.assertThrows(AnalysisException.class,
                () -> new AdminRestoreDbSnapshotCommand("abc123", "mydb", null)
                        .validate(connectContext));
    }

    @Test
    public void testEmptySnapshotIdRejected() {
        Config.deploy_mode = "cloud";
        Assertions.assertThrows(AnalysisException.class,
                () -> new AdminRestoreDbSnapshotCommand(null, "mydb", null)
                        .validate(connectContext));
        Assertions.assertThrows(AnalysisException.class,
                () -> new AdminRestoreDbSnapshotCommand("", "mydb", null)
                        .validate(connectContext));
    }

    @Test
    public void testEmptySourceDbRejected() {
        Config.deploy_mode = "cloud";
        Assertions.assertThrows(AnalysisException.class,
                () -> new AdminRestoreDbSnapshotCommand("abc123", null, "target")
                        .validate(connectContext));
        Assertions.assertThrows(AnalysisException.class,
                () -> new AdminRestoreDbSnapshotCommand("abc123", "", "target")
                        .validate(connectContext));
    }

    @Test
    public void testValidInputWithExplicitTarget() throws Exception {
        Config.deploy_mode = "cloud";
        Assertions.assertDoesNotThrow(
                () -> new AdminRestoreDbSnapshotCommand("abc123", "mydb", "mydb_new")
                        .validate(connectContext));
    }

    @Test
    public void testDefaultTargetSuffix() throws Exception {
        // When targetDb is null, constructor defaults to sourceDb + "_restored".
        Config.deploy_mode = "cloud";
        AdminRestoreDbSnapshotCommand cmd =
                new AdminRestoreDbSnapshotCommand("abc123", "orders", null);
        // validate() does not throw for root user in cloud mode with valid inputs.
        Assertions.assertDoesNotThrow(() -> cmd.validate(connectContext));
    }

    @Test
    public void testRootModeNonRootDenied() {
        Config.deploy_mode = "cloud";
        Config.cluster_snapshot_min_privilege = "root";
        UserIdentity nonRoot = new UserIdentity("analyst", "%");
        nonRoot.setIsAnalyzed();
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(nonRoot);
        Assertions.assertThrows(AnalysisException.class,
                () -> new AdminRestoreDbSnapshotCommand("abc123", "mydb", "mydb_new")
                        .validate(connectContext));
    }

    @Test
    public void testAdminModeAdminAllowed() {
        Config.deploy_mode = "cloud";
        Config.cluster_snapshot_min_privilege = "admin";
        Mockito.when(accessControllerManager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class),
                Mockito.eq(PrivPredicate.ADMIN))).thenReturn(true);
        Assertions.assertDoesNotThrow(
                () -> new AdminRestoreDbSnapshotCommand("abc123", "mydb", "mydb_new")
                        .validate(connectContext));
    }

    @Test
    public void testAdminModeNonAdminDenied() {
        Config.deploy_mode = "cloud";
        Config.cluster_snapshot_min_privilege = "admin";
        Mockito.when(accessControllerManager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class),
                Mockito.eq(PrivPredicate.ADMIN))).thenReturn(false);
        UserIdentity normalUser = new UserIdentity("normal_user", "%");
        normalUser.setIsAnalyzed();
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(normalUser);
        Assertions.assertThrows(AnalysisException.class,
                () -> new AdminRestoreDbSnapshotCommand("abc123", "mydb", "mydb_new")
                        .validate(connectContext));
    }
}
