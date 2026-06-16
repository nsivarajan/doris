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

public class AdminRestoreClusterSnapshotCommandTest {

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

    @Test
    public void testDiskModeRejected() {
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        Config.deploy_mode = "";
        Assertions.assertThrows(AnalysisException.class,
                () -> new AdminRestoreClusterSnapshotCommand("snapshot_id", "00000000abc")
                        .validate(connectContext));
    }

    @Test
    public void testSnapshotIdKeyAccepted() throws Exception {
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        Config.deploy_mode = "cloud";
        // snapshot_id is the only valid key
        Assertions.assertDoesNotThrow(() ->
                new AdminRestoreClusterSnapshotCommand("snapshot_id", "00000000abc")
                        .validate(connectContext));
    }

    @Test
    public void testLabelKeyRejected() {
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        Config.deploy_mode = "cloud";
        // label is no longer accepted — snapshot_id is required
        Assertions.assertThrows(AnalysisException.class, () ->
                new AdminRestoreClusterSnapshotCommand("label", "my-snap")
                        .validate(connectContext));
    }

    @Test
    public void testNullKeyRejected() {
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        Config.deploy_mode = "cloud";
        Assertions.assertThrows(AnalysisException.class, () ->
                new AdminRestoreClusterSnapshotCommand(null, "00000000abc")
                        .validate(connectContext));
    }

    @Test
    public void testEmptySnapshotIdRejected() {
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        Config.deploy_mode = "cloud";
        Assertions.assertThrows(AnalysisException.class, () ->
                new AdminRestoreClusterSnapshotCommand("snapshot_id", null)
                        .validate(connectContext));
        Assertions.assertThrows(AnalysisException.class, () ->
                new AdminRestoreClusterSnapshotCommand("snapshot_id", "")
                        .validate(connectContext));
    }

    @Test
    public void testRootModeNonRootDenied() {
        Config.cluster_snapshot_min_privilege = "root";
        Config.deploy_mode = "cloud";
        UserIdentity nonRoot = new UserIdentity("admin", "%");
        nonRoot.setIsAnalyzed();
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(nonRoot);
        Assertions.assertThrows(AnalysisException.class, () ->
                new AdminRestoreClusterSnapshotCommand("snapshot_id", "00000000abc")
                        .validate(connectContext));
    }

    @Test
    public void testAdminModeAllowed() {
        Config.cluster_snapshot_min_privilege = "admin";
        Config.deploy_mode = "cloud";
        UserIdentity adminUser = new UserIdentity("admin", "%");
        adminUser.setIsAnalyzed();
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(adminUser);
        Mockito.when(accessControllerManager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class),
                Mockito.eq(PrivPredicate.ADMIN))).thenReturn(true);
        Assertions.assertDoesNotThrow(() ->
                new AdminRestoreClusterSnapshotCommand("snapshot_id", "00000000abc")
                        .validate(connectContext));
    }
}
