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

import java.util.Collections;

public class AdminAlterClusterSnapshotCommandTest {

    private Env env;
    private ConnectContext connectContext;
    private AccessControllerManager accessControllerManager;
    private MockedStatic<Env> envMockedStatic;
    private String originalDeployMode;
    private String originalMinPrivilege;
    private long originalMaxTtl;

    @BeforeEach
    public void setUp() {
        originalDeployMode = Config.deploy_mode;
        originalMinPrivilege = Config.cluster_snapshot_min_privilege;
        originalMaxTtl = Config.cloud_snapshot_max_ttl_seconds;

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
        Config.cloud_snapshot_max_ttl_seconds = originalMaxTtl;
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
    }

    private AdminAlterClusterSnapshotCommand cmd(String key, String snapshotId, long ttlSeconds)
            throws AnalysisException {
        return new AdminAlterClusterSnapshotCommand(key, snapshotId,
                Collections.singletonMap("ttl", String.valueOf(ttlSeconds)));
    }

    @Test
    public void testDiskModeRejected() throws AnalysisException {
        Config.deploy_mode = "";
        Assertions.assertThrows(AnalysisException.class,
                () -> cmd("snapshot_id", "00000000abc1", 259200).validate(connectContext));
    }

    @Test
    public void testWrongKeyRejected() throws AnalysisException {
        Config.deploy_mode = "cloud";
        Assertions.assertThrows(AnalysisException.class,
                () -> cmd("label", "00000000abc1", 259200).validate(connectContext));
        Assertions.assertThrows(AnalysisException.class,
                () -> cmd(null, "00000000abc1", 259200).validate(connectContext));
    }

    @Test
    public void testEmptySnapshotIdRejected() throws AnalysisException {
        Config.deploy_mode = "cloud";
        Assertions.assertThrows(AnalysisException.class,
                () -> cmd("snapshot_id", null, 259200).validate(connectContext));
        Assertions.assertThrows(AnalysisException.class,
                () -> cmd("snapshot_id", "", 259200).validate(connectContext));
    }

    @Test
    public void testNonPositiveTtlRejectedAtConstruction() {
        // Constructor validates TTL > 0
        Assertions.assertThrows(AnalysisException.class,
                () -> new AdminAlterClusterSnapshotCommand("snapshot_id", "abc",
                        Collections.singletonMap("ttl", "0")));
        Assertions.assertThrows(AnalysisException.class,
                () -> new AdminAlterClusterSnapshotCommand("snapshot_id", "abc",
                        Collections.singletonMap("ttl", "-3600")));
    }

    @Test
    public void testMissingTtlPropertyRejected() {
        Assertions.assertThrows(AnalysisException.class,
                () -> new AdminAlterClusterSnapshotCommand("snapshot_id", "abc",
                        Collections.emptyMap()));
    }

    @Test
    public void testTtlExceedsMaxRejected() throws AnalysisException {
        Config.deploy_mode = "cloud";
        Config.cloud_snapshot_max_ttl_seconds = 86400;
        Assertions.assertThrows(AnalysisException.class,
                () -> cmd("snapshot_id", "00000000abc1", 172800).validate(connectContext));
    }

    @Test
    public void testTtlWithinMaxAccepted() throws AnalysisException {
        Config.deploy_mode = "cloud";
        Config.cloud_snapshot_max_ttl_seconds = 604800;
        Assertions.assertDoesNotThrow(
                () -> cmd("snapshot_id", "00000000abc1", 259200).validate(connectContext));
    }

    @Test
    public void testNoMaxTtlCapAccepted() throws AnalysisException {
        Config.deploy_mode = "cloud";
        Config.cloud_snapshot_max_ttl_seconds = 0;  // 0 = no cap
        Assertions.assertDoesNotThrow(
                () -> cmd("snapshot_id", "00000000abc1", 99999999).validate(connectContext));
    }

    @Test
    public void testRootModeNonRootDenied() throws AnalysisException {
        Config.deploy_mode = "cloud";
        Config.cluster_snapshot_min_privilege = "root";
        UserIdentity nonRoot = new UserIdentity("analyst", "%");
        nonRoot.setIsAnalyzed();
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(nonRoot);
        Assertions.assertThrows(AnalysisException.class,
                () -> cmd("snapshot_id", "00000000abc1", 259200).validate(connectContext));
    }

    @Test
    public void testAdminModeAdminAllowed() throws AnalysisException {
        Config.deploy_mode = "cloud";
        Config.cluster_snapshot_min_privilege = "admin";
        Mockito.when(accessControllerManager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class),
                Mockito.eq(PrivPredicate.ADMIN))).thenReturn(true);
        Assertions.assertDoesNotThrow(
                () -> cmd("snapshot_id", "00000000abc1", 259200).validate(connectContext));
    }

    @Test
    public void testAdminModeNonAdminDenied() throws AnalysisException {
        Config.deploy_mode = "cloud";
        Config.cluster_snapshot_min_privilege = "admin";
        UserIdentity nonAdmin = new UserIdentity("analyst", "%");
        nonAdmin.setIsAnalyzed();
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(nonAdmin);
        Mockito.when(accessControllerManager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class),
                Mockito.eq(PrivPredicate.ADMIN))).thenReturn(false);
        Assertions.assertThrows(AnalysisException.class,
                () -> cmd("snapshot_id", "00000000abc1", 259200).validate(connectContext));
    }

    @Test
    public void testNonNumericTtlRejectedAtConstruction() {
        Assertions.assertThrows(AnalysisException.class,
                () -> new AdminAlterClusterSnapshotCommand("snapshot_id", "abc",
                        Collections.singletonMap("ttl", "two-days")));
    }
}
