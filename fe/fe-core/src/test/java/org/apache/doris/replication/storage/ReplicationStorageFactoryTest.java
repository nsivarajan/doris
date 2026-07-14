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

package org.apache.doris.replication.storage;

import org.apache.doris.replication.ReplicationConfig;
import org.junit.Assert;
import org.junit.Test;

public class ReplicationStorageFactoryTest {

    @Test
    public void testCreateOSSWithAkSk() {
        ReplicationConfig config = ReplicationConfig.builder()
                .storageType(ReplicationConfig.StorageType.OSS)
                .credentialType(ReplicationConfig.CredentialType.AK_SK)
                .bucket("test-bucket")
                .endpoint("oss-cn-beijing-internal.aliyuncs.com")
                .accessKey("ak")
                .secretKey("sk")
                .build();
        // should succeed and return OSSReplicationStorage
        ReplicationStorageBackend backend = ReplicationStorageFactory.create(config);
        Assert.assertNotNull(backend);
        Assert.assertTrue(backend instanceof OSSReplicationStorage);
    }

    @Test
    public void testCreateS3WithAkSk() {
        ReplicationConfig config = ReplicationConfig.builder()
                .storageType(ReplicationConfig.StorageType.S3)
                .credentialType(ReplicationConfig.CredentialType.AK_SK)
                .bucket("test-bucket")
                .endpoint("s3.amazonaws.com")
                .accessKey("ak")
                .secretKey("sk")
                .build();
        ReplicationStorageBackend backend = ReplicationStorageFactory.create(config);
        Assert.assertNotNull(backend);
        Assert.assertTrue(backend instanceof S3ReplicationStorage);
    }

    @Test
    public void testCreateOSSWithInstanceProfile() {
        ReplicationConfig config = ReplicationConfig.builder()
                .storageType(ReplicationConfig.StorageType.OSS)
                .credentialType(ReplicationConfig.CredentialType.INSTANCE_PROFILE)
                .bucket("test-bucket")
                .endpoint("oss-cn-beijing-internal.aliyuncs.com")
                .build();
        // factory creation should succeed even without calling getCredentials()
        ReplicationStorageBackend backend = ReplicationStorageFactory.create(config);
        Assert.assertNotNull(backend);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAssumeRoleWithoutArnThrows() {
        ReplicationConfig config = ReplicationConfig.builder()
                .storageType(ReplicationConfig.StorageType.OSS)
                .credentialType(ReplicationConfig.CredentialType.ASSUME_ROLE)
                .roleArn("")   // missing — should throw
                .bucket("b")
                .endpoint("e")
                .build();
        ReplicationStorageFactory.create(config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnsupportedStorageTypeThrows() {
        ReplicationConfig config = ReplicationConfig.builder()
                .storageType(ReplicationConfig.StorageType.GCS)  // not yet implemented
                .credentialType(ReplicationConfig.CredentialType.AK_SK)
                .bucket("b").endpoint("e").accessKey("ak").secretKey("sk")
                .build();
        ReplicationStorageFactory.create(config);
    }
}
