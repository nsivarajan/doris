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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.datasource.property.ConnectorPropertiesUtils;
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class AliyunDLFBasePropertiesTest {

    @Test
    void testAutoGenerateEndpointWithPublicAccess() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.access_key", "ak");
        props.put("dlf.secret_key", "sk");
        props.put("dlf.region", "cn-hangzhou");
        props.put("dlf.access.public", "true");

        AliyunDLFBaseProperties dlfProps = AliyunDLFBaseProperties.of(props);
        Assertions.assertEquals("dlf.cn-hangzhou.aliyuncs.com", dlfProps.dlfEndpoint);
    }

    @Test
    void testAutoGenerateEndpointWithVpcAccess() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.access_key", "ak");
        props.put("dlf.secret_key", "sk");
        props.put("dlf.region", "cn-hangzhou");
        props.put("dlf.access.public", "false");

        AliyunDLFBaseProperties dlfProps = AliyunDLFBaseProperties.of(props);
        Assertions.assertEquals("dlf-vpc.cn-hangzhou.aliyuncs.com", dlfProps.dlfEndpoint);
    }

    @Test
    void testExplicitEndpointOverridesAutoGeneration() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.access_key", "ak");
        props.put("dlf.secret_key", "sk");
        props.put("dlf.region", "cn-beijing");
        props.put("dlf.endpoint", "custom.endpoint.com");

        AliyunDLFBaseProperties dlfProps = AliyunDLFBaseProperties.of(props);
        Assertions.assertEquals("custom.endpoint.com", dlfProps.dlfEndpoint);
    }

    @Test
    void testMissingEndpointAndRegionThrowsException() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.access_key", "ak");
        props.put("dlf.secret_key", "sk");

        StoragePropertiesException ex = Assertions.assertThrows(
                StoragePropertiesException.class,
                () -> AliyunDLFBaseProperties.of(props)
        );
        Assertions.assertEquals("dlf.endpoint is required.", ex.getMessage());
    }

    @Test
    void testMissingAccessKeyThrowsException() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.secret_key", "sk");
        props.put("dlf.endpoint", "custom.endpoint.com");

        Exception ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AliyunDLFBaseProperties.of(props)
        );
        Assertions.assertTrue(ex.getMessage().contains("dlf.access_key is required"));
    }

    @Test
    void testMissingSecretKeyThrowsException() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.access_key", "ak");
        props.put("dlf.endpoint", "custom.endpoint.com");

        Exception ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AliyunDLFBaseProperties.of(props)
        );
        Assertions.assertTrue(ex.getMessage().contains("dlf.secret_key is required"));
    }

    @Test
    void testRoleArnRequiresRegion() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.role_arn", "acs:ram::123456789:role/my-role");

        Exception ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AliyunDLFBaseProperties.of(props)
        );
        Assertions.assertTrue(ex.getMessage().contains("dlf.region is required when dlf.role_arn is set"));
    }

    @Test
    void testRoleArnExtractsUidAndDerivesEndpoint() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.role_arn", "acs:ram::123456789:role/my-role");
        props.put("dlf.region", "cn-hangzhou");

        AliyunDLFBaseProperties dlfProps = AliyunDLFBaseProperties.of(props);
        Assertions.assertEquals("123456789", dlfProps.dlfUid);
        Assertions.assertEquals("123456789", dlfProps.dlfCatalogId);
        Assertions.assertEquals("dlf-vpc.cn-hangzhou.aliyuncs.com", dlfProps.dlfEndpoint);
        // STS is resolved lazily at first catalog use (resolveCredentials()), not at validation time.
        Assertions.assertEquals("", dlfProps.dlfAccessKey);
    }

    @Test
    void testRoleArnInvalidFormatThrows() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.role_arn", "invalid-arn-format");
        props.put("dlf.region", "cn-hangzhou");

        StoragePropertiesException ex = Assertions.assertThrows(
                StoragePropertiesException.class,
                () -> AliyunDLFBaseProperties.of(props)
        );
        Assertions.assertTrue(ex.getMessage().contains("Invalid dlf.role_arn format"));
    }

    @Test
    void testRrsaValidationPassesOnDlf1x() {
        Map<String, String> props = new HashMap<>();
        props.put("dlf.role_arn", "acs:ram::123456789:role/my-role");
        props.put("dlf.region", "cn-hangzhou");
        props.put("dlf.oidc_provider_arn", "acs:oidc::123456789:oidc-provider/my-cluster");

        // Should succeed. OIDC token resolution is NOT called here — lazy at first catalog use.
        AliyunDLFBaseProperties dlfProps = AliyunDLFBaseProperties.of(props);
        Assertions.assertEquals("123456789", dlfProps.dlfUid);
        Assertions.assertEquals("", dlfProps.dlfAccessKey);
    }

    @Test
    void testGetSensitiveKeys() {
        Set<String> keys = ConnectorPropertiesUtils.getSensitiveKeys(AliyunDLFBaseProperties.class);
        Assertions.assertTrue(keys.contains("dlf.access_key"));
        Assertions.assertTrue(keys.contains("dlf.catalog.accessKeyId"));
        Assertions.assertTrue(keys.contains("dlf.secret_key"));
        Assertions.assertTrue(keys.contains("dlf.catalog.accessKeySecret"));
        Assertions.assertTrue(keys.contains("dlf.session_token"));
        Assertions.assertTrue(keys.contains("dlf.catalog.sessionToken"));
    }
}
