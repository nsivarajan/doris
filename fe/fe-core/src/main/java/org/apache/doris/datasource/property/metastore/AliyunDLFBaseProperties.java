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
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.ParamRules;
import org.apache.doris.datasource.property.credentials.aliyun.AliyunSTSCredentialResolver;
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import com.aliyun.datalake.metastore.common.DataLakeConfig;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class AliyunDLFBaseProperties {

    @ConnectorProperty(names = {"dlf.access_key", "dlf.catalog.accessKeyId"},
            required = false,
            sensitive = true,
            description = "The access key of the Aliyun DLF. Not required when dlf.role_arn is set.")
    protected String dlfAccessKey = "";

    @ConnectorProperty(names = {"dlf.secret_key", "dlf.catalog.accessKeySecret"},
            required = false,
            sensitive = true,
            description = "The secret key of the Aliyun DLF. Not required when dlf.role_arn is set.")
    protected String dlfSecretKey = "";

    @ConnectorProperty(names = {"dlf.session_token", "dlf.catalog.sessionToken"},
            required = false,
            sensitive = true,
            description = "The session token of the Aliyun DLF.")
    protected String dlfSessionToken = "";

    @ConnectorProperty(names = {"dlf.region"},
            required = false,
            description = "The region of the Aliyun DLF.")
    protected String dlfRegion = "";

    @ConnectorProperty(names = {"dlf.endpoint", "dlf.catalog.endpoint"},
            required = false,
            description = "The endpoint of the Aliyun DLF.")
    protected String dlfEndpoint = "";

    @ConnectorProperty(names = {"dlf.catalog.uid", "dlf.uid"},
            required = false,
            description = "The Alibaba Cloud account ID. Optional when dlf.role_arn is set "
                    + "— the account ID is extracted from the ARN automatically.")
    protected String dlfUid = "";

    @ConnectorProperty(names = {"dlf.catalog.id", "dlf.catalog_id"},
            required = false,
            description = "The catalog id of the Aliyun DLF. If not set, it will be the same as dlf.uid.")
    protected String dlfCatalogId = "";

    @ConnectorProperty(names = {"dlf.access.public", "dlf.catalog.accessPublic"},
            required = false,
            description = "Enable public access to Aliyun DLF.")
    protected String dlfAccessPublic = "false";

    @ConnectorProperty(names = {DataLakeConfig.CATALOG_PROXY_MODE, "dlf.proxy.mode"},
            required = false,
            description = "The proxy mode of the Aliyun DLF. Default is DLF_ONLY.")
    protected String dlfProxyMode = "DLF_ONLY";

    @ConnectorProperty(names = {"dlf.role_arn"},
            required = false,
            description = "RAM Role ARN for AssumeRole. When set, ECS instance profile credentials "
                    + "are used as base to call STS AssumeRole. dlf.access_key and dlf.secret_key "
                    + "are not required. dlf.region must be set.")
    protected String dlfRoleArn = "";

    @ConnectorProperty(names = {"dlf.sts_endpoint"},
            required = false,
            description = "STS endpoint for AssumeRole. Defaults to sts.<region>.aliyuncs.com. "
                    + "Use sts-vpc.<region>.aliyuncs.com to keep traffic inside VPC.")
    protected String dlfStsEndpoint = "";

    // TODO: use dlfCredentialExpiration to auto-renew STS credentials before expiry.
    protected String dlfCredentialExpiration = "";

    public static AliyunDLFBaseProperties of(Map<String, String> properties) {
        AliyunDLFBaseProperties propertiesObj = new AliyunDLFBaseProperties();
        ConnectorPropertiesUtils.bindConnectorProperties(propertiesObj, properties);
        propertiesObj.checkAndInit();
        return propertiesObj;
    }

    private ParamRules buildRules() {
        if (StringUtils.isNotBlank(dlfRoleArn)) {
            return new ParamRules()
                    .require(dlfRegion, "dlf.region is required when dlf.role_arn is set");
        }
        return new ParamRules()
                .require(dlfAccessKey, "dlf.access_key is required")
                .require(dlfSecretKey, "dlf.secret_key is required");
    }

    private void checkAndInit() {
        buildRules().validate();
        if (StringUtils.isBlank(dlfUid) && StringUtils.isNotBlank(dlfRoleArn)) {
            dlfUid = extractAccountIdFromRoleArn(dlfRoleArn);
        }
        if (StringUtils.isBlank(dlfEndpoint) && StringUtils.isNotBlank(dlfRegion)) {
            if (BooleanUtils.toBoolean(dlfAccessPublic)) {
                dlfEndpoint = "dlf." + dlfRegion + ".aliyuncs.com";
            } else {
                dlfEndpoint = "dlf-vpc." + dlfRegion + ".aliyuncs.com";
            }
        }
        if (StringUtils.isBlank(dlfEndpoint)) {
            throw new StoragePropertiesException("dlf.endpoint is required.");
        }
        if (StringUtils.isBlank(dlfCatalogId)) {
            dlfCatalogId = dlfUid;
        }
    }

    // Resolves STS credentials lazily at connection time. Idempotent: skips if already resolved or AK/SK path.
    public synchronized void resolveCredentials() {
        if (StringUtils.isNotBlank(dlfRoleArn) && StringUtils.isBlank(dlfAccessKey)) {
            AliyunSTSCredentialResolver.Credentials creds =
                    AliyunSTSCredentialResolver.resolve(dlfRoleArn, dlfRegion, dlfStsEndpoint);
            dlfAccessKey = creds.accessKeyId;
            dlfSecretKey = creds.accessKeySecret;
            dlfSessionToken = creds.securityToken;
            dlfCredentialExpiration = creds.expiration;
        }
    }

    // ARN format: acs:ram::<account_id>:role/<role_name>
    private static String extractAccountIdFromRoleArn(String roleArn) {
        String[] parts = roleArn.split("::");
        if (parts.length < 2) {
            throw new StoragePropertiesException(
                    "Invalid dlf.role_arn format. Expected acs:ram::<account_id>:role/<name>, got: " + roleArn);
        }
        int colonIdx = parts[1].indexOf(':');
        if (colonIdx <= 0) {
            throw new StoragePropertiesException(
                    "Cannot extract account ID from dlf.role_arn: " + roleArn);
        }
        return parts[1].substring(0, colonIdx);
    }
}
