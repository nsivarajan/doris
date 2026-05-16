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

package org.apache.doris.datasource.property.credentials.aliyun;

import org.apache.doris.datasource.property.ParamRules;
import org.apache.doris.datasource.property.credentials.RestCatalogCredentialContext;
import org.apache.doris.datasource.property.credentials.RestCatalogCredentialProvider;

import org.apache.logging.log4j.util.Strings;

/**
 * Base class for all Alibaba Cloud REST catalog credential providers.
 * Handles shared logic: signing-region validation, idempotency, credential injection.
 * Subclasses implement {@link #acquireCredentials} and optionally {@link #validateExtra}.
 */
public abstract class AliyunRestCredentialProvider implements RestCatalogCredentialProvider {

    @Override
    public final void validate(ParamRules rules, RestCatalogCredentialContext ctx) {
        if (Strings.isNotBlank(ctx.getRoleArn())) {
            rules.require(ctx.getSigningRegion(),
                    "iceberg.rest.signing-region is required when iceberg.rest.role_arn is set");
            validateExtra(rules, ctx);
        }
    }

    @Override
    public final void resolve(RestCatalogCredentialContext ctx) {
        if (Strings.isNotBlank(ctx.getAccessKeyId())) {
            return;
        }
        AliyunSTSCredentialResolver.Credentials creds = acquireCredentials(ctx);
        ctx.setResolvedCredentials(
                creds.accessKeyId, creds.accessKeySecret, creds.securityToken, creds.expiration);
    }

    /** Hook for subclass-specific validation. Default: no-op. */
    protected void validateExtra(ParamRules rules, RestCatalogCredentialContext ctx) {}

    /** Subclasses implement the credential acquisition strategy. */
    protected abstract AliyunSTSCredentialResolver.Credentials acquireCredentials(
            RestCatalogCredentialContext ctx);
}
