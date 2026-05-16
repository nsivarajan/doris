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

/**
 * Credential provider for Alibaba Cloud REST catalogs running on ACK pods with RRSA enabled.
 * Acquires credentials via OIDC token (injected by ACK) + sts:AssumeRoleWithOIDC.
 * Activated by: {@code iceberg.rest.credential-provider = 'alibaba-cloud-rrsa'}.
 *
 * <p>Prerequisites: ACK cluster with RRSA enabled, ServiceAccount annotated with
 * the target RAM Role ARN, and ALIBABA_CLOUD_OIDC_TOKEN_FILE env var set by ACK.
 */
public class AliyunRRSARestCredentialProvider extends AliyunRestCredentialProvider {

    public static final String PROVIDER_ID = "alibaba-cloud-rrsa";

    @Override
    public String providerId() {
        return PROVIDER_ID;
    }

    @Override
    protected void validateExtra(ParamRules rules, RestCatalogCredentialContext ctx) {
        rules.require(ctx.getOidcProviderArn(),
                "iceberg.rest.oidc_provider_arn is required for alibaba-cloud-rrsa. "
                        + "Format: acs:oidc::<account_id>:oidc-provider/<cluster_id>.");
    }

    @Override
    protected AliyunSTSCredentialResolver.Credentials acquireCredentials(RestCatalogCredentialContext ctx) {
        return AliyunOIDCCredentialResolver.resolve(
                ctx.getRoleArn(), ctx.getOidcProviderArn(),
                ctx.getOidcTokenFile(), ctx.getSigningRegion(), ctx.getStsEndpoint());
    }
}
