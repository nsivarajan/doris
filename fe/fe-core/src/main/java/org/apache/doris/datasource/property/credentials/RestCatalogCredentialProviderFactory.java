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

package org.apache.doris.datasource.property.credentials;

import org.apache.doris.datasource.property.ParamRules;
import org.apache.doris.datasource.property.credentials.aliyun.AliyunECSRestCredentialProvider;
import org.apache.doris.datasource.property.credentials.aliyun.AliyunRRSARestCredentialProvider;
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import org.apache.logging.log4j.util.Strings;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Dispatches to the correct {@link RestCatalogCredentialProvider} by matching
 * {@code iceberg.rest.credential-provider} against each provider's {@link RestCatalogCredentialProvider#providerId()}.
 *
 * <p>To add support for a new cloud, implement {@link RestCatalogCredentialProvider} and add it
 * to {@link #PROVIDERS}. No other changes required.
 */
public class RestCatalogCredentialProviderFactory {

    public static final List<RestCatalogCredentialProvider> PROVIDERS =
            Collections.unmodifiableList(Arrays.asList(
                    new AliyunECSRestCredentialProvider(),    // alibaba-cloud-ram-role (ECS + AssumeRole)
                    new AliyunRRSARestCredentialProvider()    // alibaba-cloud-rrsa    (ACK + OIDC)
                    // Add new cloud providers here
            ));

    private RestCatalogCredentialProviderFactory() {}

    public static void validate(ParamRules rules, RestCatalogCredentialContext ctx) {
        if (Strings.isBlank(ctx.getRoleArn())) {
            return;
        }
        findProvider(ctx.getCredentialProvider()).validate(rules, ctx);
    }

    public static void resolve(RestCatalogCredentialContext ctx) {
        if (Strings.isBlank(ctx.getRoleArn())) {
            return;
        }
        findProvider(ctx.getCredentialProvider()).resolve(ctx);
    }

    private static RestCatalogCredentialProvider findProvider(String credentialProvider) {
        if (Strings.isBlank(credentialProvider)) {
            throw new StoragePropertiesException(
                    "iceberg.rest.role_arn requires iceberg.rest.credential-provider to be set. "
                            + "Supported: " + providerIds() + ".");
        }
        return PROVIDERS.stream()
                .filter(p -> p.providerId().equals(credentialProvider))
                .findFirst()
                .orElseThrow(() -> new StoragePropertiesException(
                        "Unknown iceberg.rest.credential-provider: '" + credentialProvider + "'. "
                                + "Supported: " + providerIds() + "."));
    }

    private static String providerIds() {
        return PROVIDERS.stream()
                .map(RestCatalogCredentialProvider::providerId)
                .collect(Collectors.joining(", "));
    }
}
