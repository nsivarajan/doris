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

/**
 * SPI for cloud-specific credential resolution in Iceberg REST catalogs.
 * Selected via {@code iceberg.rest.credential-provider} matching {@link #providerId()}.
 *
 * <p>To add a new cloud provider, implement this interface and register in
 * {@link RestCatalogCredentialProviderFactory#PROVIDERS}.
 */
public interface RestCatalogCredentialProvider {

    /** Unique provider identifier matched against {@code iceberg.rest.credential-provider}. */
    String providerId();

    /** Adds cloud-specific validation rules evaluated at CREATE CATALOG time. */
    void validate(ParamRules rules, RestCatalogCredentialContext ctx);

    /** Resolves credentials and injects them into ctx. No-op if already resolved. */
    void resolve(RestCatalogCredentialContext ctx);
}
