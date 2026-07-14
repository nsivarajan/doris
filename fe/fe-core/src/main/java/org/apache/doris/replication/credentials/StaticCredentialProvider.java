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

package org.apache.doris.replication.credentials;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Returns static AK/SK from config.
 * FOR DEVELOPMENT AND TESTING ONLY — always logs a WARNING when constructed
 * so accidental production use is visible in logs.
 */
public class StaticCredentialProvider implements ReplicationCredentialProvider {

    private static final Logger LOG = LogManager.getLogger(StaticCredentialProvider.class);

    private final ReplicationCredentials credentials;

    public StaticCredentialProvider(String accessKey, String secretKey) {
        // warn on every construction so prod misuse is caught in log review
        LOG.warn("[Replication] StaticCredentialProvider constructed with AK/SK. "
                + "This is for dev/test only. Use instance_profile or assume_role in production.");
        this.credentials = ReplicationCredentials.longTerm(accessKey, secretKey);
    }

    @Override
    public ReplicationCredentials getCredentials() {
        return credentials;
    }

    @Override
    public String describe() {
        return "Static(ak=" + credentials.accessKey.substring(0, Math.min(4, credentials.accessKey.length())) + "***)";
    }
}
