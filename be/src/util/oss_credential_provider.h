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

#pragma once

#ifdef USE_OSS

#include <alibabacloud/oss/auth/Credentials.h>
#include <alibabacloud/oss/auth/CredentialsProvider.h>

#include <chrono>
#include <memory>
#include <mutex>
#include <string>

namespace doris {

// ECS RAM Role Credentials Provider
// Fetches credentials from ECS metadata service at http://100.100.100.200
class EcsRamRoleCredentialsProvider : public AlibabaCloud::OSS::CredentialsProvider {
public:
    EcsRamRoleCredentialsProvider();
    ~EcsRamRoleCredentialsProvider() override = default;

    AlibabaCloud::OSS::Credentials getCredentials() override;

private:
    std::string fetchRoleName();
    AlibabaCloud::OSS::Credentials fetchCredentialsFromMetadata(const std::string& role_name);
    bool needsRefresh() const;

    mutable std::mutex _mutex;
    AlibabaCloud::OSS::Credentials _cached_credentials;
    std::chrono::system_clock::time_point _expiration;
    std::string _role_name;

    static constexpr const char* ECS_METADATA_SERVICE_HOST = "100.100.100.200";
    static constexpr const char* ECS_METADATA_FETCH_ROLE_URL =
            "http://100.100.100.200/latest/meta-data/ram/security-credentials/";
    static constexpr int REFRESH_THRESHOLD_SECONDS = 180; // Refresh 3 minutes before expiration
};

// STS AssumeRole Credentials Provider
// Uses base credentials to assume another role via STS
class StsAssumeRoleCredentialsProvider : public AlibabaCloud::OSS::CredentialsProvider {
public:
    StsAssumeRoleCredentialsProvider(
            std::shared_ptr<AlibabaCloud::OSS::CredentialsProvider> base_provider,
            const std::string& role_arn, const std::string& session_name, int duration_seconds,
            const std::string& external_id = "");
    ~StsAssumeRoleCredentialsProvider() override = default;

    AlibabaCloud::OSS::Credentials getCredentials() override;

private:
    AlibabaCloud::OSS::Credentials assumeRole();
    bool needsRefresh() const;

    std::shared_ptr<AlibabaCloud::OSS::CredentialsProvider> _base_provider;
    std::string _role_arn;
    std::string _session_name;
    int _duration_seconds;
    std::string _external_id;

    mutable std::recursive_mutex _mutex;  // Recursive to allow safe nested locking
    AlibabaCloud::OSS::Credentials _cached_credentials;
    std::chrono::system_clock::time_point _expiration;

    static constexpr int REFRESH_THRESHOLD_SECONDS = 180;
};

// Default Credentials Provider with RRSA support
// Checks multiple sources in order:
// 1. Environment variables (ALIBABA_CLOUD_ACCESS_KEY_ID, ALIBABA_CLOUD_ACCESS_KEY_SECRET)
// 2. RRSA OIDC Token (if ALIBABA_CLOUD_OIDC_TOKEN_FILE exists)
// 3. ECS RAM Role (Instance Metadata Service)
class DefaultCredentialsProvider : public AlibabaCloud::OSS::CredentialsProvider {
public:
    DefaultCredentialsProvider();
    ~DefaultCredentialsProvider() override = default;

    AlibabaCloud::OSS::Credentials getCredentials() override;

private:
    AlibabaCloud::OSS::Credentials getCredentialsFromEnvironment();
    AlibabaCloud::OSS::Credentials getCredentialsFromRRSA();
    AlibabaCloud::OSS::Credentials getCredentialsFromECS();

    std::shared_ptr<AlibabaCloud::OSS::CredentialsProvider> _provider;
};

} // namespace doris

#endif // USE_OSS
