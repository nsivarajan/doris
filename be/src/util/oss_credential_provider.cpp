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

#include "util/oss_credential_provider.h"

#ifdef USE_OSS

#include <curl/curl.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/stringbuffer.h>

#include <chrono>
#include <fstream>
#include <sstream>

#include "common/logging.h"

#ifdef USE_STS
#include <alibabacloud/sts/StsClient.h>
#include <alibabacloud/sts/model/AssumeRoleRequest.h>
#include <alibabacloud/sts/model/AssumeRoleResult.h>
#include <alibabacloud/sts/model/AssumeRoleWithOIDCRequest.h>
#include <alibabacloud/sts/model/AssumeRoleWithOIDCResult.h>
#endif

namespace doris {

namespace {

// Callback for libcurl to write response data
size_t write_callback(void* contents, size_t size, size_t nmemb, std::string* userp) {
    userp->append((char*)contents, size * nmemb);
    return size * nmemb;
}

// Perform HTTP GET request using libcurl
std::string http_get(const std::string& url) {
    CURL* curl = curl_easy_init();
    if (!curl) {
        LOG(WARNING) << "Failed to initialize curl for URL: " << url;
        return "";
    }

    std::string response_string;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L); // 10 second timeout

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
        LOG(WARNING) << "curl_easy_perform() failed: " << curl_easy_strerror(res) << " for URL: "
                     << url;
    }

    curl_easy_cleanup(curl);
    return response_string;
}

// Parse ISO 8601 datetime string to system_clock::time_point
std::chrono::system_clock::time_point parse_iso8601(const std::string& datetime_str) {
    // Parse format: 2024-11-30T12:34:56Z
    std::tm tm = {};
    std::istringstream ss(datetime_str);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");

    if (ss.fail()) {
        LOG(WARNING) << "Failed to parse datetime: " << datetime_str;
        return std::chrono::system_clock::now();
    }

    std::time_t time = timegm(&tm);
    return std::chrono::system_clock::from_time_t(time);
}

} // anonymous namespace

// ==================== EcsRamRoleCredentialsProvider ====================

EcsRamRoleCredentialsProvider::EcsRamRoleCredentialsProvider()
        : _cached_credentials("", ""), _expiration(std::chrono::system_clock::now()) {}

std::string EcsRamRoleCredentialsProvider::fetchRoleName() {
    std::string role_name = http_get(ECS_METADATA_FETCH_ROLE_URL);
    if (role_name.empty()) {
        LOG(WARNING) << "Failed to fetch ECS RAM role name from metadata service";
        return "";
    }

    // Trim whitespace
    role_name.erase(role_name.find_last_not_of(" \n\r\t") + 1);
    return role_name;
}

AlibabaCloud::OSS::Credentials EcsRamRoleCredentialsProvider::fetchCredentialsFromMetadata(
        const std::string& role_name) {
    std::string url = std::string(ECS_METADATA_FETCH_ROLE_URL) + role_name;
    std::string response = http_get(url);

    if (response.empty()) {
        LOG(WARNING) << "Failed to fetch credentials from ECS metadata service for role: "
                     << role_name;
        return AlibabaCloud::OSS::Credentials("", "");
    }

    // Parse JSON response
    rapidjson::Document doc;
    doc.Parse(response.c_str());

    if (doc.HasParseError()) {
        LOG(WARNING) << "Failed to parse ECS metadata JSON: "
                     << rapidjson::GetParseError_En(doc.GetParseError());
        return AlibabaCloud::OSS::Credentials("", "");
    }

    if (!doc.HasMember("AccessKeyId") || !doc.HasMember("AccessKeySecret") ||
        !doc.HasMember("SecurityToken") || !doc.HasMember("Expiration")) {
        LOG(WARNING) << "Invalid ECS metadata response, missing required fields";
        return AlibabaCloud::OSS::Credentials("", "");
    }

    std::string access_key_id = doc["AccessKeyId"].GetString();
    std::string access_key_secret = doc["AccessKeySecret"].GetString();
    std::string security_token = doc["SecurityToken"].GetString();
    std::string expiration_str = doc["Expiration"].GetString();

    _expiration = parse_iso8601(expiration_str);

    return AlibabaCloud::OSS::Credentials(access_key_id, access_key_secret, security_token);
}

bool EcsRamRoleCredentialsProvider::needsRefresh() const {
    auto now = std::chrono::system_clock::now();
    auto time_until_expiration =
            std::chrono::duration_cast<std::chrono::seconds>(_expiration - now).count();
    return time_until_expiration < REFRESH_THRESHOLD_SECONDS;
}

AlibabaCloud::OSS::Credentials EcsRamRoleCredentialsProvider::getCredentials() {
    std::lock_guard<std::mutex> lock(_mutex);

    if (!needsRefresh() && !_cached_credentials.AccessKeyId().empty()) {
        return _cached_credentials;
    }

    // Fetch role name if not cached
    if (_role_name.empty()) {
        _role_name = fetchRoleName();
        if (_role_name.empty()) {
            LOG(WARNING) << "Failed to get ECS RAM role name";
            return AlibabaCloud::OSS::Credentials("", "");
        }
    }

    // Fetch credentials
    _cached_credentials = fetchCredentialsFromMetadata(_role_name);

    if (_cached_credentials.AccessKeyId().empty()) {
        LOG(WARNING) << "Failed to get credentials from ECS metadata service";
    }

    return _cached_credentials;
}

// ==================== StsAssumeRoleCredentialsProvider ====================

StsAssumeRoleCredentialsProvider::StsAssumeRoleCredentialsProvider(
        std::shared_ptr<AlibabaCloud::OSS::CredentialsProvider> base_provider,
        const std::string& role_arn, const std::string& session_name, int duration_seconds,
        const std::string& external_id)
        : _base_provider(base_provider),
          _role_arn(role_arn),
          _session_name(session_name),
          _duration_seconds(duration_seconds),
          _external_id(external_id),
          _cached_credentials("", ""),
          _expiration(std::chrono::system_clock::now()) {}

bool StsAssumeRoleCredentialsProvider::needsRefresh() const {
    auto now = std::chrono::system_clock::now();
    auto time_until_expiration =
            std::chrono::duration_cast<std::chrono::seconds>(_expiration - now).count();
    return time_until_expiration < REFRESH_THRESHOLD_SECONDS;
}

AlibabaCloud::OSS::Credentials StsAssumeRoleCredentialsProvider::assumeRole() {
    auto base_creds = _base_provider->getCredentials();
    if (base_creds.AccessKeyId().empty()) {
        LOG(WARNING) << "Base credentials provider returned empty credentials";
        return AlibabaCloud::OSS::Credentials("", "");
    }

#ifdef USE_STS
    try {
        // Create client configuration
        AlibabaCloud::ClientConfiguration config;
        config.setEndpoint("sts.aliyuncs.com");
        config.setConnectTimeout(5000);
        config.setReadTimeout(10000);

        // Create credentials from base provider
        AlibabaCloud::Credentials credentials(base_creds.AccessKeyId(),
                                              base_creds.AccessKeySecret(),
                                              base_creds.SessionToken());

        // Create STS client
        AlibabaCloud::Sts::StsClient client(credentials, config);

        // Create AssumeRole request
        AlibabaCloud::Sts::Model::AssumeRoleRequest request;
        request.setRoleArn(_role_arn);
        request.setRoleSessionName(_session_name);
        request.setDurationSeconds(_duration_seconds);

        // Set external_id if provided
        if (!_external_id.empty()) {
            request.setExternalId(_external_id);
        }

        // Call STS AssumeRole API
        auto outcome = client.assumeRole(request);

        if (!outcome.isSuccess()) {
            LOG(WARNING) << "STS AssumeRole failed: " << outcome.error().errorMessage()
                         << ", Code: " << outcome.error().errorCode()
                         << ", Role ARN: " << _role_arn;
            return AlibabaCloud::OSS::Credentials("", "");
        }

        // Extract credentials from result
        auto result = outcome.result();
        auto creds = result.getCredentials();

        std::string access_key = creds.accessKeyId;
        std::string secret_key = creds.accessKeySecret;
        std::string token = creds.securityToken;
        std::string expiration_str = creds.expiration;

        // Parse expiration time for auto-refresh
        _expiration = parse_iso8601(expiration_str);

        LOG(INFO) << "STS AssumeRole successful. Role: " << _role_arn << ", Session: "
                  << _session_name << ", Expires at: " << expiration_str;

        return AlibabaCloud::OSS::Credentials(access_key, secret_key, token);

    } catch (const std::exception& e) {
        LOG(WARNING) << "STS AssumeRole exception: " << e.what() << ", Role ARN: " << _role_arn;
        return AlibabaCloud::OSS::Credentials("", "");
    }
#else
    LOG(WARNING) << "STS AssumeRole requires AliCloud STS SDK. "
                 << "Build with BUILD_STS=ON to enable this feature. "
                 << "Role ARN: " << _role_arn << ", Session: " << _session_name;
    return AlibabaCloud::OSS::Credentials("", "");
#endif
}

AlibabaCloud::OSS::Credentials StsAssumeRoleCredentialsProvider::getCredentials() {
    std::lock_guard<std::mutex> lock(_mutex);

    if (!needsRefresh() && !_cached_credentials.AccessKeyId().empty()) {
        return _cached_credentials;
    }

    _cached_credentials = assumeRole();
    return _cached_credentials;
}

// ==================== DefaultCredentialsProvider ====================

DefaultCredentialsProvider::DefaultCredentialsProvider() {}

AlibabaCloud::OSS::Credentials DefaultCredentialsProvider::getCredentialsFromEnvironment() {
    const char* ak = std::getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
    const char* sk = std::getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");
    const char* token = std::getenv("ALIBABA_CLOUD_SECURITY_TOKEN");

    if (ak && sk) {
        return AlibabaCloud::OSS::Credentials(ak, sk, token ? token : "");
    }

    return AlibabaCloud::OSS::Credentials("", "");
}

AlibabaCloud::OSS::Credentials DefaultCredentialsProvider::getCredentialsFromRRSA() {
    const char* oidc_token_file = std::getenv("ALIBABA_CLOUD_OIDC_TOKEN_FILE");
    const char* role_arn = std::getenv("ALIBABA_CLOUD_ROLE_ARN");

    if (!oidc_token_file || !role_arn) {
        return AlibabaCloud::OSS::Credentials("", "");
    }

    // Read OIDC token from file
    std::ifstream token_file(oidc_token_file);
    if (!token_file.is_open()) {
        LOG(WARNING) << "Failed to open OIDC token file: " << oidc_token_file;
        return AlibabaCloud::OSS::Credentials("", "");
    }

    std::string oidc_token((std::istreambuf_iterator<char>(token_file)),
                           std::istreambuf_iterator<char>());
    token_file.close();

    if (oidc_token.empty()) {
        LOG(WARNING) << "OIDC token file is empty: " << oidc_token_file;
        return AlibabaCloud::OSS::Credentials("", "");
    }

#ifdef USE_STS
    try {
        // Create client configuration (OIDC doesn't need credentials)
        AlibabaCloud::ClientConfiguration config;
        config.setEndpoint("sts.aliyuncs.com");
        config.setConnectTimeout(5000);
        config.setReadTimeout(10000);

        // Create STS client with empty credentials for OIDC
        AlibabaCloud::Credentials empty_creds("", "");
        AlibabaCloud::Sts::StsClient client(empty_creds, config);

        // Create AssumeRoleWithOIDC request
        AlibabaCloud::Sts::Model::AssumeRoleWithOIDCRequest request;
        request.setRoleArn(role_arn);
        request.setOIDCToken(oidc_token);
        request.setRoleSessionName("doris-rrsa-session");
        request.setDurationSeconds(3600);

        const char* oidc_provider_arn = std::getenv("ALIBABA_CLOUD_OIDC_PROVIDER_ARN");
        if (oidc_provider_arn) {
            request.setOIDCProviderArn(oidc_provider_arn);
        }

        // Call STS AssumeRoleWithOIDC API
        auto outcome = client.assumeRoleWithOIDC(request);

        if (!outcome.isSuccess()) {
            LOG(WARNING) << "RRSA AssumeRoleWithOIDC failed: " << outcome.error().errorMessage()
                         << ", Code: " << outcome.error().errorCode()
                         << ", Role ARN: " << role_arn;
            return AlibabaCloud::OSS::Credentials("", "");
        }

        // Extract credentials from result
        auto result = outcome.result();
        auto creds = result.getCredentials();

        LOG(INFO) << "RRSA AssumeRoleWithOIDC successful. Role: " << role_arn;

        return AlibabaCloud::OSS::Credentials(creds.accessKeyId, creds.accessKeySecret,
                                              creds.securityToken);

    } catch (const std::exception& e) {
        LOG(WARNING) << "RRSA AssumeRoleWithOIDC exception: " << e.what()
                     << ", Role ARN: " << role_arn;
        return AlibabaCloud::OSS::Credentials("", "");
    }
#else
    LOG(INFO) << "RRSA detected but AssumeRoleWithOIDC requires AliCloud STS SDK. "
              << "Build with BUILD_STS=ON to enable RRSA support. "
              << "Role ARN: " << role_arn << ", Token file: " << oidc_token_file;
    return AlibabaCloud::OSS::Credentials("", "");
#endif
}

AlibabaCloud::OSS::Credentials DefaultCredentialsProvider::getCredentialsFromECS() {
    auto ecs_provider = std::make_shared<EcsRamRoleCredentialsProvider>();
    return ecs_provider->getCredentials();
}

AlibabaCloud::OSS::Credentials DefaultCredentialsProvider::getCredentials() {
    // Try environment variables first
    auto creds = getCredentialsFromEnvironment();
    if (!creds.AccessKeyId().empty()) {
        return creds;
    }

    // Try RRSA (Kubernetes)
    creds = getCredentialsFromRRSA();
    if (!creds.AccessKeyId().empty()) {
        return creds;
    }

    // Try ECS RAM Role
    creds = getCredentialsFromECS();
    if (!creds.AccessKeyId().empty()) {
        return creds;
    }

    LOG(WARNING) << "Failed to get credentials from all sources (env, RRSA, ECS)";
    return AlibabaCloud::OSS::Credentials("", "");
}

} // namespace doris

#endif // USE_OSS
