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
#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <chrono>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <random>
#include <algorithm>

#include "common/logging.h"

#ifdef USE_STS
#include <alibabacloud/sts/StsClient.h>
#include <alibabacloud/sts/model/AssumeRoleRequest.h>
#include <alibabacloud/sts/model/AssumeRoleResult.h>
#include <alibabacloud/sts/model/AssumeRoleWithOIDCRequest.h>
#include <alibabacloud/sts/model/AssumeRoleWithOIDCResult.h>
#endif

namespace doris {

// ============================================================================
// WORKAROUND: Manual STS AssumeRole Implementation
// ============================================================================
// Why: aliyun-openapi-cpp-sdk v1 has JSON parsing bug (empty Expiration field)
//      aliyun-openapi-cpp-sdk v2 (Darabonba) causes build dependency conflicts
// When: Can be removed when either:
//       1. v1 SDK bug is fixed upstream, OR
//       2. v2 SDK build issues are resolved
// See: sts_assumerole_implementation_summary.md for details
// ============================================================================

namespace {

// Credential redaction utility functions for secure logging
std::string redact_url(const std::string& url) {
    // Find and redact Signature parameter
    std::string redacted = url;
    size_t sig_pos = redacted.find("Signature=");
    if (sig_pos != std::string::npos) {
        size_t sig_end = redacted.find("&", sig_pos);
        if (sig_end == std::string::npos) {
            sig_end = redacted.length();
        }
        redacted.replace(sig_pos + 10, sig_end - sig_pos - 10, "[REDACTED]");
    }

    // Find and redact SecurityToken parameter
    size_t token_pos = redacted.find("SecurityToken=");
    if (token_pos != std::string::npos) {
        size_t token_end = redacted.find("&", token_pos);
        if (token_end == std::string::npos) {
            token_end = redacted.length();
        }
        redacted.replace(token_pos + 14, token_end - token_pos - 14, "[REDACTED]");
    }

    return redacted;
}

std::string redact_json_response(const std::string& response) {
    std::string redacted = response;

    // List of sensitive fields to redact
    std::vector<std::string> sensitive_fields = {
        "AccessKeyId", "AccessKeySecret", "SecurityToken",
        "accessKeyId", "accessKeySecret", "securityToken"
    };

    for (const auto& field : sensitive_fields) {
        std::string pattern = "\"" + field + "\":\"";
        size_t pos = 0;
        while ((pos = redacted.find(pattern, pos)) != std::string::npos) {
            size_t value_start = pos + pattern.length();
            size_t value_end = redacted.find("\"", value_start);
            if (value_end != std::string::npos) {
                redacted.replace(value_start, value_end - value_start, "[REDACTED]");
                pos = value_start + 10; // length of "[REDACTED]"
            } else {
                break;
            }
        }
    }

    return redacted;
}

// Curl initialization verification
bool verify_curl_initialized() {
    static bool checked = false;
    static bool initialized = false;

    if (!checked) {
        // Test if curl is initialized by checking if we can get version info
        curl_version_info_data* version_info = curl_version_info(CURLVERSION_NOW);
        initialized = (version_info != nullptr);
        checked = true;

        if (!initialized) {
            LOG(ERROR) << "libcurl is not properly initialized. "
                       << "Ensure curl_global_init() was called during application startup.";
        }
    }

    return initialized;
}

// Callback for libcurl to write response data
size_t write_callback(void* contents, size_t size, size_t nmemb, std::string* userp) {
    userp->append(static_cast<const char*>(contents), size * nmemb);
    return size * nmemb;
}

// URL encode function (RFC 3986)
std::string url_encode(const std::string& str) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;

    for (char c : str) {
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            escaped << c;
        } else {
            escaped << std::uppercase;
            escaped << '%' << std::setw(2) << int((unsigned char)c);
            escaped << std::nouppercase;
        }
    }

    return escaped.str();
}

// Base64 encode
std::string base64_encode(const unsigned char* buffer, size_t length) {
    static const char base64_chars[] =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    std::string result;
    int i = 0;
    int j = 0;
    unsigned char char_array_3[3];
    unsigned char char_array_4[4];

    while (length--) {
        char_array_3[i++] = *(buffer++);
        if (i == 3) {
            char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
            char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
            char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
            char_array_4[3] = char_array_3[2] & 0x3f;

            for (i = 0; i < 4; i++) {
                result += base64_chars[char_array_4[i]];
            }
            i = 0;
        }
    }

    if (i) {
        for (j = i; j < 3; j++) {
            char_array_3[j] = '\0';
        }

        char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
        char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
        char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);

        for (j = 0; j < i + 1; j++) {
            result += base64_chars[char_array_4[j]];
        }

        while (i++ < 3) {
            result += '=';
        }
    }

    return result;
}

// HMAC-SHA1 signature
std::string hmac_sha1(const std::string& key, const std::string& data) {
    unsigned char* digest = HMAC(EVP_sha1(), key.c_str(), key.length(),
                                  reinterpret_cast<const unsigned char*>(data.c_str()),
                                  data.length(), nullptr, nullptr);
    if (!digest) {
        LOG(WARNING) << "HMAC-SHA1 signature generation failed";
        return "";
    }
    return base64_encode(digest, SHA_DIGEST_LENGTH);
}

// Generate UUID for nonce
std::string generate_uuid() {
    // Thread-local to ensure thread safety in concurrent BE operations
    thread_local static std::random_device rd;
    thread_local static std::mt19937 gen(rd());
    thread_local static std::uniform_int_distribution<> dis(0, 15);
    thread_local static std::uniform_int_distribution<> dis2(8, 11);

    std::stringstream ss;
    ss << std::hex;
    for (int i = 0; i < 8; i++) {
        ss << dis(gen);
    }
    ss << "-";
    for (int i = 0; i < 4; i++) {
        ss << dis(gen);
    }
    ss << "-4";
    for (int i = 0; i < 3; i++) {
        ss << dis(gen);
    }
    ss << "-";
    ss << dis2(gen);
    for (int i = 0; i < 3; i++) {
        ss << dis(gen);
    }
    ss << "-";
    for (int i = 0; i < 12; i++) {
        ss << dis(gen);
    }
    return ss.str();
}

// Get current UTC timestamp in ISO 8601 format
std::string get_iso8601_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto itt = std::chrono::system_clock::to_time_t(now);
    std::ostringstream ss;
    ss << std::put_time(gmtime(&itt), "%Y-%m-%dT%H:%M:%SZ");
    return ss.str();
}

// Perform HTTP GET request using libcurl with secure logging
std::string http_get(const std::string& url) {
    if (!verify_curl_initialized()) {
        LOG(ERROR) << "Cannot perform HTTP request: libcurl not initialized";
        return "";
    }

    CURL* curl = curl_easy_init();
    if (!curl) {
        LOG(WARNING) << "Failed to initialize curl for URL: " << redact_url(url);
        return "";
    }

    std::string response_string;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);  // Verify SSL certificate
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);  // Verify hostname matches cert
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L); // 10 second timeout

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    if (res != CURLE_OK) {
        LOG(WARNING) << "curl_easy_perform() failed: " << curl_easy_strerror(res) << " for URL: "
                     << redact_url(url);
        curl_easy_cleanup(curl);
        return "";
    }

    if (http_code != 200) {
        LOG(WARNING) << "STS API returned HTTP " << http_code << ", response: "
                     << redact_json_response(response_string.substr(0, 200));
        curl_easy_cleanup(curl);
        return "";
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

// Manual STS AssumeRole API call to work around buggy SDK
// Returns JSON response as string, or empty string on error
std::string call_sts_assume_role(const std::string& access_key, const std::string& secret_key,
                                  const std::string& security_token, const std::string& role_arn,
                                  const std::string& session_name, int duration_seconds,
                                  const std::string& external_id, const std::string& region) {
    try {
        // Build request parameters
        std::map<std::string, std::string> params;
        params["Action"] = "AssumeRole";
        params["RoleArn"] = role_arn;
        params["RoleSessionName"] = session_name;
        params["DurationSeconds"] = std::to_string(duration_seconds);
        params["Format"] = "JSON";
        params["Version"] = "2015-04-01";
        params["AccessKeyId"] = access_key;
        params["SignatureMethod"] = "HMAC-SHA1";
        params["Timestamp"] = get_iso8601_timestamp();
        params["SignatureVersion"] = "1.0";
        params["SignatureNonce"] = generate_uuid();

        if (!security_token.empty()) {
            params["SecurityToken"] = security_token;
        }

        if (!external_id.empty()) {
            params["ExternalId"] = external_id;
        }

        // Sort parameters for canonical query string
        std::vector<std::pair<std::string, std::string>> sorted_params(params.begin(),
                                                                        params.end());
        std::sort(sorted_params.begin(), sorted_params.end());

        // Build canonical query string
        std::ostringstream canonical_query;
        for (size_t i = 0; i < sorted_params.size(); i++) {
            if (i > 0) canonical_query << "&";
            canonical_query << url_encode(sorted_params[i].first) << "="
                            << url_encode(sorted_params[i].second);
        }

        // Build string to sign
        std::string string_to_sign =
                "GET&" + url_encode("/") + "&" + url_encode(canonical_query.str());

        // Sign the request
        std::string signature = hmac_sha1(secret_key + "&", string_to_sign);
        if (signature.empty()) {
            LOG(WARNING) << "Failed to generate HMAC-SHA1 signature for STS request";
            return "";
        }

        // Build final URL
        std::ostringstream url;
        // Use regional endpoint for better reliability
        if (!region.empty()) {
            url << "https://sts." << region << ".aliyuncs.com/?";
        } else {
            url << "https://sts.aliyuncs.com/?";
        }

        url << canonical_query.str() << "&Signature=" << url_encode(signature);

        // Make HTTP request
        LOG(INFO) << "Calling STS AssumeRole API for role: " << role_arn;
        std::string response = http_get(url.str());

        if (response.empty()) {
            LOG(WARNING) << "STS AssumeRole API returned empty response";
            return "";
        }

        return response;

    } catch (const std::exception& e) {
        LOG(WARNING) << "Exception in STS AssumeRole API call: " << e.what();
        return "";
    }
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

AlibabaCloud::OSS::Credentials EcsRamRoleCredentialsProvider::getCredentials() {
    std::lock_guard<std::mutex> lock(_mutex);

    // Move needsRefresh check inside the lock to prevent race conditions
    auto now = std::chrono::system_clock::now();
    auto time_until_expiration =
            std::chrono::duration_cast<std::chrono::seconds>(_expiration - now).count();
    bool needs_refresh = time_until_expiration < REFRESH_THRESHOLD_SECONDS;

    if (!needs_refresh && !_cached_credentials.AccessKeyId().empty()) {
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

AlibabaCloud::OSS::Credentials StsAssumeRoleCredentialsProvider::assumeRole() {
    auto base_creds = _base_provider->getCredentials();
    if (base_creds.AccessKeyId().empty()) {
        LOG(WARNING) << "Base credentials provider returned empty credentials";
        return AlibabaCloud::OSS::Credentials("", "");
    }

    // Manual STS AssumeRole API call to work around SDK JSON parsing bug
    // See: https://github.com/aliyun/aliyun-openapi-cpp-sdk/issues/xxx
    std::string response_json = call_sts_assume_role(
            base_creds.AccessKeyId(), base_creds.AccessKeySecret(), base_creds.SessionToken(),
            _role_arn, _session_name, _duration_seconds, _external_id, "");

    if (response_json.empty()) {
        LOG(WARNING) << "STS AssumeRole API call failed, empty response";
        return AlibabaCloud::OSS::Credentials("", "");
    }

    // Parse JSON response using rapidjson
    rapidjson::Document doc;
    doc.Parse(response_json.c_str());

    if (doc.HasParseError()) {
        LOG(WARNING) << "Failed to parse STS response JSON: "
                     << rapidjson::GetParseError_En(doc.GetParseError())
                     << ", response: " << redact_json_response(response_json.substr(0, 200));
        return AlibabaCloud::OSS::Credentials("", "");
    }

    // Check for error response
    if (doc.HasMember("Code") && doc.HasMember("Message")) {
        LOG(WARNING) << "STS AssumeRole failed: " << doc["Message"].GetString()
                     << ", Code: " << doc["Code"].GetString() << ", Role: " << _role_arn;
        return AlibabaCloud::OSS::Credentials("", "");
    }

    // Extract credentials from response
    if (!doc.HasMember("Credentials")) {
        LOG(WARNING) << "STS response missing Credentials field, response: "
                     << redact_json_response(response_json.substr(0, 200));
        return AlibabaCloud::OSS::Credentials("", "");
    }

    auto& creds = doc["Credentials"];

    if (!creds.HasMember("AccessKeyId") || !creds.HasMember("AccessKeySecret") ||
        !creds.HasMember("SecurityToken") || !creds.HasMember("Expiration")) {
        LOG(WARNING) << "STS Credentials missing required fields";
        return AlibabaCloud::OSS::Credentials("", "");
    }

    // Validate JSON field types before calling GetString()
    if (!creds["AccessKeyId"].IsString() || !creds["AccessKeySecret"].IsString() ||
        !creds["SecurityToken"].IsString() || !creds["Expiration"].IsString()) {
        LOG(WARNING) << "STS Credentials have invalid field types";
        return AlibabaCloud::OSS::Credentials("", "");
    }

    std::string access_key = creds["AccessKeyId"].GetString();
    std::string secret_key = creds["AccessKeySecret"].GetString();
    std::string token = creds["SecurityToken"].GetString();
    std::string expiration_str = creds["Expiration"].GetString();

    if (access_key.empty() || secret_key.empty() || token.empty() || expiration_str.empty()) {
        LOG(WARNING) << "STS Credentials contain empty fields: "
                     << "AK=" << (access_key.empty() ? "empty" : "ok") << ", "
                     << "SK=" << (secret_key.empty() ? "empty" : "ok") << ", "
                     << "Token=" << (token.empty() ? "empty" : "ok") << ", "
                     << "Expiration=" << (expiration_str.empty() ? "empty" : "ok");
        return AlibabaCloud::OSS::Credentials("", "");
    }

    // Parse expiration time for auto-refresh
    _expiration = parse_iso8601(expiration_str);

    LOG(INFO) << "STS AssumeRole successful (manual API). Role: " << _role_arn << ", Session: "
              << _session_name << ", Expires at: " << expiration_str;

    return AlibabaCloud::OSS::Credentials(access_key, secret_key, token);
}

AlibabaCloud::OSS::Credentials StsAssumeRoleCredentialsProvider::getCredentials() {
    std::lock_guard<std::recursive_mutex> lock(_mutex);

    // Move needsRefresh check inside the lock to prevent race conditions
    auto now = std::chrono::system_clock::now();
    auto time_until_expiration =
            std::chrono::duration_cast<std::chrono::seconds>(_expiration - now).count();
    bool needs_refresh = time_until_expiration < REFRESH_THRESHOLD_SECONDS;

    if (!needs_refresh && !_cached_credentials.AccessKeyId().empty()) {
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
