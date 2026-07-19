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

#include "recycler/oss_accessor.h"

#include <alibabacloud/oss/OssClient.h>
#include <alibabacloud/oss/client/ClientConfiguration.h>
#include <bvar/reducer.h>
#include <gen_cpp/cloud.pb.h>

#include <iomanip>
#include <memory>
#include <mutex>
#include <sstream>
#include <utility>

#include "common/config.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/string_util.h"
#include "common/util.h"
#include "cpp/aws_common.h"
#include "cpp/oss_credential_provider.h"
#include "recycler/oss_obj_client.h"

namespace doris::cloud {

namespace oss_bvar {
bvar::LatencyRecorder oss_put_latency("oss_put");
bvar::LatencyRecorder oss_delete_object_latency("oss_delete_object");
bvar::LatencyRecorder oss_delete_objects_latency("oss_delete_objects");
bvar::LatencyRecorder oss_head_latency("oss_head");
bvar::LatencyRecorder oss_list_latency("oss_list");
} // namespace oss_bvar

// OSS LastModified is ISO-8601 UTC: "2024-01-02T03:04:05.000Z"
int64_t parse_oss_last_modified(const std::string& s) {
    if (s.empty()) {
        return INT64_MAX;
    }
    std::tm tm {};
    std::istringstream ss(s);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    if (ss.fail()) {
        LOG(WARNING) << "failed to parse OSS LastModified: " << s;
        return INT64_MAX;
    }
    return static_cast<int64_t>(timegm(&tm));
}

// Wraps ObjectListIterator to strip the vault prefix and return FileMeta
class OSSListIterator final : public ListIterator {
public:
    OSSListIterator(std::unique_ptr<ObjectListIterator> iter, size_t prefix_length)
            : iter_(std::move(iter)), prefix_length_(prefix_length) {}

    ~OSSListIterator() override = default;

    bool is_valid() override { return iter_->is_valid(); }
    bool has_next() override { return iter_->has_next(); }

    std::optional<FileMeta> next() override {
        auto obj = iter_->next();
        if (!obj.has_value()) {
            return std::nullopt;
        }
        return FileMeta {.path = obj->key.substr(prefix_length_),
                         .size = obj->size,
                         .mtime_s = obj->mtime_s};
    }

private:
    std::unique_ptr<ObjectListIterator> iter_;
    size_t prefix_length_;
};

// OSSConf

std::optional<OSSConf> OSSConf::from_obj_store_info(const ObjectStoreInfoPB& obj_info,
                                                    bool skip_aksk) {
    if (obj_info.provider() != ObjectStoreInfoPB_Provider_OSS) {
        return std::nullopt;
    }

    OSSConf conf;
    conf.endpoint = normalize_oss_endpoint(obj_info.endpoint());
    conf.bucket = obj_info.bucket();
    conf.prefix = obj_info.prefix();
    conf.region = obj_info.region();

    if (!skip_aksk) {
        if (obj_info.has_cred_provider_type()) {
            switch (obj_info.cred_provider_type()) {
            case CredProviderTypePB::INSTANCE_PROFILE:
                conf.provider_type = OSSCredProviderType::INSTANCE_PROFILE;
                break;
            case CredProviderTypePB::ENV:
                conf.provider_type = OSSCredProviderType::ENV;
                break;
            case CredProviderTypePB::ANONYMOUS:
                conf.provider_type = OSSCredProviderType::ANONYMOUS;
                break;
            case CredProviderTypePB::DEFAULT:
                conf.provider_type = OSSCredProviderType::DEFAULT;
                break;
            case CredProviderTypePB::SIMPLE:
                conf.provider_type = OSSCredProviderType::SIMPLE;
                break;
            default:
                // WEB_IDENTITY / CONTAINER are AWS-only; fall to DEFAULT chain
                conf.provider_type = OSSCredProviderType::DEFAULT;
                break;
            }
        }

        if (!obj_info.ak().empty() && !obj_info.sk().empty()) {
            if (!obj_info.has_cred_provider_type()) {
                conf.provider_type = OSSCredProviderType::SIMPLE;
            }
            if (obj_info.has_encryption_info()) {
                AkSkPair plain;
                int ret = decrypt_ak_sk_helper(obj_info.ak(), obj_info.sk(),
                                               obj_info.encryption_info(), &plain);
                if (ret != 0) {
                    LOG(WARNING) << "failed to decrypt OSS ak/sk";
                    return std::nullopt;
                }
                conf.access_key_id = std::move(plain.first);
                conf.access_key_secret = std::move(plain.second);
            } else {
                conf.access_key_id = obj_info.ak();
                conf.access_key_secret = obj_info.sk();
            }
        }

        if (obj_info.has_role_arn() && !obj_info.role_arn().empty()) {
            conf.role_arn = obj_info.role_arn();
            conf.external_id = obj_info.external_id();
        }
    }

    return conf;
}

// OSSAccessor

OSSAccessor::OSSAccessor(OSSConf conf)
        : StorageVaultAccessor(AccessorType::OSS), conf_(std::move(conf)) {
    uri_ = fmt::format("oss://{}/{}", conf_.bucket, conf_.prefix.empty() ? "" : conf_.prefix + "/");
}

OSSAccessor::~OSSAccessor() = default;

int OSSAccessor::create(OSSConf conf, std::shared_ptr<OSSAccessor>* accessor) {
    *accessor = std::make_shared<OSSAccessor>(std::move(conf));
    return (*accessor)->init();
}

int OSSAccessor::init() {
    static std::once_flag sdk_init_flag;
    std::call_once(sdk_init_flag, []() {
        AlibabaCloud::OSS::InitializeSdk();
        LOG(INFO) << "Alibaba Cloud OSS SDK initialized for recycler";
    });

    _ca_cert_file_path =
            get_valid_ca_cert_path(doris::cloud::split(config::ca_cert_file_paths, ';'));

    AlibabaCloud::OSS::ClientConfiguration oss_config;
    oss_config.maxConnections = conf_.max_connections;
    oss_config.connectTimeoutMs = conf_.connect_timeout_ms;
    oss_config.requestTimeoutMs = conf_.request_timeout_ms;
    if (!_ca_cert_file_path.empty()) {
        oss_config.caFile = _ca_cert_file_path;
    }

    std::shared_ptr<AlibabaCloud::OSS::OssClient> oss_client;

    switch (conf_.provider_type) {
    case OSSCredProviderType::SIMPLE: {
        AlibabaCloud::OSS::Credentials creds(conf_.access_key_id, conf_.access_key_secret,
                                             conf_.security_token);
        oss_client =
                std::make_shared<AlibabaCloud::OSS::OssClient>(conf_.endpoint, creds, oss_config);
        break;
    }
    case OSSCredProviderType::INSTANCE_PROFILE: {
        if (!conf_.role_arn.empty()) {
            std::string region = conf_.region.empty() ? "cn-hangzhou" : conf_.region;
            auto provider = std::make_shared<OSSSTSCredentialProvider>(
                    conf_.role_arn, region, conf_.external_id, _ca_cert_file_path);
            oss_client = std::make_shared<AlibabaCloud::OSS::OssClient>(
                    conf_.endpoint,
                    std::static_pointer_cast<AlibabaCloud::OSS::CredentialsProvider>(provider),
                    oss_config);
        } else {
            auto provider = std::make_shared<ECSMetadataCredentialsProvider>();
            oss_client = std::make_shared<AlibabaCloud::OSS::OssClient>(
                    conf_.endpoint,
                    std::static_pointer_cast<AlibabaCloud::OSS::CredentialsProvider>(provider),
                    oss_config);
        }
        break;
    }
    case OSSCredProviderType::DEFAULT: {
        auto provider = std::make_shared<OSSDefaultCredentialsProvider>();
        oss_client = std::make_shared<AlibabaCloud::OSS::OssClient>(
                conf_.endpoint,
                std::static_pointer_cast<AlibabaCloud::OSS::CredentialsProvider>(provider),
                oss_config);
        break;
    }
    case OSSCredProviderType::ENV: {
        // OSS SDK reads ALIBABA_CLOUD_ACCESS_KEY_ID / ALIBABA_CLOUD_ACCESS_KEY_SECRET
        auto provider =
                std::make_shared<AlibabaCloud::OSS::EnvironmentVariableCredentialsProvider>();
        oss_client = std::make_shared<AlibabaCloud::OSS::OssClient>(
                conf_.endpoint,
                std::static_pointer_cast<AlibabaCloud::OSS::CredentialsProvider>(provider),
                oss_config);
        break;
    }
    case OSSCredProviderType::ANONYMOUS: {
        // Public bucket access — empty credentials
        AlibabaCloud::OSS::Credentials empty_creds("", "");
        oss_client = std::make_shared<AlibabaCloud::OSS::OssClient>(conf_.endpoint, empty_creds,
                                                                    oss_config);
        break;
    }
    default:
        LOG(ERROR) << "unsupported OSS credential provider type: "
                   << static_cast<int>(conf_.provider_type);
        return -1;
    }

    obj_client_ = std::make_shared<OssObjClient>(std::move(oss_client), conf_.endpoint);
    LOG_INFO("OSS accessor initialized")
            .tag("endpoint", conf_.endpoint)
            .tag("bucket", conf_.bucket)
            .tag("prefix", conf_.prefix)
            .tag("provider_type", static_cast<int>(conf_.provider_type));
    return 0;
}

std::string OSSAccessor::get_key(const std::string& relative_path) const {
    return conf_.prefix.empty() ? relative_path : conf_.prefix + '/' + relative_path;
}

std::string OSSAccessor::to_uri(const std::string& relative_path) const {
    return uri_ + relative_path;
}

int OSSAccessor::delete_prefix(const std::string& path_prefix, int64_t expiration_time) {
    auto norm = path_prefix;
    strip_leading(norm, "/");
    if (norm.empty()) {
        LOG_WARNING("invalid path_prefix {}", path_prefix);
        return -1;
    }
    LOG_INFO("delete prefix").tag("uri", to_uri(norm));
    return obj_client_
            ->delete_objects_recursively({.bucket = conf_.bucket, .key = get_key(norm)}, {},
                                         expiration_time)
            .ret;
}

int OSSAccessor::delete_directory(const std::string& dir_path) {
    auto norm = dir_path;
    strip_leading(norm, "/");
    if (norm.empty()) {
        LOG_WARNING("invalid dir_path {}", dir_path);
        return -1;
    }
    if (!norm.ends_with('/')) {
        norm += '/';
    }
    LOG_INFO("delete directory").tag("uri", to_uri(norm));
    return obj_client_
            ->delete_objects_recursively({.bucket = conf_.bucket, .key = get_key(norm)}, {}, 0)
            .ret;
}

int OSSAccessor::delete_all(int64_t expiration_time) {
    return obj_client_
            ->delete_objects_recursively({.bucket = conf_.bucket, .key = get_key("")}, {},
                                         expiration_time)
            .ret;
}

int OSSAccessor::delete_files(const std::vector<std::string>& paths) {
    if (paths.empty()) {
        return 0;
    }
    std::vector<std::string> keys;
    keys.reserve(paths.size());
    for (auto&& path : paths) {
        LOG_INFO("delete file").tag("uri", to_uri(path));
        keys.emplace_back(get_key(path));
    }
    return obj_client_->delete_objects(conf_.bucket, std::move(keys), {}).ret;
}

int OSSAccessor::delete_file(const std::string& path) {
    LOG_INFO("delete file").tag("uri", to_uri(path));
    int ret = obj_client_->delete_object({.bucket = conf_.bucket, .key = get_key(path)}).ret;
    static_assert(ObjectStorageResponse::OK == 0);
    return (ret == ObjectStorageResponse::OK || ret == ObjectStorageResponse::NOT_FOUND) ? 0 : ret;
}

int OSSAccessor::list_prefix(const std::string& path_prefix, std::unique_ptr<ListIterator>* res) {
    size_t prefix_length = conf_.prefix.empty() ? 0 : conf_.prefix.length() + 1;
    *res = std::make_unique<OSSListIterator>(
            obj_client_->list_objects({.bucket = conf_.bucket, .key = get_key(path_prefix)}),
            prefix_length);
    return 0;
}

int OSSAccessor::list_directory(const std::string& dir_path, std::unique_ptr<ListIterator>* res) {
    auto norm = dir_path;
    strip_leading(norm, "/");
    if (norm.empty()) {
        LOG_WARNING("invalid dir_path {}", dir_path);
        return -1;
    }
    if (!norm.ends_with('/')) {
        norm += '/';
    }
    return list_prefix(norm, res);
}

int OSSAccessor::list_all(std::unique_ptr<ListIterator>* res) {
    return list_prefix("", res);
}

int OSSAccessor::put_file(const std::string& path, const std::string& content) {
    return obj_client_->put_object({.bucket = conf_.bucket, .key = get_key(path)}, content).ret;
}

int OSSAccessor::exists(const std::string& path) {
    ObjectMeta meta;
    return obj_client_->head_object({.bucket = conf_.bucket, .key = get_key(path)}, &meta).ret;
}

int OSSAccessor::abort_multipart_upload(const std::string& path, const std::string& upload_id) {
    LOG_INFO("abort multipart upload").tag("uri", to_uri(path)).tag("upload_id", upload_id);
    int ret = obj_client_
                      ->abort_multipart_upload({.bucket = conf_.bucket, .key = get_key(path)},
                                               upload_id)
                      .ret;
    static_assert(ObjectStorageResponse::OK == 0);
    if (ret == ObjectStorageResponse::OK || ret == ObjectStorageResponse::NOT_FOUND) {
        return 0;
    }
    LOG_WARNING("fail abort multipart upload")
            .tag("uri", to_uri(path))
            .tag("upload_id", upload_id)
            .tag("ret", ret);
    return ret;
}

} // namespace doris::cloud
