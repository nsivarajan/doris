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

#include "recycler/oss_obj_client.h"

#include <alibabacloud/oss/OssClient.h>
#include <alibabacloud/oss/model/AbortMultipartUploadRequest.h>
#include <alibabacloud/oss/model/DeleteObjectsRequest.h>
#include <alibabacloud/oss/model/ListObjectsRequest.h>
#include <alibabacloud/oss/model/PutObjectRequest.h>

#include <algorithm>
#include <ranges>
#include <sstream>

#include "common/logging.h"
#include "common/stopwatch.h"
#include "recycler/oss_accessor.h"
#include "recycler/util.h"

namespace doris::cloud {

// OSS paginated list iterator
class OssObjListIterator final : public ObjectListIterator {
public:
    OssObjListIterator(std::shared_ptr<AlibabaCloud::OSS::OssClient> client, std::string bucket,
                       std::string prefix, std::string endpoint)
            : client_(std::move(client)),
              bucket_(std::move(bucket)),
              prefix_(std::move(prefix)),
              endpoint_(std::move(endpoint)) {}

    ~OssObjListIterator() override = default;

    bool is_valid() override { return is_valid_; }

    bool has_next() override {
        if (!is_valid_) {
            return false;
        }
        if (!results_.empty()) {
            return true;
        }
        if (!has_more_) {
            return false;
        }
        return fetch_next_batch();
    }

    std::optional<ObjectMeta> next() override {
        if (!has_next()) {
            return std::nullopt;
        }
        auto meta = std::move(results_.back());
        results_.pop_back();
        return meta;
    }

private:
    bool fetch_next_batch() {
        AlibabaCloud::OSS::ListObjectsRequest request(bucket_);
        request.setPrefix(prefix_);
        request.setMaxKeys(1000);
        if (!next_marker_.empty()) {
            request.setMarker(next_marker_);
        }

        auto outcome = [&] {
            SCOPED_BVAR_LATENCY(oss_bvar::oss_list_latency);
            return client_->ListObjects(request);
        }();

        if (!outcome.isSuccess()) {
            LOG_WARNING("failed to list OSS objects")
                    .tag("endpoint", endpoint_)
                    .tag("bucket", bucket_)
                    .tag("prefix", prefix_)
                    .tag("error_code", outcome.error().Code())
                    .tag("error", outcome.error().Message());
            is_valid_ = false;
            return false;
        }

        const auto& result = outcome.result();
        has_more_ = result.IsTruncated();
        next_marker_ = result.NextMarker();

        const auto& objects = result.ObjectSummarys();
        results_.reserve(objects.size());
        for (const auto& obj : std::ranges::reverse_view(objects)) {
            results_.emplace_back(
                    ObjectMeta {.key = obj.Key(),
                                .size = obj.Size(),
                                .mtime_s = parse_oss_last_modified(obj.LastModified())});
        }
        return !results_.empty() || has_more_;
    }

    std::shared_ptr<AlibabaCloud::OSS::OssClient> client_;
    std::string bucket_;
    std::string prefix_;
    std::string endpoint_;
    std::string next_marker_;
    std::vector<ObjectMeta> results_;
    bool is_valid_ {true};
    bool has_more_ {true};
};

static constexpr size_t MAX_DELETE_BATCH = 1000;

OssObjClient::~OssObjClient() = default;

ObjectStorageResponse OssObjClient::put_object(ObjectStoragePathRef path, std::string_view stream) {
    auto content = std::make_shared<std::stringstream>(std::string(stream));
    AlibabaCloud::OSS::PutObjectRequest request(path.bucket, path.key, content);
    auto outcome = [&] {
        SCOPED_BVAR_LATENCY(oss_bvar::oss_put_latency);
        return oss_client_->PutObject(request);
    }();
    if (!outcome.isSuccess()) {
        LOG_WARNING("failed to put OSS object")
                .tag("endpoint", endpoint_)
                .tag("bucket", path.bucket)
                .tag("key", path.key)
                .tag("error_code", outcome.error().Code())
                .tag("error", outcome.error().Message());
        return {-1};
    }
    return {0};
}

ObjectStorageResponse OssObjClient::head_object(ObjectStoragePathRef path, ObjectMeta* res) {
    auto outcome = [&] {
        SCOPED_BVAR_LATENCY(oss_bvar::oss_head_latency);
        return oss_client_->GetObjectMeta(path.bucket, path.key);
    }();
    if (outcome.isSuccess()) {
        res->key = path.key;
        res->size = outcome.result().ContentLength();
        res->mtime_s = parse_oss_last_modified(outcome.result().LastModified());
        return {0};
    }
    if (outcome.error().Code() == "NoSuchKey") {
        return {ObjectStorageResponse::NOT_FOUND};
    }
    LOG_WARNING("failed to head OSS object")
            .tag("endpoint", endpoint_)
            .tag("bucket", path.bucket)
            .tag("key", path.key)
            .tag("error_code", outcome.error().Code())
            .tag("error", outcome.error().Message());
    return {-1};
}

std::unique_ptr<ObjectListIterator> OssObjClient::list_objects(ObjectStoragePathRef path) {
    return std::make_unique<OssObjListIterator>(oss_client_, path.bucket, path.key, endpoint_);
}

ObjectStorageResponse OssObjClient::delete_objects(const std::string& bucket,
                                                   std::vector<std::string> keys,
                                                   ObjClientOptions option) {
    if (keys.empty()) {
        return {0};
    }

    for (size_t i = 0; i < keys.size(); i += MAX_DELETE_BATCH) {
        size_t end = std::min(i + MAX_DELETE_BATCH, keys.size());
        if (end - i == 1) {
            int ret = delete_object({.bucket = bucket, .key = keys[i]}).ret;
            if (ret != 0) {
                return {ret};
            }
            continue;
        }
        AlibabaCloud::OSS::DeleteObjectsRequest req(bucket);
        for (size_t j = i; j < end; ++j) {
            req.addKey(keys[j]);
        }
        req.setQuiet(true);
        auto outcome = [&] {
            SCOPED_BVAR_LATENCY(oss_bvar::oss_delete_objects_latency);
            return oss_client_->DeleteObjects(req);
        }();
        if (!outcome.isSuccess()) {
            LOG_WARNING("failed to delete OSS objects")
                    .tag("endpoint", endpoint_)
                    .tag("bucket", bucket)
                    .tag("error_code", outcome.error().Code())
                    .tag("error", outcome.error().Message());
            return {-1};
        }
    }
    return {0};
}

ObjectStorageResponse OssObjClient::delete_object(ObjectStoragePathRef path) {
    auto outcome = [&] {
        SCOPED_BVAR_LATENCY(oss_bvar::oss_delete_object_latency);
        return oss_client_->DeleteObject(path.bucket, path.key);
    }();
    // OSS returns success even if object doesn't exist
    if (!outcome.isSuccess() && outcome.error().Code() != "NoSuchKey") {
        LOG_WARNING("failed to delete OSS object")
                .tag("endpoint", endpoint_)
                .tag("bucket", path.bucket)
                .tag("key", path.key)
                .tag("error_code", outcome.error().Code())
                .tag("error", outcome.error().Message());
        return {ObjectStorageResponse::UNDEFINED, outcome.error().Message()};
    }
    return {ObjectStorageResponse::OK};
}

ObjectStorageResponse OssObjClient::delete_objects_recursively(ObjectStoragePathRef path,
                                                               ObjClientOptions option,
                                                               int64_t expiration_time) {
    return delete_objects_recursively_(path, option, expiration_time, MAX_DELETE_BATCH);
}

ObjectStorageResponse OssObjClient::get_life_cycle(const std::string& bucket,
                                                   int64_t* expiration_days) {
    // checker.cpp only calls this for AccessorType::S3; not reached for OSS vaults.
    *expiration_days = INT64_MAX;
    return {0};
}

ObjectStorageResponse OssObjClient::check_versioning(const std::string& bucket) {
    return {0};
}

ObjectStorageResponse OssObjClient::abort_multipart_upload(ObjectStoragePathRef path,
                                                           const std::string& upload_id) {
    AlibabaCloud::OSS::AbortMultipartUploadRequest request(path.bucket, path.key, upload_id);
    auto outcome = oss_client_->AbortMultipartUpload(request);
    if (!outcome.isSuccess()) {
        // Treat NoSuchUpload as success (already aborted or never existed)
        if (outcome.error().Code() == "NoSuchUpload") {
            return {ObjectStorageResponse::OK};
        }
        LOG_WARNING("failed to abort OSS multipart upload")
                .tag("endpoint", endpoint_)
                .tag("bucket", path.bucket)
                .tag("key", path.key)
                .tag("upload_id", upload_id)
                .tag("error_code", outcome.error().Code())
                .tag("error", outcome.error().Message());
        return {ObjectStorageResponse::UNDEFINED, outcome.error().Message()};
    }
    return {ObjectStorageResponse::OK};
}

} // namespace doris::cloud
