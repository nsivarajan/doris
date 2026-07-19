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

#include <bvar/latency_recorder.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "cpp/oss_common.h"
#include "recycler/obj_storage_client.h"
#include "recycler/storage_vault_accessor.h"

namespace doris::cloud {

class ObjectStoreInfoPB;

namespace oss_bvar {
extern bvar::LatencyRecorder oss_put_latency;
extern bvar::LatencyRecorder oss_delete_object_latency;
extern bvar::LatencyRecorder oss_delete_objects_latency;
extern bvar::LatencyRecorder oss_head_latency;
extern bvar::LatencyRecorder oss_list_latency;
} // namespace oss_bvar

// Parses ISO-8601 UTC string (e.g. "2024-01-02T03:04:05.000Z") to Unix epoch seconds.
// Returns INT64_MAX on parse failure to prevent premature deletion.
int64_t parse_oss_last_modified(const std::string& last_modified_str);

struct OSSConf {
    std::string endpoint;
    std::string bucket;
    std::string prefix;
    std::string region;

    std::string access_key_id;
    std::string access_key_secret;
    std::string security_token;

    std::string role_arn;
    std::string external_id;

    OSSCredProviderType provider_type = OSSCredProviderType::INSTANCE_PROFILE;

    int max_connections = 100;
    int connect_timeout_ms = 10000;
    int request_timeout_ms = 30000;

    static std::optional<OSSConf> from_obj_store_info(const ObjectStoreInfoPB& obj_info,
                                                      bool skip_aksk = false);
};

class OSSAccessor : public StorageVaultAccessor {
public:
    explicit OSSAccessor(OSSConf conf);
    ~OSSAccessor() override;

    static int create(OSSConf conf, std::shared_ptr<OSSAccessor>* accessor);

    int init();

    int delete_prefix(const std::string& path_prefix, int64_t expiration_time = 0) override;
    int delete_directory(const std::string& dir_path) override;
    int delete_all(int64_t expiration_time = 0) override;
    int delete_files(const std::vector<std::string>& paths) override;
    int delete_file(const std::string& path) override;
    int list_directory(const std::string& dir_path, std::unique_ptr<ListIterator>* res) override;
    int list_all(std::unique_ptr<ListIterator>* res) override;
    int put_file(const std::string& path, const std::string& content) override;
    int exists(const std::string& path) override;
    int abort_multipart_upload(const std::string& path, const std::string& upload_id) override;

protected:
    int list_prefix(const std::string& path_prefix, std::unique_ptr<ListIterator>* res);
    std::string get_key(const std::string& relative_path) const;
    std::string to_uri(const std::string& relative_path) const;

    OSSConf conf_;
    std::shared_ptr<ObjStorageClient> obj_client_;
    std::string _ca_cert_file_path;
};

} // namespace doris::cloud
