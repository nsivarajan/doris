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

#include "io/fs/oss_obj_storage_client.h"

#ifdef USE_OSS

#include <alibabacloud/oss/OssClient.h>
#include <alibabacloud/oss/model/CompleteMultipartUploadRequest.h>
#include <alibabacloud/oss/model/DeleteObjectRequest.h>
#include <alibabacloud/oss/model/DeleteObjectsRequest.h>
#include <alibabacloud/oss/model/GeneratePresignedUrlRequest.h>
#include <alibabacloud/oss/model/GetObjectRequest.h>
#include <alibabacloud/oss/model/HeadObjectRequest.h>
#include <alibabacloud/oss/model/InitiateMultipartUploadRequest.h>
#include <alibabacloud/oss/model/ListObjectsRequest.h>
#include <alibabacloud/oss/model/PutObjectRequest.h>
#include <alibabacloud/oss/model/UploadPartRequest.h>

#include <sstream>

#include "common/logging.h"
#include "common/status.h"
#include "io/fs/obj_storage_client.h"
#include "util/s3_util.h"

using namespace AlibabaCloud::OSS;

namespace doris::io {

OssObjStorageClient::OssObjStorageClient(std::shared_ptr<AlibabaCloud::OSS::OssClient> client,
                                         const std::string& bucket)
        : _client(std::move(client)), _bucket(bucket) {}

ObjectStorageResponse OssObjStorageClient::put_object(const ObjectStoragePathOptions& opts,
                                                      std::string_view stream) {
    auto content = std::make_shared<std::stringstream>();
    content->write(stream.data(), static_cast<std::streamsize>(stream.size()));

    PutObjectRequest request(_bucket, opts.key, content);

    auto outcome = _client->PutObject(request);

    if (!outcome.isSuccess()) {
        ObjectStorageResponse resp;
        resp.status.code = -1;
        resp.status.msg = outcome.error().Message();
        resp.request_id = outcome.error().RequestId();
        return resp;
    }

    return ObjectStorageResponse::OK();
}

ObjectStorageResponse OssObjStorageClient::get_object(const ObjectStoragePathOptions& opts,
                                                      void* buffer, size_t offset,
                                                      size_t bytes_read, size_t* size_return) {
    GetObjectRequest request(_bucket, opts.key);

    // Set range for partial read
    if (bytes_read > 0) {
        request.setRange(offset, offset + bytes_read - 1);
    }

    auto outcome = _client->GetObject(request);

    if (!outcome.isSuccess()) {
        ObjectStorageResponse resp;
        resp.status.code = -1;
        resp.status.msg = outcome.error().Message();
        resp.request_id = outcome.error().RequestId();
        return resp;
    }

    auto& result = outcome.result();
    auto& stream = result.Content();
    stream->read(static_cast<char*>(buffer), bytes_read);
    *size_return = stream->gcount();

    return ObjectStorageResponse::OK();
}

ObjectStorageHeadResponse OssObjStorageClient::head_object(const ObjectStoragePathOptions& opts) {
    HeadObjectRequest request(_bucket, opts.key);

    auto outcome = _client->HeadObject(request);

    ObjectStorageHeadResponse resp;
    if (!outcome.isSuccess()) {
        if (outcome.error().Code() == "NoSuchKey") {
            resp.resp.status.code = -1;
            resp.resp.status.msg = "Object not found";
        } else {
            resp.resp.status.code = -1;
            resp.resp.status.msg = outcome.error().Message();
        }
        resp.resp.request_id = outcome.error().RequestId();
        return resp;
    }

    resp.resp = ObjectStorageResponse::OK();
    resp.file_size = outcome.result().ContentLength();
    return resp;
}

ObjectStorageResponse OssObjStorageClient::list_objects(const ObjectStoragePathOptions& opts,
                                                        std::vector<FileInfo>* files) {
    ListObjectsRequest request(_bucket);
    request.setPrefix(opts.prefix);

    auto outcome = _client->ListObjects(request);

    if (!outcome.isSuccess()) {
        ObjectStorageResponse resp;
        resp.status.code = -1;
        resp.status.msg = outcome.error().Message();
        resp.request_id = outcome.error().RequestId();
        return resp;
    }

    for (const auto& object : outcome.result().ObjectSummarys()) {
        FileInfo file_info;
        file_info.file_name = object.Key();
        file_info.file_size = static_cast<int64_t>(object.Size());
        files->emplace_back(std::move(file_info));
    }

    return ObjectStorageResponse::OK();
}

ObjectStorageResponse OssObjStorageClient::delete_object(const ObjectStoragePathOptions& opts) {
    DeleteObjectRequest request(_bucket, opts.key);

    auto outcome = _client->DeleteObject(request);

    if (!outcome.isSuccess()) {
        ObjectStorageResponse resp;
        resp.status.code = -1;
        resp.status.msg = outcome.error().Message();
        resp.request_id = outcome.error().RequestId();
        return resp;
    }

    return ObjectStorageResponse::OK();
}

ObjectStorageResponse OssObjStorageClient::delete_objects(const ObjectStoragePathOptions& opts,
                                                          std::vector<std::string> objs) {
    DeleteObjectsRequest request(_bucket);
    for (const auto& key : objs) {
        request.addKey(key);
    }

    auto outcome = _client->DeleteObjects(request);

    if (!outcome.isSuccess()) {
        ObjectStorageResponse resp;
        resp.status.code = -1;
        resp.status.msg = outcome.error().Message();
        resp.request_id = outcome.error().RequestId();
        return resp;
    }

    return ObjectStorageResponse::OK();
}

ObjectStorageResponse OssObjStorageClient::delete_objects_recursively(
        const ObjectStoragePathOptions& opts) {
    // List all objects with prefix, then delete
    std::vector<FileInfo> files;
    auto list_result = list_objects(opts, &files);

    if (list_result.status.code != 0) {
        return list_result;
    }

    std::vector<std::string> keys;
    for (const auto& file : files) {
        keys.push_back(file.file_name);
    }

    if (keys.empty()) {
        return ObjectStorageResponse::OK();
    }

    return delete_objects(opts, keys);
}

ObjectStorageUploadResponse OssObjStorageClient::create_multipart_upload(
        const ObjectStoragePathOptions& opts) {
    InitiateMultipartUploadRequest request(_bucket, opts.key);

    auto outcome = _client->InitiateMultipartUpload(request);

    ObjectStorageUploadResponse resp;
    if (!outcome.isSuccess()) {
        resp.resp.status.code = -1;
        resp.resp.status.msg = outcome.error().Message();
        resp.resp.request_id = outcome.error().RequestId();
        return resp;
    }

    resp.resp = ObjectStorageResponse::OK();
    resp.upload_id = outcome.result().UploadId();
    return resp;
}

ObjectStorageUploadResponse OssObjStorageClient::upload_part(const ObjectStoragePathOptions& opts,
                                                             std::string_view stream,
                                                             int partNum) {
    auto content = std::make_shared<std::stringstream>();
    content->write(stream.data(), static_cast<std::streamsize>(stream.size()));

    UploadPartRequest request(_bucket, opts.key, partNum, *opts.upload_id, content);

    auto outcome = _client->UploadPart(request);

    ObjectStorageUploadResponse resp;
    if (!outcome.isSuccess()) {
        resp.resp.status.code = -1;
        resp.resp.status.msg = outcome.error().Message();
        resp.resp.request_id = outcome.error().RequestId();
        return resp;
    }

    resp.resp = ObjectStorageResponse::OK();
    resp.etag = outcome.result().ETag();
    return resp;
}

ObjectStorageResponse OssObjStorageClient::complete_multipart_upload(
        const ObjectStoragePathOptions& opts,
        const std::vector<ObjectCompleteMultiPart>& completed_parts) {
    PartList parts;
    for (const auto& part : completed_parts) {
        Part ossPart(part.part_num, part.etag);
        parts.push_back(ossPart);
    }

    CompleteMultipartUploadRequest request(_bucket, opts.key, parts, *opts.upload_id);

    auto outcome = _client->CompleteMultipartUpload(request);

    if (!outcome.isSuccess()) {
        ObjectStorageResponse resp;
        resp.status.code = -1;
        resp.status.msg = outcome.error().Message();
        resp.request_id = outcome.error().RequestId();
        return resp;
    }

    return ObjectStorageResponse::OK();
}

std::string OssObjStorageClient::generate_presigned_url(const ObjectStoragePathOptions& opts,
                                                        int64_t expiration_secs,
                                                        const S3ClientConf& conf) {
    GeneratePresignedUrlRequest request(_bucket, opts.key, Http::Put);
    request.setExpires(expiration_secs);

    auto outcome = _client->GeneratePresignedUrl(request);

    if (!outcome.isSuccess()) {
        return "";
    }

    return outcome.result();
}

} // namespace doris::io

#endif // USE_OSS
