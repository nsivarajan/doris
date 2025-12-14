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

package org.apache.doris.fs.remote;

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.backup.Status;
import org.apache.doris.datasource.property.storage.OSSProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.obj.OssObjStorage;

import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.PartETag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * OSS FileSystem implementation using native Aliyun OSS SDK.
 * This class provides file system operations for Alibaba Cloud Object Storage Service.
 */
public class OssFileSystem extends ObjFileSystem {
    private final OSSProperties ossProperties;

    public OssFileSystem(OSSProperties ossProperties) {
        super("OSS", StorageBackend.StorageType.S3, new OssObjStorage(ossProperties));
        this.ossProperties = ossProperties;
        this.properties.putAll(ossProperties.getOrigProps());
    }

    @Override
    public Status renameDir(String origFilePath, String destFilePath) {
        throw new UnsupportedOperationException("Renaming directories is not supported in OSS File System.");
    }

    @Override
    public Status listFiles(String remotePath, boolean recursive, List<RemoteFile> result) {
        OssObjStorage ossObjStorage = (OssObjStorage) getObjStorage();
        return ossObjStorage.globList(remotePath, result, false);
    }

    @Override
    public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        OssObjStorage ossObjStorage = (OssObjStorage) getObjStorage();
        return ossObjStorage.globList(remotePath, result, fileNameOnly);
    }

    @Override
    public Status listDirectories(String remotePath, Set<String> result) {
        // OSS doesn't have native directory concept, return OK
        return Status.OK;
    }

    @Override
    public StorageProperties getStorageProperties() {
        return ossProperties;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                objStorage.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void completeMultipartUpload(String bucket, String key, String uploadId, Map<Integer, String> parts) {
        OssObjStorage ossObjStorage = (OssObjStorage) getObjStorage();
        List<PartETag> partETags = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : parts.entrySet()) {
            partETags.add(new PartETag(entry.getKey(), entry.getValue()));
        }

        try {
            CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest(
                    bucket, key, uploadId, partETags);
            ossObjStorage.getClient().completeMultipartUpload(request);
        } catch (Exception e) {
            throw new RuntimeException("Failed to complete multipart upload", e);
        }
    }
}
