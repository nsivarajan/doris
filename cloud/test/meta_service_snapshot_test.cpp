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
#include <brpc/controller.h>
#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_set>

#include "common/defer.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service.h"
#include "meta-service/txn_lazy_committer.h"
#include "meta-store/codec.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/meta_reader.h"
#include "meta-store/versioned_value.h"
#include "mock_accessor.h"
#include "recycler/recycler.h"
#include "recycler/snapshot_chain_compactor.h"
#include "snapshot/snapshot_manager.h"

namespace doris::cloud {

extern std::unique_ptr<MetaServiceProxy> get_meta_service(bool mock_resource_mgr);

TEST(MetaServiceSnapshotTest, BeginSnapshotTest) {
    auto meta_service = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";

    // Setup SyncPoint for encryption
    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "selectdbselectdbselectdbselectdb";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });

    // Cleanup SyncPoint when test finishes
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
        sp->clear_all_call_backs();
    };

    // Create test instance first
    {
        brpc::Controller cntl;
        CreateInstanceRequest req;
        req.set_instance_id("test_instance");
        req.set_user_id("test_user");
        req.set_name("test_name");
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_external_endpoint("888");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);

        CreateInstanceResponse res;
        meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // test invalid argument - empty cloud_unique_id
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test normal begin snapshot
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_auto_snapshot(true);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_FALSE(res.image_url().empty());
        ASSERT_FALSE(res.snapshot_id().empty());
        ASSERT_TRUE(res.image_url().find("/snapshot/") != std::string::npos);
    }

    // test begin snapshot with custom parameters
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(1800);
        req.set_auto_snapshot(false);
        req.set_ttl_seconds(14400);
        req.set_snapshot_label("custom_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_FALSE(res.image_url().empty());
        ASSERT_FALSE(res.snapshot_id().empty());
    }

    // test invalid timeout_seconds - zero
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(0);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test invalid timeout_seconds - negative
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(-100);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test invalid ttl_seconds - zero
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(0);
        req.set_snapshot_label("test_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test invalid ttl_seconds - negative
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(-500);
        req.set_snapshot_label("test_snapshot");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test empty snapshot_label
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test valid IPv4 address
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        req.set_request_ip("192.168.1.100");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // test valid IPv6 address
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        req.set_request_ip("2001:db8::1");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // test invalid IP address format
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        req.set_request_ip("invalid.ip.address");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test invalid IP address - out of range
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        req.set_request_ip("256.256.256.256");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // test empty IP address (should pass - IP is optional)
    {
        brpc::Controller cntl;
        BeginSnapshotRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_timeout_seconds(3600);
        req.set_ttl_seconds(7200);
        req.set_snapshot_label("test_snapshot");
        req.set_request_ip("");
        BeginSnapshotResponse res;
        meta_service->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
}

// ─── helpers shared by lifecycle tests ──────────────────────────────────────

namespace {

const char* const TEST_CLOUD_UNIQUE_ID = "test_cloud_unique_id";

// Every test that calls create_instance with obj_info.ak/sk must set up the
// encryption SyncPoint first — otherwise encrypt_ak_sk_helper fails.
// This RAII guard sets up and tears down the callback automatically.
struct EncryptionSyncPointGuard {
    EncryptionSyncPointGuard() {
        auto* sp = SyncPoint::get_instance();
        sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
            auto* ret = try_any_cast<int*>(args[0]);
            *ret = 0;
            auto* key = try_any_cast<std::string*>(args[1]);
            *key = "selectdbselectdbselectdbselectdb";
            auto* key_id = try_any_cast<int64_t*>(args[2]);
            *key_id = 1;
        });
        sp->enable_processing();
    }
    ~EncryptionSyncPointGuard() {
        auto* sp = SyncPoint::get_instance();
        sp->disable_processing();
        sp->clear_all_call_backs();
    }
};

std::unique_ptr<MetaServiceProxy> create_meta_service_with_instance() {
    auto meta_service = get_meta_service(true);
    brpc::Controller cntl;
    CreateInstanceRequest req;
    req.set_instance_id("test_instance");
    req.set_user_id("test_user");
    req.set_name("test_name");
    ObjectStoreInfoPB obj;
    obj.set_ak("ak");
    obj.set_sk("sk");
    obj.set_bucket("bucket");
    obj.set_prefix("prefix");
    obj.set_endpoint("endpoint");
    obj.set_region("region");
    obj.set_external_endpoint("ext");
    obj.set_provider(ObjectStoreInfoPB::BOS);
    req.mutable_obj_info()->CopyFrom(obj);
    CreateInstanceResponse res;
    meta_service->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                  &req, &res, nullptr);
    if (res.status().code() != MetaServiceCode::OK) return nullptr;
    return meta_service;
}

// Begin a snapshot and return snapshot_id. Asserts success.
std::string do_begin_snapshot(MetaServiceProxy* ms, const std::string& label,
                               int64_t ttl = 7200, int64_t timeout = 3600) {
    brpc::Controller cntl;
    BeginSnapshotRequest req;
    req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
    req.set_snapshot_label(label);
    req.set_ttl_seconds(ttl);
    req.set_timeout_seconds(timeout);
    BeginSnapshotResponse res;
    ms->begin_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                       &req, &res, nullptr);
    EXPECT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    EXPECT_FALSE(res.snapshot_id().empty());
    return res.snapshot_id();
}

} // anonymous namespace

// ─── SnapshotLifecycleTest ────────────────────────────────────────────────────

TEST(MetaServiceSnapshotTest, SnapshotLifecycleTest) {
    EncryptionSyncPointGuard enc_guard;
    auto meta_service = create_meta_service_with_instance();
    ASSERT_NE(meta_service, nullptr);

    // 1. Begin
    std::string snapshot_id = do_begin_snapshot(meta_service.get(), "lifecycle_snap");
    ASSERT_FALSE(snapshot_id.empty());

    // 2. List — should appear as PREPARE (not returned by default list)
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        // PREPARE-state snapshots are internal — not shown to callers
        ASSERT_EQ(res.snapshots_size(), 0);
    }

    // 3. Commit
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        req.set_snapshot_id(snapshot_id);
        req.set_last_journal_id(12345);
        req.set_image_url("prefix/snapshot/" + snapshot_id + "/fe_image");
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    }

    // 4. List — now appears as NORMAL
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.snapshots_size(), 1);
        EXPECT_EQ(res.snapshots(0).snapshot_id(), snapshot_id);
        EXPECT_EQ(res.snapshots(0).status(), SnapshotStatus::SNAPSHOT_NORMAL);
        EXPECT_EQ(res.snapshots(0).journal_id(), 12345);
        EXPECT_EQ(res.snapshots(0).snapshot_label(), "lifecycle_snap");
    }

    // 5. Drop
    {
        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        req.set_snapshot_id(snapshot_id);
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    }

    // 6. List — gone (RECYCLED is not shown)
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.snapshots_size(), 0);
    }
}

// ─── CommitSnapshotTest ───────────────────────────────────────────────────────

TEST(MetaServiceSnapshotTest, CommitSnapshotTest) {
    EncryptionSyncPointGuard enc_guard;
    auto meta_service = create_meta_service_with_instance();
    ASSERT_NE(meta_service, nullptr);

    // Commit non-existent snapshot → error
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        req.set_snapshot_id("0000000000000000000f");
        req.set_last_journal_id(1);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK);
    }

    // Normal commit
    std::string snapshot_id = do_begin_snapshot(meta_service.get(), "commit_test");
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        req.set_snapshot_id(snapshot_id);
        req.set_last_journal_id(999);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    }

    // Idempotent: commit again on NORMAL snapshot → OK (handles KV_TXN_MAYBE_COMMITTED retries)
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        req.set_snapshot_id(snapshot_id);
        req.set_last_journal_id(999);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << "commit must be idempotent";
    }

    // Commit already-NORMAL snapshot with different journal_id → still OK (idempotent)
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        req.set_snapshot_id(snapshot_id);
        req.set_last_journal_id(12345); // different value
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
}

// ─── AbortSnapshotTest ────────────────────────────────────────────────────────

TEST(MetaServiceSnapshotTest, AbortSnapshotTest) {
    EncryptionSyncPointGuard enc_guard;
    auto meta_service = create_meta_service_with_instance();
    ASSERT_NE(meta_service, nullptr);

    // Abort a PREPARE snapshot → ABORTED
    std::string snapshot_id = do_begin_snapshot(meta_service.get(), "abort_test");
    {
        brpc::Controller cntl;
        AbortSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        req.set_snapshot_id(snapshot_id);
        req.set_reason("test abort");
        AbortSnapshotResponse res;
        meta_service->abort_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    }

    // Aborted snapshot is not shown in default list
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.snapshots_size(), 0);
    }

    // Aborted snapshot IS shown with include_aborted=true
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        req.set_include_aborted(true);
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.snapshots_size(), 1);
        EXPECT_EQ(res.snapshots(0).status(), SnapshotStatus::SNAPSHOT_ABORTED);
    }

    // Idempotent: abort an already-ABORTED snapshot → OK
    {
        brpc::Controller cntl;
        AbortSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        req.set_snapshot_id(snapshot_id);
        req.set_reason("duplicate abort");
        AbortSnapshotResponse res;
        meta_service->abort_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << "abort must be idempotent";
    }
}

// ─── ListSnapshotTest ─────────────────────────────────────────────────────────

TEST(MetaServiceSnapshotTest, ListSnapshotTest) {
    EncryptionSyncPointGuard enc_guard;
    auto meta_service = create_meta_service_with_instance();
    ASSERT_NE(meta_service, nullptr);

    // Create two snapshots: one committed, one aborted
    std::string id_normal = do_begin_snapshot(meta_service.get(), "normal_snap");
    std::string id_aborted = do_begin_snapshot(meta_service.get(), "aborted_snap");

    // Commit the first
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        req.set_snapshot_id(id_normal);
        req.set_last_journal_id(100);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Abort the second
    {
        brpc::Controller cntl;
        AbortSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        req.set_snapshot_id(id_aborted);
        AbortSnapshotResponse res;
        meta_service->abort_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Default list: only NORMAL
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.snapshots_size(), 1);
        EXPECT_EQ(res.snapshots(0).snapshot_id(), id_normal);
        EXPECT_EQ(res.snapshots(0).status(), SnapshotStatus::SNAPSHOT_NORMAL);
    }

    // include_aborted=true: NORMAL + ABORTED, but NOT PREPARE or RECYCLED
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        req.set_include_aborted(true);
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.snapshots_size(), 2);
    }

    // List by specific snapshot_id
    {
        brpc::Controller cntl;
        ListSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        req.set_required_snapshot_id(id_normal);
        ListSnapshotResponse res;
        meta_service->list_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        ASSERT_EQ(res.snapshots_size(), 1);
        EXPECT_EQ(res.snapshots(0).snapshot_id(), id_normal);
    }
}

// ─── DropSnapshotTest ─────────────────────────────────────────────────────────

TEST(MetaServiceSnapshotTest, DropSnapshotTest) {
    EncryptionSyncPointGuard enc_guard;
    auto meta_service = create_meta_service_with_instance();
    ASSERT_NE(meta_service, nullptr);

    std::string snapshot_id = do_begin_snapshot(meta_service.get(), "drop_test");

    // Cannot drop PREPARE-state snapshot
    {
        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        req.set_snapshot_id(snapshot_id);
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK)
                << "dropping PREPARE snapshot must fail";
    }

    // Commit first
    {
        brpc::Controller cntl;
        CommitSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        req.set_snapshot_id(snapshot_id);
        req.set_last_journal_id(42);
        CommitSnapshotResponse res;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }

    // Drop NORMAL snapshot → OK
    {
        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        req.set_snapshot_id(snapshot_id);
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    }

    // Idempotent: drop again on RECYCLED → OK
    {
        brpc::Controller cntl;
        DropSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        req.set_snapshot_id(snapshot_id);
        DropSnapshotResponse res;
        meta_service->drop_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << "drop must be idempotent";
    }
}

// ─── UpdateSnapshotTest ───────────────────────────────────────────────────────

TEST(MetaServiceSnapshotTest, UpdateSnapshotTest) {
    EncryptionSyncPointGuard enc_guard;
    auto meta_service = create_meta_service_with_instance();
    ASSERT_NE(meta_service, nullptr);

    std::string snapshot_id = do_begin_snapshot(meta_service.get(), "update_test");

    // Update multipart upload tracking fields
    {
        brpc::Controller cntl;
        UpdateSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        req.set_snapshot_id(snapshot_id);
        req.set_upload_file("fe_image/part_0001");
        req.set_upload_id("upload-abc-123");
        UpdateSnapshotResponse res;
        meta_service->update_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    }

    // Update on non-PREPARE snapshot → error
    {
        brpc::Controller cntl;
        CommitSnapshotRequest creq;
        creq.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        creq.set_snapshot_id(snapshot_id);
        creq.set_last_journal_id(1);
        CommitSnapshotResponse cres;
        meta_service->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &creq, &cres, nullptr);
        ASSERT_EQ(cres.status().code(), MetaServiceCode::OK);
    }
    {
        brpc::Controller cntl;
        UpdateSnapshotRequest req;
        req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
        req.set_snapshot_id(snapshot_id);
        req.set_upload_file("new_file");
        UpdateSnapshotResponse res;
        meta_service->update_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                      &req, &res, nullptr);
        ASSERT_NE(res.status().code(), MetaServiceCode::OK)
                << "update on non-PREPARE snapshot must fail";
    }
}

// ─── helpers ──────────────────────────────────────────────────────────────────

namespace {

// Commit a snapshot and return the snapshot_id. Fails test on error.
std::string do_commit_snapshot(MetaServiceProxy* ms, const std::string& snapshot_id,
                                int64_t journal_id = 1) {
    brpc::Controller cntl;
    CommitSnapshotRequest req;
    req.set_cloud_unique_id(TEST_CLOUD_UNIQUE_ID);
    req.set_snapshot_id(snapshot_id);
    req.set_last_journal_id(journal_id);
    req.set_image_url("prefix/snapshot/" + snapshot_id + "/fe_image");
    CommitSnapshotResponse res;
    ms->commit_snapshot(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                        &req, &res, nullptr);
    EXPECT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    return snapshot_id;
}

// Retrieve InstanceInfoPB for the given instance_id. Returns nullptr on error.
std::unique_ptr<InstanceInfoPB> get_instance_info(MetaServiceProxy* ms,
                                                   const std::string& instance_id) {
    brpc::Controller cntl;
    GetInstanceRequest req;
    req.set_instance_id(instance_id);
    GetInstanceResponse res;
    ms->get_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                     &req, &res, nullptr);
    if (res.status().code() != MetaServiceCode::OK) return nullptr;
    return std::make_unique<InstanceInfoPB>(res.instance());
}

} // anonymous namespace (extends the existing one)




// ---------------------------------------------------------------------------
// RecycleSnapshotsTest — TTL expiry, S3 image cleanup, clone-reference guard
// ---------------------------------------------------------------------------

// thread_group is defined in meta_service_test.cpp (same translation unit group).
extern doris::cloud::RecyclerThreadPoolGroup thread_group;

namespace {

// Build an InstanceRecycler that shares the meta service's txn_kv.
std::unique_ptr<InstanceRecycler> make_recycler(MetaServiceProxy* ms,
                                                std::shared_ptr<MockAccessor> accessor = {}) {
    auto inst = get_instance_info(ms, "test_instance");
    if (!inst) return nullptr;
    auto txn_lazy = std::make_shared<TxnLazyCommitter>(ms->txn_kv());
    auto recycler =
            std::make_unique<InstanceRecycler>(ms->txn_kv(), *inst, thread_group, txn_lazy);
    if (accessor) {
        // Use the obj_info id (may be empty in test instances — that's OK for status-only tests).
        std::string vault_id = inst->obj_info_size() > 0 ? inst->obj_info(0).id() : "";
        recycler->TEST_add_accessor(vault_id, accessor);
    }
    return recycler;
}

// Commit a snapshot then back-date its create_at so the TTL has already expired.
std::string make_expired_snapshot(MetaServiceProxy* ms, const std::string& label,
                                   int64_t ttl_seconds) {
    std::string snapshot_id = do_begin_snapshot(ms, label, ttl_seconds);
    EXPECT_FALSE(snapshot_id.empty());
    do_commit_snapshot(ms, snapshot_id);

    Versionstamp vs;
    EXPECT_TRUE(SnapshotManager::parse_snapshot_versionstamp(snapshot_id, &vs));

    // Back-date: read → modify create_at → write back at same versionstamp.
    MetaReader reader("test_instance");
    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    SnapshotPB snap;
    EXPECT_EQ(reader.get_snapshot(txn.get(), vs, &snap), TxnErrorCode::TXN_OK);
    snap.set_create_at(snap.create_at() - ttl_seconds - 1);
    versioned::document_put(txn.get(), versioned::snapshot_full_key({"test_instance"}), vs,
                             std::move(snap));
    EXPECT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    return snapshot_id;
}

// Return the current status of a snapshot read directly from FDB.
// Returns SNAPSHOT_RECYCLED when the key no longer exists.
SnapshotStatus snapshot_status_in_fdb(MetaServiceProxy* ms, const std::string& snapshot_id) {
    Versionstamp vs;
    if (!SnapshotManager::parse_snapshot_versionstamp(snapshot_id, &vs)) {
        return SnapshotStatus::SNAPSHOT_PREPARE;
    }
    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    SnapshotPB snap;
    TxnErrorCode err = MetaReader("test_instance").get_snapshot(txn.get(), vs, &snap);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) return SnapshotStatus::SNAPSHOT_RECYCLED;
    EXPECT_EQ(err, TxnErrorCode::TXN_OK);
    return snap.status();
}

} // namespace

// TTL-expired NORMAL snapshot is transitioned to RECYCLED by the recycler.
TEST(MetaServiceSnapshotTest, RecycleSnapshots_TtlExpiry) {
    EncryptionSyncPointGuard enc_guard;
    auto ms = create_meta_service_with_instance();
    ASSERT_NE(ms, nullptr);

    std::string snapshot_id = make_expired_snapshot(ms.get(), "ttl_snap", 3600);
    ASSERT_EQ(snapshot_status_in_fdb(ms.get(), snapshot_id), SnapshotStatus::SNAPSHOT_NORMAL);

    auto recycler = make_recycler(ms.get());
    ASSERT_NE(recycler, nullptr);
    ASSERT_EQ(recycler->recycle_cluster_snapshots(), 0);

    // After one recycler cycle: NORMAL → RECYCLED.
    EXPECT_EQ(snapshot_status_in_fdb(ms.get(), snapshot_id), SnapshotStatus::SNAPSHOT_RECYCLED);
}

// NORMAL snapshot within its TTL must NOT be transitioned.
TEST(MetaServiceSnapshotTest, RecycleSnapshots_TtlNotExpired) {
    EncryptionSyncPointGuard enc_guard;
    auto ms = create_meta_service_with_instance();
    ASSERT_NE(ms, nullptr);

    // TTL = 1 hour; create_at is now — not expired.
    std::string snapshot_id = do_begin_snapshot(ms.get(), "live_snap", 3600);
    ASSERT_FALSE(snapshot_id.empty());
    do_commit_snapshot(ms.get(), snapshot_id);

    auto recycler = make_recycler(ms.get());
    ASSERT_NE(recycler, nullptr);
    ASSERT_EQ(recycler->recycle_cluster_snapshots(), 0);

    EXPECT_EQ(snapshot_status_in_fdb(ms.get(), snapshot_id), SnapshotStatus::SNAPSHOT_NORMAL);
}


// recycle_snapshot_meta_and_data deletes the image directory via the accessor.
TEST(MetaServiceSnapshotTest, RecycleSnapshotMetaAndData_DeletesDirectory) {
    auto accessor = std::make_shared<MockAccessor>();
    const std::string image_url = "prefix/snapshot/abc123/fe_image";
    // Put a fake file INSIDE the image directory (the FE uploads files under image_url/).
    ASSERT_EQ(accessor->put_file(image_url + "/current", "image_data"), 0);

    // Verify file exists before cleanup.
    std::unique_ptr<ListIterator> iter;
    ASSERT_EQ(accessor->list_directory(image_url, &iter), 0);
    ASSERT_TRUE(iter->has_next());

    // Call recycle_snapshot_meta_and_data directly.
    auto kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(kv->init(), 0);
    SnapshotManager mgr(kv);
    SnapshotPB snap;
    snap.set_image_url(image_url);
    Versionstamp vs;
    EXPECT_EQ(mgr.recycle_snapshot_meta_and_data("test_instance", "resource_id", accessor.get(),
                                                   vs, snap), 0);

    // All files under image_url/ must be gone after delete_directory.
    ASSERT_EQ(accessor->list_directory(image_url, &iter), 0);
    EXPECT_FALSE(iter->has_next());
}

// recycle_snapshot_meta_and_data is a no-op when image_url is absent.
TEST(MetaServiceSnapshotTest, RecycleSnapshotMetaAndData_NoImageUrl) {
    auto accessor = std::make_shared<MockAccessor>();
    ASSERT_EQ(accessor->put_file("some/other/file", "data"), 0);

    auto kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(kv->init(), 0);
    SnapshotManager mgr(kv);
    SnapshotPB snap; // no image_url
    Versionstamp vs;
    EXPECT_EQ(mgr.recycle_snapshot_meta_and_data("test_instance", "resource_id", accessor.get(),
                                                   vs, snap), 0);

    // Accessor contents must be untouched.
    std::unique_ptr<ListIterator> iter;
    ASSERT_EQ(accessor->list_all(&iter), 0);
    ASSERT_TRUE(iter->has_next());
    iter->next();
    EXPECT_FALSE(iter->has_next()); // exactly one file remains
}

// ---------------------------------------------------------------------------
// CompactSnapshotTest — compact_snapshot RPC + compact_snapshot_chains
// ---------------------------------------------------------------------------

namespace {

// Count how many main versionstamp entries exist for a given key prefix
// (main keys end with VERSIONSTAMP_END_TAG; sub-keys of split docs do not).
size_t count_versioned_entries(TxnKv* kv, const std::string& key_prefix) {
    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string begin = encode_versioned_key(key_prefix, Versionstamp::min());
    std::string end = encode_versioned_key(key_prefix, Versionstamp::max());
    FullRangeGetOptions opts;
    opts.txn = txn.get();
    auto it = kv->full_range_get(begin, end, std::move(opts));
    size_t count = 0;
    for (auto kv = it->next(); kv.has_value(); kv = it->next()) {
        auto [k, v] = *kv;
        std::string_view key_view(k.data(), k.size());
        if (decode_tailing_versionstamp_end(&key_view) == 0) ++count;
    }
    EXPECT_TRUE(it->is_valid());
    return count;
}

// Write N distinct committed versions of a simple versioned key.
void write_versions(TxnKv* kv, const std::string& key_prefix, int n) {
    for (int i = 0; i < n; ++i) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        versioned_put(txn.get(), key_prefix, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }
}

// Write an InstanceInfoPB for a cloned instance to FDB.
void write_clone_instance(TxnKv* kv, const std::string& clone_id,
                           const std::string& source_instance_id,
                           const std::string& source_snapshot_id,
                           std::vector<KeySetType> already_compacted = {}) {
    InstanceInfoPB inst;
    inst.set_instance_id(clone_id);
    inst.set_source_instance_id(source_instance_id);
    inst.set_source_snapshot_id(source_snapshot_id);
    inst.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_READ_WRITE);
    for (auto kt : already_compacted) inst.add_compacted_key_sets(kt);
    auto* obj = inst.mutable_obj_info()->Add();
    obj->set_id("mock_resource_id");
    obj->set_ak("ak");
    obj->set_sk("sk");
    obj->set_bucket("bucket");
    obj->set_prefix("prefix");
    obj->set_endpoint("endpoint");
    obj->set_region("region");
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(instance_key({clone_id}), inst.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
}

// Read snapshot_compact_status for an instance directly from FDB.
SnapshotCompactStatus compact_status_in_fdb(TxnKv* kv, const std::string& instance_id) {
    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string val;
    EXPECT_EQ(txn->get(instance_key({instance_id}), &val), TxnErrorCode::TXN_OK);
    InstanceInfoPB inst;
    EXPECT_TRUE(inst.ParseFromString(val));
    return inst.snapshot_compact_status();
}

// Build an InstanceChainCompactor from the current FDB state for clone_id.
std::unique_ptr<InstanceChainCompactor> make_chain_compactor(std::shared_ptr<TxnKv> kv,
                                                              const std::string& clone_id) {
    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string val;
    EXPECT_EQ(txn->get(instance_key({clone_id}), &val), TxnErrorCode::TXN_OK);
    InstanceInfoPB inst;
    EXPECT_TRUE(inst.ParseFromString(val));
    auto c = std::make_unique<InstanceChainCompactor>(kv, inst);
    EXPECT_EQ(c->init(), 0);
    return c;
}

} // namespace









// import_table_meta: write a TableFdbMetaPB and verify all FDB keys are present.
TEST(MetaServiceSnapshotTest, ImportTableMetaTest) {
    auto ms = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";
    const std::string instance_id = "test_instance";

    const int64_t table_id = 1001;
    const int64_t partition_id = 2001;
    const int64_t tablet_id = 3001;
    const int64_t end_version = 10;

    // Build a TableFdbMetaPB with one tablet, one load rowset, and one partition version.
    TableFdbMetaPB fdb_meta;
    fdb_meta.set_table_id(table_id);

    // Tablet index entry.
    {
        auto* idx = fdb_meta.add_tablet_indexes();
        idx->set_db_id(101);
        idx->set_table_id(table_id);
        idx->set_index_id(301);
        idx->set_partition_id(partition_id);
        idx->set_tablet_id(tablet_id);
    }

    // Load rowset.
    {
        auto* rs = fdb_meta.add_load_rowsets();
        rs->set_tablet_id(tablet_id);
        rs->set_partition_id(partition_id);
        rs->set_start_version(end_version);
        rs->set_end_version(end_version);
        rs->set_num_rows(1000);
        rs->set_rowset_id_v2("test_rowset_001");
    }

    // Partition version.
    {
        auto* pv = fdb_meta.add_partition_versions();
        pv->set_partition_id(partition_id);
        pv->mutable_version()->set_version(end_version);
    }

    // Call import_table_meta.
    {
        brpc::Controller cntl;
        ImportTableMetaRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_table_id(table_id);
        req.set_fdb_meta_pb(fdb_meta.SerializeAsString());

        ImportTableMetaResponse resp;
        ms->import_table_meta(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                &req, &resp, nullptr);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK)
                << resp.status().msg();
        EXPECT_EQ(resp.tablets_restored(), 1);
        EXPECT_EQ(resp.rowsets_restored(), 1);
        EXPECT_EQ(resp.partitions_restored(), 1);
    }

    // Verify: tablet index key was written.
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string key = meta_tablet_idx_key({instance_id, tablet_id});
        std::string val;
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        TabletIndexPB idx;
        ASSERT_TRUE(idx.ParseFromString(val));
        EXPECT_EQ(idx.table_id(), table_id);
        EXPECT_EQ(idx.partition_id(), partition_id);
        EXPECT_EQ(idx.tablet_id(), tablet_id);
    }

    // Verify: partition version key was written.
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader reader(instance_id, Versionstamp::max());
        VersionPB ver;
        EXPECT_EQ(reader.get_partition_version(txn.get(), partition_id, &ver, nullptr, false),
                  TxnErrorCode::TXN_OK);
        EXPECT_EQ(ver.version(), end_version);
    }

    // Verify: partition_ids filter — only restore matching partition.
    {
        // Add a second partition in the fdb_meta.
        const int64_t other_partition_id = 2002;
        const int64_t other_tablet_id = 3002;

        TableFdbMetaPB fdb_meta2;
        fdb_meta2.set_table_id(table_id);
        {
            auto* idx = fdb_meta2.add_tablet_indexes();
            idx->set_db_id(101);
            idx->set_table_id(table_id);
            idx->set_index_id(301);
            idx->set_partition_id(other_partition_id);
            idx->set_tablet_id(other_tablet_id);
        }
        {
            auto* pv = fdb_meta2.add_partition_versions();
            pv->set_partition_id(other_partition_id);
            pv->mutable_version()->set_version(5);
        }

        brpc::Controller cntl;
        ImportTableMetaRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_table_id(table_id);
        req.add_partition_ids(other_partition_id);   // only restore other_partition
        req.set_fdb_meta_pb(fdb_meta2.SerializeAsString());

        ImportTableMetaResponse resp;
        ms->import_table_meta(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                &req, &resp, nullptr);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);
        EXPECT_EQ(resp.tablets_restored(), 1);   // only other_tablet_id
        EXPECT_EQ(resp.partitions_restored(), 1); // only other_partition_id

        // other_tablet_id's index key must be present.
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string key = meta_tablet_idx_key({instance_id, other_tablet_id});
        std::string val;
        EXPECT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
    }

    // Error path: empty fdb_meta_pb → INVALID_ARGUMENT.
    {
        brpc::Controller cntl;
        ImportTableMetaRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_table_id(table_id);
        // fdb_meta_pb intentionally not set

        ImportTableMetaResponse resp;
        ms->import_table_meta(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                &req, &resp, nullptr);
        EXPECT_EQ(resp.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }
}

// Verifies that tablet_id_remap and partition_id_remap redirect all FDB keys,
// and that rowset_id_v2 is NOT changed (S3 path preserved for zero-copy sharing).
TEST(MetaServiceSnapshotTest, ImportTableMetaWithRemapTest) {
    auto ms = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";

    const int64_t old_tablet_id    = 5001;
    const int64_t new_tablet_id    = 5002;
    const int64_t old_partition_id = 6001;
    const int64_t new_partition_id = 6002;
    const int64_t table_id         = 7001;
    const int64_t end_version      = 3;
    const std::string rowset_id    = "rowset_xyz_preserved";

    // Build TableFdbMetaPB with old IDs.
    TableFdbMetaPB fdb_meta;
    fdb_meta.set_table_id(table_id);

    {
        auto* idx = fdb_meta.add_tablet_indexes();
        idx->set_db_id(1); idx->set_table_id(table_id);
        idx->set_index_id(1); idx->set_partition_id(old_partition_id);
        idx->set_tablet_id(old_tablet_id);
    }
    {
        auto* rs = fdb_meta.add_load_rowsets();
        rs->set_tablet_id(old_tablet_id);
        rs->set_partition_id(old_partition_id);
        rs->set_start_version(end_version);
        rs->set_end_version(end_version);
        rs->set_rowset_id_v2(rowset_id); // must survive unchanged
    }
    {
        auto* pv = fdb_meta.add_partition_versions();
        pv->set_partition_id(old_partition_id);
        pv->mutable_version()->set_version(end_version);
    }

    // Call import_table_meta with both remaps.
    {
        brpc::Controller cntl;
        ImportTableMetaRequest req;
        req.set_cloud_unique_id(cloud_unique_id);
        req.set_table_id(table_id);
        req.set_fdb_meta_pb(fdb_meta.SerializeAsString());
        (*req.mutable_tablet_id_remap())[old_tablet_id]    = new_tablet_id;
        (*req.mutable_partition_id_remap())[old_partition_id] = new_partition_id;

        ImportTableMetaResponse resp;
        ms->import_table_meta(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                &req, &resp, nullptr);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK) << resp.status().msg();
        EXPECT_EQ(resp.tablets_restored(), 1);
        EXPECT_EQ(resp.rowsets_restored(), 1);
        EXPECT_EQ(resp.partitions_restored(), 1);
    }

    const std::string instance_id = "test_instance";

    // FDB key for OLD tablet_id must NOT exist.
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string old_key = meta_tablet_idx_key({instance_id, old_tablet_id});
        std::string val;
        EXPECT_NE(txn->get(old_key, &val), TxnErrorCode::TXN_OK)
                << "old tablet_id key must not be present after remap";
    }

    // FDB key for NEW tablet_id must exist and carry the remapped tablet_id inside the value.
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string new_key = meta_tablet_idx_key({instance_id, new_tablet_id});
        std::string val;
        ASSERT_EQ(txn->get(new_key, &val), TxnErrorCode::TXN_OK)
                << "new tablet_id key must be present";
        TabletIndexPB idx;
        ASSERT_TRUE(idx.ParseFromString(val));
        EXPECT_EQ(idx.tablet_id(), new_tablet_id);
    }

    // Partition version must be written under the NEW partition_id.
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader reader(instance_id, Versionstamp::max());
        VersionPB ver;
        // new_partition_id must have a version entry
        EXPECT_EQ(reader.get_partition_version(txn.get(), new_partition_id, &ver, nullptr, false),
                  TxnErrorCode::TXN_OK)
                << "partition version must be written under new_partition_id";
        EXPECT_EQ(ver.version(), end_version);
    }

    // rowset stored in FDB must still carry the original rowset_id_v2 (S3 path preserved).
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader reader(instance_id, Versionstamp::max());
        std::vector<std::pair<RowsetMetaCloudPB, Versionstamp>> rowsets;
        reader.get_load_rowset_metas(txn.get(), new_tablet_id, &rowsets, false);
        ASSERT_EQ(rowsets.size(), 1u);
        EXPECT_EQ(rowsets[0].first.rowset_id_v2(), rowset_id)
                << "rowset_id_v2 must be unchanged — S3 path must be preserved";
        EXPECT_EQ(rowsets[0].first.tablet_id(), new_tablet_id)
                << "tablet_id inside stored rowset must be remapped";
        EXPECT_EQ(rowsets[0].first.source_tablet_id(), old_tablet_id)
                << "source_tablet_id must record the original for lineage";
    }
}

// V1: compact and load rowsets may share end_version in meta_rowset_key; the compact write
// must win (it is the merged definitive rowset). Verify: only the compact rowset_id_v2 is
// stored under the shared end_version key.
TEST(MetaServiceSnapshotTest, ImportTableMetaV1CompactWinsTest) {
    auto ms = get_meta_service(true);
    const std::string instance_id = "test_instance";
    SnapshotManager mgr(ms->txn_kv());

    const int64_t table_id     = 8001;
    const int64_t partition_id = 8002;
    const int64_t tablet_id    = 8003;
    const int64_t shared_ver   = 5;

    TableFdbMetaPB fdb_meta;
    fdb_meta.set_table_id(table_id);
    {
        auto* idx = fdb_meta.add_tablet_indexes();
        idx->set_db_id(1);
        idx->set_table_id(table_id);
        idx->set_index_id(1);
        idx->set_partition_id(partition_id);
        idx->set_tablet_id(tablet_id);
    }
    {
        auto* pv = fdb_meta.add_partition_versions();
        pv->set_partition_id(partition_id);
        pv->mutable_version()->set_version(shared_ver);
    }
    // Load rowset at end_version=shared_ver.
    {
        auto* rs = fdb_meta.add_load_rowsets();
        rs->set_tablet_id(tablet_id);
        rs->set_partition_id(partition_id);
        rs->set_start_version(shared_ver);
        rs->set_end_version(shared_ver);
        rs->set_rowset_id_v2("load_rowset_at_v5");
    }
    // Compact rowset at the same end_version (absorbed the load rowset).
    {
        auto* rs = fdb_meta.add_compact_rowsets();
        rs->set_tablet_id(tablet_id);
        rs->set_partition_id(partition_id);
        rs->set_start_version(1);
        rs->set_end_version(shared_ver);
        rs->set_rowset_id_v2("compact_rowset_v1_to_v5");
    }

    ImportTableMetaRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_table_id(table_id);
    req.set_fdb_meta_pb(fdb_meta.SerializeAsString());

    ImportTableMetaResponse resp;
    // is_versioned_write=false → V1 path: both rowsets use meta_rowset_key.
    mgr.import_table_meta(instance_id, req, &resp, /*is_versioned_write=*/false);
    ASSERT_EQ(resp.status().code(), MetaServiceCode::OK) << resp.status().msg();

    // Both load and compact are counted in rowsets_restored (2 written, 1 wins in FDB).
    EXPECT_EQ(resp.tablets_restored(), 1);
    EXPECT_EQ(resp.partitions_restored(), 1);
    // Two rowsets were written to the WriteBatch; compact overwrites load at same key.
    EXPECT_EQ(resp.rowsets_restored(), 2);

    // The surviving V1 key must carry the compact rowset (last write wins).
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string key = meta_rowset_key({instance_id, tablet_id, shared_ver});
        std::string val;
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK)
                << "meta_rowset_key at shared_ver must exist";
        RowsetMetaCloudPB stored;
        ASSERT_TRUE(stored.ParseFromString(val));
        EXPECT_EQ(stored.rowset_id_v2(), "compact_rowset_v1_to_v5")
                << "compact rowset must win over load rowset at the same end_version in V1";
    }
}

// ── Test 12: target_table_id applied to V2 tablet meta ────────────────────────────────────────
// Regression guard: after Fix 2, both the tablet index AND the versioned tablet meta must
// carry target_table_id when it is set in the request.
TEST(MetaServiceSnapshotTest, ImportTableMetaTargetTableIdV2Test) {
    auto ms = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";
    const std::string instance_id = "test_instance";

    const int64_t src_table_id  = 9001;
    const int64_t tgt_table_id  = 9999;
    const int64_t partition_id  = 9002;
    const int64_t tablet_id     = 9003;
    const int64_t end_version   = 7;

    TableFdbMetaPB fdb_meta;
    fdb_meta.set_table_id(src_table_id);
    {
        auto* idx = fdb_meta.add_tablet_indexes();
        idx->set_db_id(1);
        idx->set_table_id(src_table_id);
        idx->set_index_id(1);
        idx->set_partition_id(partition_id);
        idx->set_tablet_id(tablet_id);
    }
    {
        // Tablet meta with source table_id — import_table_meta must override it with
        // target_table_id on both the versioned (V2) tablet meta key AND the tablet index.
        auto* tab = fdb_meta.add_tablets();
        tab->set_tablet_id(tablet_id);
        tab->set_table_id(src_table_id);
        tab->set_partition_id(partition_id);
    }
    {
        auto* pv = fdb_meta.add_partition_versions();
        pv->set_partition_id(partition_id);
        pv->mutable_version()->set_version(end_version);
    }
    {
        auto* rs = fdb_meta.add_load_rowsets();
        rs->set_tablet_id(tablet_id);
        rs->set_partition_id(partition_id);
        rs->set_start_version(end_version);
        rs->set_end_version(end_version);
        rs->set_rowset_id_v2("rowset_for_target_table_id_test");
    }

    ImportTableMetaRequest req;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_table_id(src_table_id);
    req.set_target_table_id(tgt_table_id);
    req.set_fdb_meta_pb(fdb_meta.SerializeAsString());

    brpc::Controller cntl;
    ImportTableMetaResponse resp;
    ms->import_table_meta(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                          &req, &resp, nullptr);
    ASSERT_EQ(resp.status().code(), MetaServiceCode::OK) << resp.status().msg();
    EXPECT_EQ(resp.tablets_restored(), 1);

    // Tablet index must carry target_table_id.
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string key = meta_tablet_idx_key({instance_id, tablet_id});
        std::string val;
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        TabletIndexPB idx;
        ASSERT_TRUE(idx.ParseFromString(val));
        EXPECT_EQ(idx.table_id(), tgt_table_id)
                << "tablet index must carry target_table_id";
    }

    // V2 versioned tablet meta must also carry target_table_id (Fix 2 regression guard).
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader reader(instance_id, Versionstamp::max());
        doris::TabletMetaCloudPB tab_meta;
        ASSERT_EQ(reader.get_tablet_meta(txn.get(), tablet_id, &tab_meta, nullptr, false),
                  TxnErrorCode::TXN_OK)
                << "versioned tablet meta must exist";
        EXPECT_EQ(tab_meta.table_id(), tgt_table_id)
                << "versioned tablet meta must carry target_table_id — Fix 2 regression";
    }
}

// V1 delete bitmap import with tablet ID remap: keys must be written under the remapped
// (new) tablet_id, not the original. Uses is_versioned_write=false so the V1
// meta_delete_bitmap_key can be read back directly via transaction.
TEST(MetaServiceSnapshotTest, ImportTableMetaDeleteBitmapRemapTest) {
    auto ms = get_meta_service(true);
    const std::string instance_id = "test_instance";
    SnapshotManager mgr(ms->txn_kv());

    const int64_t old_tablet_id    = 11001;
    const int64_t new_tablet_id    = 11002;
    const int64_t partition_id     = 11003;
    const int64_t table_id         = 11004;
    const int64_t end_version      = 4;
    const std::string rowset_id    = "bm_rowset_001";
    const int64_t segment_id       = 0;
    const std::string bitmap_bytes = "\x01\x02\x03";  // dummy roaringbitmap bytes

    TableFdbMetaPB fdb_meta;
    fdb_meta.set_table_id(table_id);
    {
        auto* idx = fdb_meta.add_tablet_indexes();
        idx->set_db_id(1);
        idx->set_table_id(table_id);
        idx->set_index_id(1);
        idx->set_partition_id(partition_id);
        idx->set_tablet_id(old_tablet_id);
    }
    {
        auto* pv = fdb_meta.add_partition_versions();
        pv->set_partition_id(partition_id);
        pv->mutable_version()->set_version(end_version);
    }
    {
        auto* rs = fdb_meta.add_load_rowsets();
        rs->set_tablet_id(old_tablet_id);
        rs->set_partition_id(partition_id);
        rs->set_start_version(end_version);
        rs->set_end_version(end_version);
        rs->set_rowset_id_v2(rowset_id);
    }
    // Add a delete bitmap entry for old_tablet_id.
    {
        auto* bm = fdb_meta.add_delete_bitmaps();
        bm->set_tablet_id(old_tablet_id);
        bm->set_rowset_id(rowset_id);
        bm->set_version(end_version);
        bm->set_segment_id(segment_id);
        bm->set_bitmap(bitmap_bytes);
    }

    ImportTableMetaRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    req.set_table_id(table_id);
    req.set_fdb_meta_pb(fdb_meta.SerializeAsString());
    (*req.mutable_tablet_id_remap())[old_tablet_id] = new_tablet_id;

    ImportTableMetaResponse resp;
    // V1 path: delete bitmaps written as one key per (tablet_id, rowset_id, version, segment_id).
    mgr.import_table_meta(instance_id, req, &resp, /*is_versioned_write=*/false);
    ASSERT_EQ(resp.status().code(), MetaServiceCode::OK) << resp.status().msg();
    EXPECT_EQ(resp.tablets_restored(), 1);

    // Delete bitmap key for OLD tablet_id must NOT exist.
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string old_key = meta_delete_bitmap_key(
                {instance_id, old_tablet_id, rowset_id, end_version, segment_id});
        std::string val;
        EXPECT_NE(txn->get(old_key, &val), TxnErrorCode::TXN_OK)
                << "bitmap key for old_tablet_id must NOT exist after remap";
    }

    // Delete bitmap key for NEW tablet_id must exist with the original bitmap bytes.
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string new_key = meta_delete_bitmap_key(
                {instance_id, new_tablet_id, rowset_id, end_version, segment_id});
        std::string val;
        ASSERT_EQ(txn->get(new_key, &val), TxnErrorCode::TXN_OK)
                << "bitmap key for new_tablet_id must exist after remap";
        EXPECT_EQ(val, bitmap_bytes)
                << "bitmap bytes must be preserved after tablet ID remap";
    }
}

// export_table_meta on a V1 cluster exports rowsets to the blob without seeding ref counts.
// Seeding is now done by seed_rowset_ref_counts at commit_snapshot time.
// Verify: blob uploaded, data_rowset_ref_count_key absent, second call is idempotent.
TEST(MetaServiceSnapshotTest, ExportTableMetaV1FallbackTest) {
    auto ms = create_meta_service_with_instance();
    ASSERT_NE(ms, nullptr);

    // Create a snapshot in FDB so export_table_meta can mark table_meta_exported.
    std::string snapshot_id = do_begin_snapshot(ms.get(), "v1_export_test");
    do_commit_snapshot(ms.get(), snapshot_id);
    Versionstamp snapshot_vs;
    ASSERT_TRUE(SnapshotManager::parse_snapshot_versionstamp(snapshot_id, &snapshot_vs));

    const std::string instance_id = "test_instance";
    const int64_t table_id     = 20001;
    const int64_t db_id        = 20002;
    const int64_t partition_id = 20003;
    const int64_t index_id     = 20004;
    const int64_t tablet_id    = 20005;
    const std::string rowset_id = "rs_v1_export_test";
    const int64_t end_version  = 3;

    // Write V1 tablet index and meta_rowset_key (no 0x03 versioned rowsets).
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        TabletIndexPB idx;
        idx.set_db_id(db_id);
        idx.set_table_id(table_id);
        idx.set_index_id(index_id);
        idx.set_partition_id(partition_id);
        idx.set_tablet_id(tablet_id);
        txn->put(meta_tablet_idx_key({instance_id, tablet_id}), idx.SerializeAsString());
        RowsetMetaCloudPB rs;
        rs.set_tablet_id(tablet_id);
        rs.set_partition_id(partition_id);
        rs.set_start_version(1);
        rs.set_end_version(end_version);
        rs.set_rowset_id_v2(rowset_id);
        rs.set_num_rows(100);
        txn->put(meta_rowset_key({instance_id, tablet_id, end_version}), rs.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    SnapshotPB snap;
    snap.set_instance_id(instance_id);
    snap.add_protected_table_ids(table_id);
    snap.set_image_url("mock_vault/snapshot/vs_test/fe_image");

    MockAccessor accessor;
    SnapshotManager mgr(ms->txn_kv());

    // V1 fallback: rowset exported to blob; no inline seeding (done at commit_snapshot time).
    int ret = mgr.export_table_meta(instance_id, &accessor, snapshot_vs, snap);
    ASSERT_EQ(ret, 0) << "export_table_meta must succeed on V1 cluster";

    // No ref count key — export_table_meta no longer seeds inline.
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string ref_key = versioned::data_rowset_ref_count_key(
                {instance_id, tablet_id, rowset_id});
        std::string val;
        EXPECT_EQ(txn->get(ref_key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND)
                << "export_table_meta must not seed ref counts — that is commit_snapshot's job";
    }

    // Idempotent: second export returns 0.
    ret = mgr.export_table_meta(instance_id, &accessor, snapshot_vs, snap);
    ASSERT_EQ(ret, 0);
}

// seed_rowset_ref_counts must not double-increment when called again with the same
// snapshot versionstamp — the join key guard prevents a second atomic_add.
// Validates idempotency across recycler retry cycles.
TEST(MetaServiceSnapshotTest, SeedRowsetRefCountsIdempotent) {
    EncryptionSyncPointGuard enc_guard;
    auto ms = create_meta_service_with_instance();
    ASSERT_NE(ms, nullptr);

    // Begin and commit a snapshot so seed_rowset_ref_counts can mark rowset_refs_seeded.
    std::string snapshot_id = do_begin_snapshot(ms.get(), "seed_idem_snap");
    do_commit_snapshot(ms.get(), snapshot_id);

    Versionstamp snapshot_vs;
    ASSERT_TRUE(SnapshotManager::parse_snapshot_versionstamp(snapshot_id, &snapshot_vs));

    const std::string instance_id = "test_instance";
    const int64_t table_id     = 30001;
    const int64_t db_id        = 30002;
    const int64_t partition_id = 30003;
    const int64_t index_id     = 30004;
    const int64_t tablet_id    = 30005;
    const std::string rowset_id = "rs_seed_idem_test";
    const int64_t end_version  = 5;

    // Write a tablet index key so the scanner can find this tablet.
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        TabletIndexPB idx;
        idx.set_db_id(db_id);
        idx.set_table_id(table_id);
        idx.set_index_id(index_id);
        idx.set_partition_id(partition_id);
        idx.set_tablet_id(tablet_id);
        txn->put(meta_tablet_idx_key({instance_id, tablet_id}), idx.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Write a V1 rowset (meta_rowset_key) so the V1 fallback path in seed seeds it.
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        RowsetMetaCloudPB rs;
        rs.set_tablet_id(tablet_id);
        rs.set_partition_id(partition_id);
        rs.set_start_version(1);
        rs.set_end_version(end_version);
        rs.set_rowset_id_v2(rowset_id);
        rs.set_num_rows(200);
        txn->put(meta_rowset_key({instance_id, tablet_id, end_version}), rs.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    SnapshotManager mgr(ms->txn_kv());

    // First call: seeds the rowset and marks rowset_refs_seeded.
    ASSERT_EQ(mgr.seed_rowset_ref_counts(instance_id, snapshot_vs, {}, {}, {}), 0)
            << "first seed_rowset_ref_counts call must succeed";

    // Capture ref count key value after first call.
    std::string ref_key = versioned::data_rowset_ref_count_key(
            {instance_id, tablet_id, rowset_id});
    std::string join_key = versioned::snapshot_rowset_ref_key(
            {instance_id, snapshot_vs, tablet_id, rowset_id});

    std::string val_after_first;
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(ref_key, &val_after_first), TxnErrorCode::TXN_OK)
                << "ref_count_key must exist after first seed";
        std::string join_val;
        ASSERT_EQ(txn->get(join_key, &join_val), TxnErrorCode::TXN_OK)
                << "join_key must exist after first seed";
    }

    // Second call: join_key already exists → atomic_add is skipped → ref count unchanged.
    // The function re-marks rowset_refs_seeded; overall return must still be 0.
    ASSERT_EQ(mgr.seed_rowset_ref_counts(instance_id, snapshot_vs, {}, {}, {}), 0)
            << "second seed_rowset_ref_counts call must be idempotent (return 0)";

    std::string val_after_second;
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(ref_key, &val_after_second), TxnErrorCode::TXN_OK)
                << "ref_count_key must still exist after second seed";
    }

    // The raw bytes must be identical — idempotency guard prevented double atomic_add.
    EXPECT_EQ(val_after_first, val_after_second)
            << "ref count must not be incremented by the second seed call";
}

// import_table_meta is retry-safe: calling it twice with the same request (simulating a
// transient RPC failure) must succeed and leave the same FDB state.
// Uses V1 path (is_versioned_write=false) for straightforward key verification.
TEST(MetaServiceSnapshotTest, ImportTableMetaRetryIdempotent) {
    auto ms = get_meta_service(true);
    const char* const cloud_unique_id = "test_cloud_unique_id";
    const std::string instance_id = "test_instance";
    SnapshotManager mgr(ms->txn_kv());

    const int64_t table_id     = 31001;
    const int64_t partition_id = 31002;
    const int64_t tablet_id    = 31003;
    const int64_t index_id     = 31004;
    const int64_t db_id        = 31005;
    const int64_t end_version  = 8;
    const std::string rowset_id = "rs_retry_idem_test";

    // Build a TableFdbMetaPB with one tablet, one rowset, one partition version.
    TableFdbMetaPB fdb_meta;
    fdb_meta.set_table_id(table_id);
    {
        auto* idx = fdb_meta.add_tablet_indexes();
        idx->set_db_id(db_id);
        idx->set_table_id(table_id);
        idx->set_index_id(index_id);
        idx->set_partition_id(partition_id);
        idx->set_tablet_id(tablet_id);
    }
    {
        auto* rs = fdb_meta.add_load_rowsets();
        rs->set_tablet_id(tablet_id);
        rs->set_partition_id(partition_id);
        rs->set_start_version(end_version);
        rs->set_end_version(end_version);
        rs->set_rowset_id_v2(rowset_id);
        rs->set_num_rows(50);
    }
    {
        auto* pv = fdb_meta.add_partition_versions();
        pv->set_partition_id(partition_id);
        pv->mutable_version()->set_version(end_version);
    }

    ImportTableMetaRequest req;
    req.set_cloud_unique_id(cloud_unique_id);
    req.set_table_id(table_id);
    req.set_fdb_meta_pb(fdb_meta.SerializeAsString());

    // First call — simulates the original import.
    ImportTableMetaResponse resp1;
    mgr.import_table_meta(instance_id, req, &resp1, /*is_versioned_write=*/false);
    ASSERT_EQ(resp1.status().code(), MetaServiceCode::OK) << resp1.status().msg();
    EXPECT_EQ(resp1.tablets_restored(), 1);
    EXPECT_EQ(resp1.rowsets_restored(), 1);
    EXPECT_EQ(resp1.partitions_restored(), 1);

    // Second call — simulates a retry after a transient RPC failure.
    ImportTableMetaResponse resp2;
    mgr.import_table_meta(instance_id, req, &resp2, /*is_versioned_write=*/false);
    ASSERT_EQ(resp2.status().code(), MetaServiceCode::OK)
            << "retry must succeed (idempotent puts): " << resp2.status().msg();
    EXPECT_EQ(resp2.tablets_restored(), 1);
    EXPECT_EQ(resp2.rowsets_restored(), 1);
    EXPECT_EQ(resp2.partitions_restored(), 1);

    // Verify: tablet index key is present and carries correct IDs.
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string key = meta_tablet_idx_key({instance_id, tablet_id});
        std::string val;
        ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK)
                << "tablet index key must be present after retry";
        TabletIndexPB idx;
        ASSERT_TRUE(idx.ParseFromString(val));
        EXPECT_EQ(idx.tablet_id(), tablet_id);
        EXPECT_EQ(idx.partition_id(), partition_id);
        EXPECT_EQ(idx.table_id(), table_id);
    }

    // Verify: stats_tablet_key exists (V1 path writes it to protect against recycler).
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        StatsTabletKeyInfo ski {instance_id, table_id, index_id, partition_id, tablet_id};
        std::string stats_key = stats_tablet_key(ski);
        std::string val;
        ASSERT_EQ(txn->get(stats_key, &val), TxnErrorCode::TXN_OK)
                << "stats_tablet_key must be present after V1 import";
    }

    // Verify: partition version (V1 key) exists under the correct db/table/partition path.
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(ms->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        // V1 path writes partition_version_key({instance_id, db_id, table_id, partition_id}).
        std::string pv_key = partition_version_key({instance_id, db_id, table_id, partition_id});
        std::string pv_val;
        ASSERT_EQ(txn->get(pv_key, &pv_val), TxnErrorCode::TXN_OK)
                << "V1 partition_version_key must be present after import";
        VersionPB ver;
        ASSERT_TRUE(ver.ParseFromString(pv_val));
        EXPECT_EQ(ver.version(), end_version);
    }
}

} // namespace doris::cloud