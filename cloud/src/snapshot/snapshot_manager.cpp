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

#include "snapshot/snapshot_manager.h"

#include <fmt/format.h>

#include <algorithm>
#include <chrono>

#include "common/logging.h"
#include "meta-service/meta_service_helper.h"
#include "meta-store/keys.h"
#include "meta-store/meta_reader.h"
#include "meta-store/versioned_value.h"
#include "meta-store/versionstamp.h"
#include "recycler/checker.h"
#include "recycler/recycler.h"
#include "recycler/storage_vault_accessor.h"

namespace doris::cloud {

using namespace std::chrono;

// ── helpers ──────────────────────────────────────────────────────────────────

static int64_t unix_seconds_now() {
    return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

// Read and parse InstanceInfoPB for instance_id within an existing transaction.
static std::pair<MetaServiceCode, std::string> get_instance_info(Transaction* txn,
                                                                  std::string_view instance_id,
                                                                  InstanceInfoPB* out) {
    std::string key = instance_key({std::string(instance_id)});
    std::string val;
    TxnErrorCode err = txn->get(key, &val);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        return {MetaServiceCode::CLUSTER_NOT_FOUND,
                fmt::format("instance not found, instance_id={}", instance_id)};
    }
    if (err != TxnErrorCode::TXN_OK) {
        return {cast_as<ErrCategory::READ>(err),
                fmt::format("failed to get instance, instance_id={}, err={}", instance_id, err)};
    }
    if (!out->ParseFromString(val)) {
        return {MetaServiceCode::PROTOBUF_PARSE_ERR,
                fmt::format("failed to parse InstanceInfoPB, instance_id={}", instance_id)};
    }
    return {MetaServiceCode::OK, ""};
}

// Read StorageVaultPB for vault_id within an existing transaction.
// Returns OK and fills *out, or returns an error.
static std::pair<MetaServiceCode, std::string> get_vault(Transaction* txn,
                                                          std::string_view instance_id,
                                                          std::string_view vault_id,
                                                          StorageVaultPB* out) {
    std::string key = storage_vault_key({std::string(instance_id), std::string(vault_id)});
    std::string val;
    TxnErrorCode err = txn->get(key, &val);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        return {MetaServiceCode::STORAGE_VAULT_NOT_FOUND,
                fmt::format("vault not found, instance_id={}, vault_id={}", instance_id, vault_id)};
    }
    if (err != TxnErrorCode::TXN_OK) {
        return {cast_as<ErrCategory::READ>(err),
                fmt::format("failed to get vault, vault_id={}, err={}", vault_id, err)};
    }
    if (!out->ParseFromString(val)) {
        return {MetaServiceCode::PROTOBUF_PARSE_ERR,
                fmt::format("failed to parse StorageVaultPB, vault_id={}", vault_id)};
    }
    return {MetaServiceCode::OK, ""};
}

// Build the image_url for a snapshot given its vault path prefix and snapshot_id.
// Format: <vault_prefix>/snapshots/<snapshot_id>/image
static std::string build_image_url(std::string_view vault_prefix,
                                   std::string_view snapshot_id) {
    // Ensure no double slashes
    std::string prefix(vault_prefix);
    if (!prefix.empty() && prefix.back() == '/') {
        prefix.pop_back();
    }
    return fmt::format("{}/snapshots/{}/image", prefix, snapshot_id);
}

// ── versionstamp helpers ──────────────────────────────────────────────────────

bool SnapshotManager::parse_snapshot_versionstamp(std::string_view snapshot_id,
                                                  Versionstamp* versionstamp) {
    if (snapshot_id.size() != 20) {
        return false;
    }

    std::array<uint8_t, 10> versionstamp_data;
    for (size_t i = 0; i < 10; ++i) {
        const char* hex_chars = snapshot_id.data() + (i * 2);

        uint8_t high_nibble = 0, low_nibble = 0;
        if (hex_chars[0] >= '0' && hex_chars[0] <= '9') {
            high_nibble = hex_chars[0] - '0';
        } else if (hex_chars[0] >= 'a' && hex_chars[0] <= 'f') {
            high_nibble = hex_chars[0] - 'a' + 10;
        } else if (hex_chars[0] >= 'A' && hex_chars[0] <= 'F') {
            high_nibble = hex_chars[0] - 'A' + 10;
        } else {
            return false;
        }

        if (hex_chars[1] >= '0' && hex_chars[1] <= '9') {
            low_nibble = hex_chars[1] - '0';
        } else if (hex_chars[1] >= 'a' && hex_chars[1] <= 'f') {
            low_nibble = hex_chars[1] - 'a' + 10;
        } else if (hex_chars[1] >= 'A' && hex_chars[1] <= 'F') {
            low_nibble = hex_chars[1] - 'A' + 10;
        } else {
            return false;
        }

        versionstamp_data[i] = (high_nibble << 4) | low_nibble;
    }

    *versionstamp = Versionstamp(versionstamp_data);
    return true;
}

std::string SnapshotManager::serialize_snapshot_id(Versionstamp snapshot_versionstamp) {
    return snapshot_versionstamp.to_string();
}

// ── begin_snapshot ────────────────────────────────────────────────────────────

void SnapshotManager::begin_snapshot(std::string_view instance_id,
                                     const BeginSnapshotRequest& request,
                                     BeginSnapshotResponse* response) {
    // Validate required fields
    if (request.snapshot_label().empty()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("snapshot_label must not be empty");
        return;
    }
    if (request.ttl_seconds() <= 0) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("ttl_seconds must be positive");
        return;
    }
    // Prevent create_at + ttl_seconds int64_t overflow: max TTL = 1 year
    static constexpr int64_t kMaxTtlSeconds = 365LL * 24 * 3600; // 31,536,000
    if (request.ttl_seconds() > kMaxTtlSeconds) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(
                fmt::format("ttl_seconds={} exceeds maximum of {} (1 year)",
                            request.ttl_seconds(), kMaxTtlSeconds));
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
        response->mutable_status()->set_msg("failed to create transaction");
        return;
    }

    // Read instance to validate it exists and find the vault
    InstanceInfoPB instance;
    auto [inst_code, inst_msg] = get_instance_info(txn.get(), instance_id, &instance);
    if (inst_code != MetaServiceCode::OK) {
        response->mutable_status()->set_code(inst_code);
        response->mutable_status()->set_msg(inst_msg);
        return;
    }

    // Resolve vault: use requested vault_name, fall back to default vault
    std::string vault_id;
    std::string vault_prefix;
    ObjectStoreInfoPB obj_info_pb;
    // Fix 6: track whether vault was resolved from legacy obj_info path so we don't
    // call get_vault() for legacy instances (their vault_id is not in the vault key namespace).
    bool resolved_from_legacy_obj_info = false;

    if (!request.vault_name().empty()) {
        // Find vault_id by name
        auto& names = instance.storage_vault_names();
        auto& ids = instance.resource_ids();
        auto it = std::find(names.begin(), names.end(), request.vault_name());
        if (it == names.end()) {
            response->mutable_status()->set_code(MetaServiceCode::STORAGE_VAULT_NOT_FOUND);
            response->mutable_status()->set_msg(
                    fmt::format("vault_name={} not found", request.vault_name()));
            return;
        }
        vault_id = ids.Get(static_cast<int>(it - names.begin()));
    } else if (!instance.default_storage_vault_id().empty()) {
        vault_id = instance.default_storage_vault_id();
    } else if (!instance.obj_info().empty()) {
        // Legacy: use the first legacy obj_info
        obj_info_pb = instance.obj_info(0);
        vault_prefix = obj_info_pb.prefix();
        vault_id = obj_info_pb.id();
        resolved_from_legacy_obj_info = true;
    } else {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("no storage vault available for snapshot");
        return;
    }

    // If we found a vault_id via storage_vault_names or default path, read the StorageVaultPB.
    // Do NOT call get_vault for legacy obj_info instances — their id is not in the vault namespace.
    if (!resolved_from_legacy_obj_info && !vault_id.empty()) {
        StorageVaultPB vault;
        auto [vcode, vmsg] = get_vault(txn.get(), instance_id, vault_id, &vault);
        if (vcode != MetaServiceCode::OK) {
            response->mutable_status()->set_code(vcode);
            response->mutable_status()->set_msg(vmsg);
            return;
        }
        if (vault.has_obj_info()) {
            obj_info_pb = vault.obj_info();
            vault_prefix = obj_info_pb.prefix();
        } else if (vault.has_hdfs_info()) {
            // HDFS-backed vaults are not supported for snapshot image storage.
            // Snapshot upload/download requires object-store semantics (multipart upload,
            // presigned URLs). Use an OSS/S3 vault for snapshots.
            response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
            response->mutable_status()->set_msg(
                    fmt::format("vault_id={} is HDFS-backed; snapshot image storage requires "
                                "an object-store vault (OSS/S3). "
                                "Specify an object-store vault via vault_name property.",
                                vault_id));
            return;
        } else {
            response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
            response->mutable_status()->set_msg(
                    fmt::format("vault_id={} has no supported storage backend", vault_id));
            return;
        }
    }

    // Build SnapshotPB — status stays PREPARING until commit
    int64_t now = unix_seconds_now();
    int64_t timeout_secs = request.timeout_seconds() > 0 ? request.timeout_seconds() : 3600;

    SnapshotPB snapshot_pb;
    snapshot_pb.set_status(SnapshotStatus::SNAPSHOT_PREPARING);
    snapshot_pb.set_label(request.snapshot_label());
    snapshot_pb.set_ttl_seconds(request.ttl_seconds());
    snapshot_pb.set_timeout_seconds(timeout_secs);
    snapshot_pb.set_create_at(now);
    snapshot_pb.set_instance_id(std::string(instance_id));
    snapshot_pb.set_resource_id(vault_id);
    snapshot_pb.set_auto_(request.auto_snapshot());

    std::string snapshot_val;
    if (!snapshot_pb.SerializeToString(&snapshot_val)) {
        response->mutable_status()->set_code(MetaServiceCode::PROTOBUF_SERIALIZE_ERR);
        response->mutable_status()->set_msg("failed to serialize SnapshotPB");
        return;
    }

    // Get a FDB versionstamp to use as the snapshot_id and key suffix.
    // Pattern: first txn uses enable_get_versionstamp() + a trivial write to force a commit
    // that yields a versionstamp. Second txn writes the SnapshotPB at the versioned key.
    // This matches how get_snapshot() reads: encode_versioned_key(base_key, versionstamp).
    Versionstamp vs;
    {
        std::unique_ptr<Transaction> vs_txn;
        TxnErrorCode vs_err = txn_kv_->create_txn(&vs_txn);
        if (vs_err != TxnErrorCode::TXN_OK) {
            response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
            response->mutable_status()->set_msg("failed to create versionstamp txn");
            return;
        }
        vs_txn->enable_get_versionstamp();
        // Write an instance touch to force a real commit (needed to get a versionstamp)
        vs_txn->atomic_add(system_meta_service_instance_update_key(), 1);
        vs_err = vs_txn->commit();
        if (vs_err != TxnErrorCode::TXN_OK) {
            response->mutable_status()->set_code(cast_as<ErrCategory::COMMIT>(vs_err));
            response->mutable_status()->set_msg("failed to get versionstamp");
            return;
        }
        vs_err = vs_txn->get_versionstamp(&vs);
        if (vs_err != TxnErrorCode::TXN_OK) {
            response->mutable_status()->set_code(cast_as<ErrCategory::READ>(vs_err));
            response->mutable_status()->set_msg("failed to read versionstamp after commit");
            return;
        }
    }

    // Write the SnapshotPB at the versioned key encode_versioned_key(base_key, vs).
    // This is exactly what get_snapshot() / get_snapshots() reads.
    std::string snapshot_base_key = versioned::snapshot_full_key({std::string(instance_id)});
    std::string snapshot_key = encode_versioned_key(snapshot_base_key, vs);

    err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
        response->mutable_status()->set_msg("failed to create snapshot write txn");
        return;
    }
    txn->put(snapshot_key, snapshot_val);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(cast_as<ErrCategory::COMMIT>(err));
        response->mutable_status()->set_msg(
                fmt::format("failed to commit begin_snapshot txn, err={}", err));
        return;
    }

    std::string snapshot_id = serialize_snapshot_id(vs);
    std::string image_url = build_image_url(vault_prefix, snapshot_id);

    LOG_INFO("begin_snapshot OK")
            .tag("instance_id", instance_id)
            .tag("snapshot_id", snapshot_id)
            .tag("label", request.snapshot_label())
            .tag("ttl_seconds", request.ttl_seconds())
            .tag("image_url", image_url);

    response->set_snapshot_id(snapshot_id);
    response->set_image_url(image_url);
    *response->mutable_obj_info() = obj_info_pb;
    response->mutable_status()->set_code(MetaServiceCode::OK);
}

// ── update_snapshot ───────────────────────────────────────────────────────────
// Called by FE during multipart upload to record upload_file + upload_id
// so that a crashed upload can be aborted by abort_snapshot.

void SnapshotManager::update_snapshot(std::string_view instance_id,
                                      const UpdateSnapshotRequest& request,
                                      UpdateSnapshotResponse* response) {
    if (request.snapshot_id().empty()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("snapshot_id must not be empty");
        return;
    }

    Versionstamp vs;
    if (!parse_snapshot_versionstamp(request.snapshot_id(), &vs)) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(
                fmt::format("invalid snapshot_id={}", request.snapshot_id()));
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
        response->mutable_status()->set_msg("failed to create transaction");
        return;
    }

    MetaReader reader(instance_id);
    SnapshotPB snapshot_pb;
    err = reader.get_snapshot(txn.get(), vs, &snapshot_pb);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(
                fmt::format("snapshot not found, snapshot_id={}", request.snapshot_id()));
        return;
    }
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(cast_as<ErrCategory::READ>(err));
        response->mutable_status()->set_msg(
                fmt::format("failed to get snapshot, err={}", err));
        return;
    }

    if (snapshot_pb.status() != SnapshotStatus::SNAPSHOT_PREPARING) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(
                fmt::format("snapshot is not in PREPARING state, status={}",
                            SnapshotStatus_Name(snapshot_pb.status())));
        return;
    }

    // Record multipart upload metadata for crash recovery
    if (!request.upload_file().empty()) {
        snapshot_pb.set_upload_file(request.upload_file());
    }
    if (!request.upload_id().empty()) {
        snapshot_pb.set_upload_id(request.upload_id());
    }

    std::string snapshot_val;
    if (!snapshot_pb.SerializeToString(&snapshot_val)) {
        response->mutable_status()->set_code(MetaServiceCode::PROTOBUF_SERIALIZE_ERR);
        response->mutable_status()->set_msg("failed to serialize SnapshotPB");
        return;
    }

    std::string snapshot_base_key = versioned::snapshot_full_key({std::string(instance_id)});
    std::string snapshot_key = encode_versioned_key(snapshot_base_key, vs);
    txn->put(snapshot_key, snapshot_val);

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(cast_as<ErrCategory::COMMIT>(err));
        response->mutable_status()->set_msg(
                fmt::format("failed to commit update_snapshot txn, err={}", err));
        return;
    }

    LOG_INFO("update_snapshot OK")
            .tag("instance_id", instance_id)
            .tag("snapshot_id", request.snapshot_id());
    response->mutable_status()->set_code(MetaServiceCode::OK);
}

// ── commit_snapshot ───────────────────────────────────────────────────────────

void SnapshotManager::commit_snapshot(std::string_view instance_id,
                                      const CommitSnapshotRequest& request,
                                      CommitSnapshotResponse* response) {
    if (request.snapshot_id().empty()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("snapshot_id must not be empty");
        return;
    }

    Versionstamp vs;
    if (!parse_snapshot_versionstamp(request.snapshot_id(), &vs)) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(
                fmt::format("invalid snapshot_id={}", request.snapshot_id()));
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
        response->mutable_status()->set_msg("failed to create transaction");
        return;
    }

    MetaReader reader(instance_id);
    SnapshotPB snapshot_pb;
    err = reader.get_snapshot(txn.get(), vs, &snapshot_pb);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(
                fmt::format("snapshot not found, snapshot_id={}", request.snapshot_id()));
        return;
    }
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(cast_as<ErrCategory::READ>(err));
        response->mutable_status()->set_msg(
                fmt::format("failed to get snapshot, err={}", err));
        return;
    }

    if (snapshot_pb.status() != SnapshotStatus::SNAPSHOT_PREPARING) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(
                fmt::format("snapshot is not in PREPARING state, current={}",
                            SnapshotStatus_Name(snapshot_pb.status())));
        return;
    }

    // Transition to READY and record image metadata
    snapshot_pb.set_status(SnapshotStatus::SNAPSHOT_READY);
    snapshot_pb.set_finish_at(unix_seconds_now());
    if (!request.image_url().empty()) {
        snapshot_pb.set_image_url(request.image_url());
    }
    if (request.last_journal_id() > 0) {
        snapshot_pb.set_last_journal_id(request.last_journal_id());
    }
    if (request.snapshot_meta_image_size() > 0) {
        snapshot_pb.set_snapshot_meta_image_size(request.snapshot_meta_image_size());
    }
    if (request.snapshot_logical_data_size() > 0) {
        snapshot_pb.set_snapshot_logical_data_size(request.snapshot_logical_data_size());
    }
    if (request.fdb_read_version() > 0) {
        snapshot_pb.set_fdb_read_version(request.fdb_read_version());
    }
    // Clear upload tracking fields — upload is done
    snapshot_pb.clear_upload_file();
    snapshot_pb.clear_upload_id();

    std::string snapshot_val;
    if (!snapshot_pb.SerializeToString(&snapshot_val)) {
        response->mutable_status()->set_code(MetaServiceCode::PROTOBUF_SERIALIZE_ERR);
        response->mutable_status()->set_msg("failed to serialize SnapshotPB");
        return;
    }

    std::string snapshot_base_key = versioned::snapshot_full_key({std::string(instance_id)});
    std::string snapshot_key = encode_versioned_key(snapshot_base_key, vs);
    txn->put(snapshot_key, snapshot_val);

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(cast_as<ErrCategory::COMMIT>(err));
        response->mutable_status()->set_msg(
                fmt::format("failed to commit commit_snapshot txn, err={}", err));
        return;
    }

    LOG_INFO("commit_snapshot OK")
            .tag("instance_id", instance_id)
            .tag("snapshot_id", request.snapshot_id())
            .tag("image_url", snapshot_pb.image_url())
            .tag("last_journal_id", snapshot_pb.last_journal_id());

    // Bug 1 fix: capture fdb_read_version AFTER the snapshot record is committed.
    // The FE's pre-commit version would miss the snapshot record itself — restoring
    // FDB to that version gives an empty snapshot list. We take a post-commit sync
    // point and overwrite fdb_read_version in a second write to the snapshot key.
    // This guarantees: fdbrestore --version <fdb_read_version> includes this record.
    {
        std::unique_ptr<Transaction> post_txn;
        TxnErrorCode post_err = txn_kv_->create_txn(&post_txn);
        if (post_err == TxnErrorCode::TXN_OK) {
            post_txn->enable_get_versionstamp();
            post_txn->atomic_add(system_meta_service_instance_update_key(), 1);
            post_err = post_txn->commit();
            if (post_err == TxnErrorCode::TXN_OK) {
                Versionstamp post_vs;
                post_err = post_txn->get_versionstamp(&post_vs);
                if (post_err == TxnErrorCode::TXN_OK) {
                    int64_t post_fdb_version = static_cast<int64_t>(post_vs.version());
                    snapshot_pb.set_fdb_read_version(post_fdb_version);
                    std::string updated_val;
                    if (snapshot_pb.SerializeToString(&updated_val)) {
                        std::unique_ptr<Transaction> upd_txn;
                        if (txn_kv_->create_txn(&upd_txn) == TxnErrorCode::TXN_OK) {
                            std::string base_key = versioned::snapshot_full_key({std::string(instance_id)});
                            std::string snap_key = encode_versioned_key(base_key, vs);
                            upd_txn->put(snap_key, updated_val);
                            TxnErrorCode upd_err = upd_txn->commit();
                            if (upd_err != TxnErrorCode::TXN_OK) {
                                LOG_WARNING("commit_snapshot: failed to persist post-commit fdb_read_version; "
                                            "snapshot is READY but fdb_read_version may need manual correction")
                                        .tag("snapshot_id", request.snapshot_id())
                                        .tag("err", upd_err);
                            }
                        }
                    }
                    LOG_INFO("commit_snapshot: updated fdb_read_version to post-commit value")
                            .tag("snapshot_id", request.snapshot_id())
                            .tag("fdb_read_version", post_fdb_version);
                }
            }
        }
        if (post_err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("commit_snapshot: failed to capture post-commit fdb_read_version; "
                        "using FE-provided pre-commit value (may need +N offset at restore time)")
                    .tag("snapshot_id", request.snapshot_id())
                    .tag("err", post_err);
        }
    }

    response->mutable_status()->set_code(MetaServiceCode::OK);
}

// ── abort_snapshot ────────────────────────────────────────────────────────────

void SnapshotManager::abort_snapshot(std::string_view instance_id,
                                     const AbortSnapshotRequest& request,
                                     AbortSnapshotResponse* response) {
    if (request.snapshot_id().empty()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("snapshot_id must not be empty");
        return;
    }

    Versionstamp vs;
    if (!parse_snapshot_versionstamp(request.snapshot_id(), &vs)) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(
                fmt::format("invalid snapshot_id={}", request.snapshot_id()));
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
        response->mutable_status()->set_msg("failed to create transaction");
        return;
    }

    MetaReader reader(instance_id);
    SnapshotPB snapshot_pb;
    err = reader.get_snapshot(txn.get(), vs, &snapshot_pb);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Idempotent: already gone is fine
        response->mutable_status()->set_code(MetaServiceCode::OK);
        return;
    }
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(cast_as<ErrCategory::READ>(err));
        response->mutable_status()->set_msg(
                fmt::format("failed to get snapshot, err={}", err));
        return;
    }

    // Only PREPARING or ABORTED snapshots can be aborted — not READY or RECYCLED
    if (snapshot_pb.status() == SnapshotStatus::SNAPSHOT_READY ||
        snapshot_pb.status() == SnapshotStatus::SNAPSHOT_RECYCLED) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(
                fmt::format("cannot abort snapshot in status={}, use drop_snapshot for READY",
                            SnapshotStatus_Name(snapshot_pb.status())));
        return;
    }

    snapshot_pb.set_status(SnapshotStatus::SNAPSHOT_ABORTED);
    snapshot_pb.set_finish_at(unix_seconds_now());
    if (!request.reason().empty()) {
        snapshot_pb.set_reason(request.reason());
    }

    std::string snapshot_val;
    if (!snapshot_pb.SerializeToString(&snapshot_val)) {
        response->mutable_status()->set_code(MetaServiceCode::PROTOBUF_SERIALIZE_ERR);
        response->mutable_status()->set_msg("failed to serialize SnapshotPB");
        return;
    }

    std::string snapshot_base_key = versioned::snapshot_full_key({std::string(instance_id)});
    std::string snapshot_key = encode_versioned_key(snapshot_base_key, vs);
    txn->put(snapshot_key, snapshot_val);

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(cast_as<ErrCategory::COMMIT>(err));
        response->mutable_status()->set_msg(
                fmt::format("failed to commit abort_snapshot txn, err={}", err));
        return;
    }

    LOG_INFO("abort_snapshot OK")
            .tag("instance_id", instance_id)
            .tag("snapshot_id", request.snapshot_id())
            .tag("reason", request.reason());
    response->mutable_status()->set_code(MetaServiceCode::OK);
}

// ── drop_snapshot ─────────────────────────────────────────────────────────────

void SnapshotManager::drop_snapshot(std::string_view instance_id,
                                    const DropSnapshotRequest& request,
                                    DropSnapshotResponse* response) {
    if (request.snapshot_id().empty()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("snapshot_id must not be empty");
        return;
    }

    Versionstamp vs;
    if (!parse_snapshot_versionstamp(request.snapshot_id(), &vs)) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(
                fmt::format("invalid snapshot_id={}", request.snapshot_id()));
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
        response->mutable_status()->set_msg("failed to create transaction");
        return;
    }

    MetaReader reader(instance_id);
    SnapshotPB snapshot_pb;
    err = reader.get_snapshot(txn.get(), vs, &snapshot_pb);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // Idempotent
        response->mutable_status()->set_code(MetaServiceCode::OK);
        return;
    }
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(cast_as<ErrCategory::READ>(err));
        response->mutable_status()->set_msg(
                fmt::format("failed to get snapshot, err={}", err));
        return;
    }

    // Fix 4: guard against racing an active multipart upload.
    // If the snapshot is still in PREPARING state the FE may be actively uploading parts.
    // Mark it ABORTED first so abort_snapshot properly handles the multipart tracking.
    // RECYCLED is only valid from READY or already-RECYCLED (idempotent skip).
    if (snapshot_pb.status() == SnapshotStatus::SNAPSHOT_PREPARING) {
        // Redirect: abort instead of recycling so multipart upload tracking is cleared.
        LOG_INFO("drop_snapshot: snapshot is PREPARING, aborting instead of direct RECYCLED")
                .tag("instance_id", instance_id)
                .tag("snapshot_id", request.snapshot_id());
        snapshot_pb.set_status(SnapshotStatus::SNAPSHOT_ABORTED);
        snapshot_pb.set_finish_at(unix_seconds_now());
        snapshot_pb.set_reason("dropped by operator while upload in progress");
    } else {
        // Mark RECYCLED — recycler will delete the object store file and then remove the key
        snapshot_pb.set_status(SnapshotStatus::SNAPSHOT_RECYCLED);
        snapshot_pb.set_finish_at(unix_seconds_now());
    }

    std::string snapshot_val;
    if (!snapshot_pb.SerializeToString(&snapshot_val)) {
        response->mutable_status()->set_code(MetaServiceCode::PROTOBUF_SERIALIZE_ERR);
        response->mutable_status()->set_msg("failed to serialize SnapshotPB");
        return;
    }

    std::string snapshot_base_key = versioned::snapshot_full_key({std::string(instance_id)});
    std::string snapshot_key = encode_versioned_key(snapshot_base_key, vs);
    txn->put(snapshot_key, snapshot_val);

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(cast_as<ErrCategory::COMMIT>(err));
        response->mutable_status()->set_msg(
                fmt::format("failed to commit drop_snapshot txn, err={}", err));
        return;
    }

    LOG_INFO("drop_snapshot OK — marked RECYCLED, recycler will clean up")
            .tag("instance_id", instance_id)
            .tag("snapshot_id", request.snapshot_id());
    response->mutable_status()->set_code(MetaServiceCode::OK);
}

// ── list_snapshot ─────────────────────────────────────────────────────────────

void SnapshotManager::list_snapshot(std::string_view instance_id,
                                    const ListSnapshotRequest& request,
                                    ListSnapshotResponse* response) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
        response->mutable_status()->set_msg("failed to create transaction");
        return;
    }

    std::vector<std::pair<SnapshotPB, Versionstamp>> snapshots;
    auto [code, msg] = get_all_snapshots(txn.get(), instance_id,
                                         request.required_snapshot_id(), &snapshots);
    if (code != MetaServiceCode::OK) {
        response->mutable_status()->set_code(code);
        response->mutable_status()->set_msg(msg);
        return;
    }

    for (auto& [pb, vs] : snapshots) {
        // Filter aborted snapshots unless caller explicitly wants them
        if (pb.status() == SnapshotStatus::SNAPSHOT_ABORTED && !request.include_aborted()) {
            continue;
        }
        // Filter recycled snapshots — they are logically deleted
        if (pb.status() == SnapshotStatus::SNAPSHOT_RECYCLED) {
            continue;
        }

        SnapshotInfoPB* info = response->add_snapshots();
        info->set_snapshot_id(serialize_snapshot_id(vs));
        info->set_create_at(pb.create_at());
        info->set_finish_at(pb.finish_at());
        info->set_image_url(pb.image_url());
        info->set_status(pb.status());
        info->set_instance_id(pb.instance_id());
        info->set_auto_snapshot(pb.auto_());
        info->set_ttl_seconds(pb.ttl_seconds());
        info->set_timeout_seconds(pb.timeout_seconds());
        info->set_snapshot_label(pb.label());
        if (!pb.reason().empty()) {
            info->set_reason(pb.reason());
        }
        if (pb.last_journal_id() > 0) {
            info->set_journal_id(pb.last_journal_id());
        }
        if (pb.snapshot_meta_image_size() > 0) {
            info->set_snapshot_meta_image_size(pb.snapshot_meta_image_size());
        }
        if (pb.snapshot_logical_data_size() > 0) {
            info->set_snapshot_logical_data_size(pb.snapshot_logical_data_size());
        }
        if (pb.fdb_read_version() > 0) {
            info->set_fdb_read_version(pb.fdb_read_version());
        }
        if (!pb.resource_id().empty()) {
            info->set_resource_id(pb.resource_id());
        }
    }

    response->mutable_status()->set_code(MetaServiceCode::OK);
}

// ── clone_instance ────────────────────────────────────────────────────────────
// Creates a new READ-ONLY instance derived from an existing snapshot.
// The new instance shares the same storage vault as the source — no data copy.
// The caller downloads the FE image from the returned image_url and boots a
// separate FE process which then reconstructs FDB rowset metadata in the background.

void SnapshotManager::clone_instance(const CloneInstanceRequest& request,
                                     CloneInstanceResponse* response) {
    if (request.from_snapshot_id().empty()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("from_snapshot_id must not be empty");
        return;
    }
    if (request.new_instance_id().empty()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("new_instance_id must not be empty");
        return;
    }

    Versionstamp snapshot_vs;
    if (!parse_snapshot_versionstamp(request.from_snapshot_id(), &snapshot_vs)) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(
                fmt::format("invalid from_snapshot_id={}", request.from_snapshot_id()));
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
        response->mutable_status()->set_msg("failed to create transaction");
        return;
    }

    // Resolve from_instance_id: use explicit field or fall back to cloud_unique_id parsing
    std::string from_instance_id = request.from_instance_id();
    if (from_instance_id.empty()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("from_instance_id must not be empty");
        return;
    }

    // Read the source snapshot
    MetaReader reader(from_instance_id);
    SnapshotPB snapshot_pb;
    err = reader.get_snapshot(txn.get(), snapshot_vs, &snapshot_pb);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(
                fmt::format("snapshot not found, from_snapshot_id={}",
                            request.from_snapshot_id()));
        return;
    }
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(cast_as<ErrCategory::READ>(err));
        response->mutable_status()->set_msg(fmt::format("failed to get snapshot, err={}", err));
        return;
    }
    if (snapshot_pb.status() != SnapshotStatus::SNAPSHOT_READY) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(
                fmt::format("snapshot is not READY, status={}",
                            SnapshotStatus_Name(snapshot_pb.status())));
        return;
    }

    // Check new_instance_id does not already exist
    std::string new_key = instance_key({request.new_instance_id()});
    std::string existing;
    err = txn->get(new_key, &existing);
    if (err == TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(
                fmt::format("new_instance_id={} already exists", request.new_instance_id()));
        return;
    }
    if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        response->mutable_status()->set_code(cast_as<ErrCategory::READ>(err));
        response->mutable_status()->set_msg("failed to check new instance existence");
        return;
    }

    // Read the source instance to copy vault references
    InstanceInfoPB source_instance;
    auto [src_code, src_msg] = get_instance_info(txn.get(), from_instance_id, &source_instance);
    if (src_code != MetaServiceCode::OK) {
        response->mutable_status()->set_code(src_code);
        response->mutable_status()->set_msg(src_msg);
        return;
    }

    // Build the new instance — READ-ONLY, sharing the source's storage vault
    InstanceInfoPB new_instance;
    new_instance.set_instance_id(request.new_instance_id());
    new_instance.set_user_id(source_instance.user_id());
    new_instance.set_name(fmt::format("{}-restore-{}", source_instance.name(),
                                      request.from_snapshot_id().substr(0, 8)));
    new_instance.set_ctime(unix_seconds_now());
    new_instance.set_mtime(unix_seconds_now());
    new_instance.set_status(InstanceInfoPB::NORMAL);

    // Mark as read-only — FE enforces this after loading image
    new_instance.set_ready_only(true);

    // Record lineage for decouple_instance and audit
    new_instance.set_source_instance_id(from_instance_id);
    new_instance.set_source_snapshot_id(request.from_snapshot_id());
    new_instance.set_original_instance_id(
            source_instance.has_original_instance_id() && !source_instance.original_instance_id().empty()
                    ? source_instance.original_instance_id()
                    : from_instance_id);

    // Share vault references — same resource_ids and vault names as source.
    // Data files live in the shared object store; no copy needed.
    for (const auto& rid : source_instance.resource_ids()) {
        new_instance.add_resource_ids(rid);
    }
    for (const auto& vname : source_instance.storage_vault_names()) {
        new_instance.add_storage_vault_names(vname);
    }
    if (source_instance.has_default_storage_vault_id()) {
        new_instance.set_default_storage_vault_id(source_instance.default_storage_vault_id());
    }
    if (source_instance.has_default_storage_vault_name()) {
        new_instance.set_default_storage_vault_name(source_instance.default_storage_vault_name());
    }
    if (source_instance.has_enable_storage_vault()) {
        new_instance.set_enable_storage_vault(source_instance.enable_storage_vault());
    }
    // Copy legacy obj_info list if present
    for (const auto& oi : source_instance.obj_info()) {
        *new_instance.add_obj_info() = oi;
    }

    // Write snapshot reference key so recycle_snapshots knows this snapshot is still in use
    versioned::SnapshotReferenceKeyInfo ref_key_info {
            from_instance_id, snapshot_vs, request.new_instance_id()};
    std::string ref_key = versioned::snapshot_reference_key(ref_key_info);
    txn->put(ref_key, "");  // value is empty — key presence is the signal

    // Write the new instance record
    std::string new_val;
    if (!new_instance.SerializeToString(&new_val)) {
        response->mutable_status()->set_code(MetaServiceCode::PROTOBUF_SERIALIZE_ERR);
        response->mutable_status()->set_msg("failed to serialize new InstanceInfoPB");
        return;
    }
    txn->atomic_add(system_meta_service_instance_update_key(), 1);
    txn->put(new_key, new_val);

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(cast_as<ErrCategory::COMMIT>(err));
        response->mutable_status()->set_msg(
                fmt::format("failed to commit clone_instance txn, err={}", err));
        return;
    }

    // Return image_url and obj_info so the FE can download the image
    response->set_image_url(snapshot_pb.image_url());

    // Find the vault's ObjectStoreInfoPB to return credentials for the download
    if (!snapshot_pb.resource_id().empty() && !source_instance.obj_info().empty()) {
        for (const auto& oi : source_instance.obj_info()) {
            if (oi.id() == snapshot_pb.resource_id()) {
                *response->mutable_obj_info() = oi;
                break;
            }
        }
    }
    // If not found in legacy obj_info, caller can use default vault credentials
    if (!response->has_obj_info() && !source_instance.obj_info().empty()) {
        *response->mutable_obj_info() = source_instance.obj_info(0);
    }

    LOG_INFO("clone_instance OK")
            .tag("from_instance_id", from_instance_id)
            .tag("from_snapshot_id", request.from_snapshot_id())
            .tag("new_instance_id", request.new_instance_id())
            .tag("image_url", snapshot_pb.image_url());

    response->mutable_status()->set_code(MetaServiceCode::OK);
}

// ── compact_snapshot ──────────────────────────────────────────────────────────

std::pair<MetaServiceCode, std::string> SnapshotManager::compact_snapshot(
        std::string_view instance_id) {
    return {MetaServiceCode::UNDEFINED_ERR, "compact_snapshot: not yet implemented"};
}

// ── recycle_snapshots ─────────────────────────────────────────────────────────
// Called by the recycler once per recycle cycle.
// Marks TTL-expired READY snapshots as RECYCLED, deletes image files for
// RECYCLED snapshots, and removes ABORTED snapshot records.

int SnapshotManager::recycle_snapshots(InstanceRecycler* recycler) {
    std::string instance_id(recycler->instance_id());
    int64_t now = unix_seconds_now();

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("recycle_snapshots: failed to create txn")
                .tag("instance_id", instance_id)
                .tag("err", err);
        return -1;
    }

    std::vector<std::pair<SnapshotPB, Versionstamp>> snapshots;
    auto [code, msg] = get_all_snapshots(txn.get(), instance_id, "", &snapshots);
    if (code != MetaServiceCode::OK) {
        LOG_WARNING("recycle_snapshots: failed to list snapshots")
                .tag("instance_id", instance_id)
                .tag("msg", msg);
        return -1;
    }

    int errors = 0;
    for (auto& [pb, vs] : snapshots) {
        std::string snapshot_id = serialize_snapshot_id(vs);

        if (pb.status() == SnapshotStatus::SNAPSHOT_PREPARING) {
            // Expire timed-out PREPARING snapshots
            int64_t timeout = pb.timeout_seconds() > 0 ? pb.timeout_seconds() : 3600;
            if (now > pb.create_at() + timeout) {
                pb.set_status(SnapshotStatus::SNAPSHOT_ABORTED);
                pb.set_reason("timed out during image upload");
                pb.set_finish_at(now);
                if (!_update_snapshot_status(instance_id, vs, pb)) {
                    ++errors;
                } else {
                    LOG_INFO("recycle_snapshots: expired PREPARING snapshot")
                            .tag("instance_id", instance_id)
                            .tag("snapshot_id", snapshot_id);
                }
            }
            continue;
        }

        if (pb.status() == SnapshotStatus::SNAPSHOT_READY) {
            // TTL-expire READY snapshots only when no clone instance still references them.
            if (pb.ttl_seconds() > 0 && now > pb.create_at() + pb.ttl_seconds()) {
                // Fix 1: check snapshot_reference_key_prefix before marking RECYCLED.
                // clone_instance writes a snapshot_reference_key for each clone derived from
                // this snapshot. If any key exists the snapshot data is still in use.
                std::unique_ptr<Transaction> ref_txn;
                if (txn_kv_->create_txn(&ref_txn) != TxnErrorCode::TXN_OK) {
                    LOG_WARNING("recycle_snapshots: failed to create txn for reference check")
                            .tag("instance_id", instance_id)
                            .tag("snapshot_id", snapshot_id);
                    ++errors;
                    continue;
                }
                std::string ref_prefix_begin = versioned::snapshot_reference_key_prefix(instance_id, vs);
                // Correct exclusive end key: append \x00 rather than incrementing the last byte,
                // which overflows 0xFF→0x00 and inverts the range giving zero results.
                std::string ref_prefix_end = ref_prefix_begin + '\x00';
                std::unique_ptr<RangeGetIterator> ref_iter;
                TxnErrorCode ref_err = ref_txn->get(ref_prefix_begin, ref_prefix_end, &ref_iter,
                                                     /*snapshot=*/false, /*limit=*/1);
                if (ref_err != TxnErrorCode::TXN_OK) {
                    LOG_WARNING("recycle_snapshots: failed to scan reference keys, skipping")
                            .tag("instance_id", instance_id)
                            .tag("snapshot_id", snapshot_id)
                            .tag("err", ref_err);
                    ++errors;
                    continue;
                }
                if (ref_iter && ref_iter->has_next()) {
                    LOG_WARNING("recycle_snapshots: READY snapshot has active clone references, "
                                "skipping TTL expiry until clones are decoupled")
                            .tag("instance_id", instance_id)
                            .tag("snapshot_id", snapshot_id)
                            .tag("label", pb.label());
                    continue;
                }
                pb.set_status(SnapshotStatus::SNAPSHOT_RECYCLED);
                pb.set_finish_at(now);
                if (!_update_snapshot_status(instance_id, vs, pb)) {
                    ++errors;
                } else {
                    LOG_INFO("recycle_snapshots: TTL-expired READY snapshot → RECYCLED")
                            .tag("instance_id", instance_id)
                            .tag("snapshot_id", snapshot_id)
                            .tag("label", pb.label());
                }
            }
            continue;
        }

        if (pb.status() == SnapshotStatus::SNAPSHOT_RECYCLED) {
            // Delete object store image file, then remove FDB key
            int ret = recycler->recycle_snapshot_meta_and_data(instance_id, pb.resource_id(),
                                                                vs, pb);
            if (ret != 0) {
                LOG_WARNING("recycle_snapshots: failed to recycle snapshot data — "
                            "deleting FDB key anyway to avoid permanent recycle loop")
                        .tag("instance_id", instance_id)
                        .tag("snapshot_id", snapshot_id);
                // Still delete the FDB key: if the accessor is missing (vault removed) or
                // the image file is already gone, keeping the RECYCLED key causes the recycler
                // to retry and fail forever. The image is either gone or unreachable — clean up.
                _delete_snapshot_key(instance_id, vs);
                ++errors;
            } else {
                // Remove the FDB snapshot key now that data is cleaned up
                if (!_delete_snapshot_key(instance_id, vs)) {
                    ++errors;
                } else {
                    LOG_INFO("recycle_snapshots: recycled RECYCLED snapshot")
                            .tag("instance_id", instance_id)
                            .tag("snapshot_id", snapshot_id);
                }
            }
            continue;
        }

        if (pb.status() == SnapshotStatus::SNAPSHOT_ABORTED) {
            // Remove aborted snapshot records. Log a warning if upload_file/upload_id are set
            // because the associated in-progress multipart upload may not have been aborted
            // and could leak parts in object storage. Abort is best-effort here; key deletion
            // must proceed regardless so the record does not accumulate indefinitely.
            if (!pb.upload_file().empty() && !pb.upload_id().empty()) {
                LOG_WARNING("recycle_snapshots: deleting ABORTED snapshot with pending multipart "
                            "upload — parts may be leaked in object storage; "
                            "abort via object store console if needed")
                        .tag("instance_id", instance_id)
                        .tag("snapshot_id", snapshot_id)
                        .tag("upload_file", pb.upload_file())
                        .tag("upload_id", pb.upload_id());
            }
            if (!_delete_snapshot_key(instance_id, vs)) {
                ++errors;
            }
            continue;
        }
    }

    return errors == 0 ? 0 : -1;
}

// Delete a snapshot FDB key by versionstamp.
bool SnapshotManager::_delete_snapshot_key(std::string_view instance_id, Versionstamp vs) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return false;
    }
    std::string base_key = versioned::snapshot_full_key({std::string(instance_id)});
    std::string key = encode_versioned_key(base_key, vs);
    txn->remove(key);
    err = txn->commit();
    return err == TxnErrorCode::TXN_OK;
}

// Write an updated SnapshotPB back to FDB.
bool SnapshotManager::_update_snapshot_status(std::string_view instance_id,
                                               Versionstamp vs,
                                               const SnapshotPB& pb) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return false;
    }
    std::string val;
    if (!pb.SerializeToString(&val)) {
        return false;
    }
    std::string base_key = versioned::snapshot_full_key({std::string(instance_id)});
    std::string key = encode_versioned_key(base_key, vs);
    txn->put(key, val);
    err = txn->commit();
    return err == TxnErrorCode::TXN_OK;
}

// ── recycle_snapshot_meta_and_data ────────────────────────────────────────────
// Deletes the FE image file from object storage for a specific snapshot.
// The FDB key removal is handled by recycle_snapshots after this returns.

int SnapshotManager::recycle_snapshot_meta_and_data(std::string_view instance_id,
                                                    std::string_view resource_id,
                                                    StorageVaultAccessor* accessor,
                                                    Versionstamp snapshot_version,
                                                    const SnapshotPB& snapshot_pb) {
    if (snapshot_pb.image_url().empty()) {
        // Nothing to delete — snapshot was aborted before upload
        return 0;
    }

    // The image_url is: <vault_prefix>/snapshots/<snapshot_id>/image
    // We need to delete everything under: <vault_prefix>/snapshots/<snapshot_id>/
    std::string snapshot_id = serialize_snapshot_id(snapshot_version);
    LOG_INFO("recycle_snapshot_meta_and_data: deleting image file")
            .tag("instance_id", instance_id)
            .tag("snapshot_id", snapshot_id)
            .tag("image_url", snapshot_pb.image_url());

    // Delete the image file via the accessor (delete everything under snapshots/<snapshot_id>/)
    auto ret = accessor->delete_prefix(fmt::format("snapshots/{}/", snapshot_id));
    if (ret != 0) {
        LOG_WARNING("recycle_snapshot_meta_and_data: failed to delete image files")
                .tag("instance_id", instance_id)
                .tag("snapshot_id", snapshot_id)
                .tag("err", ret);
        return ret;
    }

    return 0;
}

// ── remaining stubs (Phase 4+) ────────────────────────────────────────────────

std::pair<MetaServiceCode, std::string> SnapshotManager::decouple_instance(
        std::string_view id) {
    std::string instance_id(id);
    LOG_INFO("decouple_instance").tag("instance_id", instance_id);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return {MetaServiceCode::KV_TXN_CREATE_ERR, "failed to create txn"};
    }

    std::string key = instance_key({instance_id});
    std::string value;
    err = txn->get(key, &value);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        return {MetaServiceCode::CLUSTER_NOT_FOUND,
                fmt::format("instance not found, instance_id={}", instance_id)};
    } else if (err != TxnErrorCode::TXN_OK) {
        return {cast_as<ErrCategory::READ>(err),
                fmt::format("failed to get instance info, instance_id={}, err={}", instance_id,
                            err)};
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(value)) {
        return {MetaServiceCode::PROTOBUF_PARSE_ERR, "failed to parse instance info"};
    }

    if (!instance.has_source_instance_id() || instance.source_instance_id().empty() ||
        !instance.has_source_snapshot_id() || instance.source_snapshot_id().empty()) {
        return {MetaServiceCode::INVALID_ARGUMENT,
                fmt::format("instance {} was not a cloned instance (created via clone_instance)",
                            instance_id)};
    }

    if (instance.snapshot_compact_status() != SnapshotCompactStatus::SNAPSHOT_COMPACT_DONE) {
        return {MetaServiceCode::INVALID_ARGUMENT,
                fmt::format("instance {} snapshot_compact_status is not SNAPSHOT_COMPACT_DONE, "
                            "current status={}",
                            instance_id,
                            SnapshotCompactStatus_Name(instance.snapshot_compact_status()))};
    }

    const std::string& source_instance_id = instance.source_instance_id();
    const std::string& source_snapshot_id = instance.source_snapshot_id();

    Versionstamp snapshot_versionstamp;
    if (!parse_snapshot_versionstamp(source_snapshot_id, &snapshot_versionstamp)) {
        return {MetaServiceCode::UNDEFINED_ERR,
                fmt::format("failed to parse source_snapshot_id={} to versionstamp",
                            source_snapshot_id)};
    }

    versioned::SnapshotReferenceKeyInfo ref_key_info {source_instance_id, snapshot_versionstamp,
                                                      instance_id};
    std::string reference_key = versioned::snapshot_reference_key(ref_key_info);
    txn->remove(reference_key);

    instance.clear_source_snapshot_id();
    instance.clear_source_instance_id();

    std::string updated_val;
    if (!instance.SerializeToString(&updated_val)) {
        return {MetaServiceCode::PROTOBUF_SERIALIZE_ERR,
                fmt::format("failed to serialize updated instance, instance_id={}", instance_id)};
    }

    txn->atomic_add(system_meta_service_instance_update_key(), 1);
    txn->put(key, updated_val);

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        return {MetaServiceCode::KV_TXN_COMMIT_ERR,
                fmt::format("failed to commit txn, instance_id={}, err={}", instance_id, err)};
    }

    LOG_INFO("decouple_instance completed successfully")
            .tag("instance_id", instance_id)
            .tag("source_instance_id", source_instance_id)
            .tag("source_snapshot_id", source_snapshot_id);

    return {MetaServiceCode::OK, ""};
}

std::pair<MetaServiceCode, std::string> SnapshotManager::set_multi_version_status(
        std::string_view instance_id, MultiVersionStatus multi_version_status) {
    return {MetaServiceCode::UNDEFINED_ERR, "set_multi_version_status: not yet implemented"};
}

int SnapshotManager::check_snapshots(InstanceChecker* checker) {
    return 0;
}

int SnapshotManager::inverted_check_snapshots(InstanceChecker* checker) {
    return 0;
}

int SnapshotManager::check_mvcc_meta_key(InstanceChecker* checker) {
    return 0;
}

int SnapshotManager::inverted_check_mvcc_meta_key(InstanceChecker* checker) {
    return 0;
}

int SnapshotManager::check_meta(MetaChecker* meta_checker) {
    return 0;
}

int SnapshotManager::migrate_to_versioned_keys(InstanceDataMigrator* migrator) {
    LOG(WARNING) << "Migrate to versioned keys is not implemented";
    return -1;
}

int SnapshotManager::compact_snapshot_chains(InstanceChainCompactor* compactor) {
    LOG(WARNING) << "Compact snapshot chains is not implemented";
    return -1;
}

// ── get_all_snapshots (static) ────────────────────────────────────────────────

std::pair<MetaServiceCode, std::string> SnapshotManager::get_all_snapshots(
        Transaction* txn, std::string_view instance_id, std::string_view required_snapshot_id,
        std::vector<std::pair<SnapshotPB, Versionstamp>>* snapshots) {
    InstanceInfoPB instance_info;
    auto [code, error_msg] = get_instance_info(txn, instance_id, &instance_info);
    if (code != MetaServiceCode::OK) {
        return {code, error_msg};
    }

    // For a cloned instance (original_instance_id set), snapshots live on the root instance.
    // Read from the root directly and return — no successor chain walk needed since clone_instance
    // does not write successor_instance_id (that field is only used by the OSS rollback chain).
    std::string read_from_id(instance_id);
    if (instance_info.has_original_instance_id() && !instance_info.original_instance_id().empty()) {
        read_from_id = instance_info.original_instance_id();
    }

    MetaReader meta_reader(read_from_id);
    if (required_snapshot_id.empty()) {
        TxnErrorCode err = meta_reader.get_snapshots(txn, snapshots);
        if (err != TxnErrorCode::TXN_OK) {
            return {cast_as<ErrCategory::READ>(err), "failed to get snapshots"};
        }
    } else {
        Versionstamp required_vs;
        if (!parse_snapshot_versionstamp(required_snapshot_id, &required_vs)) {
            return {MetaServiceCode::INVALID_ARGUMENT, "invalid snapshot_id format"};
        }
        SnapshotPB snapshot_pb;
        TxnErrorCode err = meta_reader.get_snapshot(txn, required_vs, &snapshot_pb);
        if (err == TxnErrorCode::TXN_OK) {
            snapshots->emplace_back(snapshot_pb, required_vs);
        } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            return {cast_as<ErrCategory::READ>(err), "failed to get snapshot"};
        }
        // Fix 7: when a specific snapshot_id was requested and was not found, return an explicit
        // error instead of OK with an empty vector — silent success misleads callers into thinking
        // the snapshot was found (it may live on a different instance in a rollback chain).
        if (snapshots->empty()) {
            return {MetaServiceCode::INVALID_ARGUMENT,
                    fmt::format("snapshot not found, snapshot_id={}, read_from_instance={}",
                                required_snapshot_id, read_from_id)};
        }
    }
    return {MetaServiceCode::OK, ""};
}

} // namespace doris::cloud
