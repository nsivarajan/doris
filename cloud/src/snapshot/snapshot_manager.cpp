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

#include <arpa/inet.h>
#include <fmt/format.h>

#include <climits>
#include <ctime>
#include <map>
#include <unordered_map>
#include <unordered_set>

#include <rapidjson/document.h>

#include "common/logging.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/meta_service_schema.h"
#include "meta-store/blob_message.h"
#include "meta-store/codec.h"
#include "meta-store/document_message.h"
#include "meta-store/keys.h"
#include "meta-store/meta_reader.h"
#include "meta-store/versioned_value.h"
#include "meta-store/versionstamp.h"
#include "recycler/checker.h"
#include "recycler/recycler.h"
#include "recycler/snapshot_chain_compactor.h"
#include "recycler/snapshot_data_migrator.h"

namespace doris::cloud {

// WriteBatch: flush() commits pending writes; bump() commits and reinitialises at batch_size.
// Shared by migrate_to_versioned_keys, compact_snapshot_chains, seed_rowset_ref_counts,
// unseed_rowset_ref_counts, export_table_meta, and import_table_meta.
namespace {

// Builds TabletSchemaCloudPB from the fe_table_schema_json blob embedded in TableFdbMetaPB.
// The JSON is the Java OlapTable serialisation; field names (itm/sc/sv/sh/skcc/kt/mcui)
// and type strings (e.g. "BIGINT", "VARCHAR") are stable across Doris versions.
bool build_schema_from_fe_json(const std::string& json, int64_t index_id,
                                       doris::TabletSchemaCloudPB* out) {
    // Java OlapTable.write() uses Text.writeString() which prepends a 4-byte big-endian
    // length before the UTF-8 JSON; skip those 4 bytes before parsing.
    if (json.size() <= 4 || !out) return false;
    rapidjson::Document doc;
    doc.Parse(json.data() + 4, json.size() - 4);
    if (doc.HasParseError() || !doc.IsObject() || !doc.HasMember("itm")) return false;
    const auto& itm = doc["itm"];
    if (!itm.IsObject()) return false;
    std::string idx_key = std::to_string(index_id);
    if (!itm.HasMember(idx_key.c_str())) return false;
    const auto& idx = itm[idx_key.c_str()];
    if (!idx.IsObject()) return false;

    if (idx.HasMember("sv")) out->set_schema_version(idx["sv"].GetInt());
    if (idx.HasMember("skcc")) out->set_num_short_key_columns(idx["skcc"].GetInt());
    if (idx.HasMember("mcui")) out->set_next_column_unique_id(idx["mcui"].GetInt() + 1);
    if (idx.HasMember("kt")) {
        std::string kt = idx["kt"].GetString();
        if (kt == "UNIQUE_KEYS") out->set_keys_type(doris::KeysType::UNIQUE_KEYS);
        else if (kt == "AGG_KEYS") out->set_keys_type(doris::KeysType::AGG_KEYS);
        else out->set_keys_type(doris::KeysType::DUP_KEYS);
    }
    if (!idx.HasMember("sc") || !idx["sc"].IsArray()) return false;
    for (const auto& c : idx["sc"].GetArray()) {
        if (!c.IsObject()) continue;
        auto* col = out->add_column();

        if (c.HasMember("uniqueId")) col->set_unique_id(c["uniqueId"].GetInt());
        if (c.HasMember("name")) col->set_name(c["name"].GetString());
        if (c.HasMember("isKey")) col->set_is_key(c["isKey"].GetBool());
        if (c.HasMember("isAllowNull")) col->set_is_nullable(c["isAllowNull"].GetBool());
        if (c.HasMember("aggregationType")) {
            col->set_aggregation(c["aggregationType"].GetString());
        } else {
            col->set_aggregation("NONE");
        }
        col->set_visible(true);

        int32_t str_len = 0;
        std::string type_str;
        int32_t precision = 0, scale = 0;
        if (c.HasMember("type") && c["type"].IsObject()) {
            const auto& t = c["type"];
            if (t.HasMember("type")) {
                type_str = t["type"].GetString();
                col->set_type(type_str);
            }
            if (t.HasMember("len") && t["len"].IsInt() && t["len"].GetInt() > 0)
                str_len = t["len"].GetInt();
            if (t.HasMember("precision") && t["precision"].IsInt())
                precision = t["precision"].GetInt();
            if (t.HasMember("scale") && t["scale"].IsInt())
                scale = t["scale"].GetInt();
        }
        col->set_precision(precision);
        col->set_frac(scale);

        // Compute storage length matching Java ColumnToProtobuf.getFieldLengthByType().
        int32_t length = 0;
        if (type_str == "TINYINT" || type_str == "BOOLEAN") length = 1;
        else if (type_str == "SMALLINT") length = 2;
        else if (type_str == "INT" || type_str == "FLOAT"
                || type_str == "DATEV2" || type_str == "DECIMAL32") length = 4;
        else if (type_str == "BIGINT" || type_str == "DATETIME"
                || type_str == "DATETIMEV2" || type_str == "TIMESTAMPTZ"
                || type_str == "DOUBLE" || type_str == "DECIMAL64") length = 8;
        else if (type_str == "DATE") length = 3;
        else if (type_str == "LARGEINT" || type_str == "DECIMAL128I") length = 16;
        else if (type_str == "DECIMALV2") length = 12;
        else if (type_str == "CHAR") length = str_len;
        else if (type_str == "VARCHAR" || type_str == "HLL") length = str_len + 2;
        else if (type_str == "STRING") length = str_len + 4;
        else length = 8;
        col->set_length(length);
        col->set_index_length(length);
    }
    return out->column_size() > 0;
}

struct WriteBatch {
    TxnKv* txn_kv;
    std::unique_ptr<Transaction> txn;
    int count = 0;

    explicit WriteBatch(TxnKv* kv) : txn_kv(kv) {}

    int init() {
        count = 0;
        return txn_kv->create_txn(&txn) == TxnErrorCode::TXN_OK ? 0 : -1;
    }

    // Commit pending writes without creating a new transaction.
    int flush() {
        if (count == 0 || !txn) return 0;
        TxnErrorCode err = txn->commit();
        txn.reset();
        count = 0;
        return (err == TxnErrorCode::TXN_OK || err == TxnErrorCode::TXN_MAYBE_COMMITTED) ? 0 : -1;
    }

    // Call after each write. Commits and reinitialises when batch_size is reached.
    int bump(int batch_size) {
        if (++count < batch_size) return 0;
        if (flush() != 0) return -1;
        return init();
    }
};

} // anonymous namespace

// Forward declaration — defined below after the snapshot RPCs.
static std::pair<MetaServiceCode, std::string> get_instance(Transaction* txn,
                                                            std::string_view instance_id,
                                                            InstanceInfoPB* instance_info);

static int64_t now_seconds() {
    return static_cast<int64_t>(std::time(nullptr));
}

// Validate an IP address string (IPv4 or IPv6). Empty string is allowed (optional field).
static bool is_valid_ip(const std::string& ip) {
    if (ip.empty()) return true;
    struct in_addr addr4;
    struct in6_addr addr6;
    return inet_pton(AF_INET, ip.c_str(), &addr4) == 1 ||
           inet_pton(AF_INET6, ip.c_str(), &addr6) == 1;
}

// Look up StorageVaultPB by vault_name from the instance. If vault_name is empty,
// fall back to the instance's default vault. Returns 0 on success, -1 on failure.
static int resolve_storage_vault(const InstanceInfoPB& instance, std::string_view vault_name,
                                 Transaction* txn, StorageVaultPB* vault_out,
                                 MetaServiceCode& code, std::string& msg) {
    std::string vault_id;

    if (!vault_name.empty()) {
        // Find vault_id by name: names and IDs are parallel repeated fields in InstanceInfoPB
        bool found = false;
        for (int i = 0; i < instance.storage_vault_names_size(); ++i) {
            if (instance.storage_vault_names(i) == vault_name) {
                vault_id = instance.resource_ids(i);
                found = true;
                break;
            }
        }
        if (!found) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = fmt::format("storage vault '{}' not found in instance", vault_name);
            return -1;
        }
    } else if (instance.has_default_storage_vault_id() &&
               !instance.default_storage_vault_id().empty()) {
        vault_id = instance.default_storage_vault_id();
    } else {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "no vault_name specified and instance has no default storage vault";
        return -1;
    }

    std::string key = storage_vault_key({instance.instance_id(), vault_id});
    std::string val;
    TxnErrorCode err = txn->get(key, &val);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("storage vault id='{}' not found in FDB", vault_id);
        return -1;
    }
    if (err != TxnErrorCode::TXN_OK) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        msg = fmt::format("failed to get storage vault id='{}', err={}", vault_id, err);
        return -1;
    }
    if (!vault_out->ParseFromString(val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = fmt::format("failed to parse StorageVaultPB for vault id='{}'", vault_id);
        return -1;
    }
    return 0;
}

bool SnapshotManager::parse_snapshot_versionstamp(std::string_view snapshot_id,
                                                  Versionstamp* versionstamp) {
    if (snapshot_id.size() != 20) {
        return false;
    }

    std::array<uint8_t, 10> versionstamp_data;
    for (size_t i = 0; i < 10; ++i) {
        const char* hex_chars = snapshot_id.data() + (i * 2);

        // Convert two hex digits to one byte more efficiently
        uint8_t high_nibble = 0, low_nibble = 0;

        // Parse high nibble
        if (hex_chars[0] >= '0' && hex_chars[0] <= '9') {
            high_nibble = hex_chars[0] - '0';
        } else if (hex_chars[0] >= 'a' && hex_chars[0] <= 'f') {
            high_nibble = hex_chars[0] - 'a' + 10;
        } else if (hex_chars[0] >= 'A' && hex_chars[0] <= 'F') {
            high_nibble = hex_chars[0] - 'A' + 10;
        } else {
            return false;
        }

        // Parse low nibble
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

void SnapshotManager::begin_snapshot(std::string_view instance_id,
                                     const BeginSnapshotRequest& request,
                                     BeginSnapshotResponse* response) {
    if (request.timeout_seconds() <= 0) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("timeout_seconds must be positive");
        return;
    }
    if (request.ttl_seconds() <= 0) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("ttl_seconds must be positive");
        return;
    }
    if (config::snapshot_max_ttl_seconds > 0
            && request.ttl_seconds() > config::snapshot_max_ttl_seconds) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(fmt::format("ttl_seconds={} exceeds max={}",
                request.ttl_seconds(), config::snapshot_max_ttl_seconds));
        return;
    }
    if (!request.has_snapshot_label() || request.snapshot_label().empty()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("snapshot_label must not be empty");
        return;
    }
    if (request.has_request_ip() && !is_valid_ip(request.request_ip())) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(
                fmt::format("invalid request_ip='{}'", request.request_ip()));
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(cast_as<ErrCategory::CREATE>(err));
        response->mutable_status()->set_msg("failed to create transaction");
        return;
    }

    InstanceInfoPB instance;
    auto [icode, imsg] = get_instance(txn.get(), instance_id, &instance);
    if (icode != MetaServiceCode::OK) {
        response->mutable_status()->set_code(icode);
        response->mutable_status()->set_msg(imsg);
        return;
    }

    // Reject if a live (non-timed-out) PREPARE snapshot already exists.
    MetaReader meta_reader(instance_id);
    std::vector<std::pair<SnapshotPB, Versionstamp>> existing;
    err = meta_reader.get_snapshots(txn.get(), &existing);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_GET_ERR);
        response->mutable_status()->set_msg("failed to check existing snapshots");
        return;
    }
    for (auto& [snap, vs] : existing) {
        if (snap.status() == SnapshotStatus::SNAPSHOT_PREPARE) {
            int64_t age = now_seconds() - snap.create_at();
            if (age < snap.timeout_seconds()) {
                response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
                response->mutable_status()->set_msg(fmt::format(
                        "a snapshot is already in PREPARE state, label='{}', "
                        "started {} seconds ago",
                        snap.label(), age));
                return;
            }
            // Timed-out PREPARE snapshots are treated as stale — allow a new one
        }
    }

    StorageVaultPB vault;
    MetaServiceCode vcode = MetaServiceCode::OK;
    std::string vmsg;
    if (resolve_storage_vault(instance, request.vault_name(), txn.get(), &vault, vcode, vmsg) !=
        0) {
        response->mutable_status()->set_code(vcode);
        response->mutable_status()->set_msg(vmsg);
        return;
    }

    // The versionstamp is NOT set here — FDB assigns it atomically at commit time.
    // After commit, we read it back via txn->get_versionstamp() and use it as snapshot_id.
    SnapshotPB snapshot_pb;
    snapshot_pb.set_status(SnapshotStatus::SNAPSHOT_PREPARE);
    snapshot_pb.set_type(SnapshotType::SNAPSHOT_REFERENCE);
    snapshot_pb.set_instance_id(std::string(instance_id));
    snapshot_pb.set_create_at(now_seconds());
    snapshot_pb.set_timeout_seconds(request.timeout_seconds());
    snapshot_pb.set_ttl_seconds(request.ttl_seconds());
    snapshot_pb.set_label(request.snapshot_label());
    // Always record cluster version for DataAsOf reporting; default DISABLED for old instances.
    snapshot_pb.set_snapshot_cluster_version(
            instance.has_multi_version_status() ? instance.multi_version_status()
                                                : MultiVersionStatus::MULTI_VERSION_DISABLED);
    snapshot_pb.set_auto_(request.auto_snapshot());
    snapshot_pb.set_resource_id(vault.id());
    // Store the granularity filter so the recycler can seed only the right rowsets.
    for (int64_t id : request.included_db_ids())        snapshot_pb.add_protected_db_ids(id);
    for (int64_t id : request.included_table_ids())     snapshot_pb.add_protected_table_ids(id);
    for (int64_t id : request.included_partition_ids()) snapshot_pb.add_protected_partition_ids(id);

    // Atomic write: versionstamp assigned by FDB at commit time.
    std::string key_prefix = versioned::snapshot_full_key({std::string(instance_id)});
    txn->enable_get_versionstamp();
    if (!versioned::document_put(txn.get(), key_prefix, std::move(snapshot_pb))) {
        response->mutable_status()->set_code(MetaServiceCode::PROTOBUF_SERIALIZE_ERR);
        response->mutable_status()->set_msg("failed to serialize SnapshotPB for FDB write");
        return;
    }

    err = txn->commit();
    if (err == TxnErrorCode::TXN_MAYBE_COMMITTED) {
        // Return a dedicated code so the FE retries begin_snapshot instead of calling
        // abort_snapshot — which would delete a snapshot that may have committed.
        LOG_WARNING("begin_snapshot TXN_MAYBE_COMMITTED for instance={}, label={}.",
                    instance_id, request.snapshot_label());
        response->mutable_status()->set_code(MetaServiceCode::SNAPSHOT_PREPARE_MAYBE_COMMITTED);
        response->mutable_status()->set_msg("retry begin_snapshot to discover existing PREPARE");
        return;
    }
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(cast_as<ErrCategory::COMMIT>(err));
        response->mutable_status()->set_msg(
                fmt::format("failed to commit begin_snapshot txn, err={}", err));
        return;
    }

    // Read the commit-assigned versionstamp — this becomes the snapshot_id.
    Versionstamp vs;
    err = txn->get_versionstamp(&vs);
    if (err != TxnErrorCode::TXN_OK) {
        // The write committed but we can't retrieve the versionstamp.
        // This is unusual — log and return error so the FE retries.
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_GET_ERR);
        response->mutable_status()->set_msg(
                fmt::format("snapshot committed but failed to read versionstamp, err={}", err));
        return;
    }

    std::string snapshot_id = serialize_snapshot_id(vs);

    // image_url = {vault_prefix}/snapshot/{snapshot_id}/fe_image
    std::string vault_prefix;
    if (vault.has_obj_info() && !vault.obj_info().prefix().empty()) {
        vault_prefix = vault.obj_info().prefix();
    } else if (vault.has_hdfs_info() && !vault.hdfs_info().prefix().empty()) {
        vault_prefix = vault.hdfs_info().prefix();
    } else {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(fmt::format(
                "vault '{}' has no valid storage backend configured "
                "(obj_info.prefix and hdfs_info.prefix are both empty)",
                vault.has_name() ? vault.name() : vault.id()));
        return;
    }
    std::string image_url = vault_prefix + "/snapshot/" + snapshot_id + "/fe_image";

    LOG_INFO("begin_snapshot succeeded")
            .tag("instance_id", instance_id)
            .tag("snapshot_id", snapshot_id)
            .tag("label", request.snapshot_label())
            .tag("image_url", image_url)
            .tag("vault_type", vault.has_hdfs_info() ? "hdfs" : "object_store")
            .tag("ttl_seconds", request.ttl_seconds());

    response->set_snapshot_id(snapshot_id);
    response->set_image_url(image_url);
    // obj_info is only returned for object-store vaults.
    // HDFS vaults: FE uses its existing HDFS vault config, no S3 credentials needed.
    if (vault.has_obj_info()) {
        response->mutable_obj_info()->CopyFrom(vault.obj_info());
    }
    response->mutable_status()->set_code(MetaServiceCode::OK);
}

// Helper: parse snapshot_id → versionstamp + fetch SnapshotPB; sets code/msg on error.
static bool fetch_snapshot_for_update(TxnKv* txn_kv, std::string_view instance_id,
                                      const std::string& snapshot_id,
                                      std::unique_ptr<Transaction>* txn_out,
                                      Versionstamp* vs_out, SnapshotPB* snapshot_out,
                                      MetaServiceCode& code, std::string& msg) {
    if (snapshot_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "snapshot_id must not be empty";
        return false;
    }
    if (!SnapshotManager::parse_snapshot_versionstamp(snapshot_id, vs_out)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("invalid snapshot_id='{}'", snapshot_id);
        return false;
    }

    TxnErrorCode err = txn_kv->create_txn(txn_out);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create transaction";
        return false;
    }

    MetaReader reader(instance_id);
    err = reader.get_snapshot(txn_out->get(), *vs_out, snapshot_out);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("snapshot not found, snapshot_id='{}'", snapshot_id);
        return false;
    }
    if (err != TxnErrorCode::TXN_OK) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        msg = fmt::format("failed to fetch snapshot snapshot_id='{}', err={}", snapshot_id, err);
        return false;
    }
    return true;
}

// Write a modified SnapshotPB back to its original versionstamp key and commit.
static bool commit_snapshot_update(Transaction* txn, std::string_view instance_id,
                                   const Versionstamp& vs, SnapshotPB snapshot_pb,
                                   MetaServiceCode& code, std::string& msg) {
    std::string key_prefix = versioned::snapshot_full_key({std::string(instance_id)});
    if (!versioned::document_put(txn, key_prefix, vs, std::move(snapshot_pb))) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "failed to serialize updated SnapshotPB";
        return false;
    }
    TxnErrorCode err = txn->commit();
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_MAYBE_COMMITTED) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit snapshot update, err={}", err);
        return false;
    }
    return true;
}

void SnapshotManager::update_snapshot(std::string_view instance_id,
                                      const UpdateSnapshotRequest& request,
                                      UpdateSnapshotResponse* response) {
    std::unique_ptr<Transaction> txn;
    Versionstamp vs;
    SnapshotPB snapshot;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg;

    if (!fetch_snapshot_for_update(txn_kv_.get(), instance_id, request.snapshot_id(), &txn, &vs,
                                   &snapshot, code, msg)) {
        response->mutable_status()->set_code(code);
        response->mutable_status()->set_msg(msg);
        return;
    }

    // TTL extension: allowed on NORMAL (READY) snapshots only.
    if (request.has_ttl_seconds()) {
        if (snapshot.status() != SnapshotStatus::SNAPSHOT_NORMAL) {
            response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
            response->mutable_status()->set_msg(fmt::format(
                    "TTL can only be updated on NORMAL snapshots, current status={}, snapshot_id='{}'",
                    SnapshotStatus_Name(snapshot.status()), request.snapshot_id()));
            return;
        }
        if (request.ttl_seconds() <= 0) {
            response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
            response->mutable_status()->set_msg("ttl_seconds must be positive");
            return;
        }
        if (config::snapshot_max_ttl_seconds > 0
                && request.ttl_seconds() > config::snapshot_max_ttl_seconds) {
            response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
            response->mutable_status()->set_msg(fmt::format(
                    "ttl_seconds={} exceeds max={}", request.ttl_seconds(),
                    config::snapshot_max_ttl_seconds));
            return;
        }
        snapshot.set_ttl_seconds(request.ttl_seconds());
    } else {
        // Non-TTL update path: only valid during PREPARE state (multipart upload tracking).
        if (snapshot.status() != SnapshotStatus::SNAPSHOT_PREPARE) {
            response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
            response->mutable_status()->set_msg(fmt::format(
                    "snapshot is not in PREPARE state, current status={}, snapshot_id='{}'",
                    SnapshotStatus_Name(snapshot.status()), request.snapshot_id()));
            return;
        }
        // Record the multipart upload file and upload_id so abort_snapshot() can
        // issue AbortMultipartUpload if the upload is abandoned.
        if (request.has_upload_file()) snapshot.set_upload_file(request.upload_file());
        if (request.has_upload_id())   snapshot.set_upload_id(request.upload_id());
    }

    if (!commit_snapshot_update(txn.get(), instance_id, vs, std::move(snapshot), code, msg)) {
        response->mutable_status()->set_code(code);
        response->mutable_status()->set_msg(msg);
        return;
    }

    LOG_INFO("update_snapshot succeeded")
            .tag("instance_id", instance_id)
            .tag("snapshot_id", request.snapshot_id())
            .tag("ttl_seconds", request.has_ttl_seconds() ? request.ttl_seconds() : -1L);

    response->mutable_status()->set_code(MetaServiceCode::OK);
}

void SnapshotManager::commit_snapshot(std::string_view instance_id,
                                      const CommitSnapshotRequest& request,
                                      CommitSnapshotResponse* response) {
    std::unique_ptr<Transaction> txn;
    Versionstamp vs;
    SnapshotPB snapshot;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg;

    if (!fetch_snapshot_for_update(txn_kv_.get(), instance_id, request.snapshot_id(), &txn, &vs,
                                   &snapshot, code, msg)) {
        response->mutable_status()->set_code(code);
        response->mutable_status()->set_msg(msg);
        return;
    }

    // Idempotent: if already committed, return OK (handles KV_TXN_MAYBE_COMMITTED retries).
    if (snapshot.status() == SnapshotStatus::SNAPSHOT_NORMAL) {
        response->mutable_status()->set_code(MetaServiceCode::OK);
        return;
    }

    if (snapshot.status() != SnapshotStatus::SNAPSHOT_PREPARE) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(fmt::format(
                "snapshot is not in PREPARE state, current status={}, snapshot_id='{}'",
                SnapshotStatus_Name(snapshot.status()), request.snapshot_id()));
        return;
    }

    // Reject timed-out snapshots: the FE held the quiesce lock for nothing if this fires.
    if (now_seconds() > snapshot.create_at() + snapshot.timeout_seconds()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(fmt::format(
                "snapshot has timed out, snapshot_id='{}', created_at={}, timeout={}s",
                request.snapshot_id(), snapshot.create_at(), snapshot.timeout_seconds()));
        return;
    }

    // Seed before NORMAL to close the recycler race: rowsets could be recycled between
    // commit and an async seed. seed_rowset_ref_counts commits its own WriteBatches,
    // so open a fresh txn after to stay within FDB's MVCC window.
    {
        std::vector<int64_t> db_ids(snapshot.protected_db_ids().begin(),
                                    snapshot.protected_db_ids().end());
        std::vector<int64_t> table_ids(snapshot.protected_table_ids().begin(),
                                       snapshot.protected_table_ids().end());
        std::vector<int64_t> partition_ids(snapshot.protected_partition_ids().begin(),
                                           snapshot.protected_partition_ids().end());
        if (seed_rowset_ref_counts(instance_id, vs, db_ids, table_ids, partition_ids) != 0) {
            response->mutable_status()->set_code(MetaServiceCode::KV_TXN_COMMIT_ERR);
            response->mutable_status()->set_msg("commit_snapshot: failed to seed rowset ref counts");
            return;
        }
    }

    // Seeding is complete — open a fresh transaction for the snapshot PB update.
    // The original txn may have exceeded FDB's MVCC window during seeding.
    txn.reset();
    if (txn_kv_->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
        response->mutable_status()->set_msg("commit_snapshot: failed to create txn after seeding");
        return;
    }

    snapshot.set_status(SnapshotStatus::SNAPSHOT_NORMAL);
    snapshot.set_rowset_refs_seeded(true);
    snapshot.set_finish_at(now_seconds());
    if (request.has_last_journal_id())         snapshot.set_last_journal_id(request.last_journal_id());
    if (request.has_image_url())               snapshot.set_image_url(request.image_url());
    if (request.has_snapshot_meta_image_size()) snapshot.set_snapshot_meta_image_size(request.snapshot_meta_image_size());
    if (request.has_snapshot_logical_data_size()) snapshot.set_snapshot_logical_data_size(request.snapshot_logical_data_size());
    // Store FE-captured table schemas so export_table_meta can embed them in blobs.
    for (const auto& [tid, json] : request.table_schema_jsons()) {
        (*snapshot.mutable_table_schema_jsons())[tid] = json;
    }
    // Store table name list for dropped-DB restore (no catalog needed on restore side).
    for (const auto& ct : request.captured_tables()) {
        *snapshot.add_captured_tables() = ct;
    }
    // Clear multipart-upload tracking fields: upload is complete
    snapshot.clear_upload_file();
    snapshot.clear_upload_id();

    if (!commit_snapshot_update(txn.get(), instance_id, vs, std::move(snapshot), code, msg)) {
        response->mutable_status()->set_code(code);
        response->mutable_status()->set_msg(msg);
        return;
    }

    LOG_INFO("commit_snapshot succeeded")
            .tag("instance_id", instance_id)
            .tag("snapshot_id", request.snapshot_id())
            .tag("last_journal_id",
                 request.has_last_journal_id() ? request.last_journal_id() : -1);

    response->mutable_status()->set_code(MetaServiceCode::OK);
}

void SnapshotManager::abort_snapshot(std::string_view instance_id,
                                     const AbortSnapshotRequest& request,
                                     AbortSnapshotResponse* response) {
    std::unique_ptr<Transaction> txn;
    Versionstamp vs;
    SnapshotPB snapshot;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg;

    if (!fetch_snapshot_for_update(txn_kv_.get(), instance_id, request.snapshot_id(), &txn, &vs,
                                   &snapshot, code, msg)) {
        response->mutable_status()->set_code(code);
        response->mutable_status()->set_msg(msg);
        return;
    }

    // Already in a terminal state — treat as a no-op to make abort idempotent.
    if (snapshot.status() == SnapshotStatus::SNAPSHOT_ABORTED ||
        snapshot.status() == SnapshotStatus::SNAPSHOT_RECYCLED) {
        LOG_INFO("abort_snapshot called on already-terminal snapshot, no-op")
                .tag("instance_id", instance_id)
                .tag("snapshot_id", request.snapshot_id())
                .tag("status", SnapshotStatus_Name(snapshot.status()));
        response->mutable_status()->set_code(MetaServiceCode::OK);
        return;
    }

    // Log upload info so recycler can issue AbortMultipartUpload if snapshot is abandoned.
    if (snapshot.has_upload_file() && !snapshot.upload_file().empty()) {
        LOG_INFO("abort_snapshot: multipart upload will be cleaned by recycler")
                .tag("instance_id", instance_id)
                .tag("snapshot_id", request.snapshot_id())
                .tag("upload_file", snapshot.upload_file())
                .tag("upload_id", snapshot.upload_id());
    }

    snapshot.set_status(SnapshotStatus::SNAPSHOT_ABORTED);
    if (request.has_reason()) snapshot.set_reason(request.reason());
    snapshot.set_finish_at(now_seconds());

    if (!commit_snapshot_update(txn.get(), instance_id, vs, std::move(snapshot), code, msg)) {
        response->mutable_status()->set_code(code);
        response->mutable_status()->set_msg(msg);
        return;
    }

    LOG_INFO("abort_snapshot succeeded")
            .tag("instance_id", instance_id)
            .tag("snapshot_id", request.snapshot_id())
            .tag("reason", request.has_reason() ? request.reason() : "(none)");

    response->mutable_status()->set_code(MetaServiceCode::OK);
}

// Populate one SnapshotInfoPB from a SnapshotPB + its versionstamp.
static void fill_snapshot_info(const SnapshotPB& snap, const Versionstamp& vs,
                                SnapshotInfoPB* info) {
    info->set_snapshot_id(SnapshotManager::serialize_snapshot_id(vs));
    info->set_create_at(snap.create_at());
    if (snap.has_finish_at())              info->set_finish_at(snap.finish_at());
    if (snap.has_image_url())              info->set_image_url(snap.image_url());
    if (snap.has_last_journal_id())        info->set_journal_id(snap.last_journal_id());
    if (snap.has_status())                 info->set_status(snap.status());
    if (snap.has_type())                   info->set_type(snap.type());
    if (snap.has_instance_id())            info->set_instance_id(snap.instance_id());
    if (snap.has_auto_())                   info->set_auto_snapshot(snap.auto_());
    if (snap.has_ttl_seconds())            info->set_ttl_seconds(snap.ttl_seconds());
    if (snap.has_timeout_seconds())        info->set_timeout_seconds(snap.timeout_seconds());
    if (snap.has_label())                  info->set_snapshot_label(snap.label());
    if (snap.has_reason())                 info->set_reason(snap.reason());
    if (snap.has_resource_id())            info->set_resource_id(snap.resource_id());
    if (snap.has_snapshot_ancestor())      info->set_ancestor_id(snap.snapshot_ancestor());
    if (snap.has_snapshot_meta_image_size())
        info->set_snapshot_meta_image_size(snap.snapshot_meta_image_size());
    if (snap.has_snapshot_logical_data_size())
        info->set_snapshot_logical_data_size(snap.snapshot_logical_data_size());
    if (snap.has_snapshot_retained_data_size())
        info->set_snapshot_retained_data_size(snap.snapshot_retained_data_size());
    if (snap.has_snapshot_billable_data_size())
        info->set_snapshot_billable_data_size(snap.snapshot_billable_data_size());
    for (int64_t id : snap.protected_db_ids())        info->add_protected_db_ids(id);
    for (int64_t id : snap.protected_table_ids())     info->add_protected_table_ids(id);
    for (int64_t id : snap.protected_partition_ids()) info->add_protected_partition_ids(id);
    if (snap.has_rowset_refs_seeded())    info->set_rowset_refs_seeded(snap.rowset_refs_seeded());
    if (snap.has_table_meta_exported())   info->set_table_meta_exported(snap.table_meta_exported());
    if (snap.has_exported_at())           info->set_exported_at(snap.exported_at());
    if (snap.has_snapshot_cluster_version())
        info->set_snapshot_cluster_version(snap.snapshot_cluster_version());
    for (const auto& ct : snap.captured_tables()) *info->add_captured_tables() = ct;
}

void SnapshotManager::list_snapshot(std::string_view instance_id,
                                    const ListSnapshotRequest& request,
                                    ListSnapshotResponse* response) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(cast_as<ErrCategory::CREATE>(err));
        response->mutable_status()->set_msg("failed to create transaction");
        return;
    }

    std::vector<std::pair<SnapshotPB, Versionstamp>> snapshots;
    MetaReader reader(instance_id);

    if (request.has_required_snapshot_id() && !request.required_snapshot_id().empty()) {
        Versionstamp vs;
        if (!parse_snapshot_versionstamp(request.required_snapshot_id(), &vs)) {
            response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
            response->mutable_status()->set_msg(fmt::format(
                    "invalid required_snapshot_id='{}'", request.required_snapshot_id()));
            return;
        }
        SnapshotPB snap;
        err = reader.get_snapshot(txn.get(), vs, &snap);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            // Return empty list — snapshot not found is not an error for list
            response->mutable_status()->set_code(MetaServiceCode::OK);
            return;
        }
        if (err != TxnErrorCode::TXN_OK) {
            response->mutable_status()->set_code(cast_as<ErrCategory::READ>(err));
            response->mutable_status()->set_msg(fmt::format(
                    "failed to fetch snapshot snapshot_id='{}', err={}",
                    request.required_snapshot_id(), err));
            return;
        }
        snapshots.emplace_back(std::move(snap), vs);
    } else {
        err = reader.get_snapshots(txn.get(), &snapshots);
        if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            response->mutable_status()->set_code(cast_as<ErrCategory::READ>(err));
            response->mutable_status()->set_msg(
                    fmt::format("failed to list snapshots, err={}", err));
            return;
        }
    }

    bool include_aborted = request.has_include_aborted() && request.include_aborted();
    for (auto& [snap, vs] : snapshots) {
        // PREPARE and RECYCLED are internal states — never expose to callers.
        // NORMAL is always visible. ABORTED is visible only when explicitly requested.
        if (snap.status() == SnapshotStatus::SNAPSHOT_NORMAL) {
            // always include
        } else if (snap.status() == SnapshotStatus::SNAPSHOT_ABORTED && include_aborted) {
            // include only when caller opts in
        } else {
            continue;
        }
        fill_snapshot_info(snap, vs, response->add_snapshots());
    }

    LOG_INFO("list_snapshot succeeded")
            .tag("instance_id", instance_id)
            .tag("returned", response->snapshots_size());

    response->mutable_status()->set_code(MetaServiceCode::OK);
}

void SnapshotManager::drop_snapshot(std::string_view instance_id,
                                    const DropSnapshotRequest& request,
                                    DropSnapshotResponse* response) {
    std::unique_ptr<Transaction> txn;
    Versionstamp vs;
    SnapshotPB snapshot;
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg;

    if (!fetch_snapshot_for_update(txn_kv_.get(), instance_id, request.snapshot_id(), &txn, &vs,
                                   &snapshot, code, msg)) {
        response->mutable_status()->set_code(code);
        response->mutable_status()->set_msg(msg);
        return;
    }

    // Idempotent: already dropped → OK (handles KV_TXN_MAYBE_COMMITTED retries).
    if (snapshot.status() == SnapshotStatus::SNAPSHOT_RECYCLED) {
        response->mutable_status()->set_code(MetaServiceCode::OK);
        return;
    }

    // Only NORMAL snapshots can be dropped. PREPARE must be aborted first.
    // ABORTED snapshots are terminal and must not be re-dropped.
    if (snapshot.status() != SnapshotStatus::SNAPSHOT_NORMAL) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(fmt::format(
                "snapshot cannot be dropped in current status={}, snapshot_id='{}'",
                SnapshotStatus_Name(snapshot.status()), request.snapshot_id()));
        return;
    }

    // Reject drop if any clone instance still references this snapshot.
    // Deleting a snapshot that a DR standby depends on would break that standby.
    bool has_refs = false;
    TxnErrorCode err = MetaReader(instance_id).has_snapshot_references(txn.get(), vs, &has_refs);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(cast_as<ErrCategory::READ>(err));
        response->mutable_status()->set_msg(fmt::format(
                "failed to check snapshot references, snapshot_id='{}', err={}",
                request.snapshot_id(), err));
        return;
    }
    if (has_refs) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(fmt::format(
                "snapshot has active clone references and cannot be dropped, snapshot_id='{}'",
                request.snapshot_id()));
        return;
    }

    snapshot.set_status(SnapshotStatus::SNAPSHOT_RECYCLED);
    snapshot.set_finish_at(now_seconds());

    if (!commit_snapshot_update(txn.get(), instance_id, vs, std::move(snapshot), code, msg)) {
        response->mutable_status()->set_code(code);
        response->mutable_status()->set_msg(msg);
        return;
    }

    LOG_INFO("drop_snapshot succeeded")
            .tag("instance_id", instance_id)
            .tag("snapshot_id", request.snapshot_id());

    response->mutable_status()->set_code(MetaServiceCode::OK);
}

void SnapshotManager::clone_instance(const CloneInstanceRequest& request,
                                     CloneInstanceResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

std::pair<MetaServiceCode, std::string> SnapshotManager::compact_snapshot(
        std::string_view instance_id_sv) {
    return {MetaServiceCode::UNDEFINED_ERR, "Not implemented"};
}
std::pair<MetaServiceCode, std::string> SnapshotManager::decouple_instance(std::string_view id) {
    return {MetaServiceCode::UNDEFINED_ERR, "Not implemented"};
}

std::pair<MetaServiceCode, std::string> SnapshotManager::set_multi_version_status(
        std::string_view instance_id, MultiVersionStatus multi_version_status) {
    return {MetaServiceCode::UNDEFINED_ERR, "Not implemented"};
}

int SnapshotManager::recycle_snapshots(InstanceRecycler* recycler) {
    std::string_view instance_id = recycler->instance_id();

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("recycle_snapshots: failed to create txn")
                .tag("instance_id", instance_id)
                .tag("err", err);
        return -1;
    }

    std::vector<std::pair<SnapshotPB, Versionstamp>> snapshots;
    MetaReader reader(instance_id);
    err = reader.get_snapshots(txn.get(), &snapshots);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("recycle_snapshots: failed to list snapshots")
                .tag("instance_id", instance_id)
                .tag("err", err);
        return -1;
    }

    int recycled = 0;
    int errors   = 0;
    int64_t now  = now_seconds();

    for (auto& [snap, vs] : snapshots) {
        // Expire stale PREPARE snapshots that timed out during upload.
        if (snap.status() == SnapshotStatus::SNAPSHOT_PREPARE) {
            if (now > snap.create_at() + snap.timeout_seconds()) {
                // Reuse abort path: sets ABORTED + logs upload tracking fields
                AbortSnapshotRequest abort_req;
                abort_req.set_snapshot_id(serialize_snapshot_id(vs));
                abort_req.set_reason("timed out during recycle");
                AbortSnapshotResponse abort_resp;
                abort_snapshot(instance_id, abort_req, &abort_resp);
                if (abort_resp.status().code() == MetaServiceCode::OK) {
                    LOG_INFO("recycle_snapshots: aborted stale PREPARE snapshot")
                            .tag("instance_id", instance_id)
                            .tag("snapshot_id", abort_req.snapshot_id());
                } else {
                    LOG_WARNING("recycle_snapshots: failed to abort stale PREPARE snapshot")
                            .tag("instance_id", instance_id)
                            .tag("snapshot_id", abort_req.snapshot_id())
                            .tag("err", abort_resp.status().msg());
                    ++errors;
                }
            }
            continue; // PREPARE snapshots are not yet ready for data cleanup
        }

        // Seed rowset ref counts first (blocks TTL expiry until complete),
        // then TTL-expire NORMAL snapshots that have passed their deadline.
        if (snap.status() == SnapshotStatus::SNAPSHOT_NORMAL) {
            if (!snap.rowset_refs_seeded()) {
                std::vector<int64_t> db_ids(snap.protected_db_ids().begin(),
                                            snap.protected_db_ids().end());
                std::vector<int64_t> table_ids(snap.protected_table_ids().begin(),
                                               snap.protected_table_ids().end());
                std::vector<int64_t> partition_ids(snap.protected_partition_ids().begin(),
                                                   snap.protected_partition_ids().end());
                if (seed_rowset_ref_counts(instance_id, vs, db_ids, table_ids, partition_ids) != 0) {
                    LOG_WARNING("recycle_snapshots: rowset ref seeding failed, retry next cycle")
                            .tag("instance_id", instance_id)
                            .tag("snapshot_id", serialize_snapshot_id(vs));
                    ++errors;
                    continue; // Cannot expire until seeding completes successfully.
                }
                snap.set_rowset_refs_seeded(true); // reflects FDB state after seed
            }

            // Export FDB metadata to S3; enables same-cluster restore.
            bool needs_export = !snap.protected_table_ids().empty()     ||
                                 !snap.protected_partition_ids().empty() ||
                                 !snap.protected_db_ids().empty();        // DB-level also exports per-table
            if (needs_export && snap.rowset_refs_seeded() && !snap.table_meta_exported()) {
                if (!snap.has_resource_id() || snap.resource_id().empty()) {
                    // Cannot export without a vault accessor — skip this snapshot permanently.
                    // HDFS-backed snapshots have no resource_id and cannot use same-cluster restore.
                    LOG_WARNING("recycle_snapshots: cannot export table meta — no resource_id "
                                "(HDFS snapshot); same-cluster restore unavailable")
                            .tag("instance_id", instance_id)
                            .tag("snapshot_id", serialize_snapshot_id(vs));
                } else if (recycler->export_table_meta(std::string(instance_id),
                                                        snap.resource_id(), vs, snap) != 0) {
                    LOG_WARNING("recycle_snapshots: table meta export failed, retry next cycle")
                            .tag("instance_id", instance_id)
                            .tag("snapshot_id", serialize_snapshot_id(vs));
                    ++errors;
                    continue;
                } else {
                    snap.set_table_meta_exported(true);
                    // Keep in-memory snap consistent with FDB so the TTL anchor is correct
                    // in the same recycler cycle without waiting for a fresh FDB read.
                    snap.set_exported_at(now_seconds());
                }
            }

            // Use exported_at as TTL anchor (falls back to create_at for full-cluster/old snapshots).
            int64_t ttl_anchor = snap.has_exported_at() ? snap.exported_at() : snap.create_at();
            bool ttl_expired = snap.has_ttl_seconds() && snap.ttl_seconds() > 0 &&
                               now > ttl_anchor + snap.ttl_seconds();
            if (ttl_expired) {
                const std::string snapshot_id = serialize_snapshot_id(vs);
                std::unique_ptr<Transaction> exp_txn;
                Versionstamp exp_vs;
                SnapshotPB exp_snap;
                MetaServiceCode exp_code = MetaServiceCode::OK;
                std::string exp_msg;
                if (!fetch_snapshot_for_update(txn_kv_.get(), instance_id, snapshot_id,
                                               &exp_txn, &exp_vs, &exp_snap, exp_code,
                                               exp_msg)) {
                    LOG_WARNING("recycle_snapshots: failed to fetch snapshot for TTL expiry")
                            .tag("instance_id", instance_id)
                            .tag("snapshot_id", snapshot_id)
                            .tag("err", exp_msg);
                    ++errors;
                } else if (exp_snap.status() == SnapshotStatus::SNAPSHOT_NORMAL) {
                    // Don't expire snapshots with active clone references — DR standby depends on them.
                    bool has_refs = false;
                    TxnErrorCode ref_err = MetaReader(instance_id).has_snapshot_references(
                            exp_txn.get(), exp_vs, &has_refs);
                    if (ref_err != TxnErrorCode::TXN_OK) {
                        LOG_WARNING("recycle_snapshots: failed to check clone references")
                                .tag("instance_id", instance_id)
                                .tag("snapshot_id", snapshot_id)
                                .tag("err", ref_err);
                        ++errors;
                    } else if (has_refs) {
                        LOG_INFO("recycle_snapshots: TTL-expired snapshot deferred — "
                                 "active clone references exist")
                                .tag("instance_id", instance_id)
                                .tag("snapshot_id", snapshot_id);
                        // Leave for operator to drop explicitly after decommissioning the standby.
                    } else {
                        // No clone references — safe to transition to RECYCLED.
                        exp_snap.set_status(SnapshotStatus::SNAPSHOT_RECYCLED);
                        if (!commit_snapshot_update(exp_txn.get(), instance_id, exp_vs,
                                                    std::move(exp_snap), exp_code, exp_msg)) {
                            LOG_WARNING("recycle_snapshots: failed to transition NORMAL→RECYCLED")
                                    .tag("instance_id", instance_id)
                                    .tag("snapshot_id", snapshot_id)
                                    .tag("err", exp_msg);
                            ++errors;
                        } else {
                            LOG_INFO("recycle_snapshots: TTL-expired snapshot → RECYCLED")
                                    .tag("instance_id", instance_id)
                                    .tag("snapshot_id", snapshot_id)
                                    .tag("ttl_seconds", snap.ttl_seconds());
                        }
                    }
                }
                // else: snapshot was concurrently modified; leave for next cycle.
            }
            continue; // NORMAL snapshots are live; if just RECYCLED, cleanup happens next cycle.
        }

        // Only RECYCLED and ABORTED snapshots reach this point.
        // ABORTED: S3 image may be partially uploaded — clean up opportunistically.
        if (snap.status() != SnapshotStatus::SNAPSHOT_RECYCLED &&
            snap.status() != SnapshotStatus::SNAPSHOT_ABORTED) {
            continue;
        }

        // resource_id links to the storage vault; HDFS snapshots have no resource_id.
        if (snap.has_resource_id() && !snap.resource_id().empty()) {
            int ret = recycler->recycle_snapshot_meta_and_data(
                    std::string(instance_id), snap.resource_id(), vs, snap);
            if (ret != 0) {
                LOG_WARNING("recycle_snapshots: failed to recycle snapshot data")
                        .tag("instance_id", instance_id)
                        .tag("snapshot_id", serialize_snapshot_id(vs))
                        .tag("resource_id", snap.resource_id());
                ++errors;
                continue; // Leave FDB key intact; retry next cycle
            }
        } else {
            LOG_INFO("recycle_snapshots: snapshot has no resource_id, skipping S3 cleanup "
                     "(expected for HDFS-backed snapshots)")
                    .tag("instance_id", instance_id)
                    .tag("snapshot_id", serialize_snapshot_id(vs))
                    .tag("status", SnapshotStatus_Name(snap.status()));
        }

        // Remove the FDB key for this snapshot.
        std::unique_ptr<Transaction> del_txn;
        err = txn_kv_->create_txn(&del_txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("recycle_snapshots: failed to create del txn")
                    .tag("instance_id", instance_id)
                    .tag("err", err);
            ++errors;
            continue;
        }
        std::string key_prefix = versioned::snapshot_full_key({std::string(instance_id)});
        versioned::document_remove<SnapshotPB>(del_txn.get(), key_prefix, vs);
        err = del_txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("recycle_snapshots: failed to commit FDB key deletion")
                    .tag("instance_id", instance_id)
                    .tag("snapshot_id", serialize_snapshot_id(vs))
                    .tag("err", err);
            ++errors;
            continue;
        }

        LOG_INFO("recycle_snapshots: snapshot fully recycled")
                .tag("instance_id", instance_id)
                .tag("snapshot_id", serialize_snapshot_id(vs))
                .tag("status", SnapshotStatus_Name(snap.status()));
        ++recycled;
    }

    LOG_INFO("recycle_snapshots completed")
            .tag("instance_id", instance_id)
            .tag("recycled", recycled)
            .tag("errors", errors);

    return errors > 0 ? -1 : 0;
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

int SnapshotManager::recycle_snapshot_meta_and_data(std::string_view instance_id,
                                                    std::string_view resource_id,
                                                    StorageVaultAccessor* accessor,
                                                    Versionstamp snapshot_version,
                                                    const SnapshotPB& snapshot_pb) {
    // Unseed rowset ref counts; skip if seeding never ran (rowset_refs_seeded=false).
    if (snapshot_pb.rowset_refs_seeded()) {
        if (unseed_rowset_ref_counts(instance_id, snapshot_version) != 0) {
            LOG_WARNING("recycle_snapshot_meta_and_data: unseed failed, will retry next cycle")
                    .tag("instance_id", instance_id)
                    .tag("snapshot_id", serialize_snapshot_id(snapshot_version));
            return -1; // Leave FDB key intact; retry next recycler cycle
        }
    }

    if (!snapshot_pb.has_image_url() || snapshot_pb.image_url().empty()) {
        LOG_INFO("recycle_snapshot_meta_and_data: no image_url, skipping S3 cleanup")
                .tag("instance_id", instance_id)
                .tag("snapshot_id", serialize_snapshot_id(snapshot_version))
                .tag("resource_id", resource_id);
        return 0;
    }

    // Delete the entire snapshot directory (parent of fe_image and any fdb_meta_*.pb files).
    // Use dirname(image_url) so delete_directory correctly prefixes "/" and sweeps all objects.
    auto slash_pos = snapshot_pb.image_url().rfind('/');
    if (slash_pos == std::string::npos) {
        LOG_WARNING("recycle_snapshot_meta_and_data: malformed image_url (no slash) — "
                    "cannot derive snapshot directory, skipping S3 cleanup")
                .tag("instance_id", instance_id)
                .tag("snapshot_id", serialize_snapshot_id(snapshot_version))
                .tag("image_url", snapshot_pb.image_url());
        return -1;
    }
    std::string snapshot_dir = snapshot_pb.image_url().substr(0, slash_pos);
    int ret = accessor->delete_directory(snapshot_dir);
    if (ret != 0) {
        LOG_WARNING("recycle_snapshot_meta_and_data: failed to delete snapshot directory")
                .tag("instance_id", instance_id)
                .tag("snapshot_id", serialize_snapshot_id(snapshot_version))
                .tag("resource_id", resource_id)
                .tag("snapshot_dir", snapshot_dir);
        return ret;
    }

    LOG_INFO("recycle_snapshot_meta_and_data: snapshot directory deleted")
            .tag("instance_id", instance_id)
            .tag("snapshot_id", serialize_snapshot_id(snapshot_version))
            .tag("resource_id", resource_id)
            .tag("snapshot_dir", snapshot_dir);
    return 0;
}

int SnapshotManager::export_table_meta(std::string_view instance_id_sv,
                                        StorageVaultAccessor* accessor, Versionstamp snapshot_vs,
                                        const SnapshotPB& snap) {
    const std::string instance_id(instance_id_sv);

    if (!snap.has_image_url() || snap.image_url().empty()) {
        LOG_WARNING("export_table_meta: snapshot has no image_url, cannot derive S3 path")
                .tag("instance_id", instance_id);
        return -1;
    }
    // Use snapshot_vs to derive S3 path — avoids double-prefixing from image_url.
    const std::string snapshot_key = "snapshot/" + serialize_snapshot_id(snapshot_vs);

    // Determine filter sets (mutually exclusive by FE validation).
    std::unordered_set<int64_t> db_filter(snap.protected_db_ids().begin(),
                                           snap.protected_db_ids().end());
    std::unordered_set<int64_t> table_filter(snap.protected_table_ids().begin(),
                                              snap.protected_table_ids().end());
    std::unordered_set<int64_t> partition_filter(snap.protected_partition_ids().begin(),
                                                  snap.protected_partition_ids().end());
    const bool partition_level = !partition_filter.empty();

    // Scan tablet indexes to group tablets by table_id.
    struct TableTablets {
        std::vector<TabletIndexPB> indexes;
        std::vector<int64_t> tablet_ids;
        std::unordered_set<int64_t> partition_ids;
    };
    std::unordered_map<int64_t, TableTablets> table_map;

    {
        std::string begin_key = meta_tablet_idx_key({instance_id, 0});
        std::string end_key   = meta_tablet_idx_key({instance_id, INT64_MAX});
        FullRangeGetOptions scan_opts(txn_kv_);
        scan_opts.prefetch = true;
        auto it = txn_kv_->full_range_get(begin_key, end_key, std::move(scan_opts));

        for (auto kv = it->next(); kv.has_value(); kv = it->next()) {
            auto [k, v] = *kv;
            TabletIndexPB idx;
            if (!idx.ParseFromArray(v.data(), v.size())) continue;

            bool include = partition_level ? partition_filter.count(idx.partition_id())
                                           : (!table_filter.empty() ? table_filter.count(idx.table_id())
                                           : db_filter.count(idx.db_id()));
            if (!include) continue;

            auto& tt = table_map[idx.table_id()];
            tt.indexes.push_back(idx);
            tt.tablet_ids.push_back(idx.tablet_id());
            tt.partition_ids.insert(idx.partition_id());
        }
        if (!it->is_valid()) {
            LOG_WARNING("export_table_meta: tablet index scan error").tag("instance_id", instance_id);
            return -1;
        }
    }

    if (table_map.empty()) {
        // All tablets for the protected scope have been recycled since snapshot time.
        // Nothing to export — proceed to mark_done below so TTL expiry is not permanently blocked.
        LOG_INFO("export_table_meta: no matching tablets found (all recycled), will mark done")
                .tag("instance_id", instance_id)
                .tag("snapshot_id", serialize_snapshot_id(snapshot_vs));
    }

    int errors = 0;
    MetaReader reader(instance_id, snapshot_vs);

    for (auto& [table_id, tt] : table_map) {
        TableFdbMetaPB fdb_meta;
        fdb_meta.set_table_id(table_id);
        // Encode versionstamp: high = bytes 0-7 (FDB transaction version), low = bytes 8-9 (order).
        fdb_meta.set_snapshot_vs_high(static_cast<int64_t>(snapshot_vs.version()));
        fdb_meta.set_snapshot_vs_low(static_cast<int32_t>(snapshot_vs.order()));

        // Embed FE schema JSON captured at commit_snapshot time.
        // Required for dropped-table restore; absent means only new-table restore (with remap) is supported.
        auto schema_it = snap.table_schema_jsons().find(table_id);
        if (schema_it != snap.table_schema_jsons().end()) {
            fdb_meta.set_fe_table_schema_json(schema_it->second);
        }

        if (partition_level) {
            for (int64_t pid : tt.partition_ids) {
                fdb_meta.add_partition_ids(pid);
            }
        }
        for (auto& idx : tt.indexes) {
            *fdb_meta.add_tablet_indexes() = idx;
        }

        // Per-tablet reads: rowsets and tablet meta (fresh txn per tablet to avoid FDB timeout).
        for (int64_t tablet_id : tt.tablet_ids) {
            std::unique_ptr<Transaction> read_txn;
            if (txn_kv_->create_txn(&read_txn) != TxnErrorCode::TXN_OK) { ++errors; continue; }

            std::vector<std::pair<RowsetMetaCloudPB, Versionstamp>> load_rs, compact_rs;
            TxnErrorCode err  = reader.get_load_rowset_metas(read_txn.get(), tablet_id, &load_rs);
            TxnErrorCode err2 = reader.get_compact_rowset_metas(read_txn.get(), tablet_id, &compact_rs);
            if (err  != TxnErrorCode::TXN_OK && err  != TxnErrorCode::TXN_KEY_NOT_FOUND) { ++errors; continue; }
            if (err2 != TxnErrorCode::TXN_OK && err2 != TxnErrorCode::TXN_KEY_NOT_FOUND) { ++errors; continue; }

            // V1 fallback: rowset metadata was stored in snapshot_rowset_meta_key at
            // seed time (inside commit_snapshot, before compaction can interfere).
            // This is fully agnostic to compaction and recycler state.
            if (load_rs.empty() && compact_rs.empty()) {
                std::string meta_pfx = versioned::snapshot_rowset_meta_key_prefix(
                        instance_id, snapshot_vs, tablet_id);
                std::string meta_end = meta_pfx;
                meta_end.push_back('\xff');
                FullRangeGetOptions opts(txn_kv_);
                opts.prefetch = true;
                auto meta_it = txn_kv_->full_range_get(meta_pfx, meta_end, std::move(opts));
                for (auto kv = meta_it->next(); kv.has_value(); kv = meta_it->next()) {
                    auto [k, v] = *kv;
                    RowsetMetaCloudPB rs_meta;
                    if (rs_meta.ParseFromArray(v.data(), v.size())) {
                        *fdb_meta.add_load_rowsets() = std::move(rs_meta);
                    }
                }
                if (!meta_it->is_valid()) {
                    LOG_WARNING("export_table_meta: V1 snapshot rowset meta scan error")
                            .tag("instance_id", instance_id)
                            .tag("tablet_id", tablet_id);
                    ++errors;
                }
            }

            for (auto& [rs, _] : load_rs)    *fdb_meta.add_load_rowsets() = rs;
            for (auto& [rs, _] : compact_rs) *fdb_meta.add_compact_rowsets() = rs;

            TabletMetaCloudPB tablet_meta;
            TxnErrorCode meta_err =
                    reader.get_tablet_meta(read_txn.get(), tablet_id, &tablet_meta, nullptr, true);
            if (meta_err == TxnErrorCode::TXN_OK) {
                *fdb_meta.add_tablets() = std::move(tablet_meta);
            } else if (meta_err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                // V1 fallback: read meta_tablet_key (0x01) if versioned key absent.
                for (const auto& idx : tt.indexes) {
                    if (idx.tablet_id() != tablet_id) continue;
                    MetaTabletKeyInfo ki {instance_id, idx.table_id(), idx.index_id(),
                                         idx.partition_id(), tablet_id};
                    std::string tm_key = meta_tablet_key(ki);
                    std::string tm_val;
                    if (read_txn->get(tm_key, &tm_val) == TxnErrorCode::TXN_OK) {
                        TabletMetaCloudPB fallback_meta;
                        if (fallback_meta.ParseFromString(tm_val)) {
                            *fdb_meta.add_tablets() = std::move(fallback_meta);
                        }
                    }
                    break;
                }
            }
        }

        // Batch-read stats for all tablets in this table.
        {
            std::unique_ptr<Transaction> stats_txn;
            if (txn_kv_->create_txn(&stats_txn) == TxnErrorCode::TXN_OK) {
                std::unordered_map<int64_t, TabletStatsPB> load_stats, compact_stats;
                std::unordered_map<int64_t, Versionstamp> load_vs, compact_vs;
                TxnErrorCode ls_err = reader.get_tablet_load_stats(stats_txn.get(), tt.tablet_ids,
                                                                    &load_stats, &load_vs, true);
                TxnErrorCode cs_err = reader.get_tablet_compact_stats(
                        stats_txn.get(), tt.tablet_ids, &compact_stats, &compact_vs, true);
                if (ls_err != TxnErrorCode::TXN_OK && ls_err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
                    LOG_WARNING("export_table_meta: failed to read load stats")
                            .tag("instance_id", instance_id).tag("table_id", table_id);
                    ++errors;
                }
                if (cs_err != TxnErrorCode::TXN_OK && cs_err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
                    LOG_WARNING("export_table_meta: failed to read compact stats")
                            .tag("instance_id", instance_id).tag("table_id", table_id);
                    ++errors;
                }
                for (auto& [_, s] : load_stats)    *fdb_meta.add_load_stats() = std::move(s);
                for (auto& [_, s] : compact_stats) *fdb_meta.add_compact_stats() = std::move(s);
            } else {
                ++errors;
            }
        }

        // Batch-read partition versions for all partitions in this table.
        {
            std::vector<int64_t> pids(tt.partition_ids.begin(), tt.partition_ids.end());
            std::unique_ptr<Transaction> pv_txn;
            if (txn_kv_->create_txn(&pv_txn) != TxnErrorCode::TXN_OK) {
                LOG_WARNING("export_table_meta: failed to create txn for partition versions")
                        .tag("instance_id", instance_id).tag("table_id", table_id);
                ++errors;
            } else {
                std::unordered_map<int64_t, VersionPB> versions;
                std::unordered_map<int64_t, Versionstamp> pv_vss;
                TxnErrorCode pv_err =
                        reader.get_partition_versions(pv_txn.get(), pids, &versions, &pv_vss, true);
                if (pv_err == TxnErrorCode::TXN_OK) {
                    for (auto& [pid, ver] : versions) {
                        auto* pv = fdb_meta.add_partition_versions();
                        pv->set_partition_id(pid);
                        *pv->mutable_version() = std::move(ver);
                    }
                } else {
                    LOG_WARNING("export_table_meta: failed to read partition versions")
                            .tag("instance_id", instance_id)
                            .tag("table_id", table_id)
                            .tag("err", pv_err);
                    ++errors;
                }

                bool is_v1_cluster = (pv_err == TxnErrorCode::TXN_OK && versions.empty())
                                  || (pv_err == TxnErrorCode::TXN_KEY_NOT_FOUND);
                if (is_v1_cluster) {
                    std::unordered_map<int64_t, std::pair<int64_t, int64_t>> pid_to_db_tbl;
                    for (auto& idx : tt.indexes) {
                        if (idx.has_partition_id() && idx.has_db_id() && idx.has_table_id()) {
                            pid_to_db_tbl.emplace(idx.partition_id(),
                                                   std::make_pair(idx.db_id(), idx.table_id()));
                        }
                    }
                    for (int64_t pid : pids) {
                        auto dbt_it = pid_to_db_tbl.find(pid);
                        if (dbt_it == pid_to_db_tbl.end()) continue;
                        int64_t db_id  = dbt_it->second.first;
                        int64_t tbl_id = dbt_it->second.second;
                        std::string v1_key = partition_version_key(
                                {instance_id, db_id, tbl_id, pid});
                        std::string v1_val;
                        TxnErrorCode get_err = pv_txn->get(v1_key, &v1_val);
                        if (get_err == TxnErrorCode::TXN_OK) {
                            VersionPB ver;
                            if (ver.ParseFromString(v1_val)) {
                                // Clear lazy-commit txn IDs from the source cluster (don't exist post-restore).
                                ver.clear_pending_txn_ids();
                                auto* pv = fdb_meta.add_partition_versions();
                                pv->set_partition_id(pid);
                                *pv->mutable_version() = std::move(ver);
                            }
                        }
                    }
                    if (fdb_meta.partition_versions_size() > 0) {
                        LOG_INFO("export_table_meta: used legacy 0x01 partition versions "
                                 "(version 1 cluster)")
                                .tag("instance_id", instance_id)
                                .tag("table_id", table_id)
                                .tag("count", fdb_meta.partition_versions_size());
                    }
                }
            }
        }

        // MoW delete bitmaps: keep only entries with version ≤ V_snapshot.
        {
            std::unordered_map<int64_t, int64_t> tab_to_part;
            for (auto& idx : tt.indexes) {
                tab_to_part[idx.tablet_id()] = idx.partition_id();
            }
            std::unordered_map<int64_t, int64_t> part_to_ver;
            for (const auto& pv : fdb_meta.partition_versions()) {
                part_to_ver[pv.partition_id()] = pv.version().version();
            }

            for (int64_t tablet_id : tt.tablet_ids) {
                auto pit = tab_to_part.find(tablet_id);
                if (pit == tab_to_part.end()) continue;
                auto vit = part_to_ver.find(pit->second);
                if (vit == part_to_ver.end()) continue;
                int64_t v_snapshot = vit->second;

                // Range scan: legacy (0x01) delete bitmaps for this tablet.
                std::string bm_begin = meta_delete_bitmap_key({instance_id, tablet_id, "", 0, 0});
                std::string bm_end   = meta_delete_bitmap_key({instance_id, tablet_id + 1, "", 0, 0});
                FullRangeGetOptions bm_opts(txn_kv_);
                bm_opts.prefetch = true;
                auto bm_it = txn_kv_->full_range_get(bm_begin, bm_end, std::move(bm_opts));

                for (auto bm_kv = bm_it->next(); bm_kv.has_value(); bm_kv = bm_it->next()) {
                    auto [k, v] = *bm_kv;
                    // Decode key: 0x01 "meta" instance_id "delete_bitmap"
                    //             tablet_id rowset_id version segment_id
                    std::string_view k1 = k;
                    k1.remove_prefix(1);  // strip 0x01 space byte
                    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> dec;
                    decode_key(&k1, &dec);
                    if (dec.size() < 7) continue;
                    // Malformed FDB key: field type mismatch. Skip rather than crash.
                    if (!std::holds_alternative<std::string>(std::get<0>(dec[4]))) {
                        LOG_WARNING("export_table_meta: malformed delete bitmap key, "
                                    "field[4] is not string, skipping")
                                .tag("instance_id", instance_id)
                                .tag("tablet_id", tablet_id);
                        continue;
                    }
                    if (!std::holds_alternative<int64_t>(std::get<0>(dec[5]))) {
                        LOG_WARNING("export_table_meta: malformed delete bitmap key, "
                                    "field[5] is not int64, skipping")
                                .tag("instance_id", instance_id)
                                .tag("tablet_id", tablet_id);
                        continue;
                    }
                    if (!std::holds_alternative<int64_t>(std::get<0>(dec[6]))) {
                        LOG_WARNING("export_table_meta: malformed delete bitmap key, "
                                    "field[6] is not int64, skipping")
                                .tag("instance_id", instance_id)
                                .tag("tablet_id", tablet_id);
                        continue;
                    }
                    auto rowset_id = std::get<std::string>(std::get<0>(dec[4]));
                    auto bm_ver    = std::get<int64_t>(std::get<0>(dec[5]));
                    auto seg_id    = std::get<int64_t>(std::get<0>(dec[6]));

                    if (bm_ver == 0) { continue; } // TEMP_VERSION_COMMON: uncommitted MoW pending delete
                    if (bm_ver > v_snapshot) continue;  // exclude post-snapshot bitmaps

                    auto* entry = fdb_meta.add_delete_bitmaps();
                    entry->set_tablet_id(tablet_id);
                    entry->set_rowset_id(rowset_id);
                    entry->set_version(bm_ver);
                    entry->set_segment_id(seg_id);
                    entry->set_bitmap(std::string(v));
                }

                // Scan versioned (0x03) delete bitmaps; each key covers all (segment_id, version) pairs per rowset.
                {
                    std::string v3_begin = versioned::meta_delete_bitmap_key({instance_id, tablet_id, ""});
                    std::string v3_end   = versioned::meta_delete_bitmap_key({instance_id, tablet_id + 1, ""});
                    FullRangeGetOptions v3_opts(txn_kv_);
                    v3_opts.prefetch = true;
                    auto v3_it = txn_kv_->full_range_get(v3_begin, v3_end, std::move(v3_opts));

                    for (auto kv = v3_it->next(); kv.has_value(); kv = v3_it->next()) {
                        auto [k, v] = *kv;
                        DeleteBitmapStoragePB storage;
                        if (!storage.ParseFromString(std::string(v))) {
                            LOG_WARNING("export_table_meta: failed to parse versioned DeleteBitmapStoragePB")
                                    .tag("instance_id", instance_id)
                                    .tag("table_id", table_id)
                                    .tag("tablet_id", tablet_id);
                            continue;
                        }
                        if (!storage.store_in_fdb() || !storage.has_delete_bitmap()) {
                            // Packed-slice (out-of-FDB) bitmaps — not yet supported for snapshot export.
                            // TODO: support PackedSliceLocationPB for instances using external bitmap storage.
                            LOG_WARNING("export_table_meta: skipping out-of-FDB versioned bitmap")
                                    .tag("instance_id", instance_id)
                                    .tag("table_id", table_id)
                                    .tag("tablet_id", tablet_id);
                            continue;
                        }
                        const DeleteBitmapPB& dbm = storage.delete_bitmap();
                        int n = dbm.rowset_ids_size();
                        for (int i = 0; i < n; ++i) {
                            if (i >= dbm.versions_size() || i >= dbm.segment_ids_size()
                                    || i >= dbm.segment_delete_bitmaps_size()) {
                                break;
                            }
                            int64_t bm_ver = dbm.versions(i);
                            if (bm_ver == 0) { continue; } // TEMP_VERSION_COMMON: uncommitted MoW pending delete
                            if (bm_ver > v_snapshot) continue;
                            auto* entry = fdb_meta.add_delete_bitmaps();
                            entry->set_tablet_id(tablet_id);
                            entry->set_rowset_id(dbm.rowset_ids(i));
                            entry->set_version(bm_ver);
                            entry->set_segment_id(static_cast<int64_t>(dbm.segment_ids(i)));
                            entry->set_bitmap(dbm.segment_delete_bitmaps(i));
                        }
                    }
                }
            }
        }
        // Serialize and upload to S3.
        std::string serialized = fdb_meta.SerializeAsString();
        std::string fdb_meta_path =
                snapshot_key + "/fdb_meta_table_" + std::to_string(table_id) + ".pb";
        LOG_INFO("export_table_meta: uploading")
                .tag("instance_id", instance_id)
                .tag("table_id", table_id)
                .tag("path", fdb_meta_path)
                .tag("bytes", serialized.size())
                .tag("tablets", tt.tablet_ids.size())
                .tag("partitions", tt.partition_ids.size());
        if (accessor->put_file(fdb_meta_path, serialized) != 0) {
            LOG_WARNING("export_table_meta: S3 upload failed")
                    .tag("instance_id", instance_id)
                    .tag("table_id", table_id)
                    .tag("path", fdb_meta_path);
            ++errors;
        }
    }

    if (errors > 0) {
        LOG_WARNING("export_table_meta: completed with errors, will retry next cycle")
                .tag("instance_id", instance_id)
                .tag("snapshot_id", serialize_snapshot_id(snapshot_vs))
                .tag("errors", errors);
        return -1;
    }

    // Mark export complete in FDB — same retry pattern as seeding mark-done.
    bool mark_done = false;
    for (int attempt = 0; attempt < 3; ++attempt) {
        std::unique_ptr<Transaction> txn;
        if (txn_kv_->create_txn(&txn) != TxnErrorCode::TXN_OK) continue;
        MetaReader mark_reader(instance_id);
        SnapshotPB snap_upd;
        if (mark_reader.get_snapshot(txn.get(), snapshot_vs, &snap_upd) != TxnErrorCode::TXN_OK) {
            continue;
        }
        snap_upd.set_table_meta_exported(true);
        // exported_at: TTL countdown from readiness, not from SQL request time.
        snap_upd.set_exported_at(now_seconds());
        MetaServiceCode code = MetaServiceCode::OK;
        std::string msg;
        if (commit_snapshot_update(txn.get(), instance_id, snapshot_vs, std::move(snap_upd), code,
                                   msg)) {
            mark_done = true;
            break;
        }
    }
    if (!mark_done) {
        LOG_WARNING("export_table_meta: failed to persist table_meta_exported after 3 attempts"
                    " — will retry next cycle")
                .tag("instance_id", instance_id)
                .tag("snapshot_id", serialize_snapshot_id(snapshot_vs));
        return -1;
    }

    // Write DR manifest to S3 for full-cluster snapshots.
    // Non-fatal: a failure here does not block restore; recycler retries export next cycle.
    bool is_full_cluster = snap.protected_db_ids().empty()
                        && snap.protected_table_ids().empty()
                        && snap.protected_partition_ids().empty();
    if (is_full_cluster && accessor != nullptr) {
        upload_dr_manifest(accessor, snap, snapshot_vs, instance_id);
    }

    LOG_INFO("export_table_meta: complete")
            .tag("instance_id", instance_id)
            .tag("snapshot_id", serialize_snapshot_id(snapshot_vs))
            .tag("tables_exported", table_map.size());
    return 0;
}

void SnapshotManager::upload_dr_manifest(StorageVaultAccessor* accessor,
                                          const SnapshotPB& snap, Versionstamp snapshot_vs,
                                          const std::string& instance_id) {
    const std::string snapshot_id = serialize_snapshot_id(snapshot_vs);
    // First 8 bytes of versionstamp = FDB transaction version (big-endian int64).
    const int64_t fdb_version = static_cast<int64_t>(snapshot_vs.version());

    // Derive snapshot S3 dir from image_url (strip trailing filename).
    std::string snapshot_dir;
    if (snap.has_image_url() && !snap.image_url().empty()) {
        const auto& url = snap.image_url();
        auto pos = url.rfind('/');
        snapshot_dir = (pos != std::string::npos) ? url.substr(0, pos) : url;
    }

    // Escape label for JSON: replace \ with \\ and " with \"
    std::string label = snap.has_label() ? snap.label() : "";
    std::string escaped_label;
    escaped_label.reserve(label.size());
    for (char c : label) {
        if (c == '"' || c == '\\') escaped_label += '\\';
        escaped_label += c;
    }

    // Build JSON manifest.
    std::string json = fmt::format(
            "{{\"snapshot_id\":\"{}\","
            "\"label\":\"{}\","
            "\"created_at_utc\":{}"
            ",\"fdb_version\":{}"
            ",\"bdbje_image_url\":\"{}\""
            ",\"instance_id\":\"{}\""
            ",\"tables\":{}}}",
            snapshot_id,
            escaped_label,
            snap.has_create_at() ? snap.create_at() : 0LL,
            fdb_version,
            snap.has_image_url() ? snap.image_url() : "",
            instance_id,
            snap.captured_tables_size());

    // Build operator runbook.
    std::string runbook = fmt::format(
            "== DR RUNBOOK — snapshot {} ==\n"
            "STEP 1: fdbrestore start --dest-cluster-file /etc/fdb.cluster"
            " --source-url s3://BUCKET/fdb-backup/ --version {}\n"
            "        fdbrestore wait\n"
            "STEP 2: bin/start_ms.sh\n"
            "STEP 3: aws s3 cp {} /doris-meta/bdb/image_current\n"
            "        bin/start_fe.sh  # cloud_unique_id=1:{}:0\n"
            "STEP 4: bin/start_be.sh\n"
            "NOTE:   Do NOT restart original cluster until DR is resolved.\n",
            snapshot_id, fdb_version,
            snap.has_image_url() ? snap.image_url() : "UNKNOWN_IMAGE_URL",
            instance_id);

    if (accessor->put_file("dr/latest/snapshot.json", json) != 0 ||
        accessor->put_file("dr/latest/runbook.txt", runbook) != 0) {
        LOG_WARNING("upload_dr_manifest: failed to upload DR manifest to S3 (non-fatal)")
                .tag("instance_id", instance_id)
                .tag("snapshot_id", snapshot_id);
        return;
    }
    // Also write a per-snapshot copy so history is preserved.
    accessor->put_file(snapshot_dir + "/dr_runbook.txt", runbook);

    LOG_INFO("upload_dr_manifest: complete")
            .tag("instance_id", instance_id)
            .tag("snapshot_id", snapshot_id)
            .tag("fdb_version", fdb_version);
}

void SnapshotManager::import_table_meta(std::string_view instance_id_sv,
                                         const ImportTableMetaRequest& request,
                                         ImportTableMetaResponse* response,
                                         bool is_versioned_write) {
    const std::string instance_id(instance_id_sv);
    auto set_err = [&](MetaServiceCode code, std::string msg) {
        response->mutable_status()->set_code(code);
        response->mutable_status()->set_msg(std::move(msg));
    };

    if (!request.has_fdb_meta_pb() || request.fdb_meta_pb().empty()) {
        return set_err(MetaServiceCode::INVALID_ARGUMENT, "fdb_meta_pb is required");
    }
    TableFdbMetaPB fdb_meta;
    if (!fdb_meta.ParseFromString(request.fdb_meta_pb())) {
        return set_err(MetaServiceCode::PROTOBUF_PARSE_ERR,
                       "failed to parse TableFdbMetaPB from fdb_meta_pb");
    }

    // Build partition filter and resolve which tablet_ids to include.
    std::unordered_set<int64_t> partition_filter(request.partition_ids().begin(),
                                                  request.partition_ids().end());
    bool all_partitions = partition_filter.empty();

    std::unordered_set<int64_t> included_tablet_ids;
    for (auto& idx : fdb_meta.tablet_indexes()) {
        if (all_partitions || partition_filter.count(idx.partition_id())) {
            included_tablet_ids.insert(idx.tablet_id());
        }
    }

    if (included_tablet_ids.empty()) {
        LOG_INFO("import_table_meta: no tablets match the requested partition filter")
                .tag("instance_id", instance_id)
                .tag("table_id", fdb_meta.table_id())
                .tag("partition_filter_size", partition_filter.size());
        response->mutable_status()->set_code(MetaServiceCode::OK);
        return;
    }

    // Remap tablet IDs; identity when empty (dropped-table restore uses original IDs).
    const auto& remap_map = request.tablet_id_remap();
    auto remap_id = [&](int64_t id) -> int64_t {
        if (remap_map.empty()) return id;
        auto it = remap_map.find(id);
        return (it != remap_map.end()) ? it->second : id;
    };
    const bool path_a = !remap_map.empty();

    // Partition ID remap helper (applied to partition version keys below).
    const auto& part_remap_map = request.partition_id_remap();
    auto remap_part_id = [&](int64_t id) -> int64_t {
        if (part_remap_map.empty()) return id;
        auto it = part_remap_map.find(id);
        return (it != part_remap_map.end()) ? it->second : id;
    };

    // On V1 clusters meta_schema_key may be absent. Build the schema once from
    // fe_table_schema_json and embed it into every tablet meta and rowset meta
    // we write, so neither get_tablet_meta nor sync_tablet_rowsets_unlocked
    // ever needs an external meta_schema_key lookup for restored tablets.
    doris::TabletSchemaCloudPB table_schema_for_embed;
    bool has_embed_schema = false;
    if (!fdb_meta.tablets().empty() && !is_versioned_write) {
        const auto& first_tab = fdb_meta.tablets(0);
        if ((!first_tab.has_schema() || first_tab.schema().column_size() == 0)
                && first_tab.has_schema_version() && first_tab.index_id() > 0) {
            std::string skey = meta_schema_key(
                    {instance_id, first_tab.index_id(), first_tab.schema_version()});
            std::unique_ptr<Transaction> stxn;
            if (txn_kv_->create_txn(&stxn) == TxnErrorCode::TXN_OK) {
                ValueBuf sval;
                if (blob_get(stxn.get(), skey, &sval) == TxnErrorCode::TXN_OK) {
                    has_embed_schema = parse_schema_value(sval, &table_schema_for_embed);
                }
            }
            if (!has_embed_schema) {
                has_embed_schema = build_schema_from_fe_json(
                        fdb_meta.fe_table_schema_json(), first_tab.index_id(),
                        &table_schema_for_embed);
                if (has_embed_schema) {
                    table_schema_for_embed.set_schema_version(first_tab.schema_version());
                } else {
                    LOG_WARNING("import_table_meta: schema unavailable for V1 tablet; "
                                "SELECT on restored table will fail until schema is fixed")
                            .tag("instance_id", instance_id)
                            .tag("index_id", first_tab.index_id())
                            .tag("schema_version", first_tab.schema_version());
                }
            }
        }
    }

    int64_t tablets_written = 0, rowsets_written = 0, partitions_written = 0;
    int errors = 0;

    WriteBatch wb(txn_kv_.get());
    if (wb.init() != 0) {
        return set_err(MetaServiceCode::KV_TXN_CREATE_ERR, "failed to init write batch");
    }

    bool batch_broken = false;
    // Clamp to 1 to prevent zero/negative disabling batching and causing TXN_TOO_LARGE.
    const int32_t effective_import_batch_size =
            std::max(1, static_cast<int32_t>(config::snapshot_import_batch_size));
    auto do_bump = [&]() -> bool {
        if (wb.bump(effective_import_batch_size) != 0) {
            ++errors;
            batch_broken = true;
            return false;
        }
        return true;
    };

    // target_table_id: if set (Path A), use it for V1 0x01 keys; else use source table_id.
    const int64_t target_table_id = request.has_target_table_id()
                                     ? request.target_table_id() : 0;

    // Pre-compute snapshot visible version per original partition_id to set
    // cumulative_layer_point on restored tablet metas, preventing EMPTY_CUMULATIVE
    // compaction from flooding the BE task pool and delaying DML.
    std::unordered_map<int64_t, int64_t> part_snapshot_versions;
    for (auto& pv : fdb_meta.partition_versions()) {
        if (pv.has_version()) {
            part_snapshot_versions[pv.partition_id()] = pv.version().version();
        }
    }

    // Tablet indexes (non-versioned plain keys).
    for (auto& idx : fdb_meta.tablet_indexes()) {
        if (batch_broken) break;
        if (!included_tablet_ids.count(idx.tablet_id())) continue;
        TabletIndexPB remapped_idx = idx;
        remapped_idx.set_tablet_id(remap_id(idx.tablet_id()));
        // Remap partition_id and table_id so V1 meta_tablet_key lookup constructs the right key.
        remapped_idx.set_partition_id(remap_part_id(idx.partition_id()));
        if (target_table_id > 0) remapped_idx.set_table_id(target_table_id);
        wb.txn->put(meta_tablet_idx_key({instance_id, remap_id(idx.tablet_id())}),
                    remapped_idx.SerializeAsString());
        ++tablets_written;

        // V1 clusters: write stats_tablet_key to prevent recycler from treating this
        // table as orphaned and deleting all partition_version_keys.
        if (!is_versioned_write) {
            int64_t tbl_id = (target_table_id > 0) ? target_table_id : idx.table_id();
            StatsTabletKeyInfo ski {instance_id, tbl_id, idx.index_id(),
                                    remap_part_id(idx.partition_id()),
                                    remap_id(idx.tablet_id())};
            TabletStatsPB init_stats;
            init_stats.mutable_idx()->set_tablet_id(remap_id(idx.tablet_id()));
            init_stats.mutable_idx()->set_table_id(tbl_id);
            init_stats.mutable_idx()->set_index_id(idx.index_id());
            init_stats.mutable_idx()->set_partition_id(remap_part_id(idx.partition_id()));
            std::string stats_val;
            if (init_stats.SerializeToString(&stats_val)) {
                wb.txn->put(stats_tablet_key(ski), stats_val);
            }
        }

        if (!do_bump()) break;
    }

    // Tablet metas — write to 0x03 (V2) or 0x01 (V1) based on cluster version.
    for (auto& tablet : fdb_meta.tablets()) {
        if (batch_broken) break;
        if (!included_tablet_ids.count(tablet.tablet_id())) continue;
        TabletMetaCloudPB tab = tablet;
        tab.set_tablet_id(remap_id(tablet.tablet_id()));
        // Set cumulative_layer_point to the snapshot partition version so all restored
        // rowsets (end_version ≤ snapshot_version) are treated as base rowsets by the BE.
        {
            auto pit = part_snapshot_versions.find(tablet.partition_id());
            if (pit != part_snapshot_versions.end()) {
                tab.set_cumulative_layer_point(pit->second);
            }
        }
        if (is_versioned_write) {
            if (target_table_id > 0) tab.set_table_id(target_table_id);
            tab.set_partition_id(remap_part_id(tablet.partition_id()));
            std::string key = versioned::meta_tablet_key({instance_id, remap_id(tablet.tablet_id())});
            if (!versioned::document_put(wb.txn.get(), key, std::move(tab))) { ++errors; }
        } else {
            // V1: update table_id and partition_id in both KEY and VALUE so the BE
            // calls get_version with the restored table's keys, not the source table's.
            int64_t tbl_id = (target_table_id > 0) ? target_table_id : tab.table_id();
            int64_t part_id = remap_part_id(tab.partition_id());
            tab.set_table_id(tbl_id);
            tab.set_partition_id(part_id);
            // Embed schema so get_tablet_meta never needs a separate meta_schema_key
            // lookup — which may be absent or get recycled on some V1 clusters.
            if (has_embed_schema && (!tab.has_schema() || tab.schema().column_size() == 0)) {
                *tab.mutable_schema() = table_schema_for_embed;
            }
            MetaTabletKeyInfo ki {instance_id, tbl_id, tab.index_id(), part_id,
                                   remap_id(tablet.tablet_id())};
            std::string val;
            if (tab.SerializeToString(&val)) {
                wb.txn->put(meta_tablet_key(ki), val);
            } else { ++errors; }
        }
        if (!do_bump()) break;
    }

    // rowset_id_v2 unchanged — both tables share S3 files (zero copy). tablet_id remapped
    // so FDB key-space is independent; source_tablet_id preserved for BE::segment_path().
    // Write to 0x01 (V1) or 0x03 (V2), mirroring commit_txn key-space choice.
    for (auto& rs : fdb_meta.load_rowsets()) {
        if (batch_broken) break;
        if (!included_tablet_ids.count(rs.tablet_id())) continue;
        RowsetMetaCloudPB r = rs;
        r.set_tablet_id(remap_id(rs.tablet_id()));
        if (path_a) {
            r.set_source_tablet_id(rs.tablet_id());
            r.set_source_rowset_id(rs.rowset_id_v2());
        }
        // Embed schema so sync_tablet_rowsets_unlocked never needs meta_schema_key.
        if (!is_versioned_write && has_embed_schema
                && (!r.has_tablet_schema() || r.tablet_schema().column_size() == 0)) {
            *r.mutable_tablet_schema() = table_schema_for_embed;
        }
        if (is_versioned_write) {
            std::string key = versioned::meta_rowset_load_key(
                    {instance_id, remap_id(rs.tablet_id()), rs.end_version()});
            if (!versioned::document_put(wb.txn.get(), key, std::move(r))) { ++errors; }
        } else {
            std::string key = meta_rowset_key({instance_id, remap_id(rs.tablet_id()), rs.end_version()});
            std::string val;
            if (r.SerializeToString(&val)) { wb.txn->put(key, val); } else { ++errors; }
        }
        ++rowsets_written;
        if (!do_bump()) break;
    }

    // Same write logic as load rowsets. On V1, compact/load may share end_version;
    // writing compact after load makes compact win in the WriteBatch — correct, the
    // compacted rowset supersedes its inputs (deleted in a healthy V1 cluster).
    for (auto& rs : fdb_meta.compact_rowsets()) {
        if (batch_broken) break;
        if (!included_tablet_ids.count(rs.tablet_id())) continue;
        RowsetMetaCloudPB r = rs;
        r.set_tablet_id(remap_id(rs.tablet_id()));
        if (path_a) {
            r.set_source_tablet_id(rs.tablet_id());
            r.set_source_rowset_id(rs.rowset_id_v2());
        }
        if (!is_versioned_write && has_embed_schema
                && (!r.has_tablet_schema() || r.tablet_schema().column_size() == 0)) {
            *r.mutable_tablet_schema() = table_schema_for_embed;
        }
        if (is_versioned_write) {
            std::string key = versioned::meta_rowset_compact_key(
                    {instance_id, remap_id(rs.tablet_id()), rs.end_version()});
            if (!versioned::document_put(wb.txn.get(), key, std::move(r))) { ++errors; }
        } else {
            std::string key = meta_rowset_key({instance_id, remap_id(rs.tablet_id()), rs.end_version()});
            std::string val;
            if (r.SerializeToString(&val)) { wb.txn->put(key, val); } else { ++errors; }
        }
        ++rowsets_written;
        if (!do_bump()) break;
    }

    // Load and compact stats (versioned 0x03 only — no 0x01 equivalent; V1 uses plan defaults).
    auto write_stats = [&](const auto& stats_list, auto key_fn) {
        for (auto& stats : stats_list) {
            if (batch_broken) break;
            if (!stats.has_idx()) continue;
            if (!included_tablet_ids.count(stats.idx().tablet_id())) continue;
            TabletStatsPB s = stats;
            s.mutable_idx()->set_tablet_id(remap_id(stats.idx().tablet_id()));
            if (!versioned::document_put(wb.txn.get(), key_fn(remap_id(stats.idx().tablet_id())),
                                         std::move(s))) { ++errors; }
            if (!do_bump()) break;
        }
    };
    write_stats(fdb_meta.load_stats(), [&](int64_t tid) {
        return versioned::tablet_load_stats_key({instance_id, tid});
    });
    write_stats(fdb_meta.compact_stats(), [&](int64_t tid) {
        return versioned::tablet_compact_stats_key({instance_id, tid});
    });

    // Partition versions — dual-write 0x01 + 0x03 on V2; 0x01-only on V1.
    // Dual-write required: WRITE_ONLY has is_versioned_write=true but reads 0x01; 0x03-only
    // would cause VERSION_NOT_MATCH on INSERTs.

    // part_to_db_tbl used by both V1-only and V2 dual-write paths.
    std::unordered_map<int64_t, std::pair<int64_t, int64_t>> part_to_db_tbl;
    for (auto& idx : fdb_meta.tablet_indexes()) {
        if (idx.has_partition_id() && idx.has_db_id() && idx.has_table_id()) {
            part_to_db_tbl.emplace(idx.partition_id(),
                                   std::make_pair(idx.db_id(), idx.table_id()));
        }
    }

    // Pre-compute max rowset end_version per source partition so the written
    // partition_version_key covers all restored rowsets even when the blob version
    // is stale (V1 partition_id discrepancy in export_table_meta).
    std::unordered_map<int64_t, int64_t> tablet_to_src_part;
    for (auto& idx : fdb_meta.tablet_indexes()) {
        tablet_to_src_part[idx.tablet_id()] = idx.partition_id();
    }
    std::unordered_map<int64_t, int64_t> part_max_rowset_ver;
    auto accum_rowset_vers = [&](const auto& rowsets) {
        for (auto& rs : rowsets) {
            auto pit = tablet_to_src_part.find(rs.tablet_id());
            if (pit == tablet_to_src_part.end()) continue;
            auto& cur = part_max_rowset_ver[pit->second];
            if (rs.end_version() > cur) cur = rs.end_version();
        }
    };
    accum_rowset_vers(fdb_meta.load_rowsets());
    accum_rowset_vers(fdb_meta.compact_rowsets());

    for (auto& pv : fdb_meta.partition_versions()) {
        if (batch_broken) break;
        if (!all_partitions && !partition_filter.count(pv.partition_id())) continue;

        int64_t remapped_part_id = remap_part_id(pv.partition_id());
        VersionPB ver = pv.version();
        // Raise version to max rowset end_version for this partition if the blob version
        // was stale (V1 partition_id discrepancy: export read the wrong partition_version_key).
        {
            auto rv_it = part_max_rowset_ver.find(pv.partition_id());
            if (rv_it != part_max_rowset_ver.end() && rv_it->second > ver.version()) {
                ver.set_version(rv_it->second);
            }
        }

        bool v3_ok = true;
        if (is_versioned_write) {
            std::string key = versioned::partition_version_key({instance_id, remapped_part_id});
            VersionPB ver_copy = ver; // document_put takes rvalue; keep ver for 0x01 write below.
            if (!versioned::document_put(wb.txn.get(), key, std::move(ver_copy))) { ++errors; v3_ok = false; }
        }

        // Always write 0x01: V1 reads 0x01 exclusively; V2 WRITE_ONLY reads 0x01 too.
        bool v1_written = false;
        {
            auto it = part_to_db_tbl.find(pv.partition_id());
            if (it != part_to_db_tbl.end()) {
                int64_t tbl_id = (target_table_id > 0) ? target_table_id : it->second.second;
                std::string key = partition_version_key(
                        {instance_id, it->second.first, tbl_id, remapped_part_id});
                std::string val;
                if (ver.SerializeToString(&val)) {
                    wb.txn->put(key, val);
                    v1_written = true;
                    LOG_INFO("import_table_meta: put partition_version_key")
                            .tag("db_id", it->second.first)
                            .tag("table_id", tbl_id)
                            .tag("partition_id", remapped_part_id)
                            .tag("version", ver.version());
                } else { ++errors; }
            } else {
                // Cannot construct the 0x01 key without a db/table mapping. A missing partition
                // version causes VERSION_NOT_MATCH on first INSERT — fail loudly here instead.
                LOG_WARNING("import_table_meta: no db/table for partition_id, cannot write 0x01 key")
                        .tag("instance_id", instance_id)
                        .tag("partition_id", pv.partition_id());
                ++errors;
            }
        }

        // Count only when at least one write succeeded; both 0x03 AND 0x01 may fail.
        if (v1_written || (is_versioned_write && v3_ok)) {
            ++partitions_written;
        }
        if (!do_bump()) break;
    }

    // V1 fallback: partition_versions blob is empty (partition_id in meta_tablet_idx_key
    // has no matching partition_version_key). Derive from max rowset end_version so
    // MS does not return VERSION_NOT_FOUND → FE spec_version=1 → BE finds no data rowsets.
    if (!batch_broken && partitions_written == 0 && tablets_written > 0) {
        for (auto& [src_pid, max_ver] : part_max_rowset_ver) {
            if (batch_broken) break;
            auto dbt_it = part_to_db_tbl.find(src_pid);
            if (dbt_it == part_to_db_tbl.end()) continue;
            int64_t remapped_pid = remap_part_id(src_pid);
            int64_t tbl_id = (target_table_id > 0) ? target_table_id : dbt_it->second.second;
            VersionPB ver;
            ver.set_version(std::max(int64_t(1), max_ver));
            std::string v1_key = partition_version_key(
                    {instance_id, dbt_it->second.first, tbl_id, remapped_pid});
            std::string val;
            if (ver.SerializeToString(&val)) {
                wb.txn->put(v1_key, val);
                ++partitions_written;
                LOG_INFO("import_table_meta: derived partition_version_key from rowsets (V1 fallback)")
                        .tag("db_id", dbt_it->second.first)
                        .tag("table_id", tbl_id)
                        .tag("partition_id", remapped_pid)
                        .tag("version", ver.version());
            } else { ++errors; }
            if (!do_bump()) break;
        }
    }

    // Write table_version_key so CloudSyncVersionDaemon gets the correct visible version.
    // effective_version = max(blob_partition_version, max_rowset_end_version) guards against
    // stale blob versions on V1 clusters where export may read the wrong partition_version_key.
    if (!batch_broken && partitions_written > 0) {
        int64_t tv_db_id = -1, tv_tbl_id = -1;
        int64_t max_part_version = 0;
        for (auto& [pid, dbt] : part_to_db_tbl) {
            tv_db_id = dbt.first;
            tv_tbl_id = (target_table_id > 0) ? target_table_id : dbt.second;
            break;
        }
        for (auto& pv : fdb_meta.partition_versions()) {
            if (pv.has_version() && pv.version().version() > max_part_version) {
                max_part_version = pv.version().version();
            }
        }
        // Also consider the max end_version of all restored rowsets: on V1, the
        // partition_version_key may be stale (uses a different partition_id than
        // commit_txn) and give a lower version than the actual rowset end_versions.
        int64_t max_rowset_end_ver = 0;
        for (auto& rs : fdb_meta.load_rowsets()) {
            if (rs.end_version() > max_rowset_end_ver) max_rowset_end_ver = rs.end_version();
        }
        for (auto& rs : fdb_meta.compact_rowsets()) {
            if (rs.end_version() > max_rowset_end_ver) max_rowset_end_ver = rs.end_version();
        }
        int64_t effective_version = std::max(max_part_version, max_rowset_end_ver);
        int64_t tv_increment = std::max(int64_t(1), effective_version);
        if (tv_db_id > 0) {
            std::string ver_key = table_version_key({instance_id, tv_db_id, tv_tbl_id});
            wb.txn->atomic_add(ver_key, tv_increment);
            if (is_versioned_write) {
                std::string v3_key = versioned::table_version_key({instance_id, tv_tbl_id});
                versioned_put(wb.txn.get(), v3_key, "");
            }
            do_bump();
        }
    }

    // MoW delete bitmaps — write to 0x01 (V1) or 0x03 (V2), mirroring commit_txn's key-space choice.

    // Pre-count entries for log tag (actual writes may be fewer if batch_broken).
    int64_t bitmaps_written = 0;
    for (auto& bm : fdb_meta.delete_bitmaps()) {
        if (included_tablet_ids.count(bm.tablet_id())) ++bitmaps_written;
    }

    // MoW delete bitmaps — same dual-write rationale as partition versions.
    {
        // Group by (tablet_id, rowset_id) for the 0x03 packed StoragePB.
        // unordered_map: no ordering needed; avoids O(log n) UUID string comparisons.
        auto pair_hash = [](const std::pair<int64_t, std::string>& p) {
            return std::hash<int64_t>{}(p.first) ^ (std::hash<std::string>{}(p.second) << 1);
        };
        std::unordered_map<std::pair<int64_t, std::string>,
                           std::vector<const DeleteBitmapEntryPB*>,
                           decltype(pair_hash)> groups(0, pair_hash);
        for (int i = 0; i < fdb_meta.delete_bitmaps_size(); ++i) {
            const auto& bm = fdb_meta.delete_bitmaps(i);
            if (!included_tablet_ids.count(bm.tablet_id())) continue;
            groups[{remap_id(bm.tablet_id()), bm.rowset_id()}].push_back(&bm);
        }

        for (auto& [key_pair, bm_entries] : groups) {
            if (batch_broken) break;

            // 0x01: one key per (tablet_id, rowset_id, version, segment_id) — always written.
            for (const auto* bm : bm_entries) {
                std::string key01 = meta_delete_bitmap_key(
                        {instance_id, key_pair.first, bm->rowset_id(),
                         bm->version(), bm->segment_id()});
                wb.txn->put(key01, bm->bitmap());
            }

            // 0x03: one packed StoragePB per (tablet_id, rowset_id) — written on V2.
            if (is_versioned_write) {
                DeleteBitmapStoragePB storage;
                storage.set_store_in_fdb(true);
                DeleteBitmapPB* dbm = storage.mutable_delete_bitmap();
                for (const auto* bm : bm_entries) {
                    dbm->add_rowset_ids(bm->rowset_id());
                    dbm->add_segment_ids(static_cast<uint32_t>(bm->segment_id()));
                    dbm->add_versions(bm->version());
                    dbm->add_segment_delete_bitmaps(bm->bitmap());
                }
                std::string key03 = versioned::meta_delete_bitmap_key(
                        {instance_id, key_pair.first, key_pair.second});
                wb.txn->put(key03, storage.SerializeAsString());
            }

            // One do_bump() per group keeps 0x01 + 0x03 atomic. Per-segment bumps would split
            // them across transactions; a crash mid-group leaves 0x03 absent → phantom reads on V2.
            if (!do_bump()) break;
        }
    }

    if (!batch_broken && wb.flush() != 0) { ++errors; }

    if (errors > 0) {
        LOG_WARNING("import_table_meta: completed with errors")
                .tag("instance_id", instance_id)
                .tag("table_id", fdb_meta.table_id())
                .tag("errors", errors);
        return set_err(MetaServiceCode::KV_TXN_COMMIT_ERR,
                       fmt::format("import_table_meta completed with {} FDB write errors", errors));
    }

    LOG_INFO("import_table_meta: complete")
            .tag("instance_id", instance_id)
            .tag("table_id", fdb_meta.table_id())
            .tag("tablets_restored", tablets_written)
            .tag("rowsets_restored", rowsets_written)
            .tag("partitions_restored", partitions_written)
            .tag("bitmaps_in_scope", bitmaps_written)
            .tag("path", path_a ? "A(tablet_id_remap)" : "B(original_ids)");

    response->set_tablets_restored(tablets_written);
    response->set_rowsets_restored(rowsets_written);
    response->set_partitions_restored(partitions_written);
    response->mutable_status()->set_code(MetaServiceCode::OK);
}

int SnapshotManager::migrate_to_versioned_keys(InstanceDataMigrator* migrator) {
    LOG(WARNING) << "Migrate to versioned keys is not implemented";
    return -1;
}

int SnapshotManager::compact_snapshot_chains(InstanceChainCompactor* compactor) {
    LOG(WARNING) << "Compact snapshot chains is not implemented";
    return -1;
}

static std::pair<MetaServiceCode, std::string> get_instance(Transaction* txn,
                                                            std::string_view instance_id,
                                                            InstanceInfoPB* instance_info) {
    InstanceKeyInfo instance_key_info {instance_id};
    std::string key = instance_key(instance_key_info);
    std::string val;
    TxnErrorCode err = txn->get(key, &val);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        return {MetaServiceCode::INVALID_ARGUMENT,
                fmt::format("instance not found, instance_id={}", instance_id)};
    } else if (err != TxnErrorCode::TXN_OK) {
        return {cast_as<ErrCategory::READ>(err),
                fmt::format("failed to get instance, instance_id={}, err={}", instance_id, err)};
    }

    if (!instance_info->ParseFromString(val)) {
        return {MetaServiceCode::INVALID_ARGUMENT, "failed to parse instance info"};
    }
    return {MetaServiceCode::OK, ""};
}

std::pair<MetaServiceCode, std::string> SnapshotManager::get_all_snapshots(
        Transaction* txn, std::string_view instance_id, std::string_view required_snapshot_id,
        std::vector<std::pair<SnapshotPB, Versionstamp>>* snapshots) {
    Versionstamp required_snapshot_versionstamp;
    if (!required_snapshot_id.empty()) {
        if (!parse_snapshot_versionstamp(required_snapshot_id, &required_snapshot_versionstamp)) {
            return {MetaServiceCode::INVALID_ARGUMENT, "invalid snapshot_id format"};
        }
    }

    InstanceInfoPB instance_info;
    auto [code, error_msg] = get_instance(txn, instance_id, &instance_info);
    if (code != MetaServiceCode::OK) {
        return {code, error_msg};
    }
    std::string current_instance_id(instance_id);
    if (instance_info.has_original_instance_id() && !instance_info.original_instance_id().empty()) {
        // the earliest instance_id for rollback
        current_instance_id = instance_info.original_instance_id();
    }

    std::unordered_set<std::string> visited;
    do {
        visited.insert(current_instance_id);
        MetaReader meta_reader(current_instance_id);
        if (required_snapshot_id.empty()) {
            TxnErrorCode err = meta_reader.get_snapshots(txn, snapshots);
            if (err != TxnErrorCode::TXN_OK) {
                return {cast_as<ErrCategory::READ>(err), "failed to get snapshots"};
            }
        } else {
            SnapshotPB snapshot_pb;
            TxnErrorCode err =
                    meta_reader.get_snapshot(txn, required_snapshot_versionstamp, &snapshot_pb);
            if (err == TxnErrorCode::TXN_OK) {
                snapshots->emplace_back(snapshot_pb, required_snapshot_versionstamp);
                return {MetaServiceCode::OK, ""};
            } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
                return {cast_as<ErrCategory::READ>(err), "failed to get snapshot"};
            }
        }
        if (current_instance_id == instance_id) {
            break;
        }
        auto [code, error_msg] = get_instance(txn, current_instance_id, &instance_info);
        if (code != MetaServiceCode::OK) {
            std::string msg = fmt::format("failed to get ancestor instance {}: {}",
                                          current_instance_id, error_msg);
            LOG_WARNING(msg);
            return {code, msg};
        }
        if (!instance_info.has_successor_instance_id() ||
            instance_info.successor_instance_id().empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            error_msg = fmt::format(
                    "successor_instance_id is empty for current instance_id={}, instance_id={}",
                    current_instance_id, instance_id);
            LOG_WARNING(error_msg);
            return {code, error_msg};
        }
        // Insert before advancing to detect cycles (including 2-node A→B→A).
        if (visited.count(instance_info.successor_instance_id()) > 0) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            error_msg = fmt::format(
                    "cycle detected in instance chain, current instance_id={}, instance_id={}",
                    current_instance_id, instance_id);
            LOG_WARNING(error_msg);
            return {code, error_msg};
        }
        current_instance_id = instance_info.successor_instance_id();
    } while (true);
    return {MetaServiceCode::OK, ""};
}

int SnapshotManager::seed_rowset_ref_counts(
        std::string_view instance_id_sv, Versionstamp snapshot_vs,
        const std::vector<int64_t>& included_db_ids,
        const std::vector<int64_t>& included_table_ids,
        const std::vector<int64_t>& included_partition_ids) {
    const std::string instance_id(instance_id_sv);

    std::unordered_set<int64_t> db_filter(included_db_ids.begin(), included_db_ids.end());
    std::unordered_set<int64_t> table_filter(included_table_ids.begin(), included_table_ids.end());
    std::unordered_set<int64_t> partition_filter(included_partition_ids.begin(),
                                                  included_partition_ids.end());
    // Three filters are mutually exclusive (enforced at FE command layer).
    bool is_full_cluster = db_filter.empty() && table_filter.empty() && partition_filter.empty();

    // Scan all tablet_index_key entries to get tablet context (db_id, table_id, tablet_id).
    std::string begin_key = meta_tablet_idx_key({instance_id, 0});
    std::string end_key   = meta_tablet_idx_key({instance_id, INT64_MAX});
    FullRangeGetOptions scan_opts(txn_kv_);
    scan_opts.prefetch = true;
    auto it = txn_kv_->full_range_get(begin_key, end_key, std::move(scan_opts));

    int64_t num_tablets = 0, num_rowsets = 0, errors = 0;
    WriteBatch wb(txn_kv_.get());
    if (wb.init() != 0) return -1;
    MetaReader reader(instance_id, snapshot_vs);

    for (auto kv = it->next(); kv.has_value(); kv = it->next()) {
        auto [k, v] = *kv;
        TabletIndexPB idx;
        if (!idx.ParseFromArray(v.data(), v.size())) continue;

        // Apply granular filter (mutually exclusive: db, table, or partition).
        if (!is_full_cluster) {
            bool match = db_filter.count(idx.db_id()) || table_filter.count(idx.table_id()) ||
                         partition_filter.count(idx.partition_id());
            if (!match) continue;
        }
        ++num_tablets;

        int64_t tablet_id = idx.tablet_id();

        std::unique_ptr<Transaction> read_txn;
        if (txn_kv_->create_txn(&read_txn) != TxnErrorCode::TXN_OK) { ++errors; continue; }

        auto seed_list = [&](auto& rowset_metas) -> int {
            for (auto& [rs, vs] : rowset_metas) {
                if (rs.rowset_id_v2().empty()) continue;
                std::string ref_key = versioned::data_rowset_ref_count_key(
                        {instance_id, tablet_id, rs.rowset_id_v2()});
                // Join key encodes (instance_id, snapshot_vs, tablet_id, rowset_id).
                // Value = the ref_count key itself — used by unseed to know what to decrement.
                std::string join_key = versioned::snapshot_rowset_ref_key(
                        {instance_id, snapshot_vs, tablet_id, rs.rowset_id_v2()});
                // Idempotency: read_txn sees join keys from previous recycler cycles.
                // Each rowset appears exactly once in this loop, so no intra-call double-increment.
                std::string existing_val;
                TxnErrorCode check_err = read_txn->get(join_key, &existing_val);
                if (check_err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                    // Not previously seeded — add the ref count protection.
                    wb.txn->atomic_add(ref_key, 1);
                } else if (check_err != TxnErrorCode::TXN_OK) {
                    // Transient read error — conservatively skip to avoid double-increment.
                    // The outer loop increments errors and will cause a retry next cycle.
                    ++errors;
                    continue;
                }
                // check_err == TXN_OK: key already exists from a prior partial run — skip atomic_add.
                wb.txn->put(join_key, ref_key); // value = ref_count key (idempotent)
                // Store rowset metadata at seed time so export_table_meta can retrieve it
                // regardless of compaction or recycler state (agnostic to meta_rowset_key).
                std::string meta_key = versioned::snapshot_rowset_meta_key(
                        {instance_id, snapshot_vs, tablet_id, rs.rowset_id_v2()});
                std::string meta_val;
                if (rs.SerializeToString(&meta_val)) {
                    wb.txn->put(meta_key, meta_val);
                } else {
                    LOG_WARNING("seed_rowset_ref_counts: failed to serialize rowset meta"
                                " — rowset excluded from same-cluster restore")
                            .tag("instance_id", instance_id)
                            .tag("tablet_id", tablet_id)
                            .tag("rowset_id", rs.rowset_id_v2());
                }
                ++num_rowsets;
                if (wb.bump(config::snapshot_seed_batch_size) != 0) return -1;
            }
            return 0;
        };

        std::vector<std::pair<RowsetMetaCloudPB, Versionstamp>> load_rs, compact_rs;
        TxnErrorCode err = reader.get_load_rowset_metas(read_txn.get(), tablet_id, &load_rs);
        if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            ++errors;
            continue;
        }
        TxnErrorCode err2 = reader.get_compact_rowset_metas(read_txn.get(), tablet_id, &compact_rs);
        if (err2 != TxnErrorCode::TXN_OK && err2 != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            ++errors;
            continue;
        }

        if (seed_list(load_rs) != 0 || seed_list(compact_rs) != 0) { ++errors; break; }

        // V1 fallback: seed 0x01 rowsets when MetaReader (0x03) returns empty.
        if (load_rs.empty() && compact_rs.empty()) {
            std::string rs_begin = meta_rowset_key({instance_id, tablet_id, 0});
            std::string rs_end   = meta_rowset_key({instance_id, tablet_id + 1, 0});
            FullRangeGetOptions v1_opts(txn_kv_);
            v1_opts.prefetch = true;
            auto v1_it = txn_kv_->full_range_get(rs_begin, rs_end, std::move(v1_opts));
            std::vector<std::pair<RowsetMetaCloudPB, Versionstamp>> v1_rs;
            for (auto kv = v1_it->next(); kv.has_value(); kv = v1_it->next()) {
                auto [k, v] = *kv;
                RowsetMetaCloudPB rs_meta;
                if (rs_meta.ParseFromArray(v.data(), v.size())) {
                    v1_rs.emplace_back(std::move(rs_meta), Versionstamp{});
                }
            }
            if (!v1_it->is_valid()) {
                LOG_WARNING("seed_rowset_ref_counts: V1 rowset scan error")
                        .tag("instance_id", instance_id)
                        .tag("tablet_id", tablet_id);
                ++errors;
                continue;
            }
            if (seed_list(v1_rs) != 0) { ++errors; break; }
        }
    }

    if (wb.flush() != 0) ++errors;
    if (!it->is_valid()) { ++errors; }

    if (errors > 0) {
        LOG_WARNING("seed_rowset_ref_counts: completed with errors")
                .tag("instance_id", instance_id)
                .tag("tablets", num_tablets)
                .tag("rowsets", num_rowsets)
                .tag("errors", errors);
        return -1;
    }

    // Mark seeding complete in the SnapshotPB so Level 2 validation and
    // recycle logic know the join table is populated.
    bool mark_done = false;
    for (int attempt = 0; attempt < 3; ++attempt) {
        std::unique_ptr<Transaction> txn;
        if (txn_kv_->create_txn(&txn) != TxnErrorCode::TXN_OK) continue;
        MetaReader reader(instance_id);
        SnapshotPB snap;
        if (reader.get_snapshot(txn.get(), snapshot_vs, &snap) != TxnErrorCode::TXN_OK) continue;
        snap.set_rowset_refs_seeded(true);
        MetaServiceCode code = MetaServiceCode::OK;
        std::string msg;
        if (commit_snapshot_update(txn.get(), instance_id, snapshot_vs, std::move(snap), code,
                                   msg)) {
            mark_done = true;
            break;
        }
    }
    if (!mark_done) {
        LOG_WARNING("seed_rowset_ref_counts: failed to persist rowset_refs_seeded after 3 attempts"
                    " — will retry next cycle")
                .tag("instance_id", instance_id)
                .tag("snapshot_vs", serialize_snapshot_id(snapshot_vs));
        return -1;
    }

    LOG_INFO("seed_rowset_ref_counts: complete")
            .tag("instance_id", instance_id)
            .tag("tablets", num_tablets)
            .tag("rowsets", num_rowsets)
            .tag("is_full_cluster", is_full_cluster);
    return 0;
}

int SnapshotManager::unseed_rowset_ref_counts(std::string_view instance_id_sv,
                                               Versionstamp snapshot_vs) {
    const std::string instance_id(instance_id_sv);

    // Scan join table: each entry's VALUE = data_rowset_ref_count_key to decrement.
    std::string begin_key = versioned::snapshot_rowset_ref_key_prefix(instance_id, snapshot_vs);
    std::string end_key   = begin_key;
    end_key.push_back('\xff');

    FullRangeGetOptions opts(txn_kv_);
    opts.prefetch = true;
    auto it = txn_kv_->full_range_get(begin_key, end_key, std::move(opts));

    int64_t num_unseeded = 0, errors = 0;
    WriteBatch wb(txn_kv_.get());
    if (wb.init() != 0) return -1;

    for (auto kv = it->next(); kv.has_value(); kv = it->next()) {
        auto [k, v] = *kv;

        // Value holds the ref_count key to decrement.
        if (v.empty()) continue;
        std::string ref_key(v.data(), v.size());

        wb.txn->atomic_add(ref_key, -1);
        wb.txn->remove(std::string(k.data(), k.size())); // clean up join table entry

        // Also remove the snapshot rowset meta entry stored at seed time.
        int64_t unseed_tablet_id = 0;
        std::string unseed_rowset_id;
        std::string_view ref_key_view(ref_key);
        if (versioned::decode_data_rowset_ref_count_key(
                    &ref_key_view, &unseed_tablet_id, &unseed_rowset_id)) {
            wb.txn->remove(versioned::snapshot_rowset_meta_key(
                    {instance_id, snapshot_vs, unseed_tablet_id, unseed_rowset_id}));
        }

        ++num_unseeded;
        if (wb.bump(config::snapshot_seed_batch_size) != 0) { ++errors; break; }
    }

    if (wb.flush() != 0) ++errors;
    if (!it->is_valid()) ++errors;

    if (errors > 0) {
        LOG_WARNING("unseed_rowset_ref_counts: completed with errors")
                .tag("instance_id", instance_id)
                .tag("unseeded", num_unseeded)
                .tag("errors", errors);
        return -1;
    }

    LOG_INFO("unseed_rowset_ref_counts: complete")
            .tag("instance_id", instance_id)
            .tag("unseeded", num_unseeded);
    return 0;
}

} // namespace doris::cloud
