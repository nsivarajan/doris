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

#include "exec/scan/iceberg_column_stats.h"

#include <algorithm>
#include <cstring>
#include <set>

#include "exec/scan/file_split_stats_index.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "runtime/descriptors.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/index/zone_map/zonemap_filter_result.h"

namespace doris {

namespace {

// Iceberg spec (Appendix D) mandates little-endian encoding for all numeric bounds.
// Java Conversions.toByteBuffer() uses ByteOrder.LITTLE_ENDIAN for INTEGER/LONG/FLOAT/DOUBLE.

int64_t decode_little_endian_int(const std::string& bytes) {
    int64_t val = 0;
    for (int i = static_cast<int>(bytes.size()) - 1; i >= 0; --i) {
        val = (val << 8) | static_cast<int64_t>(static_cast<unsigned char>(bytes[i]));
    }
    // Sign-extend based on the actual byte width.
    int shift = 64 - static_cast<int>(bytes.size() * 8);
    if (shift > 0 && shift < 64) {
        val = (val << shift) >> shift;
    }
    return val;
}

float decode_little_endian_float(const std::string& bytes) {
    if (bytes.size() < 4) return 0.0f;
    uint32_t raw = 0;
    for (int i = 3; i >= 0; --i) {
        raw = (raw << 8) | static_cast<uint32_t>(static_cast<unsigned char>(bytes[i]));
    }
    float val;
    std::memcpy(&val, &raw, sizeof(val));
    return val;
}

double decode_little_endian_double(const std::string& bytes) {
    if (bytes.size() < 8) return 0.0;
    uint64_t raw = 0;
    for (int i = 7; i >= 0; --i) {
        raw = (raw << 8) | static_cast<uint64_t>(static_cast<unsigned char>(bytes[i]));
    }
    double val;
    std::memcpy(&val, &raw, sizeof(val));
    return val;
}

} // anonymous namespace

bool decode_iceberg_column_stats(const TFileSplitColBounds& s, IcebergFileColStats* out) {
    out->type_id = s.iceberg_type_id;

    // All-null file: skip regardless of the filter value.
    if (s.__isset.null_count && s.__isset.row_count && s.null_count >= s.row_count) {
        out->all_nulls = true;
        return true;
    }

    if (!s.__isset.lower_bound || !s.__isset.upper_bound) {
        return false; // stats unavailable — cannot prune
    }

    const int type_id = s.iceberg_type_id;

    if (type_id == 5 || type_id == 7 || type_id == 18 || type_id == 19) {
        // INTEGER(5), LONG(7), DATE(18 = days since epoch), TIMESTAMP(19 = micros since epoch)
        // All stored as little-endian signed integers per Iceberg spec Appendix D.
        out->min_val = decode_little_endian_int(s.lower_bound);
        out->max_val = decode_little_endian_int(s.upper_bound);
        out->has_min_max = true;
    } else if (type_id == 10) {
        // STRING(10): raw UTF-8 bytes, no length prefix, no byte order.
        // Iceberg Conversions.toByteBuffer uses CharsetEncoder(UTF_8).encode(charBuffer).
        // Lexicographic byte comparison equals code-point ordering for well-formed UTF-8.
        out->min_str = s.lower_bound;
        out->max_str = s.upper_bound;
        out->has_min_max = true;
    } else if (type_id == 8) {
        // FLOAT(8) — IEEE 754 little-endian 32-bit
        out->min_flt = decode_little_endian_float(s.lower_bound);
        out->max_flt = decode_little_endian_float(s.upper_bound);
        out->has_min_max = true;
    } else if (type_id == 9) {
        // DOUBLE(9) — IEEE 754 little-endian 64-bit
        out->min_dbl = decode_little_endian_double(s.lower_bound);
        out->max_dbl = decode_little_endian_double(s.upper_bound);
        out->has_min_max = true;
    } else {
        return false; // unsupported type — caller should not prune
    }

    return true;
}

bool file_excluded_by_minmax(const IcebergFileColStats& fs, int64_t rf_min, int64_t rf_max) {
    if (!fs.has_min_max) {
        return false;
    }
    // No overlap between [file_min, file_max] and [rf_min, rf_max].
    return fs.max_val < rf_min || fs.min_val > rf_max;
}

bool iceberg_file_excluded_by_rf(const TFileRangeDesc& range,
                                  const VExprContextSPtrs& conjuncts,
                                  const std::vector<SlotDescriptor*>& slots,
                                  FileSplitStatsIndex* index) {
    if (!range.__isset.col_bounds || range.col_bounds.empty()) {
        return false;
    }

    // Fast path: use the per-scan-node cache to avoid rebuilding ZoneMapEvalContext
    // on every call (once per file × slot count).
    if (index != nullptr) {
        const ZoneMapEvalContext* ctx = index->get_or_build(range, slots);
        if (ctx == nullptr) {
            return false;
        }
        for (const auto& conjunct : conjuncts) {
            const auto* root = conjunct->root().get();
            if (!root->is_rf_wrapper() || !root->can_evaluate_zonemap_filter()) {
                continue;
            }
            std::set<int> column_ids;
            root->collect_slot_column_ids(column_ids);
            bool have_stats = std::any_of(column_ids.begin(), column_ids.end(),
                                           [ctx](int id) { return ctx->slots.count(id) > 0; });
            if (!have_stats) {
                continue;
            }
            if (root->evaluate_zonemap_filter(*ctx) == ZoneMapFilterResult::kNoMatch) {
                return true;
            }
        }
        return false;
    }

    // Inline path (no external cache): use a temporary FileSplitStatsIndex so the
    // ZoneMap-build logic lives in one place (FileSplitStatsIndex::_build) rather
    // than being duplicated here. The temporary index is discarded after this call.
    FileSplitStatsIndex tmp_index;
    return iceberg_file_excluded_by_rf(range, conjuncts, slots, &tmp_index);
}

} // namespace doris
