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

#include "core/value/vdatetime_value.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "runtime/descriptors.h"
#include "storage/index/zone_map/zone_map_index.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/index/zone_map/zonemap_filter_result.h"
#include "core/field.h"

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

bool decode_iceberg_column_stats(const TIcebergFileColumnStats& s, IcebergFileColStats* out) {
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
                                  const std::vector<SlotDescriptor*>& slots) {
    if (!range.__isset.iceberg_column_stats || range.iceberg_column_stats.empty()) {
        return false;
    }

    // Map slot_id → manifest stats for columns present in this file's entry.
    std::unordered_map<int, const TIcebergFileColumnStats*> slot_id_to_stats;
    for (const auto* slot : slots) {
        std::string lower_name = slot->col_name();
        std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);
        auto it = range.iceberg_column_stats.find(lower_name);
        if (it != range.iceberg_column_stats.end()) {
            slot_id_to_stats[slot->id()] = &it->second;
        }
    }
    if (slot_id_to_stats.empty()) {
        return false;
    }

    // Build ZoneMapEvalContext from decoded Iceberg manifest column stats.
    ZoneMapEvalContext ctx;
    for (const auto& [slot_id, stats_ptr] : slot_id_to_stats) {
        IcebergFileColStats decoded;
        if (!decode_iceberg_column_stats(*stats_ptr, &decoded)) {
            continue;
        }

        auto zone_map = std::make_shared<segment_v2::ZoneMap>();
        zone_map->has_null     = decoded.all_nulls;
        zone_map->has_not_null = !decoded.all_nulls;

        if (decoded.all_nulls) {
            ctx.slots[slot_id] = {nullptr, zone_map};
            continue;
        }
        if (!decoded.has_min_max) {
            continue;
        }

        const int type_id = decoded.type_id;
        if (type_id == 5) {
            zone_map->min_value = Field::create_field<TYPE_INT>(static_cast<int32_t>(decoded.min_val));
            zone_map->max_value = Field::create_field<TYPE_INT>(static_cast<int32_t>(decoded.max_val));
        } else if (type_id == 7) {
            zone_map->min_value = Field::create_field<TYPE_BIGINT>(decoded.min_val);
            zone_map->max_value = Field::create_field<TYPE_BIGINT>(decoded.max_val);
        } else if (type_id == 18) {
            static const uint64_t EPOCH_DAYNR = calc_daynr(1970, 1, 1);
            auto to_datev2 = [&](int64_t days) -> std::optional<uint32_t> {
                int64_t daynr = static_cast<int64_t>(EPOCH_DAYNR) + days;
                if (daynr <= 0) return std::nullopt;
                DateV2Value<DateV2ValueType> d;
                if (!d.get_date_from_daynr(static_cast<uint64_t>(daynr))) return std::nullopt;
                return d.to_date_int_val();
            };
            auto min_opt = to_datev2(decoded.min_val);
            auto max_opt = to_datev2(decoded.max_val);
            if (!min_opt || !max_opt) continue;
            zone_map->min_value = Field::create_field<TYPE_DATEV2>(*min_opt);
            zone_map->max_value = Field::create_field<TYPE_DATEV2>(*max_opt);
        } else if (type_id == 19) {
            static const uint64_t EPOCH_DAYNR = calc_daynr(1970, 1, 1);
            static constexpr int64_t MICROS_PER_DAY    = 86400LL * 1000000LL;
            static constexpr int64_t MICROS_PER_HOUR   = 3600LL  * 1000000LL;
            static constexpr int64_t MICROS_PER_MINUTE = 60LL    * 1000000LL;
            static constexpr int64_t MICROS_PER_SECOND = 1000000LL;
            auto to_datetimev2 = [&](int64_t micros) -> std::optional<uint64_t> {
                int64_t days = micros / MICROS_PER_DAY;
                int64_t rem  = micros % MICROS_PER_DAY;
                if (rem < 0) { rem += MICROS_PER_DAY; --days; }
                int64_t daynr = static_cast<int64_t>(EPOCH_DAYNR) + days;
                if (daynr <= 0) return std::nullopt;
                DateV2Value<DateTimeV2ValueType> dt;
                if (!dt.get_date_from_daynr(static_cast<uint64_t>(daynr))) return std::nullopt;
                dt.unchecked_set_time(dt.year(), dt.month(), dt.day(),
                    static_cast<uint8_t>(rem / MICROS_PER_HOUR),
                    static_cast<uint8_t>((rem % MICROS_PER_HOUR) / MICROS_PER_MINUTE),
                    static_cast<uint16_t>((rem % MICROS_PER_MINUTE) / MICROS_PER_SECOND),
                    static_cast<uint32_t>(rem % MICROS_PER_SECOND));
                return dt.to_date_int_val();
            };
            auto min_opt = to_datetimev2(decoded.min_val);
            auto max_opt = to_datetimev2(decoded.max_val);
            if (!min_opt || !max_opt) continue;
            zone_map->min_value = Field::create_field<TYPE_DATETIMEV2>(*min_opt);
            zone_map->max_value = Field::create_field<TYPE_DATETIMEV2>(*max_opt);
        } else if (type_id == 8) {
            zone_map->min_value = Field::create_field<TYPE_FLOAT>(decoded.min_flt);
            zone_map->max_value = Field::create_field<TYPE_FLOAT>(decoded.max_flt);
        } else if (type_id == 9) {
            zone_map->min_value = Field::create_field<TYPE_DOUBLE>(decoded.min_dbl);
            zone_map->max_value = Field::create_field<TYPE_DOUBLE>(decoded.max_dbl);
        } else if (type_id == 10) {
            // STRING: raw UTF-8 bytes — lexicographic byte comparison equals code-point order.
            zone_map->min_value = Field::create_field<TYPE_STRING>(decoded.min_str);
            zone_map->max_value = Field::create_field<TYPE_STRING>(decoded.max_str);
        } else {
            continue;
        }

        for (const auto* slot : slots) {
            if (slot->id() == slot_id) {
                ctx.slots[slot_id] = {slot->get_data_type_ptr(), zone_map};
                break;
            }
        }
    }

    if (ctx.slots.empty()) {
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
                                       [&ctx](int id) { return ctx.slots.count(id) > 0; });
        if (!have_stats) {
            continue;
        }
        if (root->evaluate_zonemap_filter(ctx) == ZoneMapFilterResult::kNoMatch) {
            return true;
        }
    }
    return false;
}

} // namespace doris
