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

#include "exec/scan/file_split_stats_index.h"

#include <algorithm>

#include "core/types.h"
#include "core/value/vdatetime_value.h"
#include "core/field.h"
#include "exec/scan/iceberg_column_stats.h"
#include "storage/index/zone_map/zone_map_index.h"

namespace doris {

const ZoneMapEvalContext* FileSplitStatsIndex::get_or_build(
        const TFileRangeDesc& range, const std::vector<SlotDescriptor*>& slots) {
    if (!range.__isset.col_bounds || range.col_bounds.empty()) {
        return nullptr;
    }

    auto it = _cache.find(range.path);
    if (it != _cache.end()) {
        return it->second.slots.empty() ? nullptr : &it->second;
    }

    // Not yet cached — build and insert.
    ZoneMapEvalContext ctx = _build(range, slots);
    auto [ins_it, _inserted] = _cache.emplace(range.path, std::move(ctx));
    return ins_it->second.slots.empty() ? nullptr : &ins_it->second;
}

ZoneMapEvalContext FileSplitStatsIndex::_build(const TFileRangeDesc& range,
                                               const std::vector<SlotDescriptor*>& slots) {
    ZoneMapEvalContext ctx;

    // Map slot_id → manifest stats for columns present in this file's entry.
    for (const auto* slot : slots) {
        std::string lower_name = slot->col_name();
        std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);
        auto it = range.col_bounds.find(lower_name);
        if (it == range.col_bounds.end()) {
            continue;
        }

        const TFileSplitColBounds& thrift_stats = it->second;
        IcebergFileColStats decoded;
        if (!decode_iceberg_column_stats(thrift_stats, &decoded)) {
            continue;
        }

        auto zone_map = std::make_shared<segment_v2::ZoneMap>();
        zone_map->has_null     = decoded.all_nulls;
        zone_map->has_not_null = !decoded.all_nulls;

        if (decoded.all_nulls) {
            ctx.slots[slot->id()] = {nullptr, zone_map};
            continue;
        }
        if (!decoded.has_min_max) {
            continue;
        }

        const int type_id = decoded.type_id;
        if (type_id == 3) {
            zone_map->min_value = Field::create_field<TYPE_BOOLEAN>(
                    static_cast<uint8_t>(decoded.min_val));
            zone_map->max_value = Field::create_field<TYPE_BOOLEAN>(
                    static_cast<uint8_t>(decoded.max_val));
        } else if (type_id == 5) {
            zone_map->min_value = Field::create_field<TYPE_INT>(static_cast<int32_t>(decoded.min_val));
            zone_map->max_value = Field::create_field<TYPE_INT>(static_cast<int32_t>(decoded.max_val));
        } else if (type_id == 7) {
            zone_map->min_value = Field::create_field<TYPE_BIGINT>(decoded.min_val);
            zone_map->max_value = Field::create_field<TYPE_BIGINT>(decoded.max_val);
        } else if (type_id == 18) {
            // DATE — days since Unix epoch → DateV2
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
            // TIMESTAMP — microseconds since Unix epoch → DateTimeV2
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
            zone_map->min_value = Field::create_field<TYPE_STRING>(decoded.min_str);
            zone_map->max_value = Field::create_field<TYPE_STRING>(decoded.max_str);
        } else if (type_id == 11) {
            // DECIMAL: unscaled __int128; all values share the same schema scale.
            zone_map->min_value = Field::create_field<TYPE_DECIMAL128I>(
                    Decimal128V3(decoded.min_decimal));
            zone_map->max_value = Field::create_field<TYPE_DECIMAL128I>(
                    Decimal128V3(decoded.max_decimal));
        } else if (type_id == 20) {
            // TIMESTAMP_NANO: already truncated to micros, same path as TIMESTAMP.
            static const uint64_t EPOCH_DAYNR_NANO = calc_daynr(1970, 1, 1);
            static constexpr int64_t MICROS_PER_DAY_NANO    = 86400LL * 1000000LL;
            static constexpr int64_t MICROS_PER_HOUR_NANO   = 3600LL  * 1000000LL;
            static constexpr int64_t MICROS_PER_MINUTE_NANO = 60LL    * 1000000LL;
            static constexpr int64_t MICROS_PER_SECOND_NANO = 1000000LL;
            auto to_datetimev2_nano = [&](int64_t micros) -> std::optional<uint64_t> {
                int64_t days = micros / MICROS_PER_DAY_NANO;
                int64_t rem  = micros % MICROS_PER_DAY_NANO;
                if (rem < 0) { rem += MICROS_PER_DAY_NANO; --days; }
                int64_t daynr = static_cast<int64_t>(EPOCH_DAYNR_NANO) + days;
                if (daynr <= 0) return std::nullopt;
                DateV2Value<DateTimeV2ValueType> dt;
                if (!dt.get_date_from_daynr(static_cast<uint64_t>(daynr))) return std::nullopt;
                dt.unchecked_set_time(dt.year(), dt.month(), dt.day(),
                    static_cast<uint8_t>(rem / MICROS_PER_HOUR_NANO),
                    static_cast<uint8_t>((rem % MICROS_PER_HOUR_NANO) / MICROS_PER_MINUTE_NANO),
                    static_cast<uint16_t>((rem % MICROS_PER_MINUTE_NANO) / MICROS_PER_SECOND_NANO),
                    static_cast<uint32_t>(rem % MICROS_PER_SECOND_NANO));
                return dt.to_date_int_val();
            };
            auto min_opt = to_datetimev2_nano(decoded.min_val);
            auto max_opt = to_datetimev2_nano(decoded.max_val);
            if (!min_opt || !max_opt) continue;
            zone_map->min_value = Field::create_field<TYPE_DATETIMEV2>(*min_opt);
            zone_map->max_value = Field::create_field<TYPE_DATETIMEV2>(*max_opt);
        } else if (type_id == 21 || type_id == 22) {
            // UUID/FIXED: raw bytes; reuses the STRING ZoneMap path.
            zone_map->min_value = Field::create_field<TYPE_STRING>(decoded.min_str);
            zone_map->max_value = Field::create_field<TYPE_STRING>(decoded.max_str);
        } else {
            continue;
        }

        ctx.slots[slot->id()] = {slot->get_data_type_ptr(), zone_map};
    }

    return ctx;
}

} // namespace doris
