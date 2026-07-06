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

#include <cstring>

namespace doris {

namespace {

// Decode big-endian two's complement bytes → int64_t (Iceberg INTEGER/LONG/DATE/TIMESTAMP).
int64_t decode_big_endian_int(const std::string& bytes) {
    int64_t val = 0;
    for (unsigned char b : bytes) {
        val = (val << 8) | static_cast<int64_t>(b);
    }
    // Sign-extend based on the actual byte width.
    int shift = 64 - static_cast<int>(bytes.size() * 8);
    if (shift > 0 && shift < 64) {
        val = (val << shift) >> shift;
    }
    return val;
}

// Decode IEEE 754 big-endian bytes → float (Iceberg FLOAT).
float decode_big_endian_float(const std::string& bytes) {
    if (bytes.size() < 4) return 0.0f;
    uint32_t raw = 0;
    for (int i = 0; i < 4; ++i) {
        raw = (raw << 8) | static_cast<uint32_t>(static_cast<unsigned char>(bytes[i]));
    }
    float val;
    std::memcpy(&val, &raw, sizeof(val));
    return val;
}

// Decode IEEE 754 big-endian bytes → double (Iceberg DOUBLE).
double decode_big_endian_double(const std::string& bytes) {
    if (bytes.size() < 8) return 0.0;
    uint64_t raw = 0;
    for (int i = 0; i < 8; ++i) {
        raw = (raw << 8) | static_cast<uint64_t>(static_cast<unsigned char>(bytes[i]));
    }
    double val;
    std::memcpy(&val, &raw, sizeof(val));
    return val;
}

} // anonymous namespace

bool decode_iceberg_column_stats(const TIcebergColumnStats& s, IcebergFileColStats* out) {
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
        // INTEGER(5), LONG(7), DATE(18), TIMESTAMP(19) — big-endian signed int
        out->min_val = decode_big_endian_int(s.lower_bound);
        out->max_val = decode_big_endian_int(s.upper_bound);
        out->has_min_max = true;
    } else if (type_id == 8) {
        // FLOAT(8) — IEEE 754 big-endian 32-bit
        out->min_flt = decode_big_endian_float(s.lower_bound);
        out->max_flt = decode_big_endian_float(s.upper_bound);
        out->has_min_max = true;
    } else if (type_id == 9) {
        // DOUBLE(9) — IEEE 754 big-endian 64-bit
        out->min_dbl = decode_big_endian_double(s.lower_bound);
        out->max_dbl = decode_big_endian_double(s.upper_bound);
        out->has_min_max = true;
    } else {
        return false; // unsupported type — caller should not prune
    }

    return true;
}

bool file_excluded_by_minmax(const IcebergFileColStats& fs,
                              int64_t rf_min, int64_t rf_max) {
    if (!fs.has_min_max) {
        return false;
    }
    // No overlap between [file_min, file_max] and [rf_min, rf_max].
    return fs.max_val < rf_min || fs.min_val > rf_max;
}

} // namespace doris
