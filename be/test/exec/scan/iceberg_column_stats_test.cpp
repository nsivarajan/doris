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

#include <gtest/gtest.h>
#include <gen_cpp/PlanNodes_types.h>

namespace doris {

// Build a TIcebergColumnStats with big-endian encoded int64 bounds.
static TIcebergColumnStats make_int_stats(int64_t lo, int64_t hi, int32_t type_id = 7) {
    TIcebergColumnStats s;
    s.__set_iceberg_type_id(type_id);
    s.__set_row_count(1000);
    s.__set_null_count(0);

    auto encode = [](int64_t v) -> std::string {
        std::string b(8, '\0');
        for (int i = 7; i >= 0; --i) {
            b[i] = static_cast<char>(v & 0xFF);
            v >>= 8;
        }
        return b;
    };
    s.__set_lower_bound(encode(lo));
    s.__set_upper_bound(encode(hi));
    return s;
}

// ── decode_iceberg_column_stats ───────────────────────────────────────────────

TEST(IcebergColumnStatsTest, DecodeInt) {
    auto s = make_int_stats(100, 200, 5 /*INTEGER*/);
    IcebergFileColStats out;
    EXPECT_TRUE(decode_iceberg_column_stats(s, &out));
    EXPECT_TRUE(out.has_min_max);
    EXPECT_FALSE(out.all_nulls);
    EXPECT_EQ(out.min_val, 100);
    EXPECT_EQ(out.max_val, 200);
    EXPECT_EQ(out.type_id, 5);
}

TEST(IcebergColumnStatsTest, DecodeLong) {
    auto s = make_int_stats(1000000000LL, 9999999999LL, 7 /*LONG*/);
    IcebergFileColStats out;
    EXPECT_TRUE(decode_iceberg_column_stats(s, &out));
    EXPECT_EQ(out.min_val, 1000000000LL);
    EXPECT_EQ(out.max_val, 9999999999LL);
}

TEST(IcebergColumnStatsTest, DecodeDate) {
    // DATE is stored as days since epoch (int32, big-endian in 4 bytes)
    // Iceberg uses 4-byte big-endian for DATE
    int32_t day_min = 18000; // ~2019-04-14
    int32_t day_max = 18365; // ~2020-04-03
    TIcebergColumnStats s;
    s.__set_iceberg_type_id(18 /*DATE*/);
    s.__set_row_count(500);
    s.__set_null_count(0);
    auto encode4 = [](int32_t v) -> std::string {
        std::string b(4, '\0');
        for (int i = 3; i >= 0; --i) {
            b[i] = static_cast<char>(v & 0xFF);
            v >>= 8;
        }
        return b;
    };
    s.__set_lower_bound(encode4(day_min));
    s.__set_upper_bound(encode4(day_max));

    IcebergFileColStats out;
    EXPECT_TRUE(decode_iceberg_column_stats(s, &out));
    EXPECT_TRUE(out.has_min_max);
    EXPECT_EQ(out.min_val, day_min);
    EXPECT_EQ(out.max_val, day_max);
}

TEST(IcebergColumnStatsTest, AllNullFile) {
    TIcebergColumnStats s;
    s.__set_iceberg_type_id(7);
    s.__set_row_count(1000);
    s.__set_null_count(1000); // all nulls
    // No lower/upper bounds set

    IcebergFileColStats out;
    EXPECT_TRUE(decode_iceberg_column_stats(s, &out));
    EXPECT_TRUE(out.all_nulls);
    EXPECT_FALSE(out.has_min_max);
}

TEST(IcebergColumnStatsTest, MissingBounds) {
    TIcebergColumnStats s;
    s.__set_iceberg_type_id(7);
    // No lower_bound or upper_bound set

    IcebergFileColStats out;
    EXPECT_FALSE(decode_iceberg_column_stats(s, &out));
    EXPECT_FALSE(out.has_min_max);
}

TEST(IcebergColumnStatsTest, UnsupportedType) {
    TIcebergColumnStats s;
    s.__set_iceberg_type_id(99); // unknown type
    s.__set_lower_bound("xxxx");
    s.__set_upper_bound("yyyy");

    IcebergFileColStats out;
    EXPECT_FALSE(decode_iceberg_column_stats(s, &out));
}

TEST(IcebergColumnStatsTest, NegativeIntValues) {
    auto s = make_int_stats(-500, -100, 7);
    IcebergFileColStats out;
    EXPECT_TRUE(decode_iceberg_column_stats(s, &out));
    EXPECT_EQ(out.min_val, -500);
    EXPECT_EQ(out.max_val, -100);
}

// ── file_excluded_by_minmax ───────────────────────────────────────────────────

TEST(IcebergColumnStatsTest, ExcludedWhenRFAboveFileMax) {
    IcebergFileColStats fs;
    fs.has_min_max = true;
    fs.min_val = 100;
    fs.max_val = 200;
    // RF wants values in [300, 400] — no overlap with file [100, 200]
    EXPECT_TRUE(file_excluded_by_minmax(fs, 300, 400));
}

TEST(IcebergColumnStatsTest, ExcludedWhenRFBelowFileMin) {
    IcebergFileColStats fs;
    fs.has_min_max = true;
    fs.min_val = 500;
    fs.max_val = 600;
    // RF wants [100, 200] — no overlap with file [500, 600]
    EXPECT_TRUE(file_excluded_by_minmax(fs, 100, 200));
}

TEST(IcebergColumnStatsTest, NotExcludedWhenOverlap) {
    IcebergFileColStats fs;
    fs.has_min_max = true;
    fs.min_val = 100;
    fs.max_val = 300;
    // RF [200, 400] overlaps file [100, 300]
    EXPECT_FALSE(file_excluded_by_minmax(fs, 200, 400));
}

TEST(IcebergColumnStatsTest, NotExcludedWhenRFExactlyAtBoundary) {
    IcebergFileColStats fs;
    fs.has_min_max = true;
    fs.min_val = 100;
    fs.max_val = 200;
    EXPECT_FALSE(file_excluded_by_minmax(fs, 200, 300)); // touches max
    EXPECT_FALSE(file_excluded_by_minmax(fs, 50,  100)); // touches min
}

TEST(IcebergColumnStatsTest, NotExcludedWhenNoMinMax) {
    IcebergFileColStats fs;
    fs.has_min_max = false; // stats unavailable
    EXPECT_FALSE(file_excluded_by_minmax(fs, 100, 200));
}

} // namespace doris
