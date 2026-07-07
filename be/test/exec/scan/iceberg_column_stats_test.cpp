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

// Iceberg spec (Appendix D) uses little-endian for all numeric bounds.
// Encode int64 value as 8-byte LE (matches Java Conversions.toByteBuffer LONG/TIMESTAMP).
static TFileSplitColBounds make_int_stats(int64_t lo, int64_t hi, int32_t type_id = 7) {
    TFileSplitColBounds s;
    s.__set_iceberg_type_id(type_id);
    s.__set_row_count(1000);
    s.__set_null_count(0);

    // little-endian: LSB at index 0
    auto encode8 = [](int64_t v) -> std::string {
        std::string b(8, '\0');
        for (int i = 0; i < 8; ++i) {
            b[i] = static_cast<char>(v & 0xFF);
            v >>= 8;
        }
        return b;
    };
    s.__set_lower_bound(encode8(lo));
    s.__set_upper_bound(encode8(hi));
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
    // DATE = days since epoch, 4-byte little-endian (Iceberg INTEGER encoding).
    int32_t day_min = 18000; // ~2019-04-14
    int32_t day_max = 18365; // ~2020-04-03
    TFileSplitColBounds s;
    s.__set_iceberg_type_id(18 /*DATE*/);
    s.__set_row_count(500);
    s.__set_null_count(0);
    // little-endian 4-byte encode
    auto encode4 = [](int32_t v) -> std::string {
        std::string b(4, '\0');
        for (int i = 0; i < 4; ++i) {
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
    TFileSplitColBounds s;
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
    TFileSplitColBounds s;
    s.__set_iceberg_type_id(7);
    // No lower_bound or upper_bound set

    IcebergFileColStats out;
    EXPECT_FALSE(decode_iceberg_column_stats(s, &out));
    EXPECT_FALSE(out.has_min_max);
}

TEST(IcebergColumnStatsTest, UnsupportedType) {
    TFileSplitColBounds s;
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
    EXPECT_TRUE(file_excluded_by_minmax(fs, 300, 400));
}

TEST(IcebergColumnStatsTest, ExcludedWhenRFBelowFileMin) {
    IcebergFileColStats fs;
    fs.has_min_max = true;
    fs.min_val = 500;
    fs.max_val = 600;
    EXPECT_TRUE(file_excluded_by_minmax(fs, 100, 200));
}

TEST(IcebergColumnStatsTest, NotExcludedWhenOverlap) {
    IcebergFileColStats fs;
    fs.has_min_max = true;
    fs.min_val = 100;
    fs.max_val = 300;
    EXPECT_FALSE(file_excluded_by_minmax(fs, 200, 400));
}

TEST(IcebergColumnStatsTest, NotExcludedWhenRFExactlyAtBoundary) {
    IcebergFileColStats fs;
    fs.has_min_max = true;
    fs.min_val = 100;
    fs.max_val = 200;
    EXPECT_FALSE(file_excluded_by_minmax(fs, 200, 300)); // touches max
    EXPECT_FALSE(file_excluded_by_minmax(fs, 50, 100));  // touches min
}

TEST(IcebergColumnStatsTest, NotExcludedWhenNoMinMax) {
    IcebergFileColStats fs;
    fs.has_min_max = false; // stats unavailable
    EXPECT_FALSE(file_excluded_by_minmax(fs, 100, 200));
}

TEST(IcebergColumnStatsTest, DecodeString) {
    // STRING: raw UTF-8 bytes, no length prefix, no byte order.
    // Iceberg Conversions.toByteBuffer uses UTF_8 CharsetEncoder — byte-for-byte UTF-8.
    TFileSplitColBounds s;
    s.__set_iceberg_type_id(10 /*STRING*/);
    s.__set_row_count(200);
    s.__set_null_count(0);
    s.__set_lower_bound("apple");
    s.__set_upper_bound("mango");

    IcebergFileColStats out;
    EXPECT_TRUE(decode_iceberg_column_stats(s, &out));
    EXPECT_TRUE(out.has_min_max);
    EXPECT_EQ(out.min_str, "apple");
    EXPECT_EQ(out.max_str, "mango");
    EXPECT_EQ(out.type_id, 10);
}

TEST(IcebergColumnStatsTest, DecodeStringWithNonAsciiUtf8) {
    // Multi-byte UTF-8 code points: lexicographic byte comparison equals code-point order.
    TFileSplitColBounds s;
    s.__set_iceberg_type_id(10);
    s.__set_row_count(50);
    s.__set_null_count(0);
    // "café" in UTF-8: bytes 63 61 66 c3 a9 (c3 a9 = U+00E9 é)
    s.__set_lower_bound("cafe");
    s.__set_upper_bound("caf\xc3\xa9"); // café
    IcebergFileColStats out;
    EXPECT_TRUE(decode_iceberg_column_stats(s, &out));
    EXPECT_EQ(out.min_str, "cafe");
    EXPECT_EQ(out.max_str, "caf\xc3\xa9");
}

// ── iceberg_file_excluded_by_rf guard paths ───────────────────────────────────
// Full RF evaluation requires a running RuntimeState; these tests cover the
// guard paths that return early without touching the ZoneMapEvalContext.

TEST(IcebergFileExcludedByRFTest, ReturnsFalseWhenStatsNotSet) {
    TFileRangeDesc range;
    // col_bounds not set at all
    VExprContextSPtrs conjuncts;
    std::vector<SlotDescriptor*> slots;
    EXPECT_FALSE(iceberg_file_excluded_by_rf(range, conjuncts, slots));
}

TEST(IcebergFileExcludedByRFTest, ReturnsFalseWhenStatsEmpty) {
    TFileRangeDesc range;
    range.__isset.col_bounds = true;
    // map is empty
    VExprContextSPtrs conjuncts;
    std::vector<SlotDescriptor*> slots;
    EXPECT_FALSE(iceberg_file_excluded_by_rf(range, conjuncts, slots));
}

TEST(IcebergFileExcludedByRFTest, ReturnsFalseWhenNoSlotsMatchStats) {
    TFileRangeDesc range;
    TFileSplitColBounds col_stats;
    col_stats.__set_iceberg_type_id(7);
    col_stats.__set_row_count(1000);
    col_stats.__set_null_count(0);
    range.col_bounds["date_key"] = col_stats;
    range.__isset.col_bounds = true;

    VExprContextSPtrs conjuncts;
    // Empty slots — nothing maps "date_key" to a slot_id
    std::vector<SlotDescriptor*> slots;
    EXPECT_FALSE(iceberg_file_excluded_by_rf(range, conjuncts, slots));
}

TEST(IcebergFileExcludedByRFTest, ReturnsFalseWhenNoConjuncts) {
    TFileRangeDesc range;
    TFileSplitColBounds col_stats = make_int_stats(100, 200, 7);
    range.col_bounds["val"] = col_stats;
    range.__isset.col_bounds = true;

    // Slots present but zero conjuncts — nothing to evaluate
    VExprContextSPtrs empty_conjuncts;
    std::vector<SlotDescriptor*> slots;
    EXPECT_FALSE(iceberg_file_excluded_by_rf(range, empty_conjuncts, slots));
}

} // namespace doris


#include "exec/scan/iceberg_column_stats.h"

#include <gtest/gtest.h>
#include <gen_cpp/PlanNodes_types.h>

namespace doris {

// Iceberg spec (Appendix D) uses little-endian for all numeric bounds.
// Encode int64 value as 8-byte LE (matches Java Conversions.toByteBuffer LONG/TIMESTAMP).
static TFileSplitColBounds make_int_stats(int64_t lo, int64_t hi, int32_t type_id = 7) {
    TFileSplitColBounds s;
    s.__set_iceberg_type_id(type_id);
    s.__set_row_count(1000);
    s.__set_null_count(0);

    // little-endian: LSB at index 0
    auto encode8 = [](int64_t v) -> std::string {
        std::string b(8, '\0');
        for (int i = 0; i < 8; ++i) {
            b[i] = static_cast<char>(v & 0xFF);
            v >>= 8;
        }
        return b;
    };
    s.__set_lower_bound(encode8(lo));
    s.__set_upper_bound(encode8(hi));
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
    // DATE = days since epoch, 4-byte little-endian (Iceberg INTEGER encoding).
    int32_t day_min = 18000; // ~2019-04-14
    int32_t day_max = 18365; // ~2020-04-03
    TFileSplitColBounds s;
    s.__set_iceberg_type_id(18 /*DATE*/);
    s.__set_row_count(500);
    s.__set_null_count(0);
    // little-endian 4-byte encode
    auto encode4 = [](int32_t v) -> std::string {
        std::string b(4, '\0');
        for (int i = 0; i < 4; ++i) {
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
    TFileSplitColBounds s;
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
    TFileSplitColBounds s;
    s.__set_iceberg_type_id(7);
    // No lower_bound or upper_bound set

    IcebergFileColStats out;
    EXPECT_FALSE(decode_iceberg_column_stats(s, &out));
    EXPECT_FALSE(out.has_min_max);
}

TEST(IcebergColumnStatsTest, UnsupportedType) {
    TFileSplitColBounds s;
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
    EXPECT_TRUE(file_excluded_by_minmax(fs, 300, 400));
}

TEST(IcebergColumnStatsTest, ExcludedWhenRFBelowFileMin) {
    IcebergFileColStats fs;
    fs.has_min_max = true;
    fs.min_val = 500;
    fs.max_val = 600;
    EXPECT_TRUE(file_excluded_by_minmax(fs, 100, 200));
}

TEST(IcebergColumnStatsTest, NotExcludedWhenOverlap) {
    IcebergFileColStats fs;
    fs.has_min_max = true;
    fs.min_val = 100;
    fs.max_val = 300;
    EXPECT_FALSE(file_excluded_by_minmax(fs, 200, 400));
}

TEST(IcebergColumnStatsTest, NotExcludedWhenRFExactlyAtBoundary) {
    IcebergFileColStats fs;
    fs.has_min_max = true;
    fs.min_val = 100;
    fs.max_val = 200;
    EXPECT_FALSE(file_excluded_by_minmax(fs, 200, 300)); // touches max
    EXPECT_FALSE(file_excluded_by_minmax(fs, 50, 100));  // touches min
}

TEST(IcebergColumnStatsTest, NotExcludedWhenNoMinMax) {
    IcebergFileColStats fs;
    fs.has_min_max = false; // stats unavailable
    EXPECT_FALSE(file_excluded_by_minmax(fs, 100, 200));
}

} // namespace doris
