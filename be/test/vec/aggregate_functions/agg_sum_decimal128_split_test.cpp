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

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>

#include "vec/aggregate_functions/aggregate_function_sum.h"

namespace doris::vectorized {

// ---------------------------------------------------------------------------
// Convenience alias so tests are readable.
// ---------------------------------------------------------------------------
using SplitAcc = AggregateFunctionSumDataDecimal128Split;

class Decimal128SplitTest : public testing::Test {};

// ---------------------------------------------------------------------------
// 1. DefaultInit
//    A default-constructed accumulator must have sum_high=0, sum_low=0, and
//    get()==0.
// ---------------------------------------------------------------------------
TEST_F(Decimal128SplitTest, DefaultInit) {
    SplitAcc acc;
    EXPECT_EQ(acc.sum_high, 0);
    EXPECT_EQ(acc.sum_low, static_cast<uint64_t>(0));
    EXPECT_EQ(acc.get(), static_cast<int128_t>(0));
}

// ---------------------------------------------------------------------------
// 2. AddPositiveValues
//    Several positive int64 values — result must match a naive __int128 sum.
// ---------------------------------------------------------------------------
TEST_F(Decimal128SplitTest, AddPositiveValues) {
    SplitAcc acc;
    int128_t ref = 0;

    const int64_t values[] = {1LL, 100LL, 999999999LL, 1000000000LL, 42LL};
    for (int64_t v : values) {
        acc.add(static_cast<int128_t>(v));
        ref += v;
    }

    EXPECT_EQ(acc.get(), ref);
}

// ---------------------------------------------------------------------------
// 3. AddNegativeValues
//    Several negative int64 values — result must match a naive __int128 sum.
// ---------------------------------------------------------------------------
TEST_F(Decimal128SplitTest, AddNegativeValues) {
    SplitAcc acc;
    int128_t ref = 0;

    const int64_t values[] = {-1LL, -100LL, -999999999LL, -1000000000LL, -42LL};
    for (int64_t v : values) {
        acc.add(static_cast<int128_t>(v));
        ref += v;
    }

    EXPECT_EQ(acc.get(), ref);
}

// ---------------------------------------------------------------------------
// 4. AddMixedValues
//    Mix of positive and negative values — result must match a naive sum.
// ---------------------------------------------------------------------------
TEST_F(Decimal128SplitTest, AddMixedValues) {
    SplitAcc acc;
    int128_t ref = 0;

    const int64_t values[] = {500LL,          -300LL,       123456789LL,
                               -987654321LL,   1LL,          -1LL,
                               999999999999LL, -999999999LL, 0LL};
    for (int64_t v : values) {
        acc.add(static_cast<int128_t>(v));
        ref += v;
    }

    EXPECT_EQ(acc.get(), ref);
}

// ---------------------------------------------------------------------------
// 5. AddAtInt64Boundary
//    Add INT64_MAX once → get()==INT64_MAX.
//    Add INT64_MAX again → get()==2*INT64_MAX.
// ---------------------------------------------------------------------------
TEST_F(Decimal128SplitTest, AddAtInt64Boundary) {
    SplitAcc acc;
    constexpr int64_t MAX64 = std::numeric_limits<int64_t>::max(); // 9223372036854775807

    acc.add(static_cast<int128_t>(MAX64));
    EXPECT_EQ(acc.get(), static_cast<int128_t>(MAX64));

    acc.add(static_cast<int128_t>(MAX64));
    EXPECT_EQ(acc.get(), static_cast<int128_t>(MAX64) * 2);
}

// ---------------------------------------------------------------------------
// 6. OverflowCarry
//    Force sum_low to wrap past 2^64, verifying carry increments sum_high.
//
//    Steps:
//      1) add(INT64_MAX)  -> sum_low = 0x7FFFFFFFFFFFFFFF, sum_high = 0
//      2) add(INT64_MAX)  -> sum_low = 0xFFFFFFFFFFFFFFFE, sum_high = 0  (no wrap yet)
//      3) add(2)          -> sum_low wraps to 0, carry fires -> sum_high = 1
//
//    All three values fit in int64_t.  Total = 2*INT64_MAX + 2.
// ---------------------------------------------------------------------------
TEST_F(Decimal128SplitTest, OverflowCarry) {
    SplitAcc acc;
    int128_t ref = 0;

    constexpr int64_t HALF = std::numeric_limits<int64_t>::max(); // INT64_MAX

    // Two additions that nearly fill sum_low — no carry yet.
    acc.add(static_cast<int128_t>(HALF));
    ref += HALF;
    EXPECT_EQ(acc.sum_high, 0) << "no carry after first add";

    acc.add(static_cast<int128_t>(HALF));
    ref += HALF;
    EXPECT_EQ(acc.sum_high, 0) << "no carry after second add";

    // This addition wraps sum_low past 2^64, triggering carry.
    acc.add(static_cast<int128_t>(2));
    ref += 2;
    EXPECT_EQ(acc.sum_high, 1) << "carry must have incremented sum_high to 1";
    EXPECT_EQ(acc.get(), ref);
}

// ---------------------------------------------------------------------------
// 7. NegativeTotal
//    A sum that ends negative — get() must return the correct negative int128.
// ---------------------------------------------------------------------------
TEST_F(Decimal128SplitTest, NegativeTotal) {
    SplitAcc acc;
    int128_t ref = 0;

    const int64_t values[] = {-1000000000LL, -2000000000LL, 500000000LL};
    for (int64_t v : values) {
        acc.add(static_cast<int128_t>(v));
        ref += v;
    }

    EXPECT_LT(acc.get(), static_cast<int128_t>(0)) << "total must be negative";
    EXPECT_EQ(acc.get(), ref);
}

// ---------------------------------------------------------------------------
// 8. MergeTwo
//    Two independent accumulators with different values; after merge the
//    result must equal the naive sum of all values from both.
// ---------------------------------------------------------------------------
TEST_F(Decimal128SplitTest, MergeTwo) {
    SplitAcc a, b;
    int128_t ref = 0;

    const int64_t va[] = {1LL, 2LL, 3LL, 4LL, 5LL};
    const int64_t vb[] = {100LL, 200LL, 300LL};

    for (int64_t v : va) {
        a.add(static_cast<int128_t>(v));
        ref += v;
    }
    for (int64_t v : vb) {
        b.add(static_cast<int128_t>(v));
        ref += v;
    }

    a.merge(b);
    EXPECT_EQ(a.get(), ref);
}

// ---------------------------------------------------------------------------
// 9. MergeWithNegative
//    Merge an accumulator that has a negative partial sum into one that has
//    a positive partial sum.
// ---------------------------------------------------------------------------
TEST_F(Decimal128SplitTest, MergeWithNegative) {
    SplitAcc pos, neg;
    int128_t ref = 0;

    const int64_t vpos[] = {1000000000LL, 2000000000LL, 3000000000LL};
    const int64_t vneg[] = {-999999999LL, -1234567890LL};

    for (int64_t v : vpos) {
        pos.add(static_cast<int128_t>(v));
        ref += v;
    }
    for (int64_t v : vneg) {
        neg.add(static_cast<int128_t>(v));
        ref += v;
    }

    pos.merge(neg);
    EXPECT_EQ(pos.get(), ref);
}

// ---------------------------------------------------------------------------
// 10. LargeRowCount
//     Simulate 1 billion additions of value 1,000,000.
//     Expected result: 10^15 (1,000,000,000 * 1,000,000).
// ---------------------------------------------------------------------------
TEST_F(Decimal128SplitTest, LargeRowCount) {
    SplitAcc acc;
    constexpr int64_t VALUE   = 1'000'000LL;          // 10^6
    constexpr int64_t ROWS    = 1'000'000'000LL;      // 10^9
    constexpr int128_t EXPECT = static_cast<int128_t>(VALUE) * ROWS; // 10^15

    for (int64_t i = 0; i < ROWS; ++i) {
        acc.add(static_cast<int128_t>(VALUE));
    }

    EXPECT_EQ(acc.get(), EXPECT);
}

// ---------------------------------------------------------------------------
// 11. Reset
//     After accumulating values, reset with aggregate-initialisation and
//     confirm get()==0 and both fields are zero.
// ---------------------------------------------------------------------------
TEST_F(Decimal128SplitTest, Reset) {
    SplitAcc acc;

    acc.add(static_cast<int128_t>(123456789LL));
    acc.add(static_cast<int128_t>(-987654321LL));
    ASSERT_NE(acc.get(), static_cast<int128_t>(0)) << "pre-condition: acc is non-zero";

    // Reset via aggregate-initialisation (equivalent to placement-new with {}).
    acc = SplitAcc {};

    EXPECT_EQ(acc.sum_high, 0);
    EXPECT_EQ(acc.sum_low, static_cast<uint64_t>(0));
    EXPECT_EQ(acc.get(), static_cast<int128_t>(0));
}

} // namespace doris::vectorized
