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

#include "pipeline/common/agg_utils.h"

#include <gtest/gtest.h>

#include "common/config.h"
#include "vec/data_types/data_type_number.h"

namespace doris::pipeline {

class AggLowCardinalityTest : public testing::Test {
protected:
    void SetUp() override { _variants = std::make_unique<AggregatedDataVariants>(); }

    std::unique_ptr<AggregatedDataVariants> _variants;
};

// When FE routes HashKeyType::low_cardinality with a 2-byte key type (Int16),
// init() must select MethodLowCardinality<UInt16, AggData<UInt16>> — the
// direct array path — rather than any hash-table-based method.
TEST_F(AggLowCardinalityTest, LowCardinalityKeyTypeRouting) {
    std::vector<vectorized::DataTypePtr> types {
            std::make_shared<vectorized::DataTypeInt16>()};
    _variants->init(types, HashKeyType::low_cardinality);
    ASSERT_TRUE(std::holds_alternative<
            vectorized::MethodLowCardinality<vectorized::UInt16, AggData<vectorized::UInt16>>>(
            _variants->method_variant));
}

// When FE routes HashKeyType::low_cardinality with a 4-byte key type (Int32),
// init() must select MethodLowCardinality<UInt32, AggData<UInt32>>.
TEST_F(AggLowCardinalityTest, LowCardinalityUInt32Routing) {
    std::vector<vectorized::DataTypePtr> types {
            std::make_shared<vectorized::DataTypeInt32>()};
    _variants->init(types, HashKeyType::low_cardinality);
    ASSERT_TRUE(std::holds_alternative<
            vectorized::MethodLowCardinality<vectorized::UInt32, AggData<vectorized::UInt32>>>(
            _variants->method_variant));
}

// When config::enable_low_cardinality_agg is false (the default), a caller
// that explicitly passes HashKeyType::int16_key must NOT get a MethodLowCardinality
// variant — it must fall through to the standard MethodOneNumber path.
TEST_F(AggLowCardinalityTest, Int16KeyUsesStandardPath) {
    // Confirm the default is false so this test exercises the right branch.
    ASSERT_FALSE(config::enable_low_cardinality_agg);

    std::vector<vectorized::DataTypePtr> types {
            std::make_shared<vectorized::DataTypeInt16>()};
    _variants->init(types, HashKeyType::int16_key);
    ASSERT_TRUE(std::holds_alternative<
            vectorized::MethodOneNumber<vectorized::UInt16, AggData<vectorized::UInt16>>>(
            _variants->method_variant));
}

// get_hash_key_type_with_phase must downgrade low_cardinality to int16_key
// during phase 2 (after network shuffle), because the per-node key range
// guarantee is no longer exclusive and routing through low_cardinality
// would be incorrect.
TEST_F(AggLowCardinalityTest, Phase2DowngradesLowCardinality) {
    HashKeyType result = get_hash_key_type_with_phase(HashKeyType::low_cardinality, true);
    ASSERT_EQ(result, HashKeyType::int16_key);
}

// get_hash_key_type_with_phase must preserve low_cardinality unchanged
// during phase 1 (before network shuffle), where the key range guarantee
// established by the FE still holds.
TEST_F(AggLowCardinalityTest, Phase1PreservesLowCardinality) {
    HashKeyType result = get_hash_key_type_with_phase(HashKeyType::low_cardinality, false);
    ASSERT_EQ(result, HashKeyType::low_cardinality);
}

} // namespace doris::pipeline
