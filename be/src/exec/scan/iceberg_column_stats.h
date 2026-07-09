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

#pragma once

#include <string>
#include <vector>
#include "exprs/vexpr_fwd.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {
class FileSplitStatsIndex;
class SlotDescriptor;

// Decoded per-file column statistics from an Iceberg manifest entry.
struct IcebergFileColStats {
    bool has_min_max = false; // bounds decoded successfully
    bool all_nulls   = false; // null_count == row_count
    int64_t     min_val = 0;      // INTEGER/LONG/DATE/TIMESTAMP/TIMESTAMP_NANO
    int64_t     max_val = 0;
    float       min_flt = 0.0f;   // FLOAT
    float       max_flt = 0.0f;
    double      min_dbl = 0.0;    // DOUBLE
    double      max_dbl = 0.0;
    std::string min_str;          // STRING/UUID/FIXED — raw bytes
    std::string max_str;
    __int128    min_decimal = 0;  // DECIMAL — unscaled big-endian decoded to int128
    __int128    max_decimal = 0;
    int32_t     decimal_scale = 0;
    int32_t type_id = 0;
    // type_id values (internal Doris FE→BE protocol, not Iceberg enum ordinals):
    // 3=BOOLEAN   5=INTEGER   7=LONG   8=FLOAT   9=DOUBLE  10=STRING
    // 11=DECIMAL  18=DATE  19=TIMESTAMP  20=TIMESTAMP_NANO  21=UUID  22=FIXED
};

bool decode_iceberg_column_stats(const TFileSplitColBounds& thrift_stats,
                                  IcebergFileColStats* out);

bool file_excluded_by_minmax(const IcebergFileColStats& file_stats,
                              int64_t rf_min, int64_t rf_max);

bool iceberg_file_excluded_by_rf(const TFileRangeDesc& range,
                                  const VExprContextSPtrs& conjuncts,
                                  const std::vector<SlotDescriptor*>& slots,
                                  FileSplitStatsIndex* index = nullptr);

} // namespace doris
