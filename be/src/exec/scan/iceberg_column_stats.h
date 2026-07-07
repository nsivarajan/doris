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
    int64_t     min_val = 0;     // INTEGER(5) / LONG(7) / DATE(18) / TIMESTAMP(19)
    int64_t     max_val = 0;
    float       min_flt = 0.0f;  // FLOAT(8)
    float       max_flt = 0.0f;
    double      min_dbl = 0.0;   // DOUBLE(9)
    double      max_dbl = 0.0;
    std::string min_str;         // STRING(10) — raw UTF-8 bytes from manifest
    std::string max_str;
    int32_t type_id = 0;
    // type_id values (internal Doris FE→BE protocol, not Iceberg enum ordinals):
    // 5=INTEGER  7=LONG  8=FLOAT  9=DOUBLE  10=STRING  18=DATE  19=TIMESTAMP
};

// Decode Iceberg binary-encoded column statistics into IcebergFileColStats.
// Returns false if stats cannot be decoded; caller should not prune in that case.
bool decode_iceberg_column_stats(const TFileSplitColBounds& thrift_stats,
                                  IcebergFileColStats* out);

// Returns true if [file_min, file_max] and [rf_min, rf_max] have no overlap.
bool file_excluded_by_minmax(const IcebergFileColStats& file_stats,
                              int64_t rf_min, int64_t rf_max);

// Returns true when Iceberg manifest column stats in `range` are disjoint from every
// ready runtime-filter conjunct, meaning the file can be skipped without opening it.
// Called by both FileScanner (old path) and FileScannerV2 (format_v2 path).
// If `index` is non-null, the decoded ZoneMapEvalContext is looked up / cached there
// so per-file ZoneMap construction is paid at most once per file path per scan node.
bool iceberg_file_excluded_by_rf(const TFileRangeDesc& range,
                                  const VExprContextSPtrs& conjuncts,
                                  const std::vector<SlotDescriptor*>& slots,
                                  FileSplitStatsIndex* index = nullptr);

} // namespace doris
