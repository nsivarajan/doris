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

#include "gen_cpp/PlanNodes_types.h"

namespace doris {

// Decoded per-file column statistics from an Iceberg manifest entry.
struct IcebergFileColStats {
    bool has_min_max = false; // bounds decoded successfully
    bool all_nulls   = false; // null_count == row_count
    int64_t min_val  = 0;     // INTEGER / LONG / DATE / TIMESTAMP
    int64_t max_val  = 0;
    float   min_flt  = 0.0f;  // FLOAT
    float   max_flt  = 0.0f;
    double  min_dbl  = 0.0;   // DOUBLE
    double  max_dbl  = 0.0;
    int32_t type_id  = 0;     // 5=INT 7=LONG 8=FLOAT 9=DOUBLE 18=DATE 19=TIMESTAMP
};

// Decode Iceberg binary-encoded column statistics into IcebergFileColStats.
// Returns false if stats cannot be decoded; caller should not prune in that case.
bool decode_iceberg_column_stats(const TIcebergColumnStats& thrift_stats,
                                  IcebergFileColStats* out);

// Returns true if [file_min, file_max] and [rf_min, rf_max] have no overlap.
bool file_excluded_by_minmax(const IcebergFileColStats& file_stats,
                              int64_t rf_min, int64_t rf_max);

} // namespace doris
