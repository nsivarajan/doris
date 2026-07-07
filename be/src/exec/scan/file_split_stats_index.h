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

#include <parallel_hashmap/phmap.h>

#include <string>
#include <vector>

#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "storage/index/zone_map/zonemap_eval_context.h"

namespace doris {

// FileSplitStatsIndex caches decoded column-bounds ZoneMapEvalContexts per file path
// to avoid rebuilding from TFileSplitColBounds binary bounds on every call.
//
// Typical usage: one instance per scan node, passed into iceberg_file_excluded_by_rf
// for each file range. The first call for a given range.path builds and caches the
// ZoneMapEvalContext; subsequent calls for the same path return the cached entry.
//
// Thread safety: NOT thread-safe. Callers must ensure external synchronisation or
// use a per-thread instance.
class FileSplitStatsIndex {
public:
    // Returns a pointer to the cached ZoneMapEvalContext for this range, building it
    // on first access. Returns nullptr if the range carries no column stats or the
    // stats cannot be decoded into any slot in `slots`.
    const ZoneMapEvalContext* get_or_build(const TFileRangeDesc& range,
                                           const std::vector<SlotDescriptor*>& slots);

    // Number of entries currently held in the cache.
    int64_t size() const { return static_cast<int64_t>(_cache.size()); }

private:
    // Builds a ZoneMapEvalContext from the Iceberg column stats in `range`.
    // Returns an empty context (slots map empty) if nothing could be decoded.
    ZoneMapEvalContext _build(const TFileRangeDesc& range,
                              const std::vector<SlotDescriptor*>& slots);

    // Key: range.path — stable for the lifetime of the split source.
    phmap::flat_hash_map<std::string, ZoneMapEvalContext> _cache;
};

} // namespace doris
