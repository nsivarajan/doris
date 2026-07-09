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

#include <deque>
#include <memory>
#include <vector>

#include "exec/scan/file_split_stats_index.h"
#include "exec/scan/split_source_connector.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/runtime_profile.h"

namespace doris {
class TupleDescriptor;

// FilteringSplitSourceConnector wraps any SplitSourceConnector and transparently skips
// file splits whose manifest column bounds (col_bounds) are disjoint from ready
// runtime-filter conjuncts.
//
// ## Race-condition elimination via deferred splits
//
// The core problem: hash-join builds (e.g. date_dim, 73K rows, ~80ms) complete
// concurrently with the scan. The first call to get_next() may happen before the
// RF is ready, so the first 1–3 files would be opened despite being excludable.
//
// Solution — deferred-split pattern:
//   - When get_next() is called and a file HAS col_bounds but NO RF is ready yet,
//     the file is pushed to `_deferred` instead of being emitted.
//   - Files WITHOUT col_bounds are emitted immediately (no pruning possible).
//   - On every subsequent get_next() call, the deferred queue is drained first
//     against the NOW-CURRENT RF state — eliminating zero wasted opens for any
//     file that was deferred before the RF arrived.
//   - Safety valve: if the deferred queue grows beyond `_max_deferred` (default 8)
//     the oldest file is emitted without filtering to bound memory use for slow
//     builds or queries without a usable RF.
//   - When the inner source is exhausted, all deferred files are re-evaluated and
//     either pruned (if RF has arrived) or emitted conservatively.
//
// ## Profile counters
//   FilesPrunedByColBounds       — files pruned immediately (RF was ready)
//   FilesDeferredPendingRF       — files held because RF wasn't ready yet
//   FilesDeferredPruned          — deferred files pruned after RF arrived
//   FilesDeferredEmitted         — deferred files emitted (passed filter or timeout)
class FilteringSplitSourceConnector : public SplitSourceConnector {
public:
    FilteringSplitSourceConnector(std::shared_ptr<SplitSourceConnector> inner,
                                   const VExprContextSPtrs* conjuncts,
                                   const TupleDescriptor* tuple_desc,
                                   RuntimeProfile::Counter* pruned_ctr,
                                   RuntimeProfile::Counter* deferred_ctr = nullptr,
                                   RuntimeProfile::Counter* deferred_pruned_ctr = nullptr,
                                   RuntimeProfile::Counter* deferred_emitted_ctr = nullptr);

    Status get_next(bool* has_next, TFileRangeDesc* range) override;

    int num_scan_ranges() override { return _inner->num_scan_ranges(); }
    TFileScanRangeParams* get_params() override { return _inner->get_params(); }
    bool all_scan_ranges_match(const TFileScanRangeParams& params,
                               const std::function<bool(const TFileScanRangeParams&,
                                                        const TFileRangeDesc&)>& predicate) override {
        return _inner->all_scan_ranges_match(params, predicate);
    }

private:
    // Returns true if any conjunct in _conjuncts is a ready RF wrapper.
    bool _has_ready_rf() const;

    // Drain _deferred against current RF state. If RF is ready, pruned files are
    // discarded and the first passing file is written to `*range` (returns true).
    // If no deferred file passes, returns false.
    bool _drain_deferred(TFileRangeDesc* range);

    std::shared_ptr<SplitSourceConnector> _inner;
    const VExprContextSPtrs* _conjuncts;
    const TupleDescriptor* _tuple_desc;
    FileSplitStatsIndex _stats_index;

    // Deferred files: held pending RF arrival, in FIFO order.
    std::deque<TFileRangeDesc> _deferred;
    // Safety valve: emit oldest deferred file without filtering once queue exceeds this.
    static constexpr int _max_deferred = 8;

    RuntimeProfile::Counter* _pruned_ctr;
    RuntimeProfile::Counter* _deferred_ctr;
    RuntimeProfile::Counter* _deferred_pruned_ctr;
    RuntimeProfile::Counter* _deferred_emitted_ctr;
};

} // namespace doris
