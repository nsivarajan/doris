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

#include "exec/scan/filtering_split_source.h"

#include "exec/scan/iceberg_column_stats.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "runtime/descriptors.h"

namespace doris {

FilteringSplitSourceConnector::FilteringSplitSourceConnector(
        std::shared_ptr<SplitSourceConnector> inner, const VExprContextSPtrs* conjuncts,
        const TupleDescriptor* tuple_desc, RuntimeProfile::Counter* pruned_ctr,
        RuntimeProfile::Counter* deferred_ctr, RuntimeProfile::Counter* deferred_pruned_ctr,
        RuntimeProfile::Counter* deferred_emitted_ctr)
        : _inner(std::move(inner)),
          _conjuncts(conjuncts),
          _tuple_desc(tuple_desc),
          _pruned_ctr(pruned_ctr),
          _deferred_ctr(deferred_ctr),
          _deferred_pruned_ctr(deferred_pruned_ctr),
          _deferred_emitted_ctr(deferred_emitted_ctr) {
    _max_scanners = _inner->num_scan_ranges();
}

bool FilteringSplitSourceConnector::_has_ready_rf() const {
    if (_conjuncts == nullptr || _conjuncts->empty()) {
        return false;
    }
    return std::any_of(_conjuncts->begin(), _conjuncts->end(), [](const auto& ctx) {
        return ctx->root()->is_rf_wrapper() && ctx->root()->can_evaluate_zonemap_filter();
    });
}

bool FilteringSplitSourceConnector::_drain_deferred(TFileRangeDesc* range) {
    while (!_deferred.empty()) {
        auto candidate = std::move(_deferred.front());
        _deferred.pop_front();

        if (iceberg_file_excluded_by_rf(candidate, *_conjuncts, _tuple_desc->slots(),
                                          &_stats_index)) {
            if (_pruned_ctr != nullptr) COUNTER_UPDATE(_pruned_ctr, 1);
            if (_deferred_pruned_ctr != nullptr) COUNTER_UPDATE(_deferred_pruned_ctr, 1);
            continue; // pruned — try next deferred file
        }
        // File passes — emit it
        if (_deferred_emitted_ctr != nullptr) COUNTER_UPDATE(_deferred_emitted_ctr, 1);
        *range = std::move(candidate);
        return true;
    }
    return false; // deferred queue is empty
}

Status FilteringSplitSourceConnector::get_next(bool* has_next, TFileRangeDesc* range) {
    DORIS_CHECK(has_next != nullptr && range != nullptr);

    // Step 1: If RF has arrived, drain any files that were deferred while waiting.
    if (!_deferred.empty() && _has_ready_rf()) {
        if (_drain_deferred(range)) {
            *has_next = true;
            return Status::OK();
        }
        // All deferred files were pruned. Fall through to fetch more from inner.
    }

    // Step 2: Fetch and evaluate files from the inner source.
    while (true) {
        bool inner_has_next = false;
        RETURN_IF_ERROR(_inner->get_next(&inner_has_next, range));

        if (!inner_has_next) {
            // Inner exhausted. Flush remaining deferred files conservatively:
            // RF may still arrive but we can't hold forever — emit without filtering.
            if (!_deferred.empty()) {
                *range = std::move(_deferred.front());
                _deferred.pop_front();
                if (_deferred_emitted_ctr != nullptr) COUNTER_UPDATE(_deferred_emitted_ctr, 1);
                *has_next = true;
                return Status::OK();
            }
            *has_next = false;
            return Status::OK();
        }

        const bool has_col_bounds =
                range->__isset.col_bounds && !range->col_bounds.empty();

        if (!has_col_bounds) {
            // No manifest stats — cannot prune, emit immediately.
            *has_next = true;
            return Status::OK();
        }

        if (_conjuncts == nullptr || !_has_ready_rf()) {
            // RF not ready yet. Defer this file rather than opening it.
            _deferred.push_back(*range);
            if (_deferred_ctr != nullptr) COUNTER_UPDATE(_deferred_ctr, 1);

            // Safety valve: if deferred queue is full, emit oldest without filtering.
            if (static_cast<int>(_deferred.size()) > _max_deferred) {
                *range = std::move(_deferred.front());
                _deferred.pop_front();
                if (_deferred_emitted_ctr != nullptr) COUNTER_UPDATE(_deferred_emitted_ctr, 1);
                *has_next = true;
                return Status::OK();
            }
            continue; // fetch the next file from inner
        }

        // RF is ready — evaluate immediately.
        if (iceberg_file_excluded_by_rf(*range, *_conjuncts, _tuple_desc->slots(),
                                          &_stats_index)) {
            if (_pruned_ctr != nullptr) COUNTER_UPDATE(_pruned_ctr, 1);
            continue; // skip, get next
        }
        *has_next = true;
        return Status::OK();
    }
}

} // namespace doris
