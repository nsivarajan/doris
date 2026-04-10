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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.pattern.PatternDescriptor;
import org.apache.doris.nereids.pattern.TypePattern;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.StatsDerive.DeriveContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * AdaptiveDecimalAccumulator — uses statistics to select the narrowest safe
 * accumulator type for sum() over DECIMAL columns.
 *
 * Problem:
 *   sum(DECIMAL(P,S)) always returns DECIMAL(38,S) = __int128 accumulator.
 *   __int128 has no AVX2 hardware opcode — scalar only, blocks vectorization.
 *
 * Solution:
 *   When statistics show the maximum possible sum fits in DECIMAL64 (int64),
 *   rewrite the return type to DECIMAL64. BE uses int64 accumulator → AVX2.
 *
 * Safety:
 *   DECIMAL(P,S) sum over R rows needs at most P + ceil(log10(R)) digits.
 *   If P + ceil(log10(R)) <= 18 (MAX_DECIMAL64_PRECISION): int64 is safe.
 *   rowCount > 1 guard: log10(1) = 0 would under-count digits.
 *   DISTINCT excluded: dedup changes effective row count bounds.
 *
 * Note for TPC-H Q1 at 10TB (DECIMAL(15,2) over 5.9B rows):
 *   neededPrecision = 15 + 10 = 25 > 18 → does not trigger (correct fallback).
 *   Helps smaller tables and lower-precision DECIMAL columns.
 *
 * Activation: SET enable_adaptive_decimal_accumulator = true; (default false)
 */
public class AdaptiveDecimalAccumulator implements RewriteRuleFactory {

    public static final AdaptiveDecimalAccumulator INSTANCE = new AdaptiveDecimalAccumulator();

    @Override
    public RulePromise defaultPromise() {
        return RulePromise.REWRITE;
    }

    @Override
    public List<Rule> buildRules() {
        PatternDescriptor<LogicalAggregate<Plan>> pattern =
                new PatternDescriptor<>(
                        new TypePattern<>(LogicalAggregate.class, Pattern.ANY),
                        defaultPromise());

        return ImmutableList.of(
            pattern.thenApply(ctx -> {
                if (ConnectContext.get() == null
                        || !ConnectContext.get().getSessionVariable()
                                .enableAdaptiveDecimalAccumulator) {
                    return null;
                }

                LogicalAggregate<Plan> agg = ctx.root;

                // Derive statistics if not yet available
                if (agg.child().getStats() == null) {
                    StatsDerive derive = new StatsDerive(false);
                    agg.child().accept(derive, new DeriveContext());
                }
                Statistics childStats = agg.child().getStats();
                if (childStats == null) {
                    return null;
                }

                double rowCount = childStats.getRowCount();
                if (rowCount <= 0) {
                    return null;
                }

                // rowCount > 1 guard: log10(1) = 0 under-counts digits.
                // Stats may return fractional values (e.g. 0.5) for tiny tables.
                int rowCountDigits = rowCount > 1
                        ? (int) Math.ceil(Math.log10(rowCount))
                        : 1;

                boolean anyChanged = false;
                List<NamedExpression> newOutputs = new ArrayList<>();
                for (NamedExpression output : agg.getOutputExpressions()) {
                    NamedExpression rewritten = tryNarrowSumPrecision(output, rowCountDigits);
                    newOutputs.add(rewritten);
                    if (rewritten != output) {
                        anyChanged = true;
                    }
                }

                if (!anyChanged) {
                    return null;
                }
                return agg.withAggOutput(newOutputs);
            }).toRule(RuleType.ADAPTIVE_DECIMAL_ACCUMULATOR)
        );
    }

    /**
     * If expression contains sum(DECIMAL(P,S)) where P+rowCountDigits <= 18,
     * rewrite to use DECIMAL64 (int64) accumulator instead of DECIMAL128 (__int128).
     */
    @SuppressWarnings("unchecked")
    private NamedExpression tryNarrowSumPrecision(NamedExpression expr, int rowCountDigits) {
        return (NamedExpression) expr.rewriteUp(e -> {
            if (!(e instanceof Sum)) {
                return e;
            }
            Sum sum = (Sum) e;
            // DISTINCT: row count bound is invalid after dedup — skip safely
            if (sum.isDistinct()) {
                return e;
            }
            if (!(sum.getDataType() instanceof DecimalV3Type)) {
                return e;
            }
            DecimalV3Type returnType = (DecimalV3Type) sum.getDataType();
            // DECIMAL256: don't touch
            if (returnType.getPrecision() > DecimalV3Type.MAX_DECIMAL128_PRECISION) {
                return e;
            }
            // Check input precision
            if (!(sum.child().getDataType() instanceof DecimalV3Type)) {
                return e;
            }
            DecimalV3Type inputType = (DecimalV3Type) sum.child().getDataType();
            int neededPrecision = inputType.getPrecision() + rowCountDigits;
            if (neededPrecision <= DecimalV3Type.MAX_DECIMAL64_PRECISION) {
                DecimalV3Type narrowedType = DecimalV3Type.createDecimalV3TypeNotCheck256(
                        neededPrecision, inputType.getScale());
                return sum.withNarrowedReturnType(narrowedType);
            }
            return e;
        });
    }
}
