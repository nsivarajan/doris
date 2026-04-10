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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AdaptiveDecimalAccumulatorTest {

    // salary table columns: id(INT), name(STRING), salary(DECIMAL128(38,2)), age(BIGINT)
    // salary column is at output index 2.
    private final LogicalOlapScan salaryScan = new LogicalOlapScan(
            PlanConstructor.getNextRelationId(),
            PlanConstructor.salary,
            ImmutableList.of("db"));

    /**
     * The session variable must default to false so the rule is opt-in only.
     */
    @Test
    void ruleDisabledByDefault() {
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        Assertions.assertFalse(ctx.getSessionVariable().enableAdaptiveDecimalAccumulator,
                "enableAdaptiveDecimalAccumulator must default to false");
    }

    /**
     * When child statistics are null (no catalog stats in unit tests), the rule
     * must bail out and leave the plan unchanged.
     */
    @Test
    void noStatsSkipsRewrite() {
        Slot decimalSlot = salaryScan.getOutput().get(2);
        Alias sumAlias = new Alias(new Sum(decimalSlot));
        LogicalAggregate<?> agg = new LogicalAggregate<>(
                ImmutableList.of(),
                ImmutableList.of(sumAlias),
                salaryScan);

        ConnectContext ctx = MemoTestUtils.createConnectContext();
        ctx.getSessionVariable().enableAdaptiveDecimalAccumulator = true;

        Plan result = PlanChecker.from(ctx, agg)
                .applyTopDown(ImmutableList.of(
                        AdaptiveDecimalAccumulator.INSTANCE.buildRules().get(0)))
                .getPlan();

        // Without stats the rule must not narrow — precision must stay > MAX_DECIMAL64_PRECISION
        LogicalAggregate<?> resultAgg = (LogicalAggregate<?>) result;
        NamedExpression out = resultAgg.getOutputExpressions().get(0);
        Sum sum = (Sum) ((Alias) out).child();
        Assertions.assertTrue(sum.getDataType() instanceof DecimalV3Type,
                "output must still be DecimalV3Type");
        Assertions.assertTrue(((DecimalV3Type) sum.getDataType()).getPrecision()
                        > DecimalV3Type.MAX_DECIMAL64_PRECISION,
                "precision must NOT be narrowed when stats are unavailable");
    }

    /**
     * DISTINCT sum must never be narrowed: the row-count bound used in the
     * overflow analysis counts physical rows, but DISTINCT deduplicates before
     * summing, so the bound is invalid for DISTINCT aggregates.
     */
    @Test
    void distinctSumNotNarrowed() {
        Slot decimalSlot = salaryScan.getOutput().get(2);
        // isDistinct=true, alwaysNullable=true
        Alias sumDistinctAlias = new Alias(new Sum(true, true, decimalSlot));
        LogicalAggregate<?> agg = new LogicalAggregate<>(
                ImmutableList.of(),
                ImmutableList.of(sumDistinctAlias),
                salaryScan);

        ConnectContext ctx = MemoTestUtils.createConnectContext();
        ctx.getSessionVariable().enableAdaptiveDecimalAccumulator = true;

        Plan result = PlanChecker.from(ctx, agg)
                .applyTopDown(ImmutableList.of(
                        AdaptiveDecimalAccumulator.INSTANCE.buildRules().get(0)))
                .getPlan();

        LogicalAggregate<?> resultAgg = (LogicalAggregate<?>) result;
        NamedExpression out = resultAgg.getOutputExpressions().get(0);
        Sum sum = (Sum) ((Alias) out).child();
        Assertions.assertTrue(sum.isDistinct(), "DISTINCT must be preserved after rule");
        Assertions.assertTrue(((DecimalV3Type) sum.getDataType()).getPrecision()
                        > DecimalV3Type.MAX_DECIMAL64_PRECISION,
                "DISTINCT sum precision must NOT be narrowed");
    }
}
