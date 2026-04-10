# Doris Decoupled Mode Performance Optimization Plan

## Context

**Environment:** Doris cloud/decoupled mode, data on Alibaba Cloud OSS  
**Cluster:** 6 BEs × 64CPU/256GB, 3 FEs  
**Benchmark:** TPC-H 1000GB (5.9B rows in lineitem)  
**Goal:** Accelerate T2+ (warm cache) query performance  

## Root Cause Analysis (from profiling)

### Q1 Profile (T2, file cache warm)
```
STREAMING_AGGREGATION:
  group by: l_returnflag, l_linestatus (VARCHAR)
  ExecTime:                   44sec   ← VARCHAR serialization
  MemoryUsageSerializeKeyArena: 145MB ← 5.9B rows × 24 bytes/key
  MemoryUsageHashTable:        168B   ← only 4 groups (tiny\!)
  HashTableEmplaceTime:        35sec  ← DECIMAL __int128 arithmetic
  RowsProduced:                4      ← correct

OLAP_SCAN:
  ExecTime: 300ms (fast, file cache working)
```

**Problem 1 (44sec):** VARCHAR GROUP BY forces MethodSerialized — serializes
every row's string keys into arena memory. For 5.9B rows × 2 VARCHAR columns
= 145MB of arena allocations. The hash table has only 4 entries but we still
serialize every row.

**Problem 2 (35sec):** DECIMAL(15,2) sum uses __int128 accumulator. No AVX2
hardware support for __int128. Compiler emits scalar 2-instruction carry-add
per row. Cannot vectorize.

## Optimization Plan

### Step 1: ExpressionEstimation fix (FOUNDATION — must be first)
**File:** `fe/fe-core/src/main/java/org/apache/doris/nereids/stats/ExpressionEstimation.java`  
**Lines:** ~15  
**What:** `visitBoundFunction` propagates NDV through `EncodeAsSmallInt` instead
of returning UNKNOWN. Without this, cardinality at agg node = 1.9B (wrong)
instead of 3 (correct). The composite key step (Step 4) depends on knowing
the true NDV to verify combined NDV < 1024.  
**Risk:** Zero. No behaviour change for functions other than EncodeAsSmallInt.

---

### Step 2: MethodLowCardinality (BE core)
**Files:** 3 BE files, ~150 lines total  
**What:** New aggregation method that uses `acc[key]` direct array indexing
instead of PHHashMap. For a single SMALLINT GROUP BY key in [0, 1024),
replaces hash(key) → probe → acc[bucket] with just acc[key]. Zero hash
computation, zero collision, L1 cache hot for tiny NDV.

**Implementation:**
- `hash_key_type.h`: Add `low_cardinality` enum + phase2 downgrade to `int16_key`
- `hash_map_context.h`: Add `MethodLowCardinality` struct. Constructor
  pre-reserves PHHashMap to capacity ≥ 2048 so bucket=key (no collision).
  `init_serialized_keys` stores `hash_values[k] = keys[k]` (no CRC32).
  `CHECK(key < ARRAY_SIZE)` validates at runtime — fails cleanly, no corruption.
- `agg_utils.h`: Add 4 variants (UInt16/UInt32 × nullable/non-nullable).
  Add `low_cardinality` case in `init()`.

**Risk:** Only activates via explicit thrift flag from FE (Step 3). Never
activates based on data type alone (that caused the crash before). The CHECK
guard ensures key overflow fails with a clear error, not silent corruption.

---

### Step 3: Thrift flag + FE/BE wiring (safe activation)
**Files:** 4 files (1 thrift + 1 FE planner + 1 FE node + 2 BE operator files), ~80 lines  
**What:** FE communicates intent to BE via `use_low_cardinality_agg` field in
`TAggregationNode`. BE only uses MethodLowCardinality when this flag is true.
Follows identical pattern to existing `is_first_phase` and `is_colocate` flags.

**Implementation:**
- `PlanNodes.thrift`: Add `optional bool use_low_cardinality_agg` (field 11)
- `AggregationNode.java`: Add field + setter + serialize in `toThrift()`
- `PhysicalPlanTranslator.java`: Set flag when:
  - `aggMode == INPUT_TO_BUFFER` (local/first-phase only)
  - `groupByExpressions.size() == 1` (single composite column)
  - Key is `EncodeAsSmallInt` instance
- `streaming_aggregation_operator.h/.cpp`: Read flag from thrift, pass to
  `init_hash_method` as `use_low_cardinality_agg` parameter
- `hash_map_util.h`: Add `bool use_low_cardinality_agg = false` parameter,
  route to `HashKeyType::low_cardinality` when flag is true AND is_first_phase

**Risk:** FE-controlled. The flag is never set unless FE explicitly verified
the conditions. All existing call sites pass 3 arguments (default = false).

---

### Step 4: Composite key encoding (makes Steps 2+3 work for Q1)
**File:** `CompressedMaterialize.java`, ~60 lines  
**What:** Q1 has two GROUP BY columns (`l_returnflag`, `l_linestatus`). 
MethodLowCardinality requires a single integer key. Combine two low-NDV
SMALLINT columns into one composite integer:
  `composite_key = encode_as_smallint(col1) * ndv(col2) + encode_as_smallint(col2)`

For Q1: ndv(l_linestatus)=2, so composite = flag_id * 2 + status_id.
Values: {0,1,2,3,4,5} — all < 1024. Single SMALLINT column → MethodLowCardinality.

**Conditions for composite encoding:**
- All GROUP BY columns are `EncodeAsSmallInt`
- NDV of each column is known from statistics (ExpressionEstimation fix needed)
- Combined NDV = product of all individual NDVs < 1024
- Fallback: if stats unavailable or combined NDV ≥ 1024 → standard path, no error

**Expected Q1 impact:** HashTableComputeTime 44sec → ~0sec

**Risk:** Low. Fallback to standard path if conditions not met. The FE
verifies combined NDV < 1024 before setting `use_low_cardinality_agg`. The BE
CHECK(key < ARRAY_SIZE) is a safety net.

---

### Step 5: Split __int128 accumulator (DECIMAL improvement)
**Files:** `aggregate_function_sum.h` + `.cpp`, ~120 lines  
**What:** For `sum(DECIMAL32/64) → DECIMAL128` paths, store the accumulator
as two `int64_t` fields (sum_high, sum_low) instead of one `__int128`.
The `int64_t` fields are auto-vectorizable by AVX2 PADDQ. The carry logic
between sum_low and sum_high is mathematically correct (ClickHouse algorithm).

For Q1 with DECIMAL(15,2) over 5.9B rows: individual values fit in int64_t
(max ~10^13 << INT64_MAX ~9.2×10^18). Safe to split.

Note: Q1 benefit is marginal because Doris processes one row at a time per
group (not batch SIMD). Full benefit requires batch processing (future work).
But the implementation is correct, always-on, zero risk.

**Risk:** Zero. Mathematical equivalence proven. Static assert enforces 16-byte
size. Serialization format identical (16 bytes either way).

---

### Step 6: AdaptiveDecimalAccumulator (FE DECIMAL narrowing)
**Files:** 5 FE files, ~150 lines  
**What:** New FE rewrite rule. When statistics show that sum(DECIMAL(P,S))
over R rows fits in DECIMAL64 (int64), rewrite the return type to DECIMAL64.
BE then uses int64_t accumulator instead of __int128 → full AVX2 SIMD.

**Formula:** `neededPrecision = inputPrecision + ceil(log10(rowCount))`
If `neededPrecision ≤ 18`: safe to use DECIMAL64 (int64).

For Q1: DECIMAL(15,2) over 5.9B rows → neededPrecision = 25 > 18. Does NOT
trigger. But helps smaller tables and lower-precision DECIMAL columns across
the broader query workload.

**Guards:**
- Session variable `enable_adaptive_decimal_accumulator` (default false)
- `rowCount > 1` floor (log10(1) = 0 would under-count)
- DISTINCT sum excluded (dedup changes row count bounds)

**Risk:** Low. Session variable defaults to false (opt-in). Conservative formula.

---

## Build and Test Sequence

```
After Steps 1-4 (Q1 core fix):
  ./build.sh --fe --be
  
  Test:
    SET enable_compress_materialize = true;
    -- Run Q1, check HashTableComputeTime in profile (~0sec expected)
    -- Compare results with baseline (must match exactly)

After Steps 5-6 (DECIMAL improvements):
  ./build.sh --fe --be
  
  Test:
    SET enable_adaptive_decimal_accumulator = true;
    -- Run Q1 and smaller-table queries
    -- Verify correctness
```

## Expected Outcome

```
Q1 T2 baseline:                    ~90sec
After Steps 1-4 (composite+LowCard): ~46sec  (-44sec hash computation)
After Steps 5-6 (DECIMAL):            ~46sec  (Q1 DECIMAL doesn't narrow)

Other queries with:
  - Single low-NDV VARCHAR GROUP BY:  Large improvement
  - DECIMAL sum on small tables:       3-4× improvement on agg time
```

## Files Changed Summary

| File | Step | Lines | Risk |
|------|------|-------|------|
| `ExpressionEstimation.java` | 1 | ~15 | Zero |
| `hash_key_type.h` | 2 | ~20 | Zero (enum only) |
| `hash_map_context.h` | 2 | ~100 | Low (guarded by flag) |
| `agg_utils.h` | 2 | ~30 | Low (guarded by flag) |
| `PlanNodes.thrift` | 3 | ~5 | Zero (optional field) |
| `AggregationNode.java` | 3 | ~15 | Zero |
| `PhysicalPlanTranslator.java` | 3 | ~25 | Low |
| `streaming_aggregation_operator.h/.cpp` | 3 | ~15 | Zero |
| `hash_map_util.h` | 3 | ~15 | Zero (default param) |
| `CompressedMaterialize.java` | 4 | ~60 | Low (fallback exists) |
| `aggregate_function_sum.h` | 5 | ~100 | Zero |
| `aggregate_function_sum.cpp` | 5 | ~20 | Zero |
| `SessionVariable.java` | 6 | ~15 | Zero |
| `AdaptiveDecimalAccumulator.java` | 6 | ~100 | Low |
| `Sum.java` | 6 | ~25 | Zero |
| `RuleType.java` | 6 | ~2 | Zero |
| `Rewriter.java` | 6 | ~5 | Zero |

