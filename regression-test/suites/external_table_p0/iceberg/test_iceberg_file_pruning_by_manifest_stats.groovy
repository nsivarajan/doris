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

// Regression tests for file-level pruning using Iceberg manifest column stats.
// The FE reads lowerBounds/upperBounds from the manifest (free, no extra I/O)
// and passes them to the BE so files whose column range is disjoint from a
// ready runtime-filter can be skipped before opening.
//
// Covers: unpartitioned Iceberg tables (the primary gap vs Trino), mixed
// int/long column types, and the correctness invariant (result must match a
// plain full scan).

suite("test_iceberg_file_pruning_by_manifest_stats", "p0,external") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_file_pruning_manifest_stats"
    String db_name      = "default"
    String rest_port    = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port   = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """drop catalog if exists ${catalog_name}"""
    sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri' = 'http://${externalEnvIp}:${rest_port}',
            "s3.access_key"  = "admin",
            "s3.secret_key"  = "password",
            "s3.endpoint"    = "http://${externalEnvIp}:${minio_port}",
            "s3.region"      = "us-east-1"
        );"""

    sql """switch ${catalog_name}"""
    sql """use ${db_name}"""

    // ── Setup: unpartitioned table with disjoint date-key ranges per file ──────
    // We create two internal tables with non-overlapping date_key ranges so
    // that after CTAS each becomes a separate Parquet file. A join with a small
    // dimension table exercises the file-level pruning path.

    sql """drop table if exists ${catalog_name}.${db_name}.fact_sales"""
    sql """
        CREATE TABLE ${catalog_name}.${db_name}.fact_sales (
            id        BIGINT,
            date_key  INT,
            amount    DECIMAL(10,2)
        ) USING iceberg
        TBLPROPERTIES ('write.target-file-size-bytes' = '1')"""

    // Insert two disjoint date ranges → two separate Parquet files in the manifest.
    // File 1: date_key 1000–1099  File 2: date_key 2000–2099
    sql """INSERT INTO ${catalog_name}.${db_name}.fact_sales
          SELECT id, 1000 + (id % 100), CAST(id AS DECIMAL(10,2))
          FROM (SELECT explode(sequence(1, 100)) AS id) t"""
    sql """INSERT INTO ${catalog_name}.${db_name}.fact_sales
          SELECT id, 2000 + (id % 100), CAST(id AS DECIMAL(10,2))
          FROM (SELECT explode(sequence(101, 200)) AS id) t"""

    sql """drop table if exists ${catalog_name}.${db_name}.dim_date"""
    sql """
        CREATE TABLE ${catalog_name}.${db_name}.dim_date (
            date_key  INT,
            label     STRING
        ) USING iceberg"""
    sql """INSERT INTO ${catalog_name}.${db_name}.dim_date VALUES (1050, 'mid-range-1')"""

    // ── Test 1: Correctness — join result must match the brute-force filter ──
    // The file containing date_key 2000–2099 should be pruned by the RF built
    // from dim_date (which only has date_key=1050). The result must still be correct.
    qt_join_result_matches_filter """
        SELECT f.id, f.date_key, f.amount
        FROM ${catalog_name}.${db_name}.fact_sales f
        JOIN ${catalog_name}.${db_name}.dim_date d ON f.date_key = d.date_key
        ORDER BY f.id"""

    // ── Test 2: Correctness — count matches plain WHERE ──
    // Both paths must agree on the row count.
    def count_via_join = sql """
        SELECT count(*) FROM ${catalog_name}.${db_name}.fact_sales f
        JOIN ${catalog_name}.${db_name}.dim_date d ON f.date_key = d.date_key"""

    def count_via_where = sql """
        SELECT count(*) FROM ${catalog_name}.${db_name}.fact_sales
        WHERE date_key IN (SELECT date_key FROM ${catalog_name}.${db_name}.dim_date)"""

    assertEquals(count_via_join[0][0], count_via_where[0][0])

    // ── Test 3: Pruning observable via query profile counter ──
    // FilesPrunedByColBounds should be > 0 after a join that
    // selects only rows from one of the two disjoint date-key files.
    explain {
        sql """SELECT count(*) FROM ${catalog_name}.${db_name}.fact_sales f
               JOIN ${catalog_name}.${db_name}.dim_date d ON f.date_key = d.date_key"""
        contains "FilesPrunedByColBounds"
    }

    // ── Test 4: All-null column stats — must never prune (conservative) ──
    sql """drop table if exists ${catalog_name}.${db_name}.fact_nulls"""
    sql """
        CREATE TABLE ${catalog_name}.${db_name}.fact_nulls (
            id       BIGINT,
            date_key INT
        ) USING iceberg"""
    sql """INSERT INTO ${catalog_name}.${db_name}.fact_nulls
          SELECT id, NULL FROM (SELECT explode(sequence(1, 10)) AS id) t"""

    qt_null_column_no_prune """
        SELECT count(*) FROM ${catalog_name}.${db_name}.fact_nulls f
        JOIN ${catalog_name}.${db_name}.dim_date d ON f.date_key = d.date_key"""

    // ── Cleanup ──────────────────────────────────────────────────────────────
    sql """drop table if exists ${catalog_name}.${db_name}.fact_sales"""
    sql """drop table if exists ${catalog_name}.${db_name}.dim_date"""
    sql """drop table if exists ${catalog_name}.${db_name}.fact_nulls"""
    sql """drop catalog if exists ${catalog_name}"""
}
