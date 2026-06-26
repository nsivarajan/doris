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

/**
 * Tests for wildcard row policy scopes: db.*, catalog.*.*, *.*.*
 *
 * Validates:
 *  1. DB-level wildcard covers all tables in a database with one policy
 *  2. Global wildcard (*.*.*) covers all tables cluster-wide
 *  3. Exact-table policy and db-wildcard policy both apply (RESTRICTIVE AND'd)
 *  4. A table created AFTER the policy is set is automatically covered
 *  5. DROP ROW POLICY with wildcard removes the policy
 *  6. SHOW ROW POLICY displays wildcard policies with "*" in the table column
 *  7. Column validation is skipped for wildcard policies at creation time
 */
suite("test_row_policy_wildcard", "p0,auth_call") {

    String user      = 'test_row_policy_wildcard_user'
    String pwd       = 'C123_567p'
    String dbName    = 'test_row_policy_wildcard_db'
    String tbl1      = 'orders'
    String tbl2      = 'products'
    String policy1   = 'test_wc_db_policy'
    String policy2   = 'test_wc_global_policy'
    String policy3   = 'test_wc_exact_policy'
    String policy4   = 'test_wc_filter_policy'

    // Clean up any leftovers from a previous failed run
    try_sql("DROP USER IF EXISTS ${user}")
    try_sql("DROP DATABASE IF EXISTS ${dbName}")

    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """GRANT SELECT_PRIV ON *.*.* TO '${user}'@'%'"""
    sql """GRANT GRANT_PRIV  ON *.*.* TO '${user}'@'%'"""

    if (isCloudMode()) {
        def clusters = sql "SHOW CLUSTERS;"
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO '${user}'@'%'"""
    }

    sql """CREATE DATABASE ${dbName}"""
    sql """
        CREATE TABLE ${dbName}.${tbl1} (
            id       BIGINT        NOT NULL,
            customer VARCHAR(64),
            amount   DECIMAL(10,2),
            status   VARCHAR(16)
        ) DUPLICATE KEY(id)
          DISTRIBUTED BY HASH(id) BUCKETS 2
          PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE ${dbName}.${tbl2} (
            id    BIGINT        NOT NULL,
            name  VARCHAR(64),
            price DECIMAL(10,2)
        ) DUPLICATE KEY(id)
          DISTRIBUTED BY HASH(id) BUCKETS 2
          PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO ${dbName}.${tbl1} VALUES (1,'alice',100,'paid'),(2,'bob',200,'pending'),(3,'carol',300,'paid')"""
    sql """INSERT INTO ${dbName}.${tbl2} VALUES (101,'laptop',999),(102,'mouse',29)"""

    try {
        // ── 1. No policy: user sees all rows ────────────────────────────────
        connect(user, "${pwd}", context.config.jdbcUrl) {
            def r1 = sql """SELECT COUNT(*) FROM ${dbName}.${tbl1}"""
            assertEquals(3, r1[0][0] as int, "baseline: tbl1 must have 3 rows")
            def r2 = sql """SELECT COUNT(*) FROM ${dbName}.${tbl2}"""
            assertEquals(2, r2[0][0] as int, "baseline: tbl2 must have 2 rows")
        }

        // ── 2. DB-level wildcard deny-all: one policy blocks both tables ─────
        sql """CREATE ROW POLICY ${policy1} ON ${dbName}.*
               AS RESTRICTIVE TO '${user}'@'%' USING (1 = 0)"""

        connect(user, "${pwd}", context.config.jdbcUrl) {
            def r1 = sql """SELECT COUNT(*) FROM ${dbName}.${tbl1}"""
            assertEquals(0, r1[0][0] as int, "db-wildcard must block tbl1")
            def r2 = sql """SELECT COUNT(*) FROM ${dbName}.${tbl2}"""
            assertEquals(0, r2[0][0] as int, "db-wildcard must block tbl2")
        }

        // ── 3. SHOW ROW POLICY displays wildcard with "*" in TableName ───────
        def policies = sql """SHOW ROW POLICY FOR '${user}'@'%'"""
        assertTrue(policies.any { row -> row[0] == policy1 && row[3] == '*' },
                "SHOW ROW POLICY must show policy1 with TableName='*'")

        // ── 4. DROP wildcard policy: rows visible again ──────────────────────
        sql """DROP ROW POLICY ${policy1} ON ${dbName}.* FOR '${user}'@'%'"""

        connect(user, "${pwd}", context.config.jdbcUrl) {
            def r = sql """SELECT COUNT(*) FROM ${dbName}.${tbl1}"""
            assertEquals(3, r[0][0] as int, "rows must reappear after wildcard policy drop")
        }

        def afterDrop = sql """SHOW ROW POLICY FOR '${user}'@'%'"""
        assertFalse(afterDrop.any { row -> row[0] == policy1 },
                "policy1 must not appear after DROP")

        // ── 5. DB-level filter (not deny-all): partial rows visible ──────────
        sql """CREATE ROW POLICY ${policy4} ON ${dbName}.*
               AS RESTRICTIVE TO '${user}'@'%' USING (amount > 150)"""

        connect(user, "${pwd}", context.config.jdbcUrl) {
            // Only bob(200) and carol(300) pass amount > 150
            def r = sql """SELECT COUNT(*) FROM ${dbName}.${tbl1}"""
            assertEquals(2, r[0][0] as int, "db-wildcard filter: only 2 rows pass amount>150")
        }

        sql """DROP ROW POLICY ${policy4} ON ${dbName}.* FOR '${user}'@'%'"""

        // ── 6. Exact + wildcard both apply (RESTRICTIVE AND'd) ───────────────
        // Wildcard: id <= 2 (alice, bob)   Exact: amount >= 200 (bob, carol)
        // Combined: id <= 2 AND amount >= 200  →  only bob (id=2, amount=200)
        sql """CREATE ROW POLICY ${policy1} ON ${dbName}.*
               AS RESTRICTIVE TO '${user}'@'%' USING (id <= 2)"""
        sql """CREATE ROW POLICY ${policy3} ON ${dbName}.${tbl1}
               AS RESTRICTIVE TO '${user}'@'%' USING (amount >= 200)"""

        connect(user, "${pwd}", context.config.jdbcUrl) {
            def r = sql """SELECT COUNT(*) FROM ${dbName}.${tbl1}"""
            assertEquals(1, r[0][0] as int,
                    "AND of wildcard(id<=2) + exact(amount>=200): only bob survives")
        }

        sql """DROP ROW POLICY ${policy1} ON ${dbName}.*        FOR '${user}'@'%'"""
        sql """DROP ROW POLICY ${policy3} ON ${dbName}.${tbl1}  FOR '${user}'@'%'"""

        // ── 7. Table created AFTER policy is covered automatically ───────────
        sql """CREATE ROW POLICY ${policy1} ON ${dbName}.*
               AS RESTRICTIVE TO '${user}'@'%' USING (1 = 0)"""

        String lateTable = 'late_arrival'
        sql """
            CREATE TABLE ${dbName}.${lateTable} (id BIGINT)
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql """INSERT INTO ${dbName}.${lateTable} VALUES (1),(2),(3)"""

        connect(user, "${pwd}", context.config.jdbcUrl) {
            def r = sql """SELECT COUNT(*) FROM ${dbName}.${lateTable}"""
            assertEquals(0, r[0][0] as int,
                    "wildcard policy must cover table created after policy was set")
        }

        sql """DROP ROW POLICY ${policy1} ON ${dbName}.* FOR '${user}'@'%'"""

        // ── 8. Global wildcard (*.*.*) covers all catalogs ───────────────────
        sql """CREATE ROW POLICY ${policy2} ON *.*.*
               AS RESTRICTIVE TO '${user}'@'%' USING (1 = 0)"""

        connect(user, "${pwd}", context.config.jdbcUrl) {
            def r1 = sql """SELECT COUNT(*) FROM ${dbName}.${tbl1}"""
            assertEquals(0, r1[0][0] as int, "global wildcard must block tbl1")
            def r2 = sql """SELECT COUNT(*) FROM ${dbName}.${tbl2}"""
            assertEquals(0, r2[0][0] as int, "global wildcard must block tbl2")
        }

        sql """DROP ROW POLICY ${policy2} ON *.*.* FOR '${user}'@'%'"""

        connect(user, "${pwd}", context.config.jdbcUrl) {
            def r = sql """SELECT COUNT(*) FROM ${dbName}.${tbl1}"""
            assertEquals(3, r[0][0] as int, "rows visible again after global policy drop")
        }

        // ── 9. Column validation skipped for wildcard at creation time ────────
        // Should succeed even though 'nonexistent_col' is not in any real table
        sql """CREATE ROW POLICY test_wc_nocol ON ${dbName}.*
               AS RESTRICTIVE TO '${user}'@'%' USING (1 = 0)"""
        sql """DROP ROW POLICY test_wc_nocol ON ${dbName}.* FOR '${user}'@'%'"""

    } finally {
        // Always clean up, even on test failure
        try_sql """DROP ROW POLICY IF EXISTS ${policy1}  ON ${dbName}.*       FOR '${user}'@'%'"""
        try_sql """DROP ROW POLICY IF EXISTS ${policy2}  ON *.*.*             FOR '${user}'@'%'"""
        try_sql """DROP ROW POLICY IF EXISTS ${policy3}  ON ${dbName}.${tbl1} FOR '${user}'@'%'"""
        try_sql """DROP ROW POLICY IF EXISTS ${policy4}  ON ${dbName}.*       FOR '${user}'@'%'"""
        try_sql """DROP ROW POLICY IF EXISTS test_wc_nocol ON ${dbName}.*     FOR '${user}'@'%'"""
        try_sql("DROP DATABASE IF EXISTS ${dbName}")
        try_sql("DROP USER IF EXISTS '${user}'@'%'")
    }
}
