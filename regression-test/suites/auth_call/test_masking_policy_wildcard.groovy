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
 * Tests for wildcard masking policy scopes: db.*, exact table.
 *
 * Validates:
 *  1. Baseline: no policy, real values visible
 *  2. Exact-table masking policy masks only flagged rows, real values pass through otherwise
 *  3. DB-level wildcard: one policy masks the same column across multiple tables
 *  4. A table created AFTER the wildcard policy is set is automatically covered
 *  5. Exact-table policy takes precedence over a simultaneously active db-wildcard policy
 *  6. A user without the targeted role sees real values (opt-in, not default-apply)
 *  7. SHOW MASKING POLICY / DROP MASKING POLICY
 */
suite("test_masking_policy_wildcard", "p0,auth_call") {

    String user       = 'test_masking_policy_wildcard_user'
    String otherUser  = 'test_masking_policy_wildcard_other_user'
    String pwd        = 'C123_567p'
    String dbName     = 'test_masking_policy_wildcard_db'
    String tbl1       = 'orders'
    String tbl2       = 'customers'
    String policyExact = 'test_mp_exact'
    String policyWc     = 'test_mp_wc'

    try_sql("DROP USER IF EXISTS ${user}")
    try_sql("DROP USER IF EXISTS ${otherUser}")
    try_sql("DROP DATABASE IF EXISTS ${dbName}")

    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${otherUser}' IDENTIFIED BY '${pwd}'"""
    sql """GRANT SELECT_PRIV ON *.*.* TO '${user}'@'%'"""
    sql """GRANT GRANT_PRIV  ON *.*.* TO '${user}'@'%'"""
    sql """GRANT SELECT_PRIV ON *.*.* TO '${otherUser}'@'%'"""

    if (isCloudMode()) {
        def clusters = sql "SHOW CLUSTERS;"
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO '${user}'@'%'"""
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO '${otherUser}'@'%'"""
    }

    sql """CREATE DATABASE ${dbName}"""
    sql """
        CREATE TABLE ${dbName}.${tbl1} (
            id             BIGINT        NOT NULL,
            ssn            VARCHAR(20),
            gdpr_action_cd BOOLEAN
        ) DUPLICATE KEY(id)
          DISTRIBUTED BY HASH(id) BUCKETS 2
          PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE ${dbName}.${tbl2} (
            id             BIGINT        NOT NULL,
            ssn            VARCHAR(20),
            gdpr_action_cd BOOLEAN
        ) DUPLICATE KEY(id)
          DISTRIBUTED BY HASH(id) BUCKETS 2
          PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO ${dbName}.${tbl1} VALUES
            (1, '111-11-1111', false),
            (2, '222-22-2222', true)"""
    sql """INSERT INTO ${dbName}.${tbl2} VALUES
            (10, '333-33-3333', true)"""

    try {
        // ── 1. Baseline: no policy, real values visible ─────────────────────
        connect(user, "${pwd}", context.config.jdbcUrl) {
            def r = sql """SELECT ssn FROM ${dbName}.${tbl1} ORDER BY id"""
            assertEquals('111-11-1111', r[0][0])
            assertEquals('222-22-2222', r[1][0])
        }

        // ── 2. Exact-table masking policy: mask only flagged rows ───────────
        sql """CREATE MASKING POLICY ${policyExact} ON ${dbName}.${tbl1} (ssn)
               TO '${user}'@'%'
               USING (CASE WHEN gdpr_action_cd THEN NULL ELSE ssn END)"""

        connect(user, "${pwd}", context.config.jdbcUrl) {
            def r = sql """SELECT ssn FROM ${dbName}.${tbl1} ORDER BY id"""
            assertEquals('111-11-1111', r[0][0], "flag=false: real value visible")
            assertEquals(null, r[1][0], "flag=true: value masked to NULL")
        }

        sql """DROP MASKING POLICY ${policyExact} ON ${dbName}.${tbl1} (ssn) FOR '${user}'@'%'"""

        connect(user, "${pwd}", context.config.jdbcUrl) {
            def r = sql """SELECT ssn FROM ${dbName}.${tbl1} ORDER BY id"""
            assertEquals('222-22-2222', r[1][0], "value visible again after DROP")
        }

        // ── 3. DB-level wildcard: one policy covers both tables ─────────────
        sql """CREATE MASKING POLICY ${policyWc} ON ${dbName}.* (ssn)
               TO '${user}'@'%'
               USING (CASE WHEN gdpr_action_cd THEN NULL ELSE ssn END)"""

        connect(user, "${pwd}", context.config.jdbcUrl) {
            def r1 = sql """SELECT ssn FROM ${dbName}.${tbl1} ORDER BY id"""
            assertEquals('111-11-1111', r1[0][0])
            assertEquals(null, r1[1][0])
            def r2 = sql """SELECT ssn FROM ${dbName}.${tbl2} ORDER BY id"""
            assertEquals(null, r2[0][0], "db-wildcard must also cover tbl2")
        }

        // ── 4. Table created AFTER the wildcard policy is auto-covered ──────
        String lateTable = 'late_arrival'
        sql """
            CREATE TABLE ${dbName}.${lateTable} (
                id BIGINT, ssn VARCHAR(20), gdpr_action_cd BOOLEAN
            ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql """INSERT INTO ${dbName}.${lateTable} VALUES (1, '444-44-4444', true)"""

        connect(user, "${pwd}", context.config.jdbcUrl) {
            def r = sql """SELECT ssn FROM ${dbName}.${lateTable}"""
            assertEquals(null, r[0][0], "wildcard policy must cover a table created after it was set")
        }

        // ── 5. Exact-table policy takes precedence over the active wildcard ─
        sql """CREATE MASKING POLICY ${policyExact} ON ${dbName}.${tbl1} (ssn)
               TO '${user}'@'%'
               USING (CONCAT('XXX-XX-', RIGHT(ssn, 4)))"""

        connect(user, "${pwd}", context.config.jdbcUrl) {
            def r = sql """SELECT ssn FROM ${dbName}.${tbl1} ORDER BY id"""
            // If the wildcard (full NULL) had won instead of the exact-table policy
            // (partial reveal), row 2 would be NULL, not a partial value.
            assertEquals('XXX-XX-1111', r[0][0], "exact policy's expression must run, not the wildcard's")
            assertEquals('XXX-XX-2222', r[1][0], "exact policy applies to every row, not just flagged ones")
        }

        // tbl2 has no exact policy, so the wildcard must still be the one applying there.
        connect(user, "${pwd}", context.config.jdbcUrl) {
            def r = sql """SELECT ssn FROM ${dbName}.${tbl2}"""
            assertEquals(null, r[0][0], "tbl2 still resolves via the wildcard policy")
        }

        // ── 6. A user without the targeted role/user sees real values ───────
        connect(otherUser, "${pwd}", context.config.jdbcUrl) {
            def r = sql """SELECT ssn FROM ${dbName}.${tbl1} ORDER BY id"""
            assertEquals('111-11-1111', r[0][0])
            assertEquals('222-22-2222', r[1][0], "opt-in model: a non-targeted user sees real values")
        }

        // ── 7. SHOW MASKING POLICY ───────────────────────────────────────────
        def policies = sql """SHOW MASKING POLICY ON ${dbName}.*"""
        assertTrue(policies.any { row -> row[0] == policyWc },
                "SHOW MASKING POLICY must list the wildcard policy")

    } finally {
        try_sql """DROP MASKING POLICY IF EXISTS ${policyExact} ON ${dbName}.${tbl1} (ssn) FOR '${user}'@'%'"""
        try_sql """DROP MASKING POLICY IF EXISTS ${policyWc}    ON ${dbName}.*       (ssn) FOR '${user}'@'%'"""
        try_sql("DROP DATABASE IF EXISTS ${dbName}")
        try_sql("DROP USER IF EXISTS '${user}'@'%'")
        try_sql("DROP USER IF EXISTS '${otherUser}'@'%'")
    }
}
