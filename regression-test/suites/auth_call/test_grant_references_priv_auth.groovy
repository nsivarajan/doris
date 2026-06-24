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

suite("test_grant_references_priv_auth", "p0,auth_call") {

    String user = 'test_references_priv_user'
    String pwd = 'C123_567p'
    String dbName = 'test_references_priv_db'
    String tableName = 'test_references_priv_tb'

    // Remove any leftovers from a prior failed run before creating fresh objects.
    try_sql("DROP USER IF EXISTS ${user}")
    try_sql("DROP DATABASE IF EXISTS ${dbName}")

    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""

    // Cloud mode requires compute group access for any connection.
    if (isCloudMode()) {
        def clusters = sql "SHOW CLUSTERS;"
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user}"""
    }

    sql """CREATE DATABASE ${dbName}"""
    sql """CREATE TABLE ${dbName}.${tableName} (
            id       BIGINT        NOT NULL,
            username VARCHAR(64)   NOT NULL,
            amount   DECIMAL(10,2)
          )
          DUPLICATE KEY(id)
          DISTRIBUTED BY HASH(id) BUCKETS 2
          PROPERTIES ("replication_num" = "1")"""

    sql """INSERT INTO ${dbName}.${tableName} VALUES (1, 'alice', 100.00), (2, 'bob', 200.00)"""

    try {
        // ── Before grant: no access at all ─────────────────────────────────
        connect(user, "${pwd}", context.config.jdbcUrl) {
            test {
                sql """DESCRIBE ${dbName}.${tableName}"""
                exception "denied"
            }
            test {
                sql """SELECT * FROM ${dbName}.${tableName}"""
                exception "denied"
            }
        }

        // ── Global level: schema access allowed, data access denied ─────────
        sql """GRANT REFERENCES_PRIV ON *.* TO '${user}'@'%'"""

        connect(user, "${pwd}", context.config.jdbcUrl) {
            sql """SHOW DATABASES"""
            sql """SHOW TABLES FROM ${dbName}"""
            sql """DESCRIBE ${dbName}.${tableName}"""
            sql """SHOW CREATE TABLE ${dbName}.${tableName}"""

            test {
                sql """SELECT * FROM ${dbName}.${tableName}"""
                exception "denied"
            }
            test {
                sql """SELECT COUNT(*) FROM ${dbName}.${tableName}"""
                exception "denied"
            }
            test {
                sql """INSERT INTO ${dbName}.${tableName} VALUES (3, 'carol', 300.00)"""
                exception "denied"
            }
        }

        // ── Revoke global: schema access removed ─────────────────────────────
        sql """REVOKE REFERENCES_PRIV ON *.* FROM '${user}'@'%'"""

        connect(user, "${pwd}", context.config.jdbcUrl) {
            test {
                sql """DESCRIBE ${dbName}.${tableName}"""
                exception "denied"
            }
        }

        // ── Database level: schema access scoped to dbName ───────────────────
        sql """GRANT REFERENCES_PRIV ON ${dbName}.* TO '${user}'@'%'"""

        connect(user, "${pwd}", context.config.jdbcUrl) {
            sql """SHOW TABLES FROM ${dbName}"""
            sql """DESCRIBE ${dbName}.${tableName}"""
            test {
                sql """SELECT * FROM ${dbName}.${tableName}"""
                exception "denied"
            }
        }

        // ── Revoke database-level grant ───────────────────────────────────────
        sql """REVOKE REFERENCES_PRIV ON ${dbName}.* FROM '${user}'@'%'"""

        // ── Table level: schema access scoped to a single table ───────────────
        // REFERENCES_PRIV context is GLOBAL,CATALOG,DATABASE,TABLE so table-level
        // grants are valid. DataHub commonly uses per-table schema grants.
        sql """GRANT REFERENCES_PRIV ON ${dbName}.${tableName} TO '${user}'@'%'"""

        connect(user, "${pwd}", context.config.jdbcUrl) {
            sql """DESCRIBE ${dbName}.${tableName}"""
            sql """SHOW CREATE TABLE ${dbName}.${tableName}"""
            test {
                sql """SELECT * FROM ${dbName}.${tableName}"""
                exception "denied"
            }
        }

        // ── Revoke table-level: access removed ───────────────────────────────
        sql """REVOKE REFERENCES_PRIV ON ${dbName}.${tableName} FROM '${user}'@'%'"""

        connect(user, "${pwd}", context.config.jdbcUrl) {
            test {
                sql """DESCRIBE ${dbName}.${tableName}"""
                exception "denied"
            }
        }

    } finally {
        // Always clean up regardless of test outcome to avoid polluting the cluster.
        try_sql("DROP DATABASE IF EXISTS ${dbName}")
        try_sql("DROP USER IF EXISTS ${user}")
    }
}
