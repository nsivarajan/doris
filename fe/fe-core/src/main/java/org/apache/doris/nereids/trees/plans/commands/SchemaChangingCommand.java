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

package org.apache.doris.nereids.trees.plans.commands;

/**
 * Marker interface for commands that modify catalog metadata (schemas, tables,
 * databases, partitions, indexes, views, functions, catalogs).
 *
 * <p>Commands implementing this interface hold the snapshot quiesce READ permit
 * for their entire duration so that ADMIN CREATE CLUSTER SNAPSHOT can acquire
 * the exclusive WRITE permit and capture a consistent BDB-JE + FDB anchor point.
 *
 * <p>Implement this in any Command subclass that calls
 * {@code editLog.logXxx()} for schema-changing operations.
 *
 * <p>Do NOT implement this in snapshot orchestration commands
 * (AdminCreateClusterSnapshotCommand etc.) — those commands ARE the snapshot
 * mechanism and must not hold the READ permit, since they call
 * {@code quiesceForSnapshot()} which requires the WRITE permit.
 *
 * <p>Phase 1 coverage (experimental): the most common schema-changing commands
 * are listed below. Add new commands here as they are introduced.
 *
 * @see CreateTableCommand
 * @see DropTableCommand
 * @see AlterTableCommand
 * @see CreateDatabaseCommand
 * @see DropDatabaseCommand
 * @see TruncateTableCommand
 * @see AlterViewCommand
 * @see CreateViewCommand
 * @see CreateMTMVCommand
 * @see DropMTMVCommand
 * @see CreateCatalogCommand
 */
public interface SchemaChangingCommand {
}
