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

package org.apache.doris.dr;

import org.apache.doris.nereids.trees.plans.commands.DeleteFromCommand;
import org.apache.doris.nereids.trees.plans.commands.DeleteFromUsingCommand;
import org.apache.doris.nereids.trees.plans.commands.DropTableCommand;
import org.apache.doris.nereids.trees.plans.commands.TruncateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.UpdateCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

/**
 * Determines whether a logical plan represents a write operation
 * that must be blocked on a STANDBY cluster.
 *
 * SELECT, SHOW, DESCRIBE, USE, SET, EXPLAIN are always allowed.
 * All DML and DDL that mutates state are blocked.
 */
public class DRReadOnlyGuard {

    private DRReadOnlyGuard() {}

    /**
     * Returns true if the plan is safe to execute on a read-only (STANDBY) cluster.
     * Returns false if the plan is a write that must be rejected.
     */
    public static boolean isReadOnlyStatement(LogicalPlan plan) {
        // DML writes
        if (plan instanceof InsertIntoTableCommand) {
            return false;
        }
        if (plan instanceof InsertOverwriteTableCommand) {
            return false;
        }
        if (plan instanceof UpdateCommand) {
            return false;
        }
        if (plan instanceof DeleteFromCommand) {
            return false;
        }
        if (plan instanceof DeleteFromUsingCommand) {
            return false;
        }
        // DDL that mutates table structure or data
        if (plan instanceof DropTableCommand) {
            return false;
        }
        if (plan instanceof TruncateTableCommand) {
            return false;
        }
        // Everything else (SELECT, SHOW, EXPLAIN, USE, SET, DESCRIBE) is allowed
        return true;
    }
}
