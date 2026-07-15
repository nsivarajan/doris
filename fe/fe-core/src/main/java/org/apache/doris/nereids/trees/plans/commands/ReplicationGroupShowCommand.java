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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.rest.ReplicationAction;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * SHOW REPLICATION GROUP STATUS | LAG
 *
 * STATUS columns: group_id, primary_site, this_site, exporter_running, dr_read_only, last_journal_id
 * LAG columns:    group_id, this_site, last_journal_id, dr_read_only, lag_note
 */
public class ReplicationGroupShowCommand extends ShowCommand {
    private static final Logger LOG = LogManager.getLogger(ReplicationGroupShowCommand.class);

    private static final ImmutableList<String> STATUS_TITLES = ImmutableList.of(
            "group_id", "primary_site", "this_site",
            "exporter_running", "dr_read_only", "last_journal_id");

    private static final ImmutableList<String> LAG_TITLES = ImmutableList.of(
            "group_id", "this_site", "last_journal_id", "dr_read_only", "lag_note");

    /** true = SHOW LAG, false = SHOW STATUS */
    private final boolean showLag;

    public ReplicationGroupShowCommand(boolean showLag) {
        super(showLag
                ? PlanType.SHOW_REPLICATION_GROUP_LAG_COMMAND
                : PlanType.SHOW_REPLICATION_GROUP_STATUS_COMMAND);
        this.showLag = showLag;
    }

    public boolean isShowLag() {
        return showLag;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        ImmutableList<String> titles = showLag ? LAG_TITLES : STATUS_TITLES;
        for (String title : titles) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(256)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // privilege check
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
            throw new AnalysisException("Access denied; requires ADMIN privilege");
        }

        String groupId = Config.replication_group_id.isEmpty()
                ? "default" : Config.replication_group_id;
        String thisSite = Config.replication_site_name.isEmpty()
                ? "unknown" : Config.replication_site_name;
        boolean drReadOnly = Config.dr_read_only_mode;
        long lastJournalId = ReplicationAction.getLastExportedJournalId();

        List<List<String>> rows = new ArrayList<>();

        if (showLag) {
            // LAG view — lag_note indicates whether this site is behind or current
            String lagNote;
            if (!Config.enable_replication_group) {
                lagNote = "replication not enabled";
            } else if (drReadOnly) {
                lagNote = "dr-standby: consuming from primary export";
            } else {
                lagNote = "primary: exporting at journal_id=" + lastJournalId;
            }
            List<String> row = Lists.newArrayList(
                    groupId,
                    thisSite,
                    String.valueOf(lastJournalId),
                    String.valueOf(drReadOnly),
                    lagNote);
            rows.add(row);
        } else {
            // STATUS view
            boolean exporterRunning = ReplicationAction.getExporterRunning();
            // primary_site: if this FE is primary, it is thisSite; otherwise unknown from this node
            String primarySite = drReadOnly ? "unknown (this is DR standby)" : thisSite;
            List<String> row = Lists.newArrayList(
                    groupId,
                    primarySite,
                    thisSite,
                    String.valueOf(exporterRunning),
                    String.valueOf(drReadOnly),
                    String.valueOf(lastJournalId));
            rows.add(row);
        }

        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }
}
