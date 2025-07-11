// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.qe;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.alter.SystemHandler;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.ParseNode;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.Database;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.StarRocksException;
import com.starrocks.datacache.DataCacheMgr;
import com.starrocks.datacache.DataCacheSelectExecutor;
import com.starrocks.datacache.DataCacheSelectMetrics;
import com.starrocks.load.EtlJobType;
import com.starrocks.plugin.PluginInfo;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AdminCancelRepairTableStmt;
import com.starrocks.sql.ast.AdminCheckTabletsStmt;
import com.starrocks.sql.ast.AdminRepairTableStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOffStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOnStmt;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.sql.ast.AdminSetPartitionVersionStmt;
import com.starrocks.sql.ast.AdminSetReplicaStatusStmt;
import com.starrocks.sql.ast.AlterCatalogStmt;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterDatabaseRenameStatement;
import com.starrocks.sql.ast.AlterLoadStmt;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterResourceGroupStmt;
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.AlterRoleStmt;
import com.starrocks.sql.ast.AlterRoutineLoadStmt;
import com.starrocks.sql.ast.AlterStorageVolumeStmt;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BackupStmt;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;
import com.starrocks.sql.ast.CancelAlterSystemStmt;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.ast.CancelBackupStmt;
import com.starrocks.sql.ast.CancelCompactionStmt;
import com.starrocks.sql.ast.CancelExportStmt;
import com.starrocks.sql.ast.CancelLoadStmt;
import com.starrocks.sql.ast.CancelRefreshDictionaryStmt;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStmt;
import com.starrocks.sql.ast.CleanTemporaryTableStmt;
import com.starrocks.sql.ast.ClearDataCacheRulesStmt;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateDataCacheRuleStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateDictionaryStmt;
import com.starrocks.sql.ast.CreateFileStmt;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.CreateRepositoryStmt;
import com.starrocks.sql.ast.CreateResourceGroupStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.CreateStorageVolumeStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateTemporaryTableLikeStmt;
import com.starrocks.sql.ast.CreateTemporaryTableStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.ast.DropAnalyzeJobStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.DropDataCacheRuleStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropDictionaryStmt;
import com.starrocks.sql.ast.DropFileStmt;
import com.starrocks.sql.ast.DropFunctionStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropRepositoryStmt;
import com.starrocks.sql.ast.DropResourceGroupStmt;
import com.starrocks.sql.ast.DropResourceStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.DropStorageVolumeStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.DropTaskStmt;
import com.starrocks.sql.ast.DropTemporaryTableStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.InstallPluginStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.PauseRoutineLoadStmt;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.RecoverPartitionStmt;
import com.starrocks.sql.ast.RecoverTableStmt;
import com.starrocks.sql.ast.RefreshDictionaryStmt;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.RestoreStmt;
import com.starrocks.sql.ast.ResumeRoutineLoadStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.sql.ast.SetDefaultStorageVolumeStmt;
import com.starrocks.sql.ast.SetUserPropertyStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.StopRoutineLoadStmt;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.SyncStmt;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.ast.UninstallPluginStmt;
import com.starrocks.sql.ast.group.CreateGroupProviderStmt;
import com.starrocks.sql.ast.group.DropGroupProviderStmt;
import com.starrocks.sql.ast.integration.AlterSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.CreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.DropSecurityIntegrationStatement;
import com.starrocks.sql.ast.pipe.AlterPipeStmt;
import com.starrocks.sql.ast.pipe.CreatePipeStmt;
import com.starrocks.sql.ast.pipe.DropPipeStmt;
import com.starrocks.sql.ast.spm.ControlBaselinePlanStmt;
import com.starrocks.sql.ast.spm.CreateBaselinePlanStmt;
import com.starrocks.sql.ast.spm.DropBaselinePlanStmt;
import com.starrocks.sql.ast.warehouse.AlterWarehouseStmt;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ResumeWarehouseStmt;
import com.starrocks.sql.ast.warehouse.SuspendWarehouseStmt;
import com.starrocks.sql.ast.warehouse.cngroup.AlterCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.CreateCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.DropCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.EnableDisableCnGroupStmt;
import com.starrocks.sql.spm.SPMStmtExecutor;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.ExternalAnalyzeJob;
import com.starrocks.statistic.NativeAnalyzeJob;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DDLStmtExecutor {

    private final StmtExecutorVisitor stmtExecutorVisitor;

    public DDLStmtExecutor(StmtExecutorVisitor stmtExecutorVisitor) {
        this.stmtExecutorVisitor = stmtExecutorVisitor;
    }

    /**
     * Execute various ddl statement
     */
    public static ShowResultSet execute(StatementBase stmt, ConnectContext context) throws Exception {
        try {
            return GlobalStateMgr.getCurrentState().getDdlStmtExecutor().stmtExecutorVisitor.visit(stmt, context);
        } catch (RuntimeException re) {
            if (re.getCause() instanceof DdlException) {
                throw (DdlException) re.getCause();
            } else if (re.getCause() instanceof IOException) {
                throw (IOException) re.getCause();
            } else if (re.getCause() != null) {
                throw new DdlException(re.getCause().getMessage() != null ?
                        re.getCause().getMessage() : re.getMessage(), re);
            } else {
                throw re;
            }
        }
    }

    public static class StmtExecutorVisitor implements AstVisitor<ShowResultSet, ConnectContext> {
        private static final Logger LOG = LogManager.getLogger(StmtExecutorVisitor.class);
        private static final StmtExecutorVisitor INSTANCE = new StmtExecutorVisitor();

        public static StmtExecutorVisitor getInstance() {
            return INSTANCE;
        }

        protected StmtExecutorVisitor() {
        }

        @Override
        public ShowResultSet visitNode(ParseNode node, ConnectContext context) {
            throw new RuntimeException(new DdlException("unsupported statement: " + node.toSql()));
        }

        @Override
        public ShowResultSet visitCreateDbStatement(CreateDbStmt stmt, ConnectContext context) {
            String fullDbName = stmt.getFullDbName();
            String catalogName = stmt.getCatalogName();
            Map<String, String> properties = stmt.getProperties();
            boolean isSetIfNotExists = stmt.isSetIfNotExists();
            ErrorReport.wrapWithRuntimeException(() -> {
                try {
                    context.getGlobalStateMgr().getMetadataMgr().createDb(context, catalogName, fullDbName, properties);
                } catch (AlreadyExistsException e) {
                    if (isSetIfNotExists) {
                        LOG.info("create database[{}] which already exists", fullDbName);
                    } else {
                        ErrorReport.reportDdlException(ErrorCode.ERR_DB_CREATE_EXISTS, fullDbName);
                    }
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropDbStatement(DropDbStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                String catalogName = stmt.getCatalogName();
                String dbName = stmt.getDbName();
                boolean isForceDrop = stmt.isForceDrop();
                try {
                    context.getGlobalStateMgr().getMetadataMgr().dropDb(context, catalogName, dbName, isForceDrop);
                } catch (MetaNotFoundException e) {
                    if (stmt.isSetIfExists()) {
                        LOG.info("drop database[{}] which does not exist", dbName);
                    } else {
                        ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
                    }
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateFunctionStatement(CreateFunctionStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                FunctionName name = stmt.getFunctionName();
                if (name.isGlobalFunction()) {
                    context.getGlobalStateMgr()
                            .getGlobalFunctionMgr()
                            .userAddFunction(stmt.getFunction(), stmt.shouldReplaceIfExists(), stmt.createIfNotExists());
                } else {
                    Database db = context.getGlobalStateMgr().getLocalMetastore().getDb(name.getDb());
                    if (db == null) {
                        ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, name.getDb());
                    }
                    db.addFunction(stmt.getFunction(), stmt.shouldReplaceIfExists(), stmt.createIfNotExists());
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropFunctionStatement(DropFunctionStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                FunctionName name = stmt.getFunctionName();
                if (name.isGlobalFunction()) {
                    context.getGlobalStateMgr().getGlobalFunctionMgr()
                            .userDropFunction(stmt.getFunctionSearchDesc(), stmt.dropIfExists());
                } else {
                    Database db = context.getGlobalStateMgr().getLocalMetastore().getDb(name.getDb());
                    if (db == null) {
                        ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, name.getDb());
                    }
                    db.dropFunction(stmt.getFunctionSearchDesc(), stmt.dropIfExists());
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateTableStatement(CreateTableStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getMetadataMgr().createTable(context, stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateTemporaryTableStatement(CreateTemporaryTableStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getMetadataMgr().createTemporaryTable(context, stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateTableLikeStatement(CreateTableLikeStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getMetadataMgr().createTableLike(context, stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateTemporaryTableLikeStatement(
                CreateTemporaryTableLikeStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getMetadataMgr()
                        .createTemporaryTable(context, (CreateTemporaryTableStmt) stmt.getCreateTableStmt());
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropTableStatement(DropTableStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                if (stmt.getTemporaryTableMark()) {
                    DropTemporaryTableStmt dropTemporaryTableStmt = new DropTemporaryTableStmt(
                            stmt.isSetIfExists(), stmt.getTbl(), stmt.isForceDrop());
                    dropTemporaryTableStmt.setSessionId(context.getSessionId());
                    context.getGlobalStateMgr().getMetadataMgr().dropTemporaryTable(dropTemporaryTableStmt);
                } else {
                    context.getGlobalStateMgr().getMetadataMgr().dropTable(context, stmt);
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropTemporaryTableStatement(DropTemporaryTableStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getMetadataMgr().dropTemporaryTable(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCleanTemporaryTableStatement(CleanTemporaryTableStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getMetadataMgr().cleanTemporaryTables(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateMaterializedViewStmt(CreateMaterializedViewStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getLocalMetastore().createMaterializedView(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateMaterializedViewStatement(CreateMaterializedViewStatement stmt,
                                                                  ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getLocalMetastore().createMaterializedView(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropMaterializedViewStatement(DropMaterializedViewStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getLocalMetastore().dropMaterializedView(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterMaterializedViewStatement(AlterMaterializedViewStmt stmt,
                                                                 ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getLocalMetastore().alterMaterializedView(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitRefreshMaterializedViewStatement(RefreshMaterializedViewStatement stmt,
                                                                   ConnectContext context) {
            List<String> info = Lists.newArrayList();
            ErrorReport.wrapWithRuntimeException(() -> {
                // The priority of manual refresh is higher than that of general refresh
                String taskRunId = context.getGlobalStateMgr().getLocalMetastore().refreshMaterializedView(stmt);
                info.add(taskRunId);
            });

            return new ShowResultSet(RefreshMaterializedViewStatement.META_DATA, Arrays.asList(info));
        }

        @Override
        public ShowResultSet visitCancelRefreshMaterializedViewStatement(CancelRefreshMaterializedViewStmt stmt,
                                                                         ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getLocalMetastore().cancelRefreshMaterializedView(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterTableStatement(AlterTableStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getMetadataMgr().alterTable(context, stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterViewStatement(AlterViewStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getMetadataMgr().alterView(context, stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCancelAlterTableStatement(CancelAlterTableStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getLocalMetastore().cancelAlter(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitLoadStatement(LoadStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                EtlJobType jobType = stmt.getEtlJobType();
                if (jobType == EtlJobType.UNKNOWN) {
                    throw new DdlException("Unknown load job type");
                }
                if (jobType == EtlJobType.HADOOP && Config.disable_hadoop_load) {
                    throw new DdlException("Load job by hadoop cluster is disabled."
                            + " Try using broker load. See 'help broker load;'");
                }

                context.getGlobalStateMgr().getLoadMgr().createLoadJobFromStmt(stmt, context);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCancelLoadStatement(CancelLoadStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getLoadMgr().cancelLoadJob(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCancelCompactionStatement(CancelCompactionStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getCompactionMgr().cancelCompaction(stmt.getTxnId());
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateRoutineLoadStatement(CreateRoutineLoadStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getRoutineLoadMgr().createRoutineLoadJob(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitPauseRoutineLoadStatement(PauseRoutineLoadStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getRoutineLoadMgr().pauseRoutineLoadJob(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitResumeRoutineLoadStatement(ResumeRoutineLoadStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getRoutineLoadMgr().resumeRoutineLoadJob(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitStopRoutineLoadStatement(StopRoutineLoadStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getRoutineLoadMgr().stopRoutineLoadJob(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterRoutineLoadStatement(AlterRoutineLoadStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getRoutineLoadMgr().alterRoutineLoadJob(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterLoadStatement(AlterLoadStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getLoadMgr().alterLoadJob(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateUserStatement(CreateUserStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getAuthenticationMgr().createUser(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterUserStatement(AlterUserStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getAuthenticationMgr()
                        .alterUser(stmt.getUserIdentity(), stmt.getAuthenticationInfo(), stmt.getProperties());
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropUserStatement(DropUserStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getAuthenticationMgr().dropUser(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitGrantRevokeRoleStatement(BaseGrantRevokeRoleStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {

                if (stmt instanceof GrantRoleStmt) {
                    context.getGlobalStateMgr().getAuthorizationMgr().grantRole((GrantRoleStmt) stmt);
                } else {
                    context.getGlobalStateMgr().getAuthorizationMgr().revokeRole((RevokeRoleStmt) stmt);
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitGrantRevokePrivilegeStatement(BaseGrantRevokePrivilegeStmt stmt,
                                                                ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                if (stmt instanceof GrantPrivilegeStmt) {

                    context.getGlobalStateMgr().getAuthorizationMgr().grant((GrantPrivilegeStmt) stmt);

                } else {
                    context.getGlobalStateMgr().getAuthorizationMgr().revoke((RevokePrivilegeStmt) stmt);
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateRoleStatement(CreateRoleStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getAuthorizationMgr().createRole(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterRoleStatement(AlterRoleStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getAuthorizationMgr().alterRole(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropRoleStatement(DropRoleStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getAuthorizationMgr().dropRole(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateSecurityIntegrationStatement(CreateSecurityIntegrationStatement stmt,
                                                                     ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
                authenticationMgr.createSecurityIntegration(stmt.getName(), stmt.getPropertyMap(), false);
            });

            return null;
        }

        @Override
        public ShowResultSet visitAlterSecurityIntegrationStatement(AlterSecurityIntegrationStatement stmt,
                                                                    ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
                authenticationMgr.alterSecurityIntegration(stmt.getName(), stmt.getProperties(), false);
            });

            return null;
        }

        @Override
        public ShowResultSet visitDropSecurityIntegrationStatement(DropSecurityIntegrationStatement stmt,
                                                                   ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
                authenticationMgr.dropSecurityIntegration(stmt.getName(), false);
            });

            return null;
        }

        @Override
        public ShowResultSet visitCreateGroupProviderStatement(CreateGroupProviderStmt statement, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
                authenticationMgr.createGroupProviderStatement(statement, context);
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropGroupProviderStatement(DropGroupProviderStmt statement, ConnectContext context) {
            AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
            authenticationMgr.dropGroupProviderStatement(statement, context);
            return null;
        }

        @Override
        public ShowResultSet visitSetUserPropertyStatement(SetUserPropertyStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getAuthenticationMgr().updateUserProperty(stmt.getUser(),
                        stmt.getPropertyPairList());

            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterSystemStatement(AlterSystemStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                SystemHandler systemHandler = GlobalStateMgr.getCurrentState().getAlterJobMgr().getClusterHandler();
                systemHandler.process(Collections.singletonList(stmt.getAlterClause()), null, null);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCancelAlterSystemStatement(CancelAlterSystemStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getAlterJobMgr().getClusterHandler().cancel(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterDatabaseQuotaStatement(AlterDatabaseQuotaStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getLocalMetastore().alterDatabaseQuota(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterDatabaseRenameStatement(AlterDatabaseRenameStatement stmt,
                                                               ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getLocalMetastore().renameDatabase(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitRecoverDbStatement(RecoverDbStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getLocalMetastore().recoverDatabase(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitRecoverTableStatement(RecoverTableStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getLocalMetastore().recoverTable(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitRecoverPartitionStatement(RecoverPartitionStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getLocalMetastore().recoverPartition(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateViewStatement(CreateViewStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                GlobalStateMgr.getCurrentState().getMetadataMgr().createView(context, stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitBackupStatement(BackupStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getBackupHandler().process(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitRestoreStatement(RestoreStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getBackupHandler().process(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCancelBackupStatement(CancelBackupStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getBackupHandler().cancel(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateRepositoryStatement(CreateRepositoryStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getBackupHandler().createRepository(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropRepositoryStatement(DropRepositoryStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getBackupHandler().dropRepository(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitSyncStatement(SyncStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
            });
            return null;
        }

        @Override
        public ShowResultSet visitTruncateTableStatement(TruncateTableStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getLocalMetastore().truncateTable(stmt, context);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAdminRepairTableStatement(AdminRepairTableStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getTabletChecker().repairTable(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAdminCancelRepairTableStatement(AdminCancelRepairTableStmt stmt,
                                                                  ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getTabletChecker().cancelRepairTable(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAdminSetConfigStatement(AdminSetConfigStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                ConfigBase.setConfig(stmt);
                if (stmt.getConfig().containsKey("mysql_server_version")) {
                    String version = stmt.getConfig().getMap().get("mysql_server_version");
                    if (!Strings.isNullOrEmpty(version)) {
                        GlobalVariable.version = version;
                    }
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateFileStatement(CreateFileStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getSmallFileMgr().createFile(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropFileStatement(DropFileStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getSmallFileMgr().dropFile(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitInstallPluginStatement(InstallPluginStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                try {
                    context.getGlobalStateMgr().getPluginMgr().installPlugin(stmt);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitUninstallPluginStatement(UninstallPluginStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                try {
                    PluginInfo info = context.getGlobalStateMgr().getPluginMgr().uninstallPlugin(stmt.getPluginName());
                    if (null != info) {
                        GlobalStateMgr.getCurrentState().getEditLog().logUninstallPlugin(info);
                    }
                    LOG.info("uninstall plugin = " + stmt.getPluginName());

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitAdminCheckTabletsStatement(AdminCheckTabletsStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getLocalMetastore().checkTablets(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAdminSetPartitionVersionStmt(AdminSetPartitionVersionStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(()
                    -> context.getGlobalStateMgr().getLocalMetastore().setPartitionVersion(stmt));
            return null;
        }

        @Override
        public ShowResultSet visitAdminSetReplicaStatusStatement(AdminSetReplicaStatusStmt stmt,
                                                                 ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getLocalMetastore().setReplicaStatus(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateResourceStatement(CreateResourceStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getResourceMgr().createResource(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropResourceStatement(DropResourceStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getResourceMgr().dropResource(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterResourceStatement(AlterResourceStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getResourceMgr().alterResource(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCancelExportStatement(CancelExportStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getExportMgr().cancelExportJob(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateAnalyzeJobStatement(CreateAnalyzeJobStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                AnalyzeJob analyzeJob;
                if (stmt.isNative()) {
                    analyzeJob = new NativeAnalyzeJob(stmt.getDbId(),
                            stmt.getTableId(),
                            stmt.getColumnNames(),
                            stmt.getColumnTypes(),
                            stmt.getAnalyzeType(),
                            StatsConstants.ScheduleType.SCHEDULE,
                            stmt.getProperties(), StatsConstants.ScheduleStatus.PENDING,
                            LocalDateTime.MIN);
                } else {
                    analyzeJob = new ExternalAnalyzeJob(stmt.getTableName().getCatalog(), stmt.getTableName().getDb(),
                            stmt.getTableName().getTbl(), stmt.getColumnNames(),
                            stmt.getColumnTypes(),
                            stmt.getAnalyzeType(),
                            StatsConstants.ScheduleType.SCHEDULE,
                            stmt.getProperties(), StatsConstants.ScheduleStatus.PENDING,
                            LocalDateTime.MIN);
                }
                boolean isSetIfNotExists = stmt.isSetIfNotExists();
                try {
                    context.getGlobalStateMgr().getAnalyzeMgr().addAnalyzeJob(analyzeJob);
                } catch (AlreadyExistsException e) {
                    if (isSetIfNotExists) {
                        LOG.info("analyze job already exists");
                    } else {
                        ErrorReport.reportDdlException(ErrorCode.ERR_ANLZ_JOB_EXISTED_ERROR);
                    }
                }

                if (Config.enable_trigger_analyze_job_immediate) {
                    ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
                    // from current session, may execute analyze stmt
                    statsConnectCtx.getSessionVariable().setStatisticCollectParallelism(
                            context.getSessionVariable().getStatisticCollectParallelism());
                    Thread thread = new Thread(() -> {
                        statsConnectCtx.setThreadLocalInfo();
                        StatisticExecutor statisticExecutor = new StatisticExecutor();
                        analyzeJob.run(statsConnectCtx, statisticExecutor);
                    });
                    thread.start();
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropAnalyzeStatement(DropAnalyzeJobStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(
                    () -> context.getGlobalStateMgr().getAnalyzeMgr().removeAnalyzeJob(stmt.getId()));
            return null;
        }

        @Override
        public ShowResultSet visitRefreshTableStatement(RefreshTableStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().refreshExternalTable(context, stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateResourceGroupStatement(CreateResourceGroupStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getResourceGroupMgr().createResourceGroup(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropResourceGroupStatement(DropResourceGroupStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getResourceGroupMgr().dropResourceGroup(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterResourceGroupStatement(AlterResourceGroupStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getResourceGroupMgr().alterResourceGroup(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateCatalogStatement(CreateCatalogStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                String catalogName = stmt.getCatalogName();
                if (context.getGlobalStateMgr().getCatalogMgr().catalogExists(catalogName)) {
                    if (stmt.isIfNotExists()) {
                        LOG.info("create catalog[{}] which already exists", catalogName);
                        return;
                    } else {
                        ErrorReport.reportDdlException(ErrorCode.ERR_CATALOG_EXISTED_ERROR, catalogName);
                    }
                }
                context.getGlobalStateMgr().getCatalogMgr().createCatalog(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropCatalogStatement(DropCatalogStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                String catalogName = stmt.getName();
                if (!context.getGlobalStateMgr().getCatalogMgr().catalogExists(catalogName)) {
                    if (stmt.isIfExists()) {
                        LOG.info("drop catalog[{}] which does not exist", catalogName);
                        return;
                    } else {
                        ErrorReport.reportDdlException(ErrorCode.ERR_BAD_CATALOG_ERROR, catalogName);
                    }
                }
                context.getGlobalStateMgr().getCatalogMgr().dropCatalog(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterCatalogStatement(AlterCatalogStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getCatalogMgr().alterCatalog(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitSubmitTaskStatement(SubmitTaskStmt stmt, ConnectContext context) {
            try {
                return context.getGlobalStateMgr().getTaskManager().handleSubmitTaskStmt(stmt);
            } catch (StarRocksException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public ShowResultSet visitDropTaskStmt(DropTaskStmt dropTaskStmt, ConnectContext context) {
            TaskManager taskManager = context.getGlobalStateMgr().getTaskManager();
            String taskName = dropTaskStmt.getTaskName().getName();
            if (!taskManager.containTask(taskName)) {
                if (dropTaskStmt.isSetIfExists()) {
                    return null;
                }
                throw new SemanticException("Task " + taskName + " is not exist");
            }
            Task task = taskManager.getTask(taskName);
            if (task.getSource() == Constants.TaskSource.MV && !dropTaskStmt.isForce()) {
                throw new RuntimeException("Can not drop task generated by materialized view. You can use " +
                        "DROP MATERIALIZED VIEW to drop task, when the materialized view is deleted, " +
                        "the related task will be deleted automatically.");
            }
            taskManager.dropTasks(Collections.singletonList(task.getId()), false);
            return null;
        }

        @Override
        public ShowResultSet visitCreateStorageVolumeStatement(CreateStorageVolumeStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                try {
                    context.getGlobalStateMgr().getStorageVolumeMgr().createStorageVolume(stmt);
                } catch (AlreadyExistsException e) {
                    if (stmt.isSetIfNotExists()) {
                        LOG.info("create storage volume[{}] which already exists", stmt.getName());
                    } else {
                        throw e;
                    }
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterStorageVolumeStatement(AlterStorageVolumeStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() ->
                    context.getGlobalStateMgr().getStorageVolumeMgr().updateStorageVolume(stmt)
            );
            return null;
        }

        @Override
        public ShowResultSet visitDropStorageVolumeStatement(DropStorageVolumeStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                try {
                    context.getGlobalStateMgr().getStorageVolumeMgr().removeStorageVolume(stmt);
                } catch (MetaNotFoundException e) {
                    if (stmt.isSetIfExists()) {
                        LOG.info("drop storage volume[{}] which does not exist", stmt.getName());
                    } else {
                        throw e;
                    }
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitSetDefaultStorageVolumeStatement(SetDefaultStorageVolumeStmt stmt,
                                                                   ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() ->
                    context.getGlobalStateMgr().getStorageVolumeMgr().setDefaultStorageVolume(stmt)
            );
            return null;
        }

        //=========================================== Pipe Statement ==================================================
        @Override
        public ShowResultSet visitCreatePipeStatement(CreatePipeStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() ->
                    context.getGlobalStateMgr().getPipeManager().createPipe(stmt)
            );
            return null;
        }

        @Override
        public ShowResultSet visitDropPipeStatement(DropPipeStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() ->
                    context.getGlobalStateMgr().getPipeManager().dropPipe(stmt)
            );
            return null;
        }

        @Override
        public ShowResultSet visitAlterPipeStatement(AlterPipeStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() ->
                    context.getGlobalStateMgr().getPipeManager().alterPipe(stmt)
            );
            return null;
        }

        // ==========================================Data Cache Management==============================================
        @Override
        public ShowResultSet visitCreateDataCacheRuleStatement(CreateDataCacheRuleStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                DataCacheMgr.getInstance().createCacheRule(stmt.getTarget(), stmt.getPredicates(), stmt.getPriority(),
                        stmt.getProperties());
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropDataCacheRuleStatement(DropDataCacheRuleStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                DataCacheMgr.getInstance().dropCacheRule(stmt.getCacheRuleId());
            });
            return null;
        }

        @Override
        public ShowResultSet visitClearDataCacheRulesStatement(ClearDataCacheRulesStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                DataCacheMgr.getInstance().clearRules();
            });
            return null;
        }

        @Override
        public ShowResultSet visitDataCacheSelectStatement(DataCacheSelectStatement statement, ConnectContext context) {
            DataCacheSelectMetrics metrics = null;
            try {
                metrics = DataCacheSelectExecutor.cacheSelect(statement, context);
            } catch (Exception e) {
                LOG.warn("Failed to execute cacheSelect", e);
                throw new RuntimeException(e.getMessage());
            }

            return metrics.getShowResultSet(statement.isVerbose());
        }

        //=========================================== Dictionary Statement ==================================================
        @Override
        public ShowResultSet visitCreateDictionaryStatement(CreateDictionaryStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getDictionaryMgr().createDictionary(stmt,
                        context.getCurrentCatalog(), context.getDatabase());
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropDictionaryStatement(DropDictionaryStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getDictionaryMgr().dropDictionary(stmt.getDictionaryName(),
                        stmt.isCacheOnly(), false);
            });
            return null;
        }

        @Override
        public ShowResultSet visitRefreshDictionaryStatement(RefreshDictionaryStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getDictionaryMgr().refreshDictionary(stmt.getDictionaryName());
            });
            return null;
        }

        @Override
        public ShowResultSet visitCancelRefreshDictionaryStatement(CancelRefreshDictionaryStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getDictionaryMgr().cancelRefreshDictionary(stmt.getDictionaryName());
            });
            return null;
        }

        //=========================================== Warehouse Statement ==================================================

        @Override
        public ShowResultSet visitCreateWarehouseStatement(CreateWarehouseStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                WarehouseManager warehouseMgr = context.getGlobalStateMgr().getWarehouseMgr();
                warehouseMgr.createWarehouse(stmt);

            });
            return null;
        }

        @Override
        public ShowResultSet visitSuspendWarehouseStatement(SuspendWarehouseStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                WarehouseManager warehouseMgr = context.getGlobalStateMgr().getWarehouseMgr();
                warehouseMgr.suspendWarehouse(stmt);

            });
            return null;
        }

        @Override
        public ShowResultSet visitResumeWarehouseStatement(ResumeWarehouseStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                WarehouseManager warehouseMgr = context.getGlobalStateMgr().getWarehouseMgr();
                warehouseMgr.resumeWarehouse(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropWarehouseStatement(DropWarehouseStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                WarehouseManager warehouseMgr = context.getGlobalStateMgr().getWarehouseMgr();
                warehouseMgr.dropWarehouse(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterWarehouseStatement(AlterWarehouseStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                WarehouseManager warehouseMgr = context.getGlobalStateMgr().getWarehouseMgr();
                warehouseMgr.alterWarehouse(stmt);
            });
            return null;
        }

        //=========================================== CNGroup Statement ==================================================
        @Override
        public ShowResultSet visitCreateCNGroupStatement(CreateCnGroupStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                WarehouseManager warehouseMgr = context.getGlobalStateMgr().getWarehouseMgr();
                warehouseMgr.createCnGroup(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropCNGroupStatement(DropCnGroupStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                WarehouseManager warehouseMgr = context.getGlobalStateMgr().getWarehouseMgr();
                warehouseMgr.dropCnGroup(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitEnableDisableCNGroupStatement(EnableDisableCnGroupStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                WarehouseManager warehouseMgr = context.getGlobalStateMgr().getWarehouseMgr();
                if (stmt.isSetEnable()) {
                    warehouseMgr.enableCnGroup(stmt);
                } else {
                    warehouseMgr.disableCnGroup(stmt);
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterCNGroupStatement(AlterCnGroupStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                WarehouseManager warehouseMgr = context.getGlobalStateMgr().getWarehouseMgr();
                warehouseMgr.alterCnGroup(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAdminSetAutomatedSnapshotOnStatement(AdminSetAutomatedSnapshotOnStmt stmt,
                                                                     ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getClusterSnapshotMgr().setAutomatedSnapshotOn(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAdminSetAutomatedSnapshotOffStatement(AdminSetAutomatedSnapshotOffStmt stmt,
                                                                        ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getClusterSnapshotMgr().setAutomatedSnapshotOff(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateBaselinePlanStatement(CreateBaselinePlanStmt statement,
                                                              ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> SPMStmtExecutor.execute(context, statement));
            return null;
        }

        @Override
        public ShowResultSet visitDropBaselinePlanStatement(DropBaselinePlanStmt statement, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> SPMStmtExecutor.execute(context, statement));
            return null;
        }

        public ShowResultSet visitControlBaselinePlanStatement(ControlBaselinePlanStmt statement,
                                                               ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> SPMStmtExecutor.execute(context, statement));
            return null;
        }
    }
}
