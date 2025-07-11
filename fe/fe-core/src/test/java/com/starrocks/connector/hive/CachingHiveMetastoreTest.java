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

package com.starrocks.connector.hive;

import com.google.common.collect.Lists;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HivePartitionKey;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.connector.DatabaseTableName;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import mockit.Expectations;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.starrocks.connector.hive.RemoteFileInputFormat.ORC;
import static org.apache.hadoop.hive.common.StatsSetupConst.TASK;
import static org.apache.hadoop.hive.common.StatsSetupConst.TOTAL_SIZE;

public class CachingHiveMetastoreTest {
    private HiveMetaClient client;
    private HiveMetastore metastore;
    private ExecutorService executor;
    private long expireAfterWriteSec = 30;
    private long refreshAfterWriteSec = -1;

    @BeforeEach
    public void setUp() throws Exception {
        client = new HiveMetastoreTest.MockedHiveMetaClient();
        metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
        executor = Executors.newFixedThreadPool(5);
    }

    @AfterEach
    public void tearDown() {
        executor.shutdown();
    }

    @Test
    public void testGetAllDatabaseNames() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        List<String> databaseNames = cachingHiveMetastore.getAllDatabaseNames();
        Assertions.assertEquals(Lists.newArrayList("db1", "db2"), databaseNames);
        CachingHiveMetastore queryLevelCache = CachingHiveMetastore.createQueryLevelInstance(cachingHiveMetastore, 100);
        Assertions.assertEquals(Lists.newArrayList("db1", "db2"), queryLevelCache.getAllDatabaseNames());
    }

    @Test
    public void testGetAllTableNames() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        List<String> databaseNames = cachingHiveMetastore.getAllTableNames("xxx");
        Assertions.assertEquals(Lists.newArrayList("table1", "table2"), databaseNames);
    }

    @Test
    public void testGetDb() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        Database database = cachingHiveMetastore.getDb("db1");
        Assertions.assertEquals("db1", database.getFullName());

        try {
            metastore.getDb("db2");
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof StarRocksConnectorException);
        }
    }

    @Test
    public void testGetTable() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        com.starrocks.catalog.Table table = cachingHiveMetastore.getTable("db1", "tbl1");
        HiveTable hiveTable = (HiveTable) table;
        Assertions.assertEquals("db1", hiveTable.getCatalogDBName());
        Assertions.assertEquals("tbl1", hiveTable.getCatalogTableName());
        Assertions.assertEquals(Lists.newArrayList("col1"), hiveTable.getPartitionColumnNames());
        Assertions.assertEquals(Lists.newArrayList("col2"), hiveTable.getDataColumnNames());
        Assertions.assertEquals("hdfs://127.0.0.1:10000/hive", hiveTable.getTableLocation());
        Assertions.assertEquals(ScalarType.INT, hiveTable.getPartitionColumns().get(0).getType());
        Assertions.assertEquals(ScalarType.INT, hiveTable.getBaseSchema().get(0).getType());
        Assertions.assertEquals("hive_catalog", hiveTable.getCatalogName());
    }

    @Test
    public void testGetTransactionalTable() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        // get insert only table
        com.starrocks.catalog.Table table = cachingHiveMetastore.getTable("transactional_db", "insert_only");
        Assertions.assertNotNull(table);
        // get full acid table
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> cachingHiveMetastore.getTable("transactional_db", "full_acid"));
    }

    @Test
    public void testTableExists() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        Assertions.assertTrue(cachingHiveMetastore.tableExists("db1", "tbl1"));
    }

    @Test
    public void testRefreshTable() {
        new Expectations(metastore) {
            {
                metastore.getTable(anyString, "notExistTbl");
                minTimes = 0;
                Throwable targetException = new NoSuchObjectException("no such obj");
                Throwable e = new InvocationTargetException(targetException);
                result = new StarRocksConnectorException("table not exist", e);
            }
        };
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        try {
            cachingHiveMetastore.refreshTable("db1", "notExistTbl", true);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof StarRocksConnectorException);
            Assertions.assertTrue(e.getMessage().contains("invalidated cache"));
        }

        try {
            cachingHiveMetastore.refreshTable("db1", "tbl1", true);
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testRefreshTableSync() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        Assertions.assertFalse(cachingHiveMetastore.tableNameLockMap.containsKey(
                DatabaseTableName.of("db1", "tbl1")));
        try {
            cachingHiveMetastore.refreshTable("db1", "tbl1", true);
        } catch (Exception e) {
            Assertions.fail();
        }
        Assertions.assertTrue(cachingHiveMetastore.tableNameLockMap.containsKey(
                DatabaseTableName.of("db1", "tbl1")));

        try {
            cachingHiveMetastore.refreshTable("db1", "tbl1", true);
        } catch (Exception e) {
            Assertions.fail();
        }

        Assertions.assertEquals(1, cachingHiveMetastore.tableNameLockMap.size());
    }

    @Test
    public void testRefreshTableBackground() throws InterruptedException {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        Assertions.assertFalse(cachingHiveMetastore.tableNameLockMap.containsKey(
                DatabaseTableName.of("db1", "tbl1")));
        try {
            // mock query table tbl1
            List<String> partitionNames = cachingHiveMetastore.getPartitionKeysByValue("db1", "tbl1",
                    HivePartitionValue.ALL_PARTITION_VALUES);
            cachingHiveMetastore.getPartitionsByNames("db1",
                    "tbl1", partitionNames);
            // put table tbl1 in table cache
            cachingHiveMetastore.refreshTable("db1", "tbl1", true);
        } catch (Exception e) {
            Assertions.fail();
        }
        Assertions.assertTrue(cachingHiveMetastore.isTablePresent(DatabaseTableName.of("db1", "tbl1")));

        try {
            cachingHiveMetastore.refreshTableBackground("db1", "tbl1", true);
        } catch (Exception e) {
            Assertions.fail();
        }
        // not skip refresh table, table cache still exist
        Assertions.assertTrue(cachingHiveMetastore.isTablePresent(DatabaseTableName.of("db1", "tbl1")));
        // sleep 1s, background refresh table will be skipped
        Thread.sleep(1000);
        long oldValue = Config.background_refresh_metadata_time_secs_since_last_access_secs;
        // not refresh table, just skip refresh table
        Config.background_refresh_metadata_time_secs_since_last_access_secs = 0;

        try {
            cachingHiveMetastore.refreshTableBackground("db1", "tbl1", true);
        } catch (Exception e) {
            Assertions.fail();
        } finally {
            Config.background_refresh_metadata_time_secs_since_last_access_secs = oldValue;
        }
        // table cache will be removed because of skip refresh table
        Assertions.assertFalse(cachingHiveMetastore.isTablePresent(DatabaseTableName.of("db1", "tbl1")));
    }

    @Test
    public void testRefreshHiveView() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        Assertions.assertFalse(cachingHiveMetastore.tableNameLockMap.containsKey(
                DatabaseTableName.of("db1", "tbl1")));
        try {
            cachingHiveMetastore.refreshView("db1", "hive_view");
        } catch (Exception e) {
            Assertions.fail();
        }
        Assertions.assertTrue(cachingHiveMetastore.tableNameLockMap.containsKey(
                DatabaseTableName.of("db1", "hive_view")));

        new Expectations(metastore) {
            {
                metastore.getTable(anyString, "notExistView");
                minTimes = 0;
                Throwable targetException = new NoSuchObjectException("no such obj");
                Throwable e = new InvocationTargetException(targetException);
                result = new StarRocksConnectorException("table not exist", e);
            }
        };
        try {
            cachingHiveMetastore.refreshView("db1", "notExistView");
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof StarRocksConnectorException);
            Assertions.assertTrue(e.getMessage().contains("invalidated cache"));
        }
    }

    @Test
    public void testGetPartitionKeys() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        Assertions.assertEquals(Lists.newArrayList("col1"), cachingHiveMetastore.getPartitionKeysByValue("db1", "tbl1",
                HivePartitionValue.ALL_PARTITION_VALUES));
    }

    @Test
    public void testGetPartition() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        Partition partition = cachingHiveMetastore.getPartition(
                "db1", "tbl1", Lists.newArrayList("par1"));
        Assertions.assertEquals(ORC, partition.getFileFormat());
        Assertions.assertEquals("100", partition.getParameters().get(TOTAL_SIZE));

        partition = metastore.getPartition("db1", "tbl1", Lists.newArrayList());
        Assertions.assertEquals("100", partition.getParameters().get(TOTAL_SIZE));
    }

    @Test
    public void testGetPartitionByNames() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        List<String> partitionNames = Lists.newArrayList("part1=1/part2=2", "part1=3/part2=4");
        Map<String, Partition> partitions =
                cachingHiveMetastore.getPartitionsByNames("db1", "table1", partitionNames);

        Partition partition1 = partitions.get("part1=1/part2=2");
        Assertions.assertEquals(ORC, partition1.getFileFormat());
        Assertions.assertEquals("100", partition1.getParameters().get(TOTAL_SIZE));
        Assertions.assertEquals("hdfs://127.0.0.1:10000/hive.db/hive_tbl/part1=1/part2=2", partition1.getFullPath());

        Partition partition2 = partitions.get("part1=3/part2=4");
        Assertions.assertEquals(ORC, partition2.getFileFormat());
        Assertions.assertEquals("100", partition2.getParameters().get(TOTAL_SIZE));
        Assertions.assertEquals("hdfs://127.0.0.1:10000/hive.db/hive_tbl/part1=3/part2=4", partition2.getFullPath());
    }

    @Test
    public void testGetTableStatistics() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        HivePartitionStats statistics = cachingHiveMetastore.getTableStatistics("db1", "table1");
        HiveCommonStats commonStats = statistics.getCommonStats();
        Assertions.assertEquals(50, commonStats.getRowNums());
        Assertions.assertEquals(100, commonStats.getTotalFileBytes());
        HiveColumnStats columnStatistics = statistics.getColumnStats().get("col1");
        Assertions.assertEquals(0, columnStatistics.getTotalSizeBytes());
        Assertions.assertEquals(1, columnStatistics.getNumNulls());
        Assertions.assertEquals(2, columnStatistics.getNdv());
    }

    @Test
    public void testGetPartitionStatistics() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        com.starrocks.catalog.Table hiveTable = cachingHiveMetastore.getTable("db1", "table1");
        Map<String, HivePartitionStats> statistics = cachingHiveMetastore.getPartitionStatistics(
                hiveTable, Lists.newArrayList("col1=1", "col1=2"));

        HivePartitionStats stats1 = statistics.get("col1=1");
        HiveCommonStats commonStats1 = stats1.getCommonStats();
        Assertions.assertEquals(50, commonStats1.getRowNums());
        Assertions.assertEquals(100, commonStats1.getTotalFileBytes());
        HiveColumnStats columnStatistics1 = stats1.getColumnStats().get("col2");
        Assertions.assertEquals(0, columnStatistics1.getTotalSizeBytes());
        Assertions.assertEquals(1, columnStatistics1.getNumNulls());
        Assertions.assertEquals(2, columnStatistics1.getNdv());

        HivePartitionStats stats2 = statistics.get("col1=2");
        HiveCommonStats commonStats2 = stats2.getCommonStats();
        Assertions.assertEquals(50, commonStats2.getRowNums());
        Assertions.assertEquals(100, commonStats2.getTotalFileBytes());
        HiveColumnStats columnStatistics2 = stats2.getColumnStats().get("col2");
        Assertions.assertEquals(0, columnStatistics2.getTotalSizeBytes());
        Assertions.assertEquals(2, columnStatistics2.getNumNulls());
        Assertions.assertEquals(5, columnStatistics2.getNdv());

        List<HivePartitionName> partitionNames = Lists.newArrayList(
                HivePartitionName.of("db1", "table1", "col1=1"),
                HivePartitionName.of("db1", "table1", "col1=2"));

        Assertions.assertEquals(2, cachingHiveMetastore.getPresentPartitionsStatistics(partitionNames).size());
    }

    @Test
    public void testPartitionNames() {
        HivePartitionKey hivePartitionKey = new HivePartitionKey();
        hivePartitionKey.pushColumn(new StringLiteral(HiveMetaClient.PARTITION_NULL_VALUE), PrimitiveType.NULL_TYPE);
        List<String> value = PartitionUtil.fromPartitionKey(hivePartitionKey);
        Assertions.assertEquals(HiveMetaClient.PARTITION_NULL_VALUE, value.get(0));
    }

    @Test
    public void testPartitionExist() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        Assertions.assertTrue(cachingHiveMetastore.partitionExists(metastore.getTable("db", "table"), Lists.newArrayList()));
    }

    @Test
    public void testDropPartition() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        cachingHiveMetastore.dropPartition("db", "table", Lists.newArrayList("1"), false);
    }

    @Test
    public void testUpdateTableStats() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        HivePartitionStats partitionStats = HivePartitionStats.empty();
        cachingHiveMetastore.updateTableStatistics("db", "table", ignore -> partitionStats);
    }

    @Test
    public void testUpdatePartitionStats() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        HivePartitionStats partitionStats = HivePartitionStats.empty();
        cachingHiveMetastore.updatePartitionStatistics("db", "table", "p1=1", ignore -> partitionStats);
    }

    @Test
    public void testRefreshTableByEvent() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);

        HiveCommonStats stats = new HiveCommonStats(10, 100, 1);

        // unpartition
        {
            HiveTable table = (HiveTable) cachingHiveMetastore.getTable("db1", "tbl1");
            Partition partition = cachingHiveMetastore.getPartition(
                    "db1", "tbl1", Lists.newArrayList("par1"));
            cachingHiveMetastore.refreshTableByEvent(table, stats, partition);
        }

        // partition
        {
            HiveTable table = (HiveTable) cachingHiveMetastore.getTable("db1", "unpartitioned_table");
            Partition partition = cachingHiveMetastore.getPartition(
                    "db1", "unpartitioned_table", Lists.newArrayList("col1"));
            cachingHiveMetastore.refreshTableByEvent(table, stats, partition);
        }
    }

    @Test
    public void testRefreshPartitionByEvent() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);

        HiveCommonStats stats = new HiveCommonStats(10, 100, 1);
        HivePartitionName hivePartitionName = HivePartitionName.of("db1", "unpartitioned_table", "col1=1");
        Partition partition = cachingHiveMetastore.getPartition(
                "db1", "unpartitioned_table", Lists.newArrayList("col1"));
        cachingHiveMetastore.refreshPartitionByEvent(hivePartitionName, stats, partition);
    }

    @Test
    public void testRefreshPartition() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, true);

        List<HivePartitionName> partitionNames = Lists.newArrayList(
                HivePartitionName.of("db1", "table1", "col1=1"),
                HivePartitionName.of("db1", "table1", "col1=2"));
        cachingHiveMetastore.refreshPartition(partitionNames);
    }

    @Test
    public void testAddPartitionFailed() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        HivePartition hivePartition = HivePartition.builder()
                // Unsupported type
                .setColumns(Lists.newArrayList(new Column("c1", Type.BITMAP)))
                .setStorageFormat(HiveStorageFormat.PARQUET)
                .setDatabaseName("db")
                .setTableName("table")
                .setLocation("location")
                .setValues(Lists.newArrayList("p1=1"))
                .setParameters(new HashMap<>()).build();

        HivePartitionStats hivePartitionStats = HivePartitionStats.empty();
        HivePartitionWithStats hivePartitionWithStats = new HivePartitionWithStats("p1=1", hivePartition, hivePartitionStats);
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> cachingHiveMetastore.addPartitions("db", "table", Lists.newArrayList(hivePartitionWithStats)));
    }

    @Test
    public void testGetCachedName() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        HiveCacheUpdateProcessor processor = new HiveCacheUpdateProcessor(
                "hive_catalog", cachingHiveMetastore, null, null, false, false);
        Assertions.assertTrue(processor.getCachedTableNames().isEmpty());

        processor = new HiveCacheUpdateProcessor("hive_catalog", metastore, null, null, false, false);
        Assertions.assertTrue(processor.getCachedTableNames().isEmpty());
    }

    @Test
    public void testAutoRefreshPartition() throws InterruptedException {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, executor,
                expireAfterWriteSec, 1, 1000L, false);
        HiveTable table = (HiveTable) cachingHiveMetastore.getTable("db1", "tbl1");
        Partition partition = cachingHiveMetastore.getPartition(
                "db1", "tbl1", Lists.newArrayList("par1"));
        HiveTable externalTable = (HiveTable) cachingHiveMetastore.getTable("db1", "external_table");
        Partition externalPartition = cachingHiveMetastore.getPartition(
                "db1", "external_table", Lists.newArrayList("par1"));

        Assertions.assertTrue(cachingHiveMetastore.isCachedExternalTable(
                DatabaseTableName.of("db1", "external_table")));
        Assertions.assertFalse(cachingHiveMetastore.isCachedExternalTable(
                DatabaseTableName.of("db1", "tbl1")));

        // Get partition for 5 times every 1s
        String mangedTableMark = partition.getParameters().get(TASK);
        String externalTableMark = externalPartition.getParameters().get(TASK);
        for (int i = 0; i < 5; i++) {
            partition =
                    cachingHiveMetastore.getPartition("db1", "tbl1", Lists.newArrayList("par1"));
            externalPartition =
                    cachingHiveMetastore.getPartition("db1", "external_table", Lists.newArrayList("par1"));
            Thread.sleep(1000);
        }
        Assertions.assertEquals(partition.getParameters().get(TASK), mangedTableMark);
        Assertions.assertNotEquals(externalPartition.getParameters().get(TASK), externalTableMark);
    }
}
