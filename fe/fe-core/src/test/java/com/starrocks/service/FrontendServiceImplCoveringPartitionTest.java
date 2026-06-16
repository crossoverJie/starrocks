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

package com.starrocks.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class FrontendServiceImplCoveringPartitionTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static final String DB_NAME = "test_covering";
    private static final String TABLE_NAME = "test_day_partition";

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_strict_storage_medium_check = false;
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        UtFrameUtils.addMockComputeNode(50001);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME)
                .withTable("CREATE TABLE " + TABLE_NAME + " (\n" +
                        "    event_day DATE,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id)\n" +
                        "PARTITION BY date_trunc('day', event_day)\n" +
                        "DISTRIBUTED BY HASH(event_day) BUCKETS 1\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");");
    }

    @AfterAll
    public static void tearDown() throws Exception {
        try {
            starRocksAssert.dropTable(TABLE_NAME);
        } catch (Exception ignored) {
        }
    }

    private OlapTable getTable() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), TABLE_NAME);
    }

    /**
     * Test findCoveringPartitionName: a day value falls within a month partition range.
     */
    @Test
    public void testFindCoveringPartitionName_MonthCoversDay() throws Exception {
        OlapTable table = getTable();
        RangePartitionInfo rangeInfo = (RangePartitionInfo) table.getPartitionInfo();
        Column partitionColumn = rangeInfo.getPartitionColumns(table.getIdToColumn()).get(0);

        // Build a range map with a month partition: [2022-06-01, 2022-07-01)
        RangeMap<PartitionKey, Long> rangeMap = TreeRangeMap.create();
        long monthPartitionId = 100L;
        PartitionKey juneStart = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue("2022-06-01")),
                Lists.newArrayList(partitionColumn));
        PartitionKey julyStart = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue("2022-07-01")),
                Lists.newArrayList(partitionColumn));
        rangeMap.put(Range.closedOpen(juneStart, julyStart), monthPartitionId);

        // Add the month partition to the OlapTable so getPartition(id) returns it
        Partition monthPartition = new Partition(monthPartitionId, "p202206",
                new HashDistributionInfo(1, Lists.newArrayList(partitionColumn)));
        table.addPartition(monthPartition);

        // A day value within June should find the month partition
        List<String> dayValue = Lists.newArrayList("2022-06-14");
        String covering = FrontendServiceImpl.findCoveringPartitionName(table, partitionColumn, rangeMap, dayValue);
        Assertions.assertEquals("p202206", covering);
    }

    /**
     * Test findCoveringPartitionName: a day value NOT covered by any partition.
     */
    @Test
    public void testFindCoveringPartitionName_NoCoverage() throws Exception {
        OlapTable table = getTable();
        RangePartitionInfo rangeInfo = (RangePartitionInfo) table.getPartitionInfo();
        Column partitionColumn = rangeInfo.getPartitionColumns(table.getIdToColumn()).get(0);

        RangeMap<PartitionKey, Long> rangeMap = TreeRangeMap.create();

        // No partitions in rangeMap — should return null
        List<String> dayValue = Lists.newArrayList("2022-08-15");
        String covering = FrontendServiceImpl.findCoveringPartitionName(table, partitionColumn, rangeMap, dayValue);
        Assertions.assertNull(covering);
    }

    /**
     * Test findCoveringPartitionName: null and NULL values should return null.
     */
    @Test
    public void testFindCoveringPartitionName_NullValue() throws Exception {
        OlapTable table = getTable();
        RangePartitionInfo rangeInfo = (RangePartitionInfo) table.getPartitionInfo();
        Column partitionColumn = rangeInfo.getPartitionColumns(table.getIdToColumn()).get(0);
        RangeMap<PartitionKey, Long> rangeMap = TreeRangeMap.create();

        Assertions.assertNull(FrontendServiceImpl.findCoveringPartitionName(table, partitionColumn, rangeMap, null));
        Assertions.assertNull(FrontendServiceImpl.findCoveringPartitionName(table, partitionColumn, rangeMap,
                Lists.newArrayList()));
        Assertions.assertNull(FrontendServiceImpl.findCoveringPartitionName(table, partitionColumn, rangeMap,
                Lists.newArrayList("NULL")));
        Assertions.assertNull(FrontendServiceImpl.findCoveringPartitionName(table, partitionColumn, rangeMap,
                Lists.newArrayList((String) null)));
    }

    /**
     * Test findCoveringPartitionName: multi-column value (size != 1) should return null.
     */
    @Test
    public void testFindCoveringPartitionName_MultiColumnValue() throws Exception {
        OlapTable table = getTable();
        RangePartitionInfo rangeInfo = (RangePartitionInfo) table.getPartitionInfo();
        Column partitionColumn = rangeInfo.getPartitionColumns(table.getIdToColumn()).get(0);
        RangeMap<PartitionKey, Long> rangeMap = TreeRangeMap.create();

        List<String> multiValue = Lists.newArrayList("2022-06-14", "extra");
        Assertions.assertNull(FrontendServiceImpl.findCoveringPartitionName(table, partitionColumn, rangeMap, multiValue));
    }

    /**
     * Test resolveLoadPartitions: all values covered by existing partitions.
     * Simulates the scenario where a day-partitioned table has a month partition
     * (created by OPTIMIZE) and a backdated write hits the month partition.
     */
    @Test
    public void testResolveLoadPartitions_AllCovered() throws Exception {
        OlapTable table = getTable();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);

        // Add a month partition to the table's RangePartitionInfo and OlapTable
        RangePartitionInfo rangeInfo = (RangePartitionInfo) table.getPartitionInfo();
        Column partitionColumn = rangeInfo.getPartitionColumns(table.getIdToColumn()).get(0);
        long monthPartitionId = 200L;

        PartitionKey juneStart = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue("2022-06-01")),
                Lists.newArrayList(partitionColumn));
        PartitionKey julyStart = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue("2022-07-01")),
                Lists.newArrayList(partitionColumn));
        Range<PartitionKey> juneRange = Range.closedOpen(juneStart, julyStart);

        rangeInfo.addPartition(monthPartitionId, false, juneRange,
                new DataProperty(TStorageMedium.HDD),
                (short) 1, false);

        Partition monthPartition = new Partition(monthPartitionId, "p202206",
                new HashDistributionInfo(1, Lists.newArrayList(partitionColumn)));
        table.addPartition(monthPartition);

        boolean savedConfig = Config.enable_auto_partition_route_into_covering_partition;
        Config.enable_auto_partition_route_into_covering_partition = true;
        try {
            // Request values that all fall within June
            List<List<String>> partitionValues = Lists.newArrayList(
                    Lists.newArrayList("2022-06-10"),
                    Lists.newArrayList("2022-06-14"),
                    Lists.newArrayList("2022-06-30"));

            FrontendServiceImpl.LoadPartitionResolution resolution =
                    FrontendServiceImpl.resolveLoadPartitions(db, table, false, partitionValues);

            // All should be covered, nothing to create
            Assertions.assertTrue(resolution.valuesToCreate.isEmpty());
            Assertions.assertEquals(1, resolution.coveringPartitionNames.size());
            Assertions.assertTrue(resolution.coveringPartitionNames.contains("p202206"));
        } finally {
            Config.enable_auto_partition_route_into_covering_partition = savedConfig;
        }
    }

    /**
     * Test resolveLoadPartitions: mixed batch — some values covered by month partition,
     * some need new day partitions.
     */
    @Test
    public void testResolveLoadPartitions_MixedBatch() throws Exception {
        OlapTable table = getTable();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);

        RangePartitionInfo rangeInfo = (RangePartitionInfo) table.getPartitionInfo();
        Column partitionColumn = rangeInfo.getPartitionColumns(table.getIdToColumn()).get(0);
        long monthPartitionId = 300L;

        PartitionKey juneStart = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue("2022-06-01")),
                Lists.newArrayList(partitionColumn));
        PartitionKey julyStart = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue("2022-07-01")),
                Lists.newArrayList(partitionColumn));
        Range<PartitionKey> juneRange = Range.closedOpen(juneStart, julyStart);

        rangeInfo.addPartition(monthPartitionId, false, juneRange,
                new DataProperty(TStorageMedium.HDD),
                (short) 1, false);

        Partition monthPartition = new Partition(monthPartitionId, "p202206",
                new HashDistributionInfo(1, Lists.newArrayList(partitionColumn)));
        table.addPartition(monthPartition);

        boolean savedConfig = Config.enable_auto_partition_route_into_covering_partition;
        Config.enable_auto_partition_route_into_covering_partition = true;
        try {
            // Mixed: one value in June (covered), one in August (not covered, needs new partition)
            List<List<String>> partitionValues = Lists.newArrayList(
                    Lists.newArrayList("2022-06-14"),
                    Lists.newArrayList("2022-08-01"));

            FrontendServiceImpl.LoadPartitionResolution resolution =
                    FrontendServiceImpl.resolveLoadPartitions(db, table, false, partitionValues);

            Assertions.assertEquals(1, resolution.valuesToCreate.size());
            Assertions.assertEquals(Lists.newArrayList("2022-08-01"), resolution.valuesToCreate.get(0));
            Assertions.assertEquals(1, resolution.coveringPartitionNames.size());
            Assertions.assertTrue(resolution.coveringPartitionNames.contains("p202206"));
        } finally {
            Config.enable_auto_partition_route_into_covering_partition = savedConfig;
        }
    }

    /**
     * Test resolveLoadPartitions: config switch disabled → falls back to original behavior
     * (all values should be in valuesToCreate, coveringPartitionNames empty).
     */
    @Test
    public void testResolveLoadPartitions_ConfigDisabled() throws Exception {
        OlapTable table = getTable();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);

        RangePartitionInfo rangeInfo = (RangePartitionInfo) table.getPartitionInfo();
        Column partitionColumn = rangeInfo.getPartitionColumns(table.getIdToColumn()).get(0);
        long monthPartitionId = 400L;

        PartitionKey juneStart = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue("2022-06-01")),
                Lists.newArrayList(partitionColumn));
        PartitionKey julyStart = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue("2022-07-01")),
                Lists.newArrayList(partitionColumn));
        rangeInfo.addPartition(monthPartitionId, false, Range.closedOpen(juneStart, julyStart),
                new DataProperty(TStorageMedium.HDD),
                (short) 1, false);
        table.addPartition(new Partition(monthPartitionId, "p202206",
                new HashDistributionInfo(1, Lists.newArrayList(partitionColumn))));

        boolean savedConfig = Config.enable_auto_partition_route_into_covering_partition;
        Config.enable_auto_partition_route_into_covering_partition = false;
        try {
            List<List<String>> partitionValues = Lists.<List<String>>newArrayList(Lists.newArrayList("2022-06-14"));
            FrontendServiceImpl.LoadPartitionResolution resolution =
                    FrontendServiceImpl.resolveLoadPartitions(db, table, false, partitionValues);

            // All values should be in valuesToCreate, no covering partitions
            Assertions.assertEquals(1, resolution.valuesToCreate.size());
            Assertions.assertTrue(resolution.coveringPartitionNames.isEmpty());
        } finally {
            Config.enable_auto_partition_route_into_covering_partition = savedConfig;
        }
    }

    /**
     * Test resolveLoadPartitions: isTemp=true → skip covering check.
     */
    @Test
    public void testResolveLoadPartitions_IsTemp() throws Exception {
        OlapTable table = getTable();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);

        boolean savedConfig = Config.enable_auto_partition_route_into_covering_partition;
        Config.enable_auto_partition_route_into_covering_partition = true;
        try {
            List<List<String>> partitionValues = Lists.<List<String>>newArrayList(Lists.newArrayList("2022-06-14"));
            FrontendServiceImpl.LoadPartitionResolution resolution =
                    FrontendServiceImpl.resolveLoadPartitions(db, table, true, partitionValues);

            // Temp partitions should not go through covering check
            Assertions.assertEquals(1, resolution.valuesToCreate.size());
            Assertions.assertTrue(resolution.coveringPartitionNames.isEmpty());
        } finally {
            Config.enable_auto_partition_route_into_covering_partition = savedConfig;
        }
    }

    /**
     * Test resolveLoadPartitions: pure day-partition table (no month partition).
     * No existing partition covers the value, so it goes to valuesToCreate.
     */
    @Test
    public void testResolveLoadPartitions_NoCoveringPartition() throws Exception {
        OlapTable table = getTable();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);

        boolean savedConfig = Config.enable_auto_partition_route_into_covering_partition;
        Config.enable_auto_partition_route_into_covering_partition = true;
        try {
            // A date not covered by any existing partition
            List<List<String>> partitionValues = Lists.<List<String>>newArrayList(
                    Lists.newArrayList("2099-01-01"));

            FrontendServiceImpl.LoadPartitionResolution resolution =
                    FrontendServiceImpl.resolveLoadPartitions(db, table, false, partitionValues);

            // No existing partition covers 2099-01-01, so it should need creation
            Assertions.assertEquals(1, resolution.valuesToCreate.size());
            Assertions.assertTrue(resolution.coveringPartitionNames.isEmpty());
        } finally {
            Config.enable_auto_partition_route_into_covering_partition = savedConfig;
        }
    }

    /**
     * Test resolveLoadPartitions: multiple different month partitions covering different values.
     */
    @Test
    public void testResolveLoadPartitions_MultipleMonthPartitions() throws Exception {
        OlapTable table = getTable();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);

        RangePartitionInfo rangeInfo = (RangePartitionInfo) table.getPartitionInfo();
        Column partitionColumn = rangeInfo.getPartitionColumns(table.getIdToColumn()).get(0);

        // Add June month partition
        long junePartitionId = 500L;
        PartitionKey juneStart = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue("2022-06-01")),
                Lists.newArrayList(partitionColumn));
        PartitionKey julyStart = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue("2022-07-01")),
                Lists.newArrayList(partitionColumn));
        rangeInfo.addPartition(junePartitionId, false, Range.closedOpen(juneStart, julyStart),
                new DataProperty(TStorageMedium.HDD),
                (short) 1, false);
        table.addPartition(new Partition(junePartitionId, "p202206",
                new HashDistributionInfo(1, Lists.newArrayList(partitionColumn))));

        // Add July month partition
        long julyPartitionId = 501L;
        PartitionKey augStart = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue("2022-08-01")),
                Lists.newArrayList(partitionColumn));
        rangeInfo.addPartition(julyPartitionId, false, Range.closedOpen(julyStart, augStart),
                new DataProperty(TStorageMedium.HDD),
                (short) 1, false);
        table.addPartition(new Partition(julyPartitionId, "p202207",
                new HashDistributionInfo(1, Lists.newArrayList(partitionColumn))));

        boolean savedConfig = Config.enable_auto_partition_route_into_covering_partition;
        Config.enable_auto_partition_route_into_covering_partition = true;
        try {
            List<List<String>> partitionValues = Lists.newArrayList(
                    Lists.newArrayList("2022-06-10"),
                    Lists.newArrayList("2022-07-20"));

            FrontendServiceImpl.LoadPartitionResolution resolution =
                    FrontendServiceImpl.resolveLoadPartitions(db, table, false, partitionValues);

            Assertions.assertTrue(resolution.valuesToCreate.isEmpty());
            Assertions.assertEquals(2, resolution.coveringPartitionNames.size());
            Assertions.assertTrue(resolution.coveringPartitionNames.contains("p202206"));
            Assertions.assertTrue(resolution.coveringPartitionNames.contains("p202207"));
        } finally {
            Config.enable_auto_partition_route_into_covering_partition = savedConfig;
        }
    }
}
