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

package com.starrocks.catalog;

import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.OperationType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.threeten.extra.PeriodDuration;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;

public class TablePropertyTest {
    private static String fileName = "./TablePropertyTest";

    @AfterEach
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testNormal() throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        HashMap<String, String> properties = new HashMap<>();
        properties.put(DynamicPartitionProperty.ENABLE, "true");
        properties.put(DynamicPartitionProperty.TIME_UNIT, "day");
        properties.put(DynamicPartitionProperty.START, "-3");
        properties.put(DynamicPartitionProperty.END, "3");
        properties.put(DynamicPartitionProperty.PREFIX, "p");
        properties.put(DynamicPartitionProperty.BUCKETS, "30");
        properties.put("otherProperty", "unknownProperty");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        TableProperty readTableProperty = TableProperty.read(in);
        DynamicPartitionProperty readDynamicPartitionProperty = readTableProperty.getDynamicPartitionProperty();
        DynamicPartitionProperty dynamicPartitionProperty = new DynamicPartitionProperty(properties);
        Assertions.assertEquals(readTableProperty.getProperties(), properties);
        Assertions.assertEquals(readDynamicPartitionProperty.isEnabled(), dynamicPartitionProperty.isEnabled());
        Assertions.assertEquals(readDynamicPartitionProperty.getBuckets(), dynamicPartitionProperty.getBuckets());
        Assertions.assertEquals(readDynamicPartitionProperty.getPrefix(), dynamicPartitionProperty.getPrefix());
        Assertions.assertEquals(readDynamicPartitionProperty.getStart(), dynamicPartitionProperty.getStart());
        Assertions.assertEquals(readDynamicPartitionProperty.getEnd(), dynamicPartitionProperty.getEnd());
        Assertions.assertEquals(readDynamicPartitionProperty.getTimeUnit(), dynamicPartitionProperty.getTimeUnit());
        in.close();
    }

    @Test
    public void testBuildDataCachePartitionDuration() throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        HashMap<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION, "3 month");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        TableProperty readTableProperty = TableProperty.read(in);
        Assertions.assertNotNull(readTableProperty.buildProperty(OperationType.OP_ALTER_TABLE_PROPERTIES));
        in.close();
    }

    @Test
    public void testPartitionTTLNumberSerialization() throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        HashMap<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER, "2");
        properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_TTL, "1 day");
        PeriodDuration duration = TimeUtils.parseHumanReadablePeriodOrDuration("1 day");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildPartitionLiveNumber();
        tableProperty.buildPartitionTTL();
        Assertions.assertEquals(2, tableProperty.getPartitionTTLNumber());
        Assertions.assertEquals(duration, tableProperty.getPartitionTTL());
        tableProperty.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        TableProperty newTableProperty = TableProperty.read(in);
        Assertions.assertEquals(2, newTableProperty.getPartitionTTLNumber());
        Assertions.assertEquals(duration, tableProperty.getPartitionTTL());
        in.close();

        // 3. Update again
        properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER, "3");
        properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_TTL, "2 day");
        duration = TimeUtils.parseHumanReadablePeriodOrDuration("2 day");
        newTableProperty.modifyTableProperties(properties);
        newTableProperty.buildPartitionLiveNumber();
        newTableProperty.buildPartitionTTL();
        Assertions.assertEquals(3, newTableProperty.getPartitionTTLNumber());
        Assertions.assertEquals(duration, newTableProperty.getPartitionTTL());
    }
}
