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

import com.starrocks.common.FeConstants;
import com.starrocks.connector.RemoteFileBlockDesc;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemotePathKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.starrocks.connector.hive.MockedRemoteFileSystem.HDFS_HIVE_TABLE;
import static com.starrocks.connector.hive.MockedRemoteFileSystem.HDFS_RECURSIVE_TABLE;

public class HiveRemoteFileIOTest {
    @Test
    public void testGetRemoteFiles() {
        FileSystem fs = new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
        HiveRemoteFileIO fileIO = new HiveRemoteFileIO(new Configuration());
        fileIO.setFileSystem(fs);
        FeConstants.runningUnitTest = true;
        String tableLocation = "hdfs://127.0.0.1:10000/hive.db/hive_tbl";
        RemotePathKey pathKey = RemotePathKey.of(tableLocation, false);
        Map<RemotePathKey, List<RemoteFileDesc>> remoteFileInfos = fileIO.getRemoteFiles(pathKey);
        List<RemoteFileDesc> fileDescs = remoteFileInfos.get(pathKey);
        Assertions.assertNotNull(fileDescs);
        Assertions.assertEquals(1, fileDescs.size());
        RemoteFileDesc fileDesc = fileDescs.get(0);
        Assertions.assertNotNull(fileDesc);
        Assertions.assertEquals("000000_0", fileDesc.getFileName());
        Assertions.assertEquals("", fileDesc.getCompression());
        Assertions.assertEquals(20, fileDesc.getLength());
        Assertions.assertEquals(1234567890, fileDesc.getModificationTime());
        Assertions.assertFalse(fileDesc.isSplittable());
        Assertions.assertNull(fileDesc.getTextFileFormatDesc());

        List<RemoteFileBlockDesc> blockDescs = fileDesc.getBlockDescs();
        Assertions.assertEquals(1, blockDescs.size());
        RemoteFileBlockDesc blockDesc = blockDescs.get(0);
        Assertions.assertEquals(0, blockDesc.getOffset());
        Assertions.assertEquals(20, blockDesc.getLength());
        Assertions.assertEquals(2, blockDesc.getReplicaHostIds().length);
    }

    @Test
    public void testGetRemoteFilesWithWildCards() {
        FileSystem fs = new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
        HiveRemoteFileIO fileIO = new HiveRemoteFileIO(new Configuration());
        fileIO.setFileSystem(fs);
        FeConstants.runningUnitTest = true;
        String tableLocation = "hdfs://127.0.0.1:10000/hive.db/*";
        RemotePathKey pathKey = RemotePathKey.of(tableLocation, false);
        Map<RemotePathKey, List<RemoteFileDesc>> remoteFileInfos = fileIO.getRemoteFiles(pathKey, true);
        List<RemoteFileDesc> fileDescs = remoteFileInfos.get(pathKey);
        Assertions.assertNotNull(fileDescs);
        Assertions.assertEquals(1, fileDescs.size());
        RemoteFileDesc fileDesc = fileDescs.get(0);
        Assertions.assertNotNull(fileDesc);
        Assertions.assertEquals("000000_0", fileDesc.getFileName());
        Assertions.assertEquals("", fileDesc.getCompression());
        Assertions.assertEquals(20, fileDesc.getLength());
        Assertions.assertEquals(1234567890, fileDesc.getModificationTime());
        Assertions.assertFalse(fileDesc.isSplittable());
        Assertions.assertNull(fileDesc.getTextFileFormatDesc());

        List<RemoteFileBlockDesc> blockDescs = fileDesc.getBlockDescs();
        Assertions.assertEquals(1, blockDescs.size());
        RemoteFileBlockDesc blockDesc = blockDescs.get(0);
        Assertions.assertEquals(0, blockDesc.getOffset());
        Assertions.assertEquals(20, blockDesc.getLength());
        Assertions.assertEquals(2, blockDesc.getReplicaHostIds().length);
    }

    @Test
    public void testGetRemoteRecursiveFiles() {
        FileSystem fs = new MockedRemoteFileSystem(HDFS_RECURSIVE_TABLE);
        HiveRemoteFileIO fileIO = new HiveRemoteFileIO(new Configuration());
        fileIO.setFileSystem(fs);
        FeConstants.runningUnitTest = true;
        String tableLocation = "hdfs://127.0.0.1:10000/hive.db/recursive_tbl";
        RemotePathKey pathKey = RemotePathKey.of(tableLocation, true);
        Map<RemotePathKey, List<RemoteFileDesc>> remoteFileInfos = fileIO.getRemoteFiles(pathKey);
        List<RemoteFileDesc> fileDescs = remoteFileInfos.get(pathKey);
        Assertions.assertNotNull(fileDescs);
        Assertions.assertEquals(2, fileDescs.size());
        RemoteFileDesc fileDesc = fileDescs.get(0);
        Assertions.assertNotNull(fileDesc);
        Assertions.assertEquals("subdir1/000000_0", fileDesc.getFileName());
        Assertions.assertEquals("", fileDesc.getCompression());
        Assertions.assertEquals(20, fileDesc.getLength());
        Assertions.assertEquals(1234567890, fileDesc.getModificationTime());
        Assertions.assertFalse(fileDesc.isSplittable());
        Assertions.assertNull(fileDesc.getTextFileFormatDesc());

        fileDesc = fileDescs.get(1);
        Assertions.assertNotNull(fileDesc);
        Assertions.assertEquals("subdir1/000000_1", fileDesc.getFileName());
        Assertions.assertEquals("", fileDesc.getCompression());
        Assertions.assertEquals(20, fileDesc.getLength());
        Assertions.assertEquals(1234567890, fileDesc.getModificationTime());
        Assertions.assertFalse(fileDesc.isSplittable());
        Assertions.assertNull(fileDesc.getTextFileFormatDesc());
    }

    @Test
    public void testGetRemoteRecursiveFilesWithWildCards() {
        FileSystem fs = new MockedRemoteFileSystem(HDFS_RECURSIVE_TABLE);
        HiveRemoteFileIO fileIO = new HiveRemoteFileIO(new Configuration());
        fileIO.setFileSystem(fs);
        FeConstants.runningUnitTest = true;
        String tableLocation = "hdfs://127.0.0.1:10000/hive.db/*";
        RemotePathKey pathKey = RemotePathKey.of(tableLocation, true);
        Map<RemotePathKey, List<RemoteFileDesc>> remoteFileInfos = fileIO.getRemoteFiles(pathKey, true);
        List<RemoteFileDesc> fileDescs = remoteFileInfos.get(pathKey);
        Assertions.assertNotNull(fileDescs);
        Assertions.assertEquals(2, fileDescs.size());
        RemoteFileDesc fileDesc = fileDescs.get(0);
        Assertions.assertNotNull(fileDesc);
        Assertions.assertEquals("subdir1/000000_0", fileDesc.getFileName());
        Assertions.assertEquals("", fileDesc.getCompression());
        Assertions.assertEquals(20, fileDesc.getLength());
        Assertions.assertEquals(1234567890, fileDesc.getModificationTime());
        Assertions.assertFalse(fileDesc.isSplittable());
        Assertions.assertNull(fileDesc.getTextFileFormatDesc());

        fileDesc = fileDescs.get(1);
        Assertions.assertNotNull(fileDesc);
        Assertions.assertEquals("subdir1/000000_1", fileDesc.getFileName());
        Assertions.assertEquals("", fileDesc.getCompression());
        Assertions.assertEquals(20, fileDesc.getLength());
        Assertions.assertEquals(1234567890, fileDesc.getModificationTime());
        Assertions.assertFalse(fileDesc.isSplittable());
        Assertions.assertNull(fileDesc.getTextFileFormatDesc());
    }
    @Test
    public void testPathContainsEmptySpace() {
        FileSystem fs = new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
        HiveRemoteFileIO fileIO = new HiveRemoteFileIO(new Configuration());
        fileIO.setFileSystem(fs);
        FeConstants.runningUnitTest = true;
        String tableLocation = "hdfs://127.0.0.1:10000/hive.db / hive_tbl";
        RemotePathKey pathKey = RemotePathKey.of(tableLocation, false);
        Map<RemotePathKey, List<RemoteFileDesc>> remoteFileInfos = fileIO.getRemoteFiles(pathKey);
        List<RemoteFileDesc> fileDescs = remoteFileInfos.get(pathKey);
        Assertions.assertNotNull(fileDescs);
        Assertions.assertEquals(1, fileDescs.size());
    }

}
