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

package com.starrocks.lake.snapshot;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ClusterSnapshotConfigTest {

    @Test
    public void testLoadFromFile() {
        ClusterSnapshotConfig config = ClusterSnapshotConfig.load("src/test/resources/conf/cluster_snapshot.yaml");

        ClusterSnapshotConfig.ClusterSnapshot clusterSnapshot = config.getClusterSnapshot();
        Assertions.assertNotNull(clusterSnapshot);
        Assertions.assertEquals(
                "s3://defaultbucket/test/f7265e80-631c-44d3-a8ac-cf7cdc7adec811019/meta/image/automated_cluster_snapshot_1704038400000",
                clusterSnapshot.getClusterSnapshotPath());
        Assertions.assertEquals("my_s3_volume", clusterSnapshot.getStorageVolumeName());
        Assertions.assertNotNull(clusterSnapshot.getStorageVolume());

        clusterSnapshot.setClusterSnapshotPath(clusterSnapshot.getClusterSnapshotPath());
        clusterSnapshot.setStorageVolumeName(clusterSnapshot.getStorageVolumeName());

        Assertions.assertEquals(2, config.getFrontends().size());
        Assertions.assertEquals(2, config.getComputeNodes().size());
        Assertions.assertEquals(2, config.getStorageVolumes().size());

        ClusterSnapshotConfig.Frontend frontend1 = config.getFrontends().get(0);
        Assertions.assertEquals("172.26.92.1", frontend1.getHost());
        Assertions.assertEquals(9010, frontend1.getEditLogPort());
        Assertions.assertEquals(ClusterSnapshotConfig.Frontend.FrontendType.FOLLOWER, frontend1.getType());
        Assertions.assertTrue(frontend1.isFollower());
        Assertions.assertFalse(frontend1.isObserver());

        ClusterSnapshotConfig.Frontend frontend2 = config.getFrontends().get(1);
        Assertions.assertEquals("172.26.92.2", frontend2.getHost());
        Assertions.assertEquals(9010, frontend2.getEditLogPort());
        Assertions.assertEquals(ClusterSnapshotConfig.Frontend.FrontendType.OBSERVER, frontend2.getType());
        Assertions.assertFalse(frontend2.isFollower());
        Assertions.assertTrue(frontend2.isObserver());

        frontend1.toString();
        frontend1.setHost(frontend2.getHost());
        frontend1.setEditLogPort(frontend2.getEditLogPort());
        frontend1.setType(frontend2.getType());

        ClusterSnapshotConfig.ComputeNode computeNode1 = config.getComputeNodes().get(0);
        Assertions.assertEquals("172.26.92.11", computeNode1.getHost());
        Assertions.assertEquals(9050, computeNode1.getHeartbeatServicePort());

        ClusterSnapshotConfig.ComputeNode computeNode2 = config.getComputeNodes().get(1);
        Assertions.assertEquals("172.26.92.12", computeNode2.getHost());
        Assertions.assertEquals(9050, computeNode2.getHeartbeatServicePort());

        computeNode1.toString();
        computeNode1.setHost(computeNode2.getHost());
        computeNode1.setHeartbeatServicePort(computeNode2.getHeartbeatServicePort());

        ClusterSnapshotConfig.StorageVolume storageVolume1 = config.getStorageVolumes().get(0);
        Assertions.assertEquals("my_s3_volume", storageVolume1.getName());
        Assertions.assertEquals("S3", storageVolume1.getType());
        Assertions.assertEquals("s3://defaultbucket/test/", storageVolume1.getLocation());
        Assertions.assertEquals("my s3 volume", storageVolume1.getComment());
        Assertions.assertEquals(4, storageVolume1.getProperties().size());
        Assertions.assertEquals("us-west-2", storageVolume1.getProperties().get("aws.s3.region"));
        Assertions.assertEquals("https://s3.us-west-2.amazonaws.com",
                storageVolume1.getProperties().get("aws.s3.endpoint"));
        Assertions.assertEquals("xxxxxxxxxx", storageVolume1.getProperties().get("aws.s3.access_key"));
        Assertions.assertEquals("yyyyyyyyyy", storageVolume1.getProperties().get("aws.s3.secret_key"));

        ClusterSnapshotConfig.StorageVolume storageVolume2 = config.getStorageVolumes().get(1);
        Assertions.assertEquals("my_hdfs_volume", storageVolume2.getName());
        Assertions.assertEquals("HDFS", storageVolume2.getType());
        Assertions.assertEquals("hdfs://127.0.0.1:9000/sr/test/", storageVolume2.getLocation());
        Assertions.assertEquals("my hdfs volume", storageVolume2.getComment());
        Assertions.assertEquals(2, storageVolume2.getProperties().size());
        Assertions.assertEquals("simple", storageVolume2.getProperties().get("hadoop.security.authentication"));
        Assertions.assertEquals("starrocks", storageVolume2.getProperties().get("username"));

        storageVolume1.setName(storageVolume2.getName());
        storageVolume1.setType(storageVolume2.getType());
        storageVolume1.setLocation(storageVolume2.getLocation());
        storageVolume1.setComment(storageVolume2.getComment());
        storageVolume1.setProperties(storageVolume2.getProperties());
    }
}
