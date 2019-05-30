/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.helix.core.assignment.segment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.ReplicaGroupStrategyConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.ColumnPartitionMetadata;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ReplicaGroupSegmentAssignmentStrategyTest {
  private static final int NUM_REPLICAS = 3;
  private static final String SEGMENT_NAME_PREFIX = "segment_";
  private static final int NUM_SEGMENTS = 108;
  private static final List<String> SEGMENTS =
      SegmentAssignmentStrategyTestUtils.getNameList(SEGMENT_NAME_PREFIX, NUM_SEGMENTS);
  private static final String INSTANCE_NAME_PREFIX = "instance_";
  private static final int NUM_INSTANCES = 18;
  private static final List<String> INSTANCES =
      SegmentAssignmentStrategyTestUtils.getNameList(INSTANCE_NAME_PREFIX, NUM_INSTANCES);
  private static final String OFFLINE_TABLE_NAME = "testTable_OFFLINE";
  private static final String PARTITION_COLUMN = "partitionColumn";
  private static final int NUM_PARTITIONS = 3;

  private SegmentAssignmentStrategy _strategyWithoutPartition;
  private InstancePartitions _instancePartitionsWithoutPartition;
  private SegmentAssignmentStrategy _strategyWithPartition;
  private InstancePartitions _instancePartitionsWithPartition;

  @BeforeClass
  public void setUp() {
    TableConfig tableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(OFFLINE_TABLE_NAME)
            .setNumReplicas(NUM_REPLICAS)
            .setSegmentAssignmentStrategy(ReplicaGroupSegmentAssignmentStrategy.class.getName()).build();
    _strategyWithoutPartition = SegmentAssignmentStrategyFactory.getSegmentAssignmentStrategy(null, tableConfig);

    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3, instance_4, instance_5],
    //   0_1=[instance_6, instance_7, instance_8, instance_9, instance_10, instance_11],
    //   0_2=[instance_12, instance_13, instance_14, instance_15, instance_16, instance_17]
    // }
    _instancePartitionsWithoutPartition = new InstancePartitions(OFFLINE_TABLE_NAME);
    int numInstancesPerReplica = NUM_INSTANCES / NUM_REPLICAS;
    int instanceIdToAdd = 0;
    for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
      List<String> instancesForReplica = new ArrayList<>(numInstancesPerReplica);
      for (int i = 0; i < numInstancesPerReplica; i++) {
        instancesForReplica.add(INSTANCES.get(instanceIdToAdd++));
      }
      _instancePartitionsWithoutPartition.setInstances(0, replicaId, instancesForReplica);
    }

    // Mock the Helix property store
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    List<ZNRecord> segmentZKMetadataZNRecords = new ArrayList<>(NUM_SEGMENTS);
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = SEGMENTS.get(segmentId);
      OfflineSegmentZKMetadata segmentZKMetadata = new OfflineSegmentZKMetadata();
      segmentZKMetadata.setSegmentName(segmentName);
      int partitionId = segmentId % NUM_PARTITIONS;
      segmentZKMetadata.setPartitionMetadata(new SegmentPartitionMetadata(Collections.singletonMap(PARTITION_COLUMN,
          new ColumnPartitionMetadata(null, NUM_PARTITIONS, Collections.singleton(partitionId)))));
      ZNRecord segmentZKMetadataZNRecord = segmentZKMetadata.toZNRecord();
      when(propertyStore
          .get(eq(ZKMetadataProvider.constructPropertyStorePathForSegment(OFFLINE_TABLE_NAME, segmentName)), any(),
              anyInt())).thenReturn(segmentZKMetadataZNRecord);
      segmentZKMetadataZNRecords.add(segmentZKMetadataZNRecord);
    }
    when(propertyStore
        .getChildren(eq(ZKMetadataProvider.constructPropertyStorePathForResource(OFFLINE_TABLE_NAME)), any(), anyInt()))
        .thenReturn(segmentZKMetadataZNRecords);

    ReplicaGroupStrategyConfig strategyConfig = new ReplicaGroupStrategyConfig();
    strategyConfig.setPartitionColumn(PARTITION_COLUMN);
    tableConfig.getValidationConfig().setReplicaGroupStrategyConfig(strategyConfig);
    _strategyWithPartition = SegmentAssignmentStrategyFactory.getSegmentAssignmentStrategy(propertyStore, tableConfig);

    // {
    //   0_0=[instance_0, instance_1],
    //   0_1=[instance_6, instance_7],
    //   0_2=[instance_12, instance_13],
    //   1_0=[instance_2, instance_3],
    //   1_1=[instance_8, instance_9],
    //   1_2=[instance_14, instance_15],
    //   2_0=[instance_4, instance_5],
    //   2_1=[instance_10, instance_11],
    //   2_2=[instance_16, instance_17]
    // }
    _instancePartitionsWithPartition = new InstancePartitions(OFFLINE_TABLE_NAME);
    int numInstancesPerPartition = numInstancesPerReplica / NUM_REPLICAS;
    instanceIdToAdd = 0;
    for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
      for (int partitionId = 0; partitionId < NUM_PARTITIONS; partitionId++) {
        List<String> instancesForPartition = new ArrayList<>(numInstancesPerPartition);
        for (int i = 0; i < numInstancesPerPartition; i++) {
          instancesForPartition.add(INSTANCES.get(instanceIdToAdd++));
        }
        _instancePartitionsWithPartition.setInstances(partitionId, replicaId, instancesForPartition);
      }
    }
  }

  @Test
  public void testFactory() {
    assertTrue(_strategyWithoutPartition instanceof ReplicaGroupSegmentAssignmentStrategy);
    assertTrue(_strategyWithPartition instanceof ReplicaGroupSegmentAssignmentStrategy);
  }

  @Test
  public void testAssignSegmentWithoutPartition() {
    int numInstancesPerReplica = NUM_INSTANCES / NUM_REPLICAS;
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = SEGMENTS.get(segmentId);
      List<String> instancesAssigned =
          _strategyWithoutPartition.assignSegment(segmentName, currentAssignment, _instancePartitionsWithoutPartition);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);
      for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {

        // Segment 0 should be assigned to instance 0, 6, 12
        // Segment 1 should be assigned to instance 1, 7, 13
        // Segment 2 should be assigned to instance 2, 8, 14
        // Segment 3 should be assigned to instance 3, 9, 15
        // Segment 4 should be assigned to instance 4, 10, 16
        // Segment 5 should be assigned to instance 5, 11, 17
        // Segment 6 should be assigned to instance 0, 6, 12
        // Segment 7 should be assigned to instance 1, 7, 13
        // ...
        int expectedAssignedInstanceId = segmentId % numInstancesPerReplica + replicaId * numInstancesPerReplica;
        assertEquals(instancesAssigned.get(replicaId), INSTANCES.get(expectedAssignedInstanceId));
      }
      currentAssignment.put(segmentName, SegmentAssignmentStrategyTestUtils
          .getInstanceStateMap(instancesAssigned, SegmentOnlineOfflineStateModel.ONLINE));
    }
  }

  @Test
  public void testAssignSegmentWithPartition() {
    int numInstancesPerReplica = NUM_INSTANCES / NUM_REPLICAS;
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    int numInstancesPerPartition = numInstancesPerReplica / NUM_PARTITIONS;
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = SEGMENTS.get(segmentId);
      List<String> instancesAssigned =
          _strategyWithPartition.assignSegment(segmentName, currentAssignment, _instancePartitionsWithPartition);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);
      int partitionId = segmentId % NUM_PARTITIONS;
      for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {

        // Segment 0 (partition 0) should be assigned to instance 0, 6, 12
        // Segment 1 (partition 1) should be assigned to instance 2, 8, 14
        // Segment 2 (partition 2) should be assigned to instance 4, 10, 16
        // Segment 3 (partition 0) should be assigned to instance 1, 7, 13
        // Segment 4 (partition 1) should be assigned to instance 3, 9, 15
        // Segment 5 (partition 2) should be assigned to instance 5, 11, 17
        // Segment 6 (partition 0) should be assigned to instance 0, 6, 12
        // Segment 7 (partition 1) should be assigned to instance 2, 8, 14
        // ...
        int expectedAssignedInstanceId =
            (segmentId % numInstancesPerReplica) / NUM_PARTITIONS + partitionId * numInstancesPerPartition
                + replicaId * numInstancesPerReplica;
        assertEquals(instancesAssigned.get(replicaId), INSTANCES.get(expectedAssignedInstanceId));
      }
      currentAssignment.put(segmentName, SegmentAssignmentStrategyTestUtils
          .getInstanceStateMap(instancesAssigned, SegmentOnlineOfflineStateModel.ONLINE));
    }
  }

  @Test
  public void testRebalanceTableWithoutPartition() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (String segmentName : SEGMENTS) {
      List<String> instancesAssigned =
          _strategyWithoutPartition.assignSegment(segmentName, currentAssignment, _instancePartitionsWithoutPartition);
      currentAssignment.put(segmentName, SegmentAssignmentStrategyTestUtils
          .getInstanceStateMap(instancesAssigned, SegmentOnlineOfflineStateModel.ONLINE));
    }

    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : currentAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 17 segments assigned
    int numSegmentsPerInstance = NUM_SEGMENTS * NUM_REPLICAS / NUM_INSTANCES;
    int[] numSegmentsAssignedPerInstance = SegmentAssignmentUtils.getNumSegmentsAssigned(currentAssignment, INSTANCES);
    for (int numSegmentsAssigned : numSegmentsAssignedPerInstance) {
      assertEquals(numSegmentsAssigned, numSegmentsPerInstance);
    }
    // Current assignment should already be balanced
    Map<String, Map<String, String>> newAssignment =
        _strategyWithoutPartition.rebalanceTable(currentAssignment, _instancePartitionsWithoutPartition);
    assertEquals(newAssignment, currentAssignment);
    Map<String, Integer> numSegmentsToBeMoved =
        SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment);
    assertTrue(numSegmentsToBeMoved.isEmpty());

    // Replace instance_0 with instance_18, instance_7 with instance_19
    // {
    //   0_0=[instance_18, instance_1, instance_2, instance_3, instance_4, instance_5],
    //   0_1=[instance_6, instance_19, instance_8, instance_9, instance_10, instance_11],
    //   0_2=[instance_12, instance_13, instance_14, instance_15, instance_16, instance_17]
    // }
    List<String> newInstances = new ArrayList<>(NUM_INSTANCES);
    List<String> newReplica0Instances = new ArrayList<>(_instancePartitionsWithoutPartition.getInstances(0, 0));
    String newReplica0Instance = INSTANCE_NAME_PREFIX + 18;
    newReplica0Instances.set(0, newReplica0Instance);
    newInstances.addAll(newReplica0Instances);
    List<String> newReplica1Instances = new ArrayList<>(_instancePartitionsWithoutPartition.getInstances(0, 1));
    String newReplica1Instance = INSTANCE_NAME_PREFIX + 19;
    newReplica1Instances.set(1, newReplica1Instance);
    newInstances.addAll(newReplica1Instances);
    List<String> newReplica2Instances = _instancePartitionsWithoutPartition.getInstances(0, 2);
    newInstances.addAll(newReplica2Instances);
    InstancePartitions newInstancePartitions = new InstancePartitions(OFFLINE_TABLE_NAME);
    newInstancePartitions.setInstances(0, 0, newReplica0Instances);
    newInstancePartitions.setInstances(0, 1, newReplica1Instances);
    newInstancePartitions.setInstances(0, 2, newReplica2Instances);
    newAssignment = _strategyWithoutPartition.rebalanceTable(currentAssignment, newInstancePartitions);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 18 segments assigned
    numSegmentsAssignedPerInstance = SegmentAssignmentUtils.getNumSegmentsAssigned(newAssignment, newInstances);
    for (int numSegmentsAssigned : numSegmentsAssignedPerInstance) {
      assertEquals(numSegmentsAssigned, numSegmentsPerInstance);
    }
    // All segments on instance_0 should be moved to instance_18, all segments on instance_7 should be moved to
    // instance_19
    numSegmentsToBeMoved = SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment);
    assertEquals(numSegmentsToBeMoved.size(), 2);
    assertEquals((int) numSegmentsToBeMoved.get(newReplica0Instance), numSegmentsPerInstance);
    assertEquals((int) numSegmentsToBeMoved.get(newReplica1Instance), numSegmentsPerInstance);
    String replica0OldInstanceName = INSTANCE_NAME_PREFIX + 0;
    String replica1OldInstanceName = INSTANCE_NAME_PREFIX + 7;
    for (String segmentName : SEGMENTS) {
      Map<String, String> oldInstanceStateMap = currentAssignment.get(segmentName);
      if (oldInstanceStateMap.containsKey(replica0OldInstanceName)) {
        assertTrue(newAssignment.get(segmentName).containsKey(newReplica0Instance));
      }
      if (oldInstanceStateMap.containsKey(replica1OldInstanceName)) {
        assertTrue(newAssignment.get(segmentName).containsKey(newReplica1Instance));
      }
    }

    // Remove 9 instances (3 from each replica)
    // {
    //   0_0=[instance_0, instance_1, instance_2],
    //   0_1=[instance_6, instance_7, instance_8],
    //   0_2=[instance_12, instance_13, instance_14]
    // }
    int newNumInstances = NUM_INSTANCES - 9;
    int newNumInstancesPerReplica = newNumInstances / NUM_REPLICAS;
    newInstances = new ArrayList<>(newNumInstances);
    for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
      List<String> newInstancesForReplica =
          _instancePartitionsWithoutPartition.getInstances(0, replicaId).subList(0, newNumInstancesPerReplica);
      newInstancePartitions.setInstances(0, replicaId, newInstancesForReplica);
      newInstances.addAll(newInstancesForReplica);
    }
    newAssignment = _strategyWithoutPartition.rebalanceTable(currentAssignment, newInstancePartitions);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 36 segments assigned
    int newNumSegmentsPerInstance = NUM_SEGMENTS / newNumInstancesPerReplica;
    numSegmentsAssignedPerInstance = SegmentAssignmentUtils.getNumSegmentsAssigned(newAssignment, newInstances);
    for (int numSegmentsAssigned : numSegmentsAssignedPerInstance) {
      assertEquals(numSegmentsAssigned, newNumSegmentsPerInstance);
    }
    // Each instance should have 18 segments to be moved to it
    numSegmentsToBeMoved = SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment);
    assertEquals(numSegmentsToBeMoved.size(), newNumInstances);
    for (String instanceName : newInstances) {
      assertEquals((int) numSegmentsToBeMoved.get(instanceName), newNumSegmentsPerInstance - numSegmentsPerInstance);
    }

    // Add 9 instances (3 to each replica)
    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3, instance_4, instance_5, instance_18, instance_19, instance_20],
    //   0_1=[instance_6, instance_7, instance_8, instance_9, instance_10, instance_11, instance_21, instance_22, instance_23],
    //   0_2=[instance_12, instance_13, instance_14, instance_15, instance_16, instance_17, instance_24, instance_25, instance_26]
    // }
    newNumInstances = NUM_INSTANCES + 9;
    newNumInstancesPerReplica = newNumInstances / NUM_REPLICAS;
    newInstances = SegmentAssignmentStrategyTestUtils.getNameList(INSTANCE_NAME_PREFIX, newNumInstances);
    int instanceIdToAdd = NUM_INSTANCES;
    for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
      List<String> newInstancesForReplica =
          new ArrayList<>(_instancePartitionsWithoutPartition.getInstances(0, replicaId));
      for (int i = 0; i < 3; i++) {
        newInstancesForReplica.add(newInstances.get(instanceIdToAdd++));
      }
      newInstancePartitions.setInstances(0, replicaId, newInstancesForReplica);
    }
    newAssignment = _strategyWithoutPartition.rebalanceTable(currentAssignment, newInstancePartitions);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 12 segments assigned
    newNumSegmentsPerInstance = NUM_SEGMENTS / newNumInstancesPerReplica;
    numSegmentsAssignedPerInstance = SegmentAssignmentUtils.getNumSegmentsAssigned(newAssignment, newInstances);
    for (int numSegmentsAssigned : numSegmentsAssignedPerInstance) {
      assertEquals(numSegmentsAssigned, newNumSegmentsPerInstance);
    }
    // Each new added instance should have 12 segments to be moved to it
    numSegmentsToBeMoved = SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment);
    assertEquals(numSegmentsToBeMoved.size(), 9);
    for (int instanceId = NUM_INSTANCES; instanceId < newNumInstances; instanceId++) {
      assertEquals((int) numSegmentsToBeMoved.get(newInstances.get(instanceId)), newNumSegmentsPerInstance);
    }

    // Change all instances
    // {
    //   0_0=[i_0, i_1, i_2, i_3, i_4, i_5],
    //   0_1=[i_6, i_7, i_8, i_9, i_10, i_11],
    //   0_2=[i_12, i_13, i_14, i_15, i_16, i_17]
    // }
    newInstances = SegmentAssignmentStrategyTestUtils.getNameList("i_", NUM_INSTANCES);
    int numInstancesPerReplica = NUM_INSTANCES / NUM_REPLICAS;
    instanceIdToAdd = 0;
    for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
      List<String> instancesForReplica = new ArrayList<>(numInstancesPerReplica);
      for (int i = 0; i < numInstancesPerReplica; i++) {
        instancesForReplica.add(newInstances.get(instanceIdToAdd++));
      }
      newInstancePartitions.setInstances(0, replicaId, instancesForReplica);
    }
    newAssignment = _strategyWithoutPartition.rebalanceTable(currentAssignment, newInstancePartitions);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 18 segments assigned
    newNumSegmentsPerInstance = NUM_SEGMENTS * NUM_REPLICAS / NUM_INSTANCES;
    numSegmentsAssignedPerInstance = SegmentAssignmentUtils.getNumSegmentsAssigned(newAssignment, newInstances);
    for (int numSegmentsAssigned : numSegmentsAssignedPerInstance) {
      assertEquals(numSegmentsAssigned, newNumSegmentsPerInstance);
    }
    // Each instance should have 18 segments to be moved to it
    numSegmentsToBeMoved = SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment);
    assertEquals(numSegmentsToBeMoved.size(), NUM_INSTANCES);
    for (String instanceName : newInstances) {
      assertEquals((int) numSegmentsToBeMoved.get(instanceName), newNumSegmentsPerInstance);
    }
  }

  @Test
  public void testRebalanceTableWithPartition() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (String segmentName : SEGMENTS) {
      List<String> instancesAssigned =
          _strategyWithPartition.assignSegment(segmentName, currentAssignment, _instancePartitionsWithPartition);
      currentAssignment.put(segmentName, SegmentAssignmentStrategyTestUtils
          .getInstanceStateMap(instancesAssigned, SegmentOnlineOfflineStateModel.ONLINE));
    }

    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : currentAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 17 segments assigned
    int numSegmentsPerInstance = NUM_SEGMENTS * NUM_REPLICAS / NUM_INSTANCES;
    int[] numSegmentsAssignedPerInstance = SegmentAssignmentUtils.getNumSegmentsAssigned(currentAssignment, INSTANCES);
    for (int numSegmentsAssigned : numSegmentsAssignedPerInstance) {
      assertEquals(numSegmentsAssigned, numSegmentsPerInstance);
    }
    // Current assignment should already be balanced
    Map<String, Map<String, String>> newAssignment =
        _strategyWithPartition.rebalanceTable(currentAssignment, _instancePartitionsWithPartition);
    assertEquals(newAssignment, currentAssignment);
    Map<String, Integer> numSegmentsToBeMoved =
        SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment);
    assertTrue(numSegmentsToBeMoved.isEmpty());

    // Since rebalance is based on each partition, other part should have already been covered in the
    // testRebalanceTableWithoutPartition()
  }
}
