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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class BalanceNumSegmentAssignmentStrategyTest {
  private static final int NUM_REPLICAS = 3;
  private static final String SEGMENT_NAME_PREFIX = "segment_";
  private static final int NUM_SEGMENTS = 100;
  private static final List<String> SEGMENTS =
      SegmentAssignmentStrategyTestUtils.getNameList(SEGMENT_NAME_PREFIX, NUM_SEGMENTS);
  private static final String INSTANCE_NAME_PREFIX = "instance_";
  private static final int NUM_INSTANCES = 10;
  private static final List<String> INSTANCES =
      SegmentAssignmentStrategyTestUtils.getNameList(INSTANCE_NAME_PREFIX, NUM_INSTANCES);
  private static final String OFFLINE_TABLE_NAME = "testTable_OFFLINE";

  private SegmentAssignmentStrategy _strategy;
  private InstancePartitions _instancePartitions;

  @BeforeClass
  public void setUp() {
    TableConfig tableConfig =
        new TableConfig.Builder(TableType.OFFLINE).setTableName(OFFLINE_TABLE_NAME).setNumReplicas(NUM_REPLICAS)
            .build();
    _strategy = SegmentAssignmentStrategyFactory.getSegmentAssignmentStrategy(null, tableConfig);

    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3, instance_4, instance_5, instance_6, instance_7, instance_8, instance_9]
    // }
    _instancePartitions = new InstancePartitions(OFFLINE_TABLE_NAME);
    _instancePartitions.setInstances(0, 0, INSTANCES);
  }

  @Test
  public void testFactory() {
    assertTrue(_strategy instanceof BalanceNumSegmentAssignmentStrategy);
  }

  @Test
  public void testAssignSegment() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();

    // Segment 0 should be assigned to instance 0, 1, 2
    // Segment 1 should be assigned to instance 3, 4, 5
    // Segment 2 should be assigned to instance 6, 7, 8
    // Segment 3 should be assigned to instance 9, 0, 1
    // Segment 4 should be assigned to instance 2, 3, 4
    // ...
    int expectedAssignedInstanceId = 0;
    for (String segmentName : SEGMENTS) {
      List<String> instancesAssigned = _strategy.assignSegment(segmentName, currentAssignment, _instancePartitions);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);
      for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
        assertEquals(instancesAssigned.get(replicaId), INSTANCES.get(expectedAssignedInstanceId));
        expectedAssignedInstanceId = (expectedAssignedInstanceId + 1) % NUM_INSTANCES;
      }
      currentAssignment.put(segmentName, SegmentAssignmentStrategyTestUtils
          .getInstanceStateMap(instancesAssigned, SegmentOnlineOfflineStateModel.ONLINE));
    }
  }

  @Test
  public void testRebalanceTable() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (String segmentName : SEGMENTS) {
      List<String> instancesAssigned = _strategy.assignSegment(segmentName, currentAssignment, _instancePartitions);
      currentAssignment.put(segmentName, SegmentAssignmentStrategyTestUtils
          .getInstanceStateMap(instancesAssigned, SegmentOnlineOfflineStateModel.ONLINE));
    }

    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : currentAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 30 segments assigned
    int numSegmentsPerInstance = NUM_SEGMENTS * NUM_REPLICAS / NUM_INSTANCES;
    int[] numSegmentsAssignedPerInstance = SegmentAssignmentUtils.getNumSegmentsAssigned(currentAssignment, INSTANCES);
    for (int numSegmentsAssigned : numSegmentsAssignedPerInstance) {
      assertEquals(numSegmentsAssigned, numSegmentsPerInstance);
    }
    // Current assignment should already be balanced
    Map<String, Map<String, String>> newAssignment = _strategy.rebalanceTable(currentAssignment, _instancePartitions);
    assertEquals(newAssignment, currentAssignment);
    Map<String, Integer> numSegmentsToBeMoved =
        SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment);
    assertTrue(numSegmentsToBeMoved.isEmpty());

    // Replace instance_0 with instance_10
    // {
    //   0_0=[instance_10, instance_1, instance_2, instance_3, instance_4, instance_5, instance_6, instance_7, instance_8, instance_9]
    // }
    List<String> newInstances = new ArrayList<>(INSTANCES);
    String newInstanceName = INSTANCE_NAME_PREFIX + 10;
    newInstances.set(0, newInstanceName);
    InstancePartitions newInstancePartitions = new InstancePartitions(OFFLINE_TABLE_NAME);
    newInstancePartitions.setInstances(0, 0, newInstances);
    newAssignment = _strategy.rebalanceTable(currentAssignment, newInstancePartitions);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 30 segments assigned
    numSegmentsAssignedPerInstance = SegmentAssignmentUtils.getNumSegmentsAssigned(newAssignment, newInstances);
    for (int numSegmentsAssigned : numSegmentsAssignedPerInstance) {
      assertEquals(numSegmentsAssigned, numSegmentsPerInstance);
    }
    // All segments on instance_0 should be moved to instance_10
    numSegmentsToBeMoved = SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment);
    assertEquals(numSegmentsToBeMoved.size(), 1);
    assertEquals((int) numSegmentsToBeMoved.get(newInstanceName), numSegmentsPerInstance);
    String oldInstanceName = INSTANCE_NAME_PREFIX + 0;
    for (String segmentName : SEGMENTS) {
      if (currentAssignment.get(segmentName).containsKey(oldInstanceName)) {
        assertTrue(newAssignment.get(segmentName).containsKey(newInstanceName));
      }
    }

    // Remove 5 instances
    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3, instance_4]
    // }
    int newNumInstances = NUM_INSTANCES - 5;
    newInstances = SegmentAssignmentStrategyTestUtils.getNameList(INSTANCE_NAME_PREFIX, newNumInstances);
    newInstancePartitions.setInstances(0, 0, newInstances);
    newAssignment = _strategy.rebalanceTable(currentAssignment, newInstancePartitions);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // The segments are not perfectly balanced, but should be deterministic
    numSegmentsAssignedPerInstance = SegmentAssignmentUtils.getNumSegmentsAssigned(newAssignment, newInstances);
    assertEquals(numSegmentsAssignedPerInstance[0], 56);
    assertEquals(numSegmentsAssignedPerInstance[1], 60);
    assertEquals(numSegmentsAssignedPerInstance[2], 60);
    assertEquals(numSegmentsAssignedPerInstance[3], 60);
    assertEquals(numSegmentsAssignedPerInstance[4], 64);
    numSegmentsToBeMoved = SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment);
    assertEquals(numSegmentsToBeMoved.size(), newNumInstances);
    assertEquals((int) numSegmentsToBeMoved.get(newInstances.get(0)), 26);
    assertEquals((int) numSegmentsToBeMoved.get(newInstances.get(1)), 30);
    assertEquals((int) numSegmentsToBeMoved.get(newInstances.get(2)), 30);
    assertEquals((int) numSegmentsToBeMoved.get(newInstances.get(3)), 30);
    assertEquals((int) numSegmentsToBeMoved.get(newInstances.get(4)), 34);

    // Add 5 instances
    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3, instance_4, instance_5, instance_6, instance_7, instance_8, instance_9, instance_10, instance_11, instance_12, instance_13, instance_14]
    // }
    newNumInstances = NUM_INSTANCES + 5;
    newInstances = SegmentAssignmentStrategyTestUtils.getNameList(INSTANCE_NAME_PREFIX, newNumInstances);
    newInstancePartitions.setInstances(0, 0, newInstances);
    newAssignment = _strategy.rebalanceTable(currentAssignment, newInstancePartitions);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 20 segments assigned
    int newNumSegmentsPerInstance = NUM_SEGMENTS * NUM_REPLICAS / (NUM_INSTANCES + 5);
    numSegmentsAssignedPerInstance = SegmentAssignmentUtils.getNumSegmentsAssigned(newAssignment, newInstances);
    for (int numSegmentsAssigned : numSegmentsAssignedPerInstance) {
      assertEquals(numSegmentsAssigned, newNumSegmentsPerInstance);
    }
    // Each new added instance should have 20 segments to be moved to it
    numSegmentsToBeMoved = SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment);
    assertEquals(numSegmentsToBeMoved.size(), 5);
    for (int instanceId = NUM_INSTANCES; instanceId < newNumInstances; instanceId++) {
      assertEquals((int) numSegmentsToBeMoved.get(newInstances.get(instanceId)), newNumSegmentsPerInstance);
    }

    // Change all instances
    // {
    //   0_0=[i_0, i_1, i_2, i_3, i_4, i_5, i_6, i_7, i_8, i_9]
    // }
    newInstances = SegmentAssignmentStrategyTestUtils.getNameList("i_", NUM_INSTANCES);
    newInstancePartitions.setInstances(0, 0, newInstances);
    newAssignment = _strategy.rebalanceTable(currentAssignment, newInstancePartitions);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 30 segments assigned
    newNumSegmentsPerInstance = NUM_SEGMENTS * NUM_REPLICAS / NUM_INSTANCES;
    numSegmentsAssignedPerInstance = SegmentAssignmentUtils.getNumSegmentsAssigned(newAssignment, newInstances);
    for (int numSegmentsAssigned : numSegmentsAssignedPerInstance) {
      assertEquals(numSegmentsAssigned, newNumSegmentsPerInstance);
    }
    // Each instance should have 30 segments to be moved to it
    numSegmentsToBeMoved = SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment);
    assertEquals(numSegmentsToBeMoved.size(), NUM_INSTANCES);
    for (String instanceName : newInstances) {
      assertEquals((int) numSegmentsToBeMoved.get(instanceName), newNumSegmentsPerInstance);
    }
  }
}
