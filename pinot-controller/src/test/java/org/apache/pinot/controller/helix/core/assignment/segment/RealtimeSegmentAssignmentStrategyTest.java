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
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class RealtimeSegmentAssignmentStrategyTest {
  private static final int NUM_REPLICAS = 3;
  private static final int NUM_PARTITIONS = 4;
  private static final int NUM_SEGMENTS = 100;
  private static final String INSTANCE_NAME_PREFIX = "instance_";
  private static final int NUM_INSTANCES = 9;
  private static final List<String> INSTANCES =
      SegmentAssignmentStrategyTestUtils.getNameList(INSTANCE_NAME_PREFIX, NUM_INSTANCES);
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);

  private List<String> _segments;
  private SegmentAssignmentStrategy _strategy;
  private InstancePartitions _instancePartitionsWithoutReplicaGroup;
  private InstancePartitions _instancePartitionsWithReplicaGroup;

  @BeforeClass
  public void setUp() {
    _segments = new ArrayList<>(NUM_SEGMENTS);
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      _segments.add(new LLCSegmentName(RAW_TABLE_NAME, segmentId % NUM_PARTITIONS, segmentId / NUM_PARTITIONS,
          System.currentTimeMillis()).getSegmentName());
    }

    TableConfig tableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME).setTableName(RAW_TABLE_NAME)
            .setNumReplicas(NUM_REPLICAS).setLLC(true).build();
    _strategy = SegmentAssignmentStrategyFactory.getSegmentAssignmentStrategy(null, tableConfig);

    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3, instance_4, instance_5, instance_6, instance_7, instance_8]
    // }
    //        p0r0        p0r1        p0r2        p1r0        p1r1        p1r2        p2r0        p2r1        p2r2
    //        p3r0        p3r1        p3r2
    _instancePartitionsWithoutReplicaGroup = new InstancePartitions(REALTIME_TABLE_NAME);
    _instancePartitionsWithoutReplicaGroup.setInstances(0, 0, INSTANCES);

    // {
    //   0_0=[instance_0, instance_1, instance_2],
    //   0_1=[instance_3, instance_4, instance_5],
    //   0_2=[instance_6, instance_7, instance_8]
    // }
    //        p0          p1          p2
    //        p3
    _instancePartitionsWithReplicaGroup = new InstancePartitions(REALTIME_TABLE_NAME);
    int numInstancesPerReplica = NUM_INSTANCES / NUM_REPLICAS;
    int instanceIdToAdd = 0;
    for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
      List<String> instancesForReplica = new ArrayList<>(numInstancesPerReplica);
      for (int i = 0; i < numInstancesPerReplica; i++) {
        instancesForReplica.add(INSTANCES.get(instanceIdToAdd++));
      }
      _instancePartitionsWithReplicaGroup.setInstances(0, replicaId, instancesForReplica);
    }
  }

  @Test
  public void testFactory() {
    assertTrue(_strategy instanceof RealtimeSegmentAssignmentStrategy);
  }

  @Test
  public void testAssignSegmentWithoutReplicaGroup() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = _segments.get(segmentId);
      List<String> instancesAssigned =
          _strategy.assignSegment(segmentName, currentAssignment, _instancePartitionsWithoutReplicaGroup);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);
      for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {

        // Segment 0 (partition 0) should be assigned to instance 0, 1, 2
        // Segment 1 (partition 1) should be assigned to instance 3, 4, 5
        // Segment 2 (partition 2) should be assigned to instance 6, 7, 8
        // Segment 3 (partition 3) should be assigned to instance 0, 1, 2
        // Segment 4 (partition 0) should be assigned to instance 0, 1, 2
        // Segment 5 (partition 1) should be assigned to instance 3, 4, 5
        // ...
        int partitionId = segmentId % NUM_PARTITIONS;
        int expectedAssignedInstanceId = (partitionId * NUM_REPLICAS + replicaId) % NUM_INSTANCES;
        assertEquals(instancesAssigned.get(replicaId), INSTANCES.get(expectedAssignedInstanceId));
      }
      currentAssignment.put(segmentName, SegmentAssignmentStrategyTestUtils
          .getInstanceStateMap(instancesAssigned, RealtimeSegmentOnlineOfflineStateModel.CONSUMING));
    }
  }

  @Test
  public void testAssignSegmentWithReplicaGroup() {
    int numInstancesPerReplica = NUM_INSTANCES / NUM_REPLICAS;
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = _segments.get(segmentId);
      List<String> instancesAssigned =
          _strategy.assignSegment(segmentName, currentAssignment, _instancePartitionsWithReplicaGroup);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);
      for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {

        // Segment 0 (partition 0) should be assigned to instance 0, 3, 6
        // Segment 1 (partition 1) should be assigned to instance 1, 4, 7
        // Segment 2 (partition 2) should be assigned to instance 2, 5, 8
        // Segment 3 (partition 3) should be assigned to instance 0, 3, 6
        // Segment 4 (partition 0) should be assigned to instance 0, 3, 6
        // Segment 5 (partition 1) should be assigned to instance 1, 4, 7
        // ...
        int partitionId = segmentId % NUM_PARTITIONS;
        int expectedAssignedInstanceId = partitionId % numInstancesPerReplica + replicaId * numInstancesPerReplica;
        assertEquals(instancesAssigned.get(replicaId), INSTANCES.get(expectedAssignedInstanceId));
      }
      currentAssignment.put(segmentName, SegmentAssignmentStrategyTestUtils
          .getInstanceStateMap(instancesAssigned, RealtimeSegmentOnlineOfflineStateModel.CONSUMING));
    }
  }

  @Test
  public void testRebalanceTableWithoutReplicaGroup() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (String segmentName : _segments) {
      List<String> instancesAssigned =
          _strategy.assignSegment(segmentName, currentAssignment, _instancePartitionsWithoutReplicaGroup);
      currentAssignment.put(segmentName, SegmentAssignmentStrategyTestUtils
          .getInstanceStateMap(instancesAssigned, RealtimeSegmentOnlineOfflineStateModel.CONSUMING));
    }

    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : currentAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Current assignment should already be balanced
    Map<String, Map<String, String>> newAssignment =
        _strategy.rebalanceTable(currentAssignment, _instancePartitionsWithoutReplicaGroup);
    assertEquals(newAssignment, currentAssignment);
    Map<String, Integer> numSegmentsToBeMoved =
        SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment);
    assertTrue(numSegmentsToBeMoved.isEmpty());

    // Since rebalance is the same as re-assigning all segments, other part should have already been covered in the
    // testAssignSegmentWithoutReplicaGroup()
  }

  @Test
  public void testRebalanceTableWithReplicaGroup() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (String segmentName : _segments) {
      List<String> instancesAssigned =
          _strategy.assignSegment(segmentName, currentAssignment, _instancePartitionsWithReplicaGroup);
      currentAssignment.put(segmentName, SegmentAssignmentStrategyTestUtils
          .getInstanceStateMap(instancesAssigned, RealtimeSegmentOnlineOfflineStateModel.CONSUMING));
    }

    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : currentAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Current assignment should already be balanced
    Map<String, Map<String, String>> newAssignment =
        _strategy.rebalanceTable(currentAssignment, _instancePartitionsWithReplicaGroup);
    assertEquals(newAssignment, currentAssignment);
    Map<String, Integer> numSegmentsToBeMoved =
        SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment);
    assertTrue(numSegmentsToBeMoved.isEmpty());

    // Since rebalance is the same as re-assigning all segments, other part should have already been covered in the
    // testAssignSegmentWithReplicaGroup()
  }
}
