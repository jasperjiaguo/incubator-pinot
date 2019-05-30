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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment assignment strategy for LLC real-time segments.
 * <p>It is very similar to replica-group based segment assignment with the following differences:
 * <ul>
 *   <li>1. Each partition always contains one server</li>
 *   <li>2. In addition to the ONLINE segments, there are also CONSUMING segments to be assigned</li>
 * </ul>
 * <p>
 *   Since each partition contains only one server (in one replica), we can directly assign or rebalance segments to the
 *   servers based on the partition id.
 * <p>
 *   The real-time segment assignment does not minimize segment moves because the server is fixed for each partition in
 *   each replica. The instance assignment is responsible for keeping minimum changes to the instance partitions to
 *   reduce the number of segments need to be moved.
 */
public class RealtimeSegmentAssignmentStrategy implements SegmentAssignmentStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeSegmentAssignmentStrategy.class);

  private String _tableNameWithType;
  private int _replication;

  @Override
  public void init(ZkHelixPropertyStore<ZNRecord> propertyStore, TableConfig tableConfig) {
    _tableNameWithType = tableConfig.getTableName();
    _replication = tableConfig.getValidationConfig().getReplicasPerPartitionNumber();

    LOGGER.info("Initialized RealtimeSegmentAssignmentStrategy for table: {} with replication: {}", _tableNameWithType,
        _replication);
  }

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions) {
    Preconditions.checkState(instancePartitions.getNumPartitions() == 1,
        "The instance partitions should contain only 1 partition");

    int numReplicas = getNumReplicas(instancePartitions);
    int partitionId = new LLCSegmentName(segmentName).getPartitionId();
    List<String> instancesAssigned = new ArrayList<>(numReplicas);
    for (int replicaId = 0; replicaId < numReplicas; replicaId++) {
      instancesAssigned.add(getInstance(instancePartitions, partitionId, replicaId));
    }
    LOGGER.info("Assigned segment: {} with partition id: {} to instances: {} for table: {}", segmentName, partitionId,
        instancesAssigned, _tableNameWithType);
    return instancesAssigned;
  }

  @Override
  public Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions) {
    Preconditions.checkState(instancePartitions.getNumPartitions() == 1,
        "The instance partitions should contain only 1 partition");

    int numReplicas = getNumReplicas(instancePartitions);
    Map<String, Map<String, String>> newAssignment = new TreeMap<>();
    for (Map.Entry<String, Map<String, String>> entry : currentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      int partitionId = new LLCSegmentName(segmentName).getPartitionId();
      String state = entry.getValue().values().iterator().next();
      newAssignment.put(segmentName, getInstanceStateMap(instancePartitions, partitionId, numReplicas, state));
    }

    LOGGER.info(
        "Rebalanced {} segments with {} replicas and instance partitions: {} for table: {}, number of segments to be moved to each instance: {}",
        currentAssignment.size(), numReplicas, instancePartitions, _tableNameWithType,
        SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment));
    return newAssignment;
  }

  private int getNumReplicas(InstancePartitions instancePartitions) {
    int numReplicas = instancePartitions.getNumReplicas();
    if (numReplicas == 1) {
      return _replication;
    } else {
      if (numReplicas != _replication) {
        LOGGER.warn("Number of replicas in table config: {} does not match instance partitions: {}, using: {}",
            _replication, numReplicas, numReplicas);
      }
      return numReplicas;
    }
  }

  /**
   * Returns the instance for the given partition id and replica id.
   * <ul>
   *   <li>
   *     When there is only one replica in instance partitions, uniformly sprays the partitions and replicas across the
   *     instances (for backward-compatibility).
   *     <p>E.g. (6 servers, 3 partitions, 4 replicas)
   *     <pre>
   *       "0_0": [i0,   i1,   i2,   i3,   i4,   i5  ]
   *               p0r0, p0r1, p0r2, p1r3, p1r0, p1r1
   *               p1r2, p1r3, p2r0, p2r1, p2r2, p2r3
   *     </pre>
   *   </li>
   *   <li>
   *     When there are multiple replicas in instance partitions, uniformly sprays the partitions across the instances
   *     for each replica
   *   </li>
   * </ul>
   */
  private String getInstance(InstancePartitions instancePartitions, int partitionId, int replicaId) {
    int numReplicas = instancePartitions.getNumReplicas();
    if (numReplicas == 1) {
      List<String> instances = instancePartitions.getInstances(0, 0);
      return instances.get((partitionId * _replication + replicaId) % instances.size());
    } else {
      List<String> instances = instancePartitions.getInstances(0, replicaId);
      return instances.get(partitionId % instances.size());
    }
  }

  private Map<String, String> getInstanceStateMap(InstancePartitions instancePartitions, int partitionId,
      int numReplicas, String state) {
    Map<String, String> instanceStateMap = new TreeMap<>();
    for (int replicaId = 0; replicaId < numReplicas; replicaId++) {
      instanceStateMap.put(getInstance(instancePartitions, partitionId, replicaId), state);
    }
    return instanceStateMap;
  }
}
