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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.ReplicaGroupStrategyConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.ColumnPartitionMetadata;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.Pairs;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment assignment strategy that assigns segment to the instance in the replica with the least number of segments.
 * <p>Among multiple replicas, always mirror the assignment (pick the same index of the instance).
 * <p>
 *   Inside each partition, assign the segment to the servers with the least segments already assigned. In case of a
 *   tie, assign to the server with the smallest index in the list. Do this for one replica and mirror the assignment to
 *   other replicas.
 * <p>
 *   To rebalance a table, inside each partition, first calculate the number of segments on each server, loop over all
 *   the segments and keep the assignment if number of segments for the server has not been reached and track the not
 *   assigned segments, then assign the left-over segments to the servers with the least segments, or the smallest index
 *   if there is a tie. Repeat the process for all the partitions in one replica, and mirror the assignment to other
 *   replicas. With this greedy algorithm, the result is deterministic and with minimum segment moves.
 */
public class ReplicaGroupSegmentAssignmentStrategy implements SegmentAssignmentStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaGroupSegmentAssignmentStrategy.class);

  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private String _tableNameWithType;
  private String _partitionColumn;

  @Override
  public void init(ZkHelixPropertyStore<ZNRecord> propertyStore, TableConfig tableConfig) {
    _propertyStore = propertyStore;
    _tableNameWithType = tableConfig.getTableName();
    ReplicaGroupStrategyConfig strategyConfig = tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    _partitionColumn = strategyConfig != null ? strategyConfig.getPartitionColumn() : null;

    if (_partitionColumn == null) {
      LOGGER.info("Initialized ReplicaGroupSegmentAssignmentStrategy for table: {} without partition column",
          _tableNameWithType);
    } else {
      LOGGER.info("Initialized ReplicaGroupSegmentAssignmentStrategy for table: {} with partition column: {}",
          _tableNameWithType, _partitionColumn);
    }
  }

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions) {
    // Fetch partition id from segment ZK metadata if partition column is configured
    int partitionId = 0;
    if (_partitionColumn == null) {
      Preconditions.checkState(instancePartitions.getNumPartitions() == 1,
          "The instance partitions should contain only 1 partition");
    } else {
      OfflineSegmentZKMetadata segmentZKMetadata =
          ZKMetadataProvider.getOfflineSegmentZKMetadata(_propertyStore, _tableNameWithType, segmentName);
      Preconditions
          .checkState(segmentZKMetadata != null, "Failed to fetch segment ZK metadata for table: %s, segment: %s",
              _tableNameWithType, segmentName);
      partitionId = getPartitionId(segmentZKMetadata, instancePartitions);
    }

    // First assign the segment to replica 0
    List<String> instances = instancePartitions.getInstances(partitionId, 0);
    int[] numSegmentsAssigned = SegmentAssignmentUtils.getNumSegmentsAssigned(currentAssignment, instances);
    int minNumSegmentsAssigned = numSegmentsAssigned[0];
    int instanceIdWithLeastSegmentsAssigned = 0;
    int numInstances = numSegmentsAssigned.length;
    for (int instanceId = 1; instanceId < numInstances; instanceId++) {
      if (numSegmentsAssigned[instanceId] < minNumSegmentsAssigned) {
        minNumSegmentsAssigned = numSegmentsAssigned[instanceId];
        instanceIdWithLeastSegmentsAssigned = instanceId;
      }
    }

    // Mirror the assignment to all replicas
    int numReplicas = instancePartitions.getNumReplicas();
    List<String> instancesAssigned = new ArrayList<>(numReplicas);
    for (int replicaId = 0; replicaId < numReplicas; replicaId++) {
      instancesAssigned
          .add(instancePartitions.getInstances(partitionId, replicaId).get(instanceIdWithLeastSegmentsAssigned));
    }

    if (_partitionColumn == null) {
      LOGGER.info("Assigned segment: {} to instances: {} for table: {}", segmentName, instancesAssigned,
          _tableNameWithType);
    } else {
      LOGGER.info("Assigned segment: {} with partition id: {} to instances: {} for table: {}", segmentName, partitionId,
          instancesAssigned, _tableNameWithType);
    }
    return instancesAssigned;
  }

  @Override
  public Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions) {
    if (_partitionColumn == null) {
      return rebalanceTableWithoutPartition(currentAssignment, instancePartitions);
    } else {
      return rebalanceTableWithPartition(currentAssignment, instancePartitions);
    }
  }

  private Map<String, Map<String, String>> rebalanceTableWithoutPartition(
      Map<String, Map<String, String>> currentAssignment, InstancePartitions instancePartitions) {
    Preconditions.checkState(instancePartitions.getNumPartitions() == 1,
        "The instance partitions should contain only 1 partition");

    Map<String, Map<String, String>> newAssignment = new TreeMap<>();
    rebalancePartition(currentAssignment, instancePartitions, 0, currentAssignment.keySet(), newAssignment);

    LOGGER.info(
        "Rebalanced {} segments with instance partitions: {} for table: {} without partition column, number of segments to be moved to each instance: {}",
        currentAssignment.size(), instancePartitions, _tableNameWithType,
        SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment));
    return newAssignment;
  }

  private Map<String, Map<String, String>> rebalanceTableWithPartition(
      Map<String, Map<String, String>> currentAssignment, InstancePartitions instancePartitions) {
    // Fetch partition id from segment ZK metadata
    List<OfflineSegmentZKMetadata> segmentZKMetadataList =
        ZKMetadataProvider.getOfflineSegmentZKMetadataListForTable(_propertyStore, _tableNameWithType);
    Map<String, OfflineSegmentZKMetadata> segmentZKMetadataMap = new HashMap<>();
    for (OfflineSegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      segmentZKMetadataMap.put(segmentZKMetadata.getSegmentName(), segmentZKMetadata);
    }
    int numPartitions = instancePartitions.getNumPartitions();
    List<Set<String>> segmentsPerPartition = new ArrayList<>(numPartitions);
    for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
      segmentsPerPartition.add(new HashSet<>());
    }
    for (String segmentName : currentAssignment.keySet()) {
      int partitionId = getPartitionId(segmentZKMetadataMap.get(segmentName), instancePartitions);
      segmentsPerPartition.get(partitionId).add(segmentName);
    }

    Map<String, Map<String, String>> newAssignment = new TreeMap<>();
    for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
      rebalancePartition(currentAssignment, instancePartitions, partitionId, segmentsPerPartition.get(partitionId),
          newAssignment);
    }

    LOGGER.info(
        "Rebalanced {} segments with instance partitions: {} for table: {} with partition column: {}, number of segments to be moved to each instance: {}",
        currentAssignment.size(), instancePartitions, _tableNameWithType, _partitionColumn,
        SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment));
    return newAssignment;
  }

  private int getPartitionId(OfflineSegmentZKMetadata segmentZKMetadata, InstancePartitions instancePartitions) {
    String segmentName = segmentZKMetadata.getSegmentName();
    ColumnPartitionMetadata partitionMetadata =
        segmentZKMetadata.getPartitionMetadata().getColumnPartitionMap().get(_partitionColumn);
    Preconditions.checkState(partitionMetadata != null,
        "Segment ZK metadata for table: %s, segment: %s does not contain partition metadata for column: %s",
        _tableNameWithType, segmentName, _partitionColumn);
    Set<Integer> partitions = partitionMetadata.getPartitions();
    Preconditions.checkState(partitions.size() == 1,
        "Segment ZK metadata for table: %s, segment: %s contains multiple partitions for column: %s",
        _tableNameWithType, segmentName, _partitionColumn);

    // Handle the case of number of partitions changed
    int segmentPartitionId = partitions.iterator().next();
    int numPartitions = instancePartitions.getNumPartitions();
    int partitionId = segmentPartitionId % numPartitions;
    if (partitionMetadata.getNumPartitions() != numPartitions) {
      LOGGER.warn(
          "Number of partitions in segment ZK metadata: {} does not match instance partitions: {} for table: {}, segment: {}, using partition id: {}",
          partitionMetadata.getNumPartitions(), numPartitions, _tableNameWithType, segmentName, partitionId);
    }
    return partitionId;
  }

  private void rebalancePartition(Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions, int partitionId, Set<String> segments,
      Map<String, Map<String, String>> newAssignment) {
    // Fetch instances in replica 0
    List<String> instances = instancePartitions.getInstances(partitionId, 0);
    Map<String, Integer> instanceNameToIdMap = SegmentAssignmentUtils.getInstanceNameToIdMap(instances);

    // Calculate target number of segments per instance
    int numInstances = instances.size();
    int numSegments = segments.size();
    int targetNumSegmentsPerInstance = (numSegments + numInstances - 1) / numInstances;

    // Do not move segment if target number of segments is not reached, track the segments need to be moved
    int[] numSegmentsAssigned = new int[numInstances];
    List<String> segmentsNotAssigned = new ArrayList<>();
    for (Map.Entry<String, Map<String, String>> entry : currentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      // Skip segments not in the partition
      if (!segments.contains(segmentName)) {
        continue;
      }
      boolean segmentAssigned = false;
      for (String instanceName : entry.getValue().keySet()) {
        Integer instanceId = instanceNameToIdMap.get(instanceName);
        if (instanceId != null && numSegmentsAssigned[instanceId] < targetNumSegmentsPerInstance) {
          newAssignment.put(segmentName, getInstanceStateMap(instancePartitions, partitionId, instanceId));
          numSegmentsAssigned[instanceId]++;
          segmentAssigned = true;
          break;
        }
      }
      if (!segmentAssigned) {
        segmentsNotAssigned.add(segmentName);
      }
    }

    // Assign each not assigned segment to the instance with the least segments, or the smallest id if there is a tie
    PriorityQueue<Pairs.IntPair> heap = new PriorityQueue<>(numInstances, Pairs.intPairComparator());
    for (int instanceId = 0; instanceId < numInstances; instanceId++) {
      heap.add(new Pairs.IntPair(numSegmentsAssigned[instanceId], instanceId));
    }
    for (String segmentName : segmentsNotAssigned) {
      Pairs.IntPair intPair = heap.remove();
      int instanceId = intPair.getRight();
      newAssignment.put(segmentName, getInstanceStateMap(instancePartitions, partitionId, instanceId));
      intPair.setLeft(intPair.getLeft() + 1);
      heap.add(intPair);
    }
  }

  private Map<String, String> getInstanceStateMap(InstancePartitions instancePartitions, int partitionId,
      int instanceId) {
    Map<String, String> instanceStateMap = new TreeMap<>();
    int numReplicas = instancePartitions.getNumReplicas();
    for (int replicaId = 0; replicaId < numReplicas; replicaId++) {
      instanceStateMap.put(instancePartitions.getInstances(partitionId, replicaId).get(instanceId),
          SegmentOnlineOfflineStateModel.ONLINE);
    }
    return instanceStateMap;
  }
}
