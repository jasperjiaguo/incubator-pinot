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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.Pairs;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment assignment strategy that assigns segment to the instance with the least number of segments. In case of a tie,
 * assigns to the instance with the smallest index in the list. The strategy ensures that replicas of the same segment
 * are not assigned to the same server.
 * <p>To rebalance a table, use Helix AutoRebalanceStrategy.
 */
public class BalanceNumSegmentAssignmentStrategy implements SegmentAssignmentStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(BalanceNumSegmentAssignmentStrategy.class);

  private String _tableNameWithType;
  private int _replication;

  @Override
  public void init(ZkHelixPropertyStore<ZNRecord> propertyStore, TableConfig tableConfig) {
    _tableNameWithType = tableConfig.getTableName();
    _replication = tableConfig.getValidationConfig().getReplicationNumber();

    LOGGER
        .info("Initialized BalanceNumSegmentAssignmentStrategy for table: {} with replication: {}", _tableNameWithType,
            _replication);
  }

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions) {
    List<String> instances = getInstances(instancePartitions);
    int[] numSegmentsAssigned = SegmentAssignmentUtils.getNumSegmentsAssigned(currentAssignment, instances);

    // Assign the segment to the instance with the least segments, or the smallest id if there is a tie
    int numInstances = numSegmentsAssigned.length;
    PriorityQueue<Pairs.IntPair> heap = new PriorityQueue<>(numInstances, Pairs.intPairComparator());
    for (int instanceId = 0; instanceId < numInstances; instanceId++) {
      heap.add(new Pairs.IntPair(numSegmentsAssigned[instanceId], instanceId));
    }
    List<String> instancesAssigned = new ArrayList<>(_replication);
    for (int i = 0; i < _replication; i++) {
      instancesAssigned.add(instances.get(heap.remove().getRight()));
    }

    LOGGER.info("Assigned segment: {} to instances: {} for table: {}", segmentName, instancesAssigned,
        _tableNameWithType);
    return instancesAssigned;
  }

  @Override
  public Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions) {
    List<String> instances = getInstances(instancePartitions);

    // Use Helix AutoRebalanceStrategy to rebalance the table
    LinkedHashMap<String, Integer> states = new LinkedHashMap<>();
    states.put(SegmentOnlineOfflineStateModel.ONLINE, _replication);
    AutoRebalanceStrategy autoRebalanceStrategy =
        new AutoRebalanceStrategy(_tableNameWithType, new ArrayList<>(currentAssignment.keySet()), states);
    // Make a copy of the current assignment because this step might change the passed in assignment
    Map<String, Map<String, String>> currentAssignmentCopy = new TreeMap<>();
    for (Map.Entry<String, Map<String, String>> entry : currentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();
      currentAssignmentCopy.put(segmentName, new TreeMap<>(instanceStateMap));
    }
    Map<String, Map<String, String>> newAssignment =
        autoRebalanceStrategy.computePartitionAssignment(instances, instances, currentAssignmentCopy, null)
            .getMapFields();

    LOGGER.info(
        "Rebalanced {} segments with {} replicas to instances: {} for table: {}, number of segments to be moved to each instance: {}",
        currentAssignment.size(), _replication, instances, _tableNameWithType,
        SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment));
    return newAssignment;
  }

  private List<String> getInstances(InstancePartitions instancePartitions) {
    Preconditions.checkArgument(instancePartitions.getNumPartitions() == 1,
        "The instance partitions should contain only 1 partition");
    Preconditions.checkArgument(instancePartitions.getNumReplicas() == 1,
        "The instance partitions should contain only 1 replica");
    List<String> instances = instancePartitions.getInstances(0, 0);
    Preconditions.checkState(instances.size() >= _replication,
        "There are less instances: %d than the replication of the table: %d", instances.size(), _replication);
    return instances;
  }
}
