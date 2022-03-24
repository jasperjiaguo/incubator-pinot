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
package org.apache.pinot.controller.helix.core.assignment.instance;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The instance replica-group/partition selector is responsible for selecting the instances for each replica-group and
 * partition, with each fault-domain maps 1:1 to a server pool
 */
public class InstanceReplicaGroupPartitionFDAwareSelector extends PartitionSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceReplicaGroupPartitionFDAwareSelector.class);

  public InstanceReplicaGroupPartitionFDAwareSelector(InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig,
      String tableNameWithType, InstancePartitions existingInstancePartitions) {
    super(replicaGroupPartitionConfig, tableNameWithType, existingInstancePartitions);
  }

  /**
   * Selects instances based on the replica-group/partition config, and stores the result into the given instance
   * partitions.
   */
  public void selectInstances(Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap,
      InstancePartitions instancePartitions) {
    int numPools = poolToInstanceConfigsMap.size();
    Preconditions.checkState(numPools != 0, "No pool qualified for selection");

    List<Integer> numInstancesPerPool =
        poolToInstanceConfigsMap.values().stream().map(List::size).sorted().collect(Collectors.toList());
    // Should have non-zero total instances
    Optional<Integer> totalInstancesOptional = numInstancesPerPool.stream().reduce(Integer::sum);
    Preconditions.checkState(totalInstancesOptional.orElse(0) > 0, "The number of total instances is zero");
    int numTotalInstances = totalInstancesOptional.get();
    // Assume best-effort round-robin assignment of instances to FDs, the assignment should be balanced
    Preconditions.checkState(numInstancesPerPool.get(numInstancesPerPool.size() - 1) - numInstancesPerPool.get(0) <= 1,
        "The instances are not balanced for each pool (fault-domain)");

    if (_replicaGroupPartitionConfig.isReplicaGroupBased()) {
      // Replica-group based selection
      int numReplicaGroups = _replicaGroupPartitionConfig.getNumReplicaGroups();
      Preconditions.checkState(numReplicaGroups > 0, "Number of replica-groups must be positive");
      Preconditions.checkState(numTotalInstances % numReplicaGroups == 0,
          "The total num instances %s cannot be assigned evenly to %s replica groups", numTotalInstances,
          numReplicaGroups);
      int numInstancesPerReplicaGroup = numTotalInstances / numReplicaGroups;
      Preconditions.checkState(
          numInstancesPerReplicaGroup == _replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup());

      if (numReplicaGroups > numPools) {
        LOGGER.info("Assigning {} replica groups to {} fault domains, "
            + "will have more than one replica group down if one fault domain is down", numReplicaGroups, numPools);
      } else {
        LOGGER.info("Assigning {} replica groups to {} fault domains", numReplicaGroups, numPools);
      }

      String[][] replicaGroupIdToInstancesMap = new String[numReplicaGroups][numInstancesPerReplicaGroup];
      int instanceInReplicaIter = 0;

      // Rotate the FD (pool) based on the table name hash
      int tableNameHash = Math.abs(_tableNameWithType.hashCode());
      List<Map.Entry<Integer, List<InstanceConfig>>> poolToInstanceEntriesList =
          poolToInstanceConfigsMap.entrySet().stream().sorted(Map.Entry.comparingByKey()).collect(Collectors.toList());
      Collections.rotate(poolToInstanceEntriesList, -(tableNameHash % poolToInstanceEntriesList.size()));

      // exhaust the instances in each pool (FD) sequentially to fill a replica group, if not enough instances, use the
      // next FD, if a replica group is filled, go to the next replica group
      for (Map.Entry<Integer, List<InstanceConfig>> instancesInPool : poolToInstanceEntriesList) {
        for (InstanceConfig instanceConfig : instancesInPool.getValue()) {
          replicaGroupIdToInstancesMap[instanceInReplicaIter / numInstancesPerReplicaGroup][instanceInReplicaIter
              % numInstancesPerReplicaGroup] = instanceConfig.getInstanceName();
          instanceInReplicaIter++;
        }
      }

      // Assign instances within a replica-group to one partition if not configured
      int numPartitions = _replicaGroupPartitionConfig.getNumPartitions();
      if (numPartitions <= 0) {
        numPartitions = 1;
      }
      // Assign all instances within a replica-group to each partition if not configured
      int numInstancesPerPartition = _replicaGroupPartitionConfig.getNumInstancesPerPartition();
      if (numInstancesPerPartition > 0) {
        Preconditions.checkState(numInstancesPerPartition <= numInstancesPerReplicaGroup,
            "Number of instances per partition: %s must be smaller or equal to number of instances per replica-group:"
                + " %s", numInstancesPerPartition, numInstancesPerReplicaGroup);
      } else {
        numInstancesPerPartition = numInstancesPerReplicaGroup;
      }
      LOGGER.info("Selecting {} partitions, {} instances per partition within a replica-group for table: {}",
          numPartitions, numInstancesPerPartition, _tableNameWithType);

      // Assign consecutive instances within a replica-group to each partition.
      // E.g. (within a replica-group, 5 instances, 3 partitions, 3 instances per partition)
      // [i0, i1, i2, i3, i4]
      //  p0  p0  p0  p1  p1
      //  p1  p2  p2  p2
      for (int replicaGroupId = 0; replicaGroupId < numReplicaGroups; replicaGroupId++) {
        int instanceIdInReplicaGroup = 0;
        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
          List<String> instancesInPartition = new ArrayList<>(numInstancesPerPartition);
          for (int instanceIdInPartition = 0; instanceIdInPartition < numInstancesPerPartition;
              instanceIdInPartition++) {
            instancesInPartition.add(replicaGroupIdToInstancesMap[replicaGroupId][instanceIdInReplicaGroup]);
            instanceIdInReplicaGroup = (instanceIdInReplicaGroup + 1) % numInstancesPerReplicaGroup;
          }
          LOGGER.info("Selecting instances: {} for replica-group: {}, partition: {} for table: {}",
              instancesInPartition, replicaGroupId, partitionId, _tableNameWithType);
          instancePartitions.setInstances(partitionId, replicaGroupId, instancesInPartition);
        }
      }
    } else {
      // TODO:Non-replica-group based selection
    }
  }
}
