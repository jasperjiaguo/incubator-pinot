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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class PoolDiverseInstancePartitionSelector extends InstancePartitionSelector {
  public PoolDiverseInstancePartitionSelector(InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig,
      String tableNameWithType, InstancePartitions existingInstancePartitions) {
    super(replicaGroupPartitionConfig, tableNameWithType, existingInstancePartitions);
  }

  @Override
  void selectInstances(Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap,
      InstancePartitions instancePartitions) {

  }
//  private static final Logger LOGGER = LoggerFactory.getLogger(PoolDiverseInstancePartitionSelector.class);
//  private final ReplicaGroupBasedAssignmentState _assignmentState = new ReplicaGroupBasedAssignmentState();
//
//  public PoolDiverseInstancePartitionSelector(InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig,
//      String tableNameWithType, @Nullable InstancePartitions existingInstancePartitions) {
//    super(replicaGroupPartitionConfig, tableNameWithType, existingInstancePartitions);
//  }
//
//  /**
//   * validate if the poolToInstanceConfigsMap is a valid input for pool-diverse replica-group selection
//   */
//  private void validatePoolDiversePreconditions(Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap,
//      int numMaxPools, int numTargetInstancesPerReplica, int numTargetReplicaGroups) {
//
//    // numMaxPools should be positive
//    Preconditions.checkState(numMaxPools > 0, "Number of max pools must be positive");
//    // numTargetReplicaGroups should be positive
//    Preconditions.checkState(numTargetReplicaGroups > 0, "Number of replica-groups must be positive");
//    // numTargetInstancesPerReplica should be positive
//    Preconditions.checkState(numTargetInstancesPerReplica > 0, "Number of instances per replica must be positive");
//
//    // Validate the existing instance partitions is null or has only one partition
//    Preconditions.checkState(_existingInstancePartitions == null || _existingInstancePartitions.getNumPartitions() == 1,
//        "This algorithm does not support table level partitioning");
//
//    // Validate the pool to instance configs map is not null or empty
//    Preconditions.checkNotNull(poolToInstanceConfigsMap, "poolToInstanceConfigsMap is null");
//    int numPools = poolToInstanceConfigsMap.size();
//    Preconditions.checkState(numPools != 0, "No pool qualified for selection");
//
//    // Collect the number of instances in each pool with non-zero instances into a list and sort
//    // the list in descending order，so that the largest pool is at the head of the list
//    List<Integer> numInstancesPerPool = poolToInstanceConfigsMap.values()
//        .stream()
//        .map(List::size)
//        .filter(i -> i > 0)
//        .sorted(Collections.reverseOrder())
//        .collect(Collectors.toList());
//
//    // Should have non-zero total instances
//    Optional<Integer> totalInstancesOptional = numInstancesPerPool.stream().reduce(Integer::sum);
//    Preconditions.checkState(totalInstancesOptional.orElse(0) > 0, "The number of total instances is zero");
//
//    // The number of pools should be at least numTargetReplicaGroups if numTargetReplicaGroups is less than numMaxPools
//    // When numTargetReplicaGroups is greater than numMaxPools, we should use all pools
//    Preconditions.checkState(numInstancesPerPool.size() >= Math.min(numMaxPools, numTargetReplicaGroups),
//        "The number of pools %s is less than the minimum of numMaxPools %s and numTargetReplicaGroups %s", numPools,
//        numMaxPools, numTargetReplicaGroups);
//
//    // The total number of instances in the provided pools
//    int numTotalInstances = totalInstancesOptional.get();
//    // The total number of instances in the target cluster
//    int targetNumTotalInstances = numTargetInstancesPerReplica * numTargetReplicaGroups;
//
//    Preconditions.checkState(numTotalInstances < targetNumTotalInstances,
//        "The total number of instances %s is less than (the number of instances per "
//            + "replica group %s * the number of replica groups %s)", numTotalInstances, numTargetInstancesPerReplica,
//        numTargetReplicaGroups);
//
//    // the integer number of replica groups that needs to be filled by each pool
//    // this is > 1 only when numTargetReplicaGroups > numMaxPools
//    int quotient = numTargetReplicaGroups / numMaxPools;
//
//    // the number of instances in each pool should also be at least sufficient to fill
//    // (numTargetReplicaGroups / numMaxPools) replica groups. This check is only effective when numTargetReplicaGroups > numMaxPools
//    Preconditions.checkState(
//        numInstancesPerPool.get(numInstancesPerPool.size() - 1) >= quotient * numTargetInstancesPerReplica,
//        "Validation failed. " + "The number of instances in each pool should also be at least sufficient to fill "
//            + "(numTargetReplicaGroups / numMaxPools) replica groups");
//
//    if (numTotalInstances == targetNumTotalInstances) {
//      // if the total number of instances is equal to the number of instances per replica group * the number of
//      // replica groups
//
//      //  the outstanding instances should be less than or equal to one replica group
//      //  otherwise the MZ diversity cannot be satisfied.
//      Preconditions.checkState(
//          numInstancesPerPool.get(0) - quotient * numTargetInstancesPerReplica <= numTargetInstancesPerReplica,
//          "Validation failed. The instances provided cannot satisfy the " + "pool diverse requirement.");
//    } else {
//      // if the total number of instances is greater than the number of instances per replica group * the number of
//      // replica groups
//
//      //  the outstanding instances should be able to fill the rest numTargetReplicaGroups % numMaxPools
//      //  replicas in a pool diverse manner.
//      validateLargePool(numInstancesPerPool, numMaxPools, numTargetInstancesPerReplica, numTargetReplicaGroups);
//    }
//    LOGGER.info("Validation passed. The instances provided can satisfy the pool diverse requirement.");
//    LOGGER.info("The number of instances per pool is {}", poolToInstanceConfigsMap.entrySet()
//        .stream()
//        .map(e -> "\n Pool:" + e.getKey() + " : " + e.getValue().size() + " instances;")
//        .collect(Collectors.joining()));
//    LOGGER.info("Trying to assign total {} instances to {} replica groups, " + "with {} instance per replica group",
//        numTotalInstances, numTargetReplicaGroups, numTargetInstancesPerReplica);
//  }
//
//  /**
//   * Validate the case where the total instances provided pools is greater than the number of instances in the target
//   * cluster
//   * @param numInstancesPerPool the provided number of instances in each pool
//   * @param numMaxPools the maximum number of pools
//   * @param numInstancesPerReplica the target number of instances per replica group
//   * @param numReplicaGroups the target number of replica groups
//   */
//  private void validateLargePool(List<Integer> numInstancesPerPool, int numMaxPools, int numInstancesPerReplica,
//      int numReplicaGroups) {
//    int targetNumTotalInstances = numInstancesPerReplica * numReplicaGroups;
//    int quotient = numReplicaGroups / numMaxPools;
//    for (Integer integer : numInstancesPerPool) {
//      int selected = Math.min(integer - quotient * numInstancesPerReplica, numInstancesPerReplica);
//      targetNumTotalInstances -= selected;
//      if (targetNumTotalInstances <= 0) {
//        return;
//      }
//    }
//    throw new IllegalStateException(
//        "Validation failed. The instances provided cannot satisfy the " + "pool diverse requirement.");
//  }
//
//  /**
//   * Selects instances based on the replica-group/partition config, and stores the result into the given instance
//   * partitions.
//   */
//  public void selectInstances(Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap,
//      InstancePartitions instancePartitions) {
//
//    if (_replicaGroupPartitionConfig.isReplicaGroupBased()) {
//      // Replica-group based selection
//
//      InstanceConfig a;
//      a.getRecord().getSimpleField("")
//      _assignmentState.init(poolToInstanceConfigsMap, _replicaGroupPartitionConfig);
//
//      // Check if the provided poolToInstanceConfigsMap can satisfy the setup specified in _replicaGroupPartitionConfig
//      _assignmentState.validatePoolDiversePreconditions(_existingInstancePartitions, poolToInstanceConfigsMap);
//
//      _assignmentState.reconstructReplicaGroupIdToInstancesMap(_existingInstancePartitions);
//
//      candidateInstanceMap.put()
//
//      // Check if the setup specified in _replicaGroupPartitionConfig can be satisfied
//      Pair<Integer, Integer> rgRet =
//          processReplicaGroupAssignmentPreconditions(numFaultDomains, numTotalInstances, _replicaGroupPartitionConfig);
//      int numReplicaGroups = rgRet.getLeft();
//      int numInstancesPerReplicaGroup = rgRet.getRight();
//
//      /*
//       * create an FD_id -> instance_names map and initialize with current instances, we will later exclude instances
//       * in existing assignment, and use the rest instances to do the assigment
//       */
//      Map<Integer, LinkedHashSet<String>> faultDomainToCandidateInstancesMap = new TreeMap<>();
//      poolToInstanceConfigsMap.forEach(
//          (k, v) -> faultDomainToCandidateInstancesMap.put(k, new LinkedHashSet<String>() {{
//            v.forEach(instance -> add(instance.getInstanceName()));
//          }}));
//
//      // create an instance_name -> FD_id map, just for look up
//      Map<String, Integer> aliveInstanceNameToFDMap = new HashMap<>();
//      faultDomainToCandidateInstancesMap.forEach(
//          (faultDomainId, value) -> value.forEach(instance -> aliveInstanceNameToFDMap.put(instance, faultDomainId)));
//
//      // replicaGroupBasedAssignmentState for assignment
//      ReplicaGroupBasedAssignmentState replicaGroupBasedAssignmentState = null;
//
//      /*
//       * initialize the new replicaGroupBasedAssignmentState for assignment,
//       * place existing instances in their corresponding positions
//       */
//      if (_replicaGroupPartitionConfig.isMinimizeDataMovement() && _existingInstancePartitions != null) {
//        int numExistingReplicaGroups = _existingInstancePartitions.getNumReplicaGroups();
//        int numExistingPartitions = _existingInstancePartitions.getNumPartitions();
//        /*
//         * reconstruct a replica group -> instance mapping from _existingInstancePartitions,
//         */
//        LinkedHashSet<String> existingReplicaGroup = new LinkedHashSet<>();
//        for (int i = 0; i < numExistingReplicaGroups; i++, existingReplicaGroup.clear()) {
//          for (int j = 0; j < numExistingPartitions; j++) {
//            existingReplicaGroup.addAll(_existingInstancePartitions.getInstances(j, i));
//          }
//
//          /*
//           * We can only know the numExistingInstancesPerReplicaGroup after we reconstructed the sequence of instances
//           * in the first replica group
//           */
//          if (i == 0) {
//            int numExistingInstancesPerReplicaGroup = existingReplicaGroup.size();
//            replicaGroupBasedAssignmentState =
//                new ReplicaGroupBasedAssignmentState(numReplicaGroups, numInstancesPerReplicaGroup,
//                    numExistingReplicaGroups, numExistingInstancesPerReplicaGroup, numFaultDomains);
//          }
//
//          replicaGroupBasedAssignmentState.reconstructExistingAssignment(existingReplicaGroup, i,
//              aliveInstanceNameToFDMap);
//        }
//      } else {
//        // Fresh new assignment
//        replicaGroupBasedAssignmentState =
//            new ReplicaGroupBasedAssignmentState(numReplicaGroups, numInstancesPerReplicaGroup, numFaultDomains);
//      }
//      /*finish replicaGroupBasedAssignmentState initialization*/
//
//      Preconditions.checkState(replicaGroupBasedAssignmentState != null);
//
//      // preprocess the downsizing and exclude unchanged existing assigment from the candidate list
//      replicaGroupBasedAssignmentState.preprocessing(faultDomainToCandidateInstancesMap);
//
//      // preprocess the problem of numReplicaGroups >= numFaultDomains to a problem
//      replicaGroupBasedAssignmentState.normalize(faultDomainToCandidateInstancesMap);
//
//      // fill the remaining vacant seats
//      replicaGroupBasedAssignmentState.fill(faultDomainToCandidateInstancesMap);
//
//      // adjust the instance assignment to achieve the invariant state
//      replicaGroupBasedAssignmentState.swapToInvariantState();
//
//      // In the following we assign instances to partitions.
//      // TODO: refine this and segment assignment to minimize movement during numInstancesPerReplicaGroup uplift
//      // Assign instances within a replica-group to one partition if not configured
//      int numPartitions = _replicaGroupPartitionConfig.getNumPartitions();
//      if (numPartitions <= 0) {
//        numPartitions = 1;
//      }
//      // Assign all instances within a replica-group to each partition if not configured
//      int numInstancesPerPartition = _replicaGroupPartitionConfig.getNumInstancesPerPartition();
//      if (numInstancesPerPartition > 0) {
//        Preconditions.checkState(numInstancesPerPartition <= numInstancesPerReplicaGroup,
//            "Number of instances per partition: %s must be smaller or equal to number of instances per replica-group:"
//                + " %s", numInstancesPerPartition, numInstancesPerReplicaGroup);
//      } else {
//        numInstancesPerPartition = numInstancesPerReplicaGroup;
//      }
//      LOGGER.info("Selecting {} partitions, {} instances per partition within a replica-group for table: {}",
//          numPartitions, numInstancesPerPartition, _tableNameWithType);
//
//      // Assign consecutive instances within a replica-group to each partition.
//      // E.g. (within a replica-group, 5 instances, 3 partitions, 3 instances per partition)
//      // [i0, i1, i2, i3, i4]
//      //  p0  p0  p0  p1  p1
//      //  p1  p2  p2  p2
//      for (int replicaGroupId = 0; replicaGroupId < numReplicaGroups; replicaGroupId++) {
//        int instanceIdInReplicaGroup = 0;
//        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
//          List<String> instancesInPartition = new ArrayList<>(numInstancesPerPartition);
//          for (int instanceIdInPartition = 0; instanceIdInPartition < numInstancesPerPartition;
//              instanceIdInPartition++) {
//            instancesInPartition.add(
//                replicaGroupBasedAssignmentState._replicaGroupIdToInstancesMap[replicaGroupId][instanceIdInReplicaGroup].getInstanceName());
//            instanceIdInReplicaGroup = (instanceIdInReplicaGroup + 1) % numInstancesPerReplicaGroup;
//          }
//          LOGGER.info("Selecting instances: {} for replica-group: {}, partition: {} for table: {}",
//              instancesInPartition, replicaGroupId, partitionId, _tableNameWithType);
//          instancePartitions.setInstances(partitionId, replicaGroupId, instancesInPartition);
//        }
//      }
//    } else {
//      throw new IllegalStateException("Does not support Non-replica-group based selection");
//    }
//  }
//
//  void fill() {
//
//  }
//
//  ;
//
//  void checkConstraint() {
//
//  }
//
//  ;
//
//  void swapToAchievePoolDiversity() {
//
//  }
//
//  ;
//
//  void upliftWithMinimizedDataMovement() {
//
//  }
//
//  ;
//
//  void initialAssignment() {
//
//  }
//
//  ;
//
//  private static class CandidateQueue {
//    NavigableMap<Integer, Deque<String>> _map;
//    Integer _iter;
//
//    CandidateQueue(Map<Integer, LinkedHashSet<String>> faultDomainToCandidateInstancesMap) {
//      _map = new TreeMap<>();
//      faultDomainToCandidateInstancesMap.entrySet()
//          .stream()
//          .filter(kv -> !kv.getValue().isEmpty())
//          .forEach(kv -> _map.put(kv.getKey(), new LinkedList<>(kv.getValue())));
//      _iter = _map.firstKey();
//    }
//
//    void seekKey(int startKey) {
//      if (_map.containsKey(startKey)) {
//        _iter = startKey;
//      } else {
//        _iter = _map.ceilingKey(_iter);
//        _iter = (_iter == null && !_map.isEmpty()) ? _map.firstKey() : _iter;
//      }
//    }
//
//    Instance getNextCandidate() {
//      if (_iter == null) {
//        throw new IllegalStateException("Illegal state in fault-domain-aware assignment");
//      }
//
//      Instance ret = new Instance(_map.get(_iter).pollFirst(), _iter, Instance.NEW_INSTANCE);
//
//      if (_map.get(_iter).isEmpty()) {
//        _map.remove(_iter);
//      }
//
//      _iter = _map.higherKey(_iter);
//      _iter = (_iter == null && !_map.isEmpty()) ? _map.firstKey() : _iter;
//      return ret;
//    }
//  }
//
//  private static class ReplicaGroupBasedAssignmentState {
//    private static final int INVALID_FD = -1;
//
//    private Instance[][] _replicaGroupIdToInstancesMap;
//
//    private int _numMaxPools;
//    private int _numTargetReplicaGroups;
//    private int _numTargetInstancesPerReplicaGroup;
//
//    private int _numCurrentReplicaGroups;
//    private int _numCurrentInstancesPerReplicaGroup;
//
//    private int _mapDimReplicaGroup;
//    private int _mapDimInstancePerReplicaGroup;
//
//    private HashMap<Integer, Deque<InstanceConfig>> _candidateInstanceMap;
//    private Map<String, Integer> _candidateInstanceToPoolMap;
//
//    private int[] _numMissingInstancesPerInstanceOffset;
//    private int _numMissingInstances = 0;
//    private int[][] _poolCounter;
//
//    private void init(Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap,
//        InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig) {
//      // filter out pools with no instances
//      _candidateInstanceMap = poolToInstanceConfigsMap.entrySet()
//          .stream()
//          .filter(e -> e.getValue().size() > 0)
//          .collect(Collectors.toMap(Map.Entry::getKey, e -> new LinkedList<>(e.getValue()), (a, b) -> {
//            a.addAll(b);
//            return a;
//          }, HashMap::new));
//
//      // Map from instance name to pool id
//      _candidateInstanceToPoolMap = _candidateInstanceMap.entrySet()
//          .stream()
//          .flatMap(kv -> kv.getValue()
//              .stream()
//              .map(instance -> new AbstractMap.SimpleEntry<>(instance.getInstanceName(), kv.getKey())))
//          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//
//      _numMaxPools = replicaGroupPartitionConfig.getNumMaxPools();
//      _numTargetReplicaGroups = replicaGroupPartitionConfig.getNumReplicaGroups();
//      _numTargetInstancesPerReplicaGroup = replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup();
//    }
//
//    private void validatePoolDiversePreconditions(InstancePartitions existingInstancePartitions,
//        Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap) {
//
//      // numMaxPools should be positive
//      Preconditions.checkState(_numMaxPools > 0, "Number of max pools must be positive");
//      // numTargetReplicaGroups should be positive
//      Preconditions.checkState(_numTargetReplicaGroups > 0, "Number of replica-groups must be positive");
//      // numTargetInstancesPerReplica should be positive
//      Preconditions.checkState(_numTargetInstancesPerReplicaGroup > 0,
//          "Number of instances per replica must be positive");
//
//      // Validate the existing instance partitions is null or has only one partition
//      Preconditions.checkState(existingInstancePartitions == null || existingInstancePartitions.getNumPartitions() == 1,
//          "This algorithm does not support table level partitioning");
//
//      // Validate the pool to instance configs map is not null or empty
//      Preconditions.checkNotNull(poolToInstanceConfigsMap, "poolToInstanceConfigsMap is null");
//      int numPools = poolToInstanceConfigsMap.size();
//      Preconditions.checkState(numPools != 0, "No pool qualified for selection");
//
//      // Collect the number of instances in each pool with non-zero instances into a list and sort
//      // the list in descending order，so that the largest pool is at the head of the list
//      List<Integer> numInstancesPerPool = poolToInstanceConfigsMap.values()
//          .stream()
//          .map(List::size)
//          .filter(i -> i > 0)
//          .sorted(Collections.reverseOrder())
//          .collect(Collectors.toList());
//
//      // Should have non-zero total instances
//      Optional<Integer> totalInstancesOptional = numInstancesPerPool.stream().reduce(Integer::sum);
//      Preconditions.checkState(totalInstancesOptional.orElse(0) > 0, "The number of total instances is zero");
//
//      // The number of pools should be at least numTargetReplicaGroups if numTargetReplicaGroups is less than numMaxPools
//      // When numTargetReplicaGroups is greater than numMaxPools, we should use all pools
//      Preconditions.checkState(numInstancesPerPool.size() >= Math.min(_numMaxPools, _numTargetReplicaGroups),
//          "The number of pools %s is less than the minimum of numMaxPools %s and numTargetReplicaGroups %s", numPools,
//          _numMaxPools, _numTargetReplicaGroups);
//
//      // The total number of instances in the provided pools
//      int numTotalInstances = totalInstancesOptional.get();
//      // The total number of instances in the target cluster
//      int targetNumTotalInstances = _numTargetInstancesPerReplicaGroup * _numTargetReplicaGroups;
//
//      Preconditions.checkState(numTotalInstances < targetNumTotalInstances,
//          "The total number of instances %s is less than (the number of instances per "
//              + "replica group %s * the number of replica groups %s)", numTotalInstances,
//          _numTargetInstancesPerReplicaGroup, _numTargetReplicaGroups);
//
//      // the integer number of replica groups that needs to be filled by each pool
//      // this is > 1 only when numTargetReplicaGroups > numMaxPools
//      int quotient = _numTargetReplicaGroups / _numMaxPools;
//
//      // the number of instances in each pool should also be at least sufficient to fill
//      // (numTargetReplicaGroups / numMaxPools) replica groups. This check is only effective when numTargetReplicaGroups > numMaxPools
//      Preconditions.checkState(
//          numInstancesPerPool.get(numInstancesPerPool.size() - 1) >= quotient * _numTargetInstancesPerReplicaGroup,
//          "Validation failed. " + "The number of instances in each pool should also be at least sufficient to fill "
//              + "(numTargetReplicaGroups / numMaxPools) replica groups");
//
//      if (numTotalInstances == targetNumTotalInstances) {
//        // if the total number of instances is equal to the number of instances per replica group * the number of
//        // replica groups
//
//        //  the outstanding instances should be less than or equal to one replica group
//        //  otherwise the MZ diversity cannot be satisfied.
//        Preconditions.checkState(numInstancesPerPool.get(0) - quotient * _numTargetInstancesPerReplicaGroup
//                <= _numTargetInstancesPerReplicaGroup,
//            "Validation failed. The instances provided cannot satisfy the " + "pool diverse requirement.");
//      } else {
//        // if the total number of instances is greater than the number of instances per replica group * the number of
//        // replica groups
//
//        //  the outstanding instances should be able to fill the rest numTargetReplicaGroups % numMaxPools
//        //  replicas in a pool diverse manner.
//        validateLargePool(numInstancesPerPool, _numMaxPools, _numTargetInstancesPerReplicaGroup,
//            _numTargetReplicaGroups);
//      }
//      LOGGER.info("Validation passed. The instances provided can satisfy the pool diverse requirement.");
//      LOGGER.info("The number of instances per pool is {}", poolToInstanceConfigsMap.entrySet()
//          .stream()
//          .map(e -> "\n Pool:" + e.getKey() + " : " + e.getValue().size() + " instances;")
//          .collect(Collectors.joining()));
//      LOGGER.info("Trying to assign total {} instances to {} replica groups, " + "with {} instance per replica group",
//          numTotalInstances, _numTargetReplicaGroups, _numTargetInstancesPerReplicaGroup);
//    }
//
//    void reconstructReplicaGroupIdToInstancesMap(InstancePartitions existingInstancePartitions) {
//
//      Preconditions.checkState(existingInstancePartitions != null,
//          "Existing instance partitions should not be null when this function is called");
//
//      _numCurrentReplicaGroups = existingInstancePartitions.getNumReplicaGroups();
//      _numCurrentInstancesPerReplicaGroup = existingInstancePartitions.getInstances(0, 0).size();
//
//      _mapDimReplicaGroup = Math.max(_numCurrentReplicaGroups, _numTargetReplicaGroups);
//      _mapDimInstancePerReplicaGroup =
//          Math.max(_numCurrentInstancesPerReplicaGroup, _numTargetInstancesPerReplicaGroup);
//
//      _replicaGroupIdToInstancesMap = new Instance[_mapDimReplicaGroup][_mapDimInstancePerReplicaGroup];
//      _poolCounter = new int[_mapDimInstancePerReplicaGroup][_numMaxPools];
//      _numMissingInstancesPerInstanceOffset = new int[_mapDimInstancePerReplicaGroup];
//
//      Set<String> usedInstances = new HashSet<>();
//
//      // Reconstruct replicaGroupIdToInstancesMap from existingInstancePartitions, fill in instances that are in both
//      // candidateInstanceMap and existingInstancePartitions
//      for (int i = 0; i < _numCurrentReplicaGroups; i++) {
//        List<String> instances = existingInstancePartitions.getInstances(0, i);
//        Preconditions.checkState(instances.size() == _numCurrentInstancesPerReplicaGroup);
//        for (int j = 0; j < _numCurrentInstancesPerReplicaGroup; j++) {
//          String instanceName = instances.get(j);
//          if (_candidateInstanceToPoolMap.containsKey(instanceName)) {
//            _replicaGroupIdToInstancesMap[i][j] =
//                new Instance(instanceName, _candidateInstanceToPoolMap.get(instanceName));
//            _poolCounter[j][_candidateInstanceToPoolMap.get(instanceName)]++;
//            usedInstances.add(instanceName);
//          } else {
//            _replicaGroupIdToInstancesMap[i][j] = null;
//            _numMissingInstancesPerInstanceOffset[j]++;
//            _numMissingInstances++;
//          }
//        }
//      }
//
//      // Remove used instances from candidateInstanceMap
//      for (Map.Entry<Integer, Deque<InstanceConfig>> entry : _candidateInstanceMap.entrySet()) {
//        Deque<InstanceConfig> instances = entry.getValue();
//        instances.removeIf(instance -> usedInstances.contains(instance.getInstanceName()));
//      }
//    }
//
//    /**
//     * Validate the case where the total instances provided pools is greater than the number of instances in the target
//     * cluster
//     * @param numInstancesPerPool the provided number of instances in each pool
//     * @param numMaxPools the maximum number of pools
//     * @param numInstancesPerReplica the target number of instances per replica group
//     * @param numReplicaGroups the target number of replica groups
//     */
//    private void validateLargePool(List<Integer> numInstancesPerPool, int numMaxPools, int numInstancesPerReplica,
//        int numReplicaGroups) {
//      int targetNumTotalInstances = numInstancesPerReplica * numReplicaGroups;
//      int quotient = numReplicaGroups / numMaxPools;
//      for (Integer integer : numInstancesPerPool) {
//        int selected = Math.min(integer - quotient * numInstancesPerReplica, numInstancesPerReplica);
//        targetNumTotalInstances -= selected;
//        if (targetNumTotalInstances <= 0) {
//          return;
//        }
//      }
//      throw new IllegalStateException(
//          "Validation failed. The instances provided cannot satisfy the " + "pool diverse requirement.");
//    }
//
//    void fillMissingInstances() {
//      // return if there is no more missing instances
//      if (_numMissingInstances == 0) {
//        return;
//      }
//      // fill in missing instances, so that for a given set of mirrored servers
//      // the number of instances in each pool is at least _numCurrentInstancesPerReplicaGroup / _numMaxPools
//      int quotient = _numCurrentInstancesPerReplicaGroup / _numMaxPools;
//      for (int j = 0; j < _numCurrentInstancesPerReplicaGroup; j++) {
//        for (int i = 0; i < _numCurrentReplicaGroups; i++) {
//          if (_replicaGroupIdToInstancesMap[i][j] == null) {
//            for (int k = 0; k < _numMaxPools; k++) {
//              if (_poolCounter[j][k] < quotient) {
//                _replicaGroupIdToInstancesMap[i][j] =
//                    new Instance(_candidateInstanceMap.get(k).removeFirst().getInstanceName(), k);
//                if (_candidateInstanceMap.get(k).isEmpty()) {
//                  _candidateInstanceMap.remove(k);
//                }
//                _poolCounter[j][k]++;
//                _numMissingInstancesPerInstanceOffset[j]--;
//                _numMissingInstances--;
//                break;
//              }
//            }
//          }
//        }
//      }
//      // return if there is no more missing instances
//      if (_numMissingInstances == 0) {
//        return;
//      }
//      // fill in the rest missing instances
//      Map<Integer, Set<Integer>> poolIdToEligibleInstanceOffset = new HashMap<>();
//      for (int j = 0; j < _numCurrentInstancesPerReplicaGroup; j++) {
//        if (_numCurrentInstancesPerReplicaGroup == 0) {
//          continue;
//        }
//        for (Map.Entry<Integer, Deque<InstanceConfig>> e : _candidateInstanceMap.entrySet()) {
//          int poolId = e.getKey();
//          int finalJ = j;
//          // the number of instances in the pool equals to the quotient, meaning
//          if (_poolCounter[j][poolId] == quotient) {
//            poolIdToEligibleInstanceOffset.compute(poolId, (k, v) -> {
//              if (v == null) {
//                v = new HashSet<>();
//              }
//              v.add(finalJ);
//              return v;
//            });
//          }
//        }
//        while (_numMissingInstances > 0) {
//          Optional<Map.Entry<Integer, Set<Integer>>> min =
//              poolIdToEligibleInstanceOffset.entrySet().stream().min(Comparator.comparing(e -> e.getValue().size()));
//          Preconditions.checkState(min.isPresent());
//          Map.Entry<Integer, Set<Integer>> e = min.get();
//
//          int poolId = e.getKey();
//          int instanceOffset = e.getValue().iterator().next();
//          InstanceConfig instanceConfig = _candidateInstanceMap.get(poolId).removeFirst();
//
//          _numMissingInstances--;
//          _numMissingInstancesPerInstanceOffset[instanceOffset]--;
//
//          if (_numMissingInstancesPerInstanceOffset[instanceOffset] == 0) {
//            poolIdToEligibleInstanceOffset.forEach((key, value) -> value.remove(instanceOffset));
//            poolIdToEligibleInstanceOffset.entrySet().removeIf(entry -> entry.getValue().isEmpty());
//          }
//
//          if (_candidateInstanceMap.get(poolId).isEmpty()) {
//            _candidateInstanceMap.remove(poolId);
//            poolIdToEligibleInstanceOffset.remove(poolId);
//          }
//
//          for (int i = 0; i < _numCurrentReplicaGroups; i++) {
//            if (_replicaGroupIdToInstancesMap[i][instanceOffset] == null) {
//              _replicaGroupIdToInstancesMap[i][instanceOffset] =
//                  new Instance(instanceConfig.getInstanceName(), poolId);
//
//              _poolCounter[instanceOffset][poolId]++;
//              _numMissingInstancesPerInstanceOffset[instanceOffset]--;
//              _numMissingInstances--;
//              break;
//            }
//          }
//
//          _replicaGroupIdToInstancesMap[Instance.NEW_INSTANCE][instanceOffset] =
//              new Instance(e.getValue().removeFirst().getInstanceName(), poolId);
//          if (e.getValue().isEmpty()) {
//            _candidateInstanceMap.remove(poolId);
//          }
//
//          _poolCounter[instanceOffset][poolId]++;
//          _numMissingInstancesPerInstanceOffset[instanceOffset]--;
//          _numMissingInstances--;
//
//        }
//      }
//    }
//  }
//
//  private static class Instance {
//    static final int NEW_INSTANCE = -1;
//    private final String _instanceName;
//    private final int _poolId;
//    private final int _existingReplicaGroupId = 0;
//
//    public Instance(String instance, int poolId) {
//      _instanceName = instance;
//      _poolId = poolId;
//    }
//
//    String getInstanceName() {
//      return _instanceName;
//    }
//
//    int getPoolId() {
//      return _poolId;
//    }
//
//    int getExistingReplicaGroupId() {
//      return _existingReplicaGroupId;
//    }
//  }
}
