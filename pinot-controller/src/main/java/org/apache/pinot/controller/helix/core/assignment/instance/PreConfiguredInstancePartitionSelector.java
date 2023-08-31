package org.apache.pinot.controller.helix.core.assignment.instance;

import com.google.common.base.Preconditions;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PreConfiguredInstancePartitionSelector extends InstancePartitionSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(PoolDiverseInstancePartitionSelector.class);
  private final InstancePartitions _preConfiguredInstancePartitions;
  private final int _numTargetInstancesPerReplicaGroup;
  private final int _numTargetReplicaGroups;
  private final int _numTargetTotalInstances;
  private final List<List<String>> _preConfiguredMirroredServerLists = new ArrayList<>();
  private final List<Set<String>> _preConfiguredMirroredServerSets = new ArrayList<>();
  private final Map<String, Integer> _preConfiguredInstanceNameToOffsetMap = new HashMap<>();
  private final List<List<String>> _existingMirroredServerLists = new ArrayList<>();
  private final List<Set<String>> _existingMirroredServerSets = new ArrayList<>();
  private final Map<String, Integer> _existingInstanceNameToOffsetMap = new HashMap<>();
  private int _numPreConfiguredReplicaGroups;
  private int _numPreConfiguredInstancesPerReplicaGroup;
  private int _numExistingReplicaGroups;
  private int _numExistingInstancesPerReplicaGroup;

  public PreConfiguredInstancePartitionSelector(InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig,
      String tableNameWithType, InstancePartitions existingInstancePartitions,
      InstancePartitions preConfiguredInstancePartitions) {
    super(replicaGroupPartitionConfig, tableNameWithType, existingInstancePartitions);
    _preConfiguredInstancePartitions = preConfiguredInstancePartitions;
    _numTargetInstancesPerReplicaGroup = _replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup();
    _numTargetReplicaGroups = _replicaGroupPartitionConfig.getNumReplicaGroups();
    _numTargetTotalInstances = _numTargetInstancesPerReplicaGroup * _numTargetReplicaGroups;
  }

  /**
   * validate if the poolToInstanceConfigsMap is a valid input for pool-diverse replica-group selection
   */
  private void validatePoolDiversePreconditions(Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap) {

    LOGGER.info("Validating pre-configured instance partitions for pre-configuration based replica-group selection");

    // numTargetInstancesPerReplica should be positive
    Preconditions.checkState(_numTargetInstancesPerReplicaGroup > 0,
        "Number of instances per replica must be positive");
    // _numTargetReplicaGroups should be positive
    Preconditions.checkState(_numTargetReplicaGroups > 0, "Number of replica-groups must be positive");
    // validate target partition count is 1
    Preconditions.checkState(_replicaGroupPartitionConfig.getNumPartitions() == 1,
        "This algorithm does not support table level partitioning for target assignment");

    // Validate the existing instance partitions is null or has only one partition
    Preconditions.checkState(
        (_existingInstancePartitions == null || _existingInstancePartitions.getNumPartitions() == 1),
        "This algorithm does not support table level partitioning for existing assignment");

    _numExistingReplicaGroups =
        _existingInstancePartitions == null ? 0 : _existingInstancePartitions.getNumReplicaGroups();
    _numExistingInstancesPerReplicaGroup =
        _existingInstancePartitions == null ? 0 : _existingInstancePartitions.getInstances(0, 0).size();

    // Validate the pre-configured instance partitions is not null and has only one partition
    Preconditions.checkState(_preConfiguredInstancePartitions != null,
        "Pre-configured instance partitions must be provided for pre-configuration based selection");
    Preconditions.checkState(_preConfiguredInstancePartitions.getNumPartitions() == 1,
        "This algorithm does not support table level partitioning for pre-configured assignment");

    // Validate the number of replica-groups in the pre-configured instance partitions is equal to the target
    // number of replica-groups
    _numPreConfiguredReplicaGroups = _preConfiguredInstancePartitions.getNumReplicaGroups();
    Preconditions.checkState(_numPreConfiguredReplicaGroups == _numTargetReplicaGroups,
        "The number of replica-groups in the pre-configured instance partitions "
            + "is not equal to the target number of replica-groups");
    // Validate the number of instances per replica-group in the pre-configured instance partitions is greater than or
    // equal to the target number of instances per replica-group
    _numPreConfiguredInstancesPerReplicaGroup = _preConfiguredInstancePartitions.getInstances(0, 0).size();
    Preconditions.checkState(_numTargetInstancesPerReplicaGroup >= _numTargetInstancesPerReplicaGroup,
        "The number of instances per replica-group in the pre-configured "
            + "instance partitions is less than the target number of instances per replica-group");

    // Validate the pool to instance configs map is not null or empty
    Preconditions.checkNotNull(poolToInstanceConfigsMap, "poolToInstanceConfigsMap is null");
    int numPools = poolToInstanceConfigsMap.size();
    Preconditions.checkState(numPools > 0, "No pool qualified for selection");
    Preconditions.checkState(poolToInstanceConfigsMap.values().stream().map(List::size).reduce(Integer::sum).get()
            >= _numTargetTotalInstances,
        "The total number of instances in all pools is less than the target number of target instances");

    LOGGER.info("Validation passed. The instances provided can satisfy the pool diverse requirement.");
    LOGGER.info("Trying to assign total {} instances to {} replica groups, " + "with {} instance per replica group",
        _numTargetTotalInstances, _numTargetReplicaGroups, _numTargetInstancesPerReplicaGroup);
  }

  void createListFromPreConfiguredInstanceAssignmentMap() {
    List<List<String>> preConfiguredReplicaGroups = new ArrayList<>(_numPreConfiguredReplicaGroups);
    for (int i = 0; i < _numPreConfiguredReplicaGroups; i++) {
      preConfiguredReplicaGroups.set(i, _preConfiguredInstancePartitions.getInstances(0, i));
    }

    for (int j = 0; j < _numPreConfiguredInstancesPerReplicaGroup; j++) {
      List<String> mirroredServerList = new ArrayList<>();
      for (int i = 0; i < _numPreConfiguredReplicaGroups; i++) {
        mirroredServerList.add(preConfiguredReplicaGroups.get(i).get(j));
      }
      _preConfiguredMirroredServerLists.add(mirroredServerList);
    }
  }

  void createLookupTablesFromPreConfiguredInstanceAssignmentMap() {
    List<List<String>> preConfiguredReplicaGroups = new ArrayList<>(_numPreConfiguredReplicaGroups);
    for (int i = 0; i < _numPreConfiguredReplicaGroups; i++) {
      preConfiguredReplicaGroups.set(i, _preConfiguredInstancePartitions.getInstances(0, i));
    }

    for (int j = 0; j < _numPreConfiguredInstancesPerReplicaGroup; j++) {
      HashSet<String> mirroredServerSet = new HashSet<>();
      for (int i = 0; i < _numPreConfiguredReplicaGroups; i++) {
        mirroredServerSet.add(preConfiguredReplicaGroups.get(i).get(j));
      }
      _preConfiguredMirroredServerSets.add(mirroredServerSet);
    }

    for (int i = 0; i < _numPreConfiguredReplicaGroups; i++) {
      for (int j = 0; j < _numPreConfiguredInstancesPerReplicaGroup; j++) {
        String instance = preConfiguredReplicaGroups.get(i).get(j);
        _preConfiguredInstanceNameToOffsetMap.put(instance, j);
      }
    }
  }

  @Override
  void selectInstances(Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap,
      InstancePartitions instancePartitions) {
    if (_replicaGroupPartitionConfig.isReplicaGroupBased()) {
      validatePoolDiversePreconditions(poolToInstanceConfigsMap);
      if (_existingInstancePartitions == null) {
        // If no existing instance partitions, create new instance partitions based on the pre-configured instance
        // partitions. This is done by just selecting _targetNumInstancesPerReplicaGroup set of mirrored servers
        // from the pre-configured instance partitions.
        LOGGER.info("No existing instance partitions found. Will build new on top of"
            + " the pre-configured instance partitions");
        // create a list of lists of mirrored servers from the pre-configured instance partitions
        createListFromPreConfiguredInstanceAssignmentMap();
        // shuffle the list of lists of mirrored servers based on the table name hash
        int tableNameHash = Math.abs(_tableNameWithType.hashCode());
        Collections.shuffle(_preConfiguredMirroredServerLists, new Random(tableNameHash));

        // create the instance partitions based on the rotated list of mirrored servers
        List<List<String>> resultReplicaGroups = new ArrayList<>(_numTargetReplicaGroups);
        for (int i = 0; i < _numTargetReplicaGroups; i++) {
          resultReplicaGroups.set(i, new ArrayList<>(_numTargetInstancesPerReplicaGroup));
        }
        for (int j = 0; j < _numTargetInstancesPerReplicaGroup; j++) {
          for (int i = 0; i < _numTargetReplicaGroups; i++) {
            resultReplicaGroups.get(i).add(_preConfiguredMirroredServerLists.get(j).get(i));
          }
        }
        for (int i = 0; i < _numTargetReplicaGroups; i++) {
          instancePartitions.setInstances(0, i, resultReplicaGroups.get(i));
        }
      } else {
        // If existing instance partitions exist, adjust the existing instance partitions based on the pre-configured
        // instance partitions. This code path takes care of instance replacement, uplift, and downlift.
        // This is done by search in the pre-configured instance partitions for the mirrored
        // servers sets that are similar to the existing sets in instance partitions.
        LOGGER.info("Existing instance partitions found. Will adjust the existing instance partitions"
            + " based on the pre-configured instance partitions");
        createListFromPreConfiguredInstanceAssignmentMap();
        createLookupTablesFromPreConfiguredInstanceAssignmentMap();
        createListAndLookupTablesFromExistingInstancePartitions();
        Set<Integer> usedInstanceOffsets = new HashSet<>();
        List<Map.Entry<Integer, Long>> results = new ArrayList<>(_numTargetInstancesPerReplicaGroup);

        for (int j = 0; j < _numExistingInstancesPerReplicaGroup; j++) {
          List<String> existingMirroredServers = _existingMirroredServerLists.get(j);
          existingMirroredServers.stream()
              .map(_preConfiguredInstanceNameToOffsetMap::get)
              .filter(offset -> !usedInstanceOffsets.contains(offset))
              .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
              .entrySet().stream().max(Map.Entry.comparingByValue()).ifPresent(e -> {
                results.add(e);
                usedInstanceOffsets.add(e.getKey());
              });
        }

        if (results.size() <= _numTargetInstancesPerReplicaGroup) {
          for (int i = 0, j = 0; i < _numTargetInstancesPerReplicaGroup - results.size(); j++) {
            if (!usedInstanceOffsets.contains(j)) {
              results.add(new AbstractMap.SimpleEntry<>(j, 0L));
              i++;
            }
          }

          List<List<String>> resultReplicaGroups = new ArrayList<>(_numTargetReplicaGroups);
          for (int i = 0; i < _numTargetReplicaGroups; i++) {
            resultReplicaGroups.set(i, new ArrayList<>(_numTargetInstancesPerReplicaGroup));
          }
          for (int j = 0; j < _numTargetInstancesPerReplicaGroup; j++) {
            List<String> mirrorServers = _preConfiguredMirroredServerLists.get(results.get(j).getKey());
            for (int i = 0; i < _numTargetReplicaGroups; i++) {
              resultReplicaGroups.get(i).add(mirrorServers.get(i));
            }
          }
          for (int i = 0; i < _numTargetReplicaGroups; i++) {
            instancePartitions.setInstances(0, i, resultReplicaGroups.get(i));
          }
        } else {
          throw new IllegalStateException("Downlift by instance per replica group is not supported yet");
        }
      }
    } else {
      throw new IllegalStateException("Does not support Non-replica-group based selection");
    }
  }

  private void createListAndLookupTablesFromExistingInstancePartitions() {
    List<List<String>> existingReplicaGroups = new ArrayList<>(_numTargetReplicaGroups);
    for (int i = 0; i < _numExistingReplicaGroups; i++) {
      existingReplicaGroups.set(i, _existingInstancePartitions.getInstances(0, i));
    }

    for (int j = 0; j < _numExistingInstancesPerReplicaGroup; j++) {
      List<String> existingMirroredServerList = new ArrayList<>();
      for (int i = 0; i < _numExistingReplicaGroups; i++) {
        existingMirroredServerList.add(existingReplicaGroups.get(i).get(j));
      }
      _existingMirroredServerLists.add(existingMirroredServerList);
    }

    for (int j = 0; j < _numExistingInstancesPerReplicaGroup; j++) {
      HashSet<String> existingMirroredServerSet = new HashSet<>();
      for (int i = 0; i < _numExistingReplicaGroups; i++) {
        existingMirroredServerSet.add(existingReplicaGroups.get(i).get(j));
      }
      _existingMirroredServerSets.add(existingMirroredServerSet);
    }

    for (int i = 0; i < _numExistingReplicaGroups; i++) {
      for (int j = 0; j < _numTargetInstancesPerReplicaGroup; j++) {
        String instance = existingReplicaGroups.get(i).get(j);
        _existingInstanceNameToOffsetMap.put(instance, j);
      }
    }
  }
}
