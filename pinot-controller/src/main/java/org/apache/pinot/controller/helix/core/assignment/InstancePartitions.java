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
package org.apache.pinot.controller.helix.core.assignment;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.ZNRecord;


/**
 * Instance (server) partitions for the table.
 *
 * <p>The instance partitions are stored as a map from partition of the format: {@code <partitionId>_<replicaId>} to
 * list of server instances.
 */
public class InstancePartitions {
  private static final char SPLITTER = '_';

  private final String _tableNameWithType;
  private final Map<String, List<String>> _partitionToInstancesMap;
  private int _numPartitions;
  private int _numReplicas;

  public InstancePartitions(String tableNameWithType) {
    _tableNameWithType = tableNameWithType;
    _partitionToInstancesMap = new TreeMap<>();
  }

  private InstancePartitions(String tableNameWithType, Map<String, List<String>> partitionToInstancesMap) {
    _tableNameWithType = tableNameWithType;
    _partitionToInstancesMap = partitionToInstancesMap;
    for (String key : partitionToInstancesMap.keySet()) {
      int splitterIndex = key.indexOf(SPLITTER);
      int partition = Integer.parseInt(key.substring(0, splitterIndex));
      int replica = Integer.parseInt(key.substring(splitterIndex + 1));
      _numPartitions = Integer.max(_numPartitions, partition + 1);
      _numReplicas = Integer.max(_numReplicas, replica + 1);
    }
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public int getNumPartitions() {
    return _numPartitions;
  }

  public int getNumReplicas() {
    return _numReplicas;
  }

  public List<String> getInstances(int partitionId, int replicaId) {
    return _partitionToInstancesMap.get(Integer.toString(partitionId) + SPLITTER + replicaId);
  }

  public void setInstances(int partitionId, int replicaId, List<String> instances) {
    String key = Integer.toString(partitionId) + SPLITTER + replicaId;
    _partitionToInstancesMap.put(key, instances);
    _numPartitions = Integer.max(_numPartitions, partitionId + 1);
    _numReplicas = Integer.max(_numReplicas, replicaId + 1);
  }

  public static InstancePartitions fromZNRecord(ZNRecord znRecord) {
    return new InstancePartitions(znRecord.getId(), znRecord.getListFields());
  }

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_tableNameWithType);
    znRecord.setListFields(_partitionToInstancesMap);
    return znRecord;
  }

  @Override
  public String toString() {
    return _partitionToInstancesMap.toString();
  }
}
