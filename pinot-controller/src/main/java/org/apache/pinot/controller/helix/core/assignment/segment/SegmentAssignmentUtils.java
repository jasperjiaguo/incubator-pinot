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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;


/**
 * Utility class for segment assignment.
 */
public class SegmentAssignmentUtils {
  private SegmentAssignmentUtils() {
  }

  /**
   * Returns a map from instance name to the instance id (the index in the given list).
   */
  public static Map<String, Integer> getInstanceNameToIdMap(List<String> instances) {
    int numInstances = instances.size();
    Map<String, Integer> instanceNameToIdMap = new HashMap<>();
    for (int i = 0; i < numInstances; i++) {
      instanceNameToIdMap.put(instances.get(i), i);
    }
    return instanceNameToIdMap;
  }

  /**
   * Returns the number of segments assigned to each instance.
   */
  public static int[] getNumSegmentsAssigned(Map<String, Map<String, String>> segmentAssignment,
      Map<String, Integer> instanceNameToIdMap) {
    int[] numSegmentsPerInstance = new int[instanceNameToIdMap.size()];
    for (Map<String, String> instanceStateMep : segmentAssignment.values()) {
      for (String instanceName : instanceStateMep.keySet()) {
        Integer instanceId = instanceNameToIdMap.get(instanceName);
        if (instanceId != null) {
          numSegmentsPerInstance[instanceId]++;
        }
      }
    }
    return numSegmentsPerInstance;
  }

  /**
   * Returns the number of segments assigned to each instance.
   */
  public static int[] getNumSegmentsAssigned(Map<String, Map<String, String>> segmentAssignment,
      List<String> instances) {
    return getNumSegmentsAssigned(segmentAssignment, getInstanceNameToIdMap(instances));
  }

  /**
   * Returns a map from instance name to number of segments to be moved to it.
   */
  public static Map<String, Integer> getNumSegmentsToBeMoved(Map<String, Map<String, String>> oldAssignment,
      Map<String, Map<String, String>> newAssignment) {
    Map<String, Integer> numSegmentsToBeMoved = new TreeMap<>();
    for (Map.Entry<String, Map<String, String>> entry : newAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Set<String> newInstancesAssigned = entry.getValue().keySet();
      Set<String> oldInstancesAssigned = oldAssignment.get(segmentName).keySet();
      // For each new assigned instance, check if the segment needs to be moved to it
      for (String instanceName : newInstancesAssigned) {
        if (!oldInstancesAssigned.contains(instanceName)) {
          numSegmentsToBeMoved.merge(instanceName, 1, Integer::sum);
        }
      }
    }
    return numSegmentsToBeMoved;
  }
}
