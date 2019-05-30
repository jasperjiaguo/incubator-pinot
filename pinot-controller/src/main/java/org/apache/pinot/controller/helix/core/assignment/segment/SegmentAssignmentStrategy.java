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

import java.util.List;
import java.util.Map;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitions;


/**
 * Strategy to assign segment to instances or rebalance all segments in a table.
 */
public interface SegmentAssignmentStrategy {

  /**
   * Initializes the segment assignment strategy.
   */
  void init(ZkHelixPropertyStore<ZNRecord> propertyStore, TableConfig tableConfig);

  /**
   * Assigns a new segment.
   *
   * @return List of servers to assign the segment to
   */
  List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions);

  /**
   * Rebalances the segments for a table.
   *
   * @return the rebalanced assignment for the segments
   */
  Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions);
}
