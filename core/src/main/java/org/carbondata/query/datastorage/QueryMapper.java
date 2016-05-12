/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.query.datastorage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;

/**
 * 1) Maintains the information about which MDX query is being executed by which
 * thread. Can use execution id to identify the MDX sequence.
 * 2) Maintains registered delta copies of cubes available for this execution.
 * Even sub query against DB for this MDX can work only on available slices to
 * give consistent results across the MDX query life cycle.
 * 3) Maintains listeners on queries and inform them when query execution is
 * finished.
 */
public final class QueryMapper {

  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(QueryMapper.class.getName());
  /**
   * Map<CubeName, Map<ThreadID, QueryID>>
   */
  private static Map<String, Map<Long, Long>> executionMap =
      new HashMap<String, Map<Long, Long>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  /**
   * Map<ThreadID, List<SliceIDs>>
   */
  private static Map<Long, List<Long>> executionToSlicesMap =
      new HashMap<Long, List<Long>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  /**
   * QueryId --> List<SliceListeners>
   */
  private static Map<Long, List<SliceListener>> listeners =
      new HashMap<Long, List<SliceListener>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  private QueryMapper() {

  }

  /**
   * Unregister the query on this thread
   *
   * @param queryID
   */
  public static synchronized void queryEnd(String cubeUniqueName, long queryID, boolean publish) {
    Long threadId = Thread.currentThread().getId();
    Map<Long, Long> cubeMap = executionMap.get(cubeUniqueName);
    cubeMap = executionMap.get(cubeUniqueName);
    if (cubeMap != null) {
      // Remove the query entry from thread
      Long queryId = cubeMap.remove(threadId);

      // Remove slices registry for thread/query
      executionToSlicesMap.remove(threadId);

      if (publish) {
        // if multiple threads are using executing single query then we
        // need to only
        // ensure that the listeners are called only for the main thread
        invokeListeners(queryId);
      }
    }
  }

  /**
   * Call the listeners registered for this query
   *
   * @param queryId
   */
  private static void invokeListeners(Long queryId) {
    List<SliceListener> listOnQuery = listeners.get(queryId);

    if (listOnQuery == null || listOnQuery.size() == 0) {
      return;
    }

    List<SliceListener> toRemove =
        new ArrayList<SliceListener>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

    for (SliceListener listener : listOnQuery) {
      // Check and intimate listeners
      listener.fireQueryFinish(queryId);

      if (!listener.stillListening()) {
        // Make listers with zero query references
        toRemove.add(listener);
      }
    }

    // Remove all unwanted listeners
    if (toRemove.size() > 0) {
      listOnQuery.removeAll(toRemove);
    }
  }

}
