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

package org.carbondata.query.filters.measurefilter;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.constants.CarbonCommonConstants;

public final class MeasureFilterUtil {
  private MeasureFilterUtil() {

  }


  /**
   * getMsrFilterIndexes
   *
   * @param measureFilters
   * @return
   */
  public static int[] getMsrFilterIndexes(MeasureFilter[][] measureFilters) {

    List<Integer> msrInexes = new ArrayList<Integer>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

    for (int i = 0; i < measureFilters.length; i++) {
      if (measureFilters[i] != null) {
        msrInexes.add(i);
      }
    }

    return convertListToArray(msrInexes);

  }

  /**
   * Checks whether measure filter is enabled or not.
   *
   * @param measureFilters
   * @return, true if measure filter is enabled, false otherwise.
   */
  public static boolean isMsrFilterEnabled(MeasureFilter[] measureFilters) {
    if (measureFilters == null) {
      return false;
    }
    for (int i = 0; i < measureFilters.length; i++) {
      if (measureFilters[i] instanceof MeasureGroupFilter) {
        if (((MeasureGroupFilter) measureFilters[i]).isMsrFilterEnabled()) {
          return true;
        }
      }

    }

    return false;

  }

  /**
   * @param msrInexes
   * @return
   */
  public static int[] convertListToArray(List<Integer> msrInexes) {
    int[] indxes = new int[msrInexes.size()];

    int i = 0;
    for (Integer integer : msrInexes) {
      indxes[i++] = integer;
    }
    return indxes;
  }

}
