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

import java.io.Serializable;

import org.carbondata.core.carbon.Exp;

public class MeasureFilterModel implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = -1959494583324268999L;

  /**
   * filterValue
   */
  private double filterValue;

  /**
   * filterType
   */
  private MeasureFilterType filterType;

  /**
   * Calc expression
   */
  private transient Exp exp;


  public MeasureFilterModel() {

  }

  /**
   * @return the filterValue
   */
  public double getFilterValue() {
    return filterValue;
  }

  /**
   * @return the filterType
   */
  public MeasureFilterType getFilterType() {
    return filterType;
  }

  /**
   * @return the exp
   */
  public Exp getExp() {
    return exp;
  }

  /**
   * It is enum class for measure filter types.
   *
   * @author R00900208
   */
  public enum MeasureFilterType {
    /**
     * filterType
     */
    EQUAL_TO,
    /**
     * NOT_EQUAL_TO
     */
    NOT_EQUAL_TO,
    /**
     * GREATER_THAN
     */
    GREATER_THAN,
    /**
     * LESS_THAN
     */
    LESS_THAN,
    /**
     * LESS_THAN_EQUAL
     */
    LESS_THAN_EQUAL,
    /**
     * GREATER_THAN_EQUAL
     */
    GREATER_THAN_EQUAL,
    /**
     * NOT_EMPTY
     */
    NOT_EMPTY;
  }

}
