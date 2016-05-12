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

package org.carbondata.query.executer.impl;

import java.util.List;

import org.carbondata.core.carbon.SqlStatement;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.query.datastorage.InMemoryTable;

public class QueryExecuterProperties {

  /**
   * dimension table array
   */
  protected Dimension[] dimTables;

  /**
   * list of active slices present for execution
   */
  protected List<InMemoryTable> slices;

  /**
   * global key generator basically it is the last slice updated keygenerator
   */
  protected KeyGenerator globalKeyGenerator;

  /**
   * uniqueValue
   */
  protected Object[] uniqueValue;

  /**
   * masked bytes
   */
  protected int[] maskedBytes;

  /**
   * isCountMsrExistInCurrTable
   */
  protected boolean isCountMsrExistInCurrTable;

  /**
   * count msr index in current table
   */
  protected int countMsrIndex = -1;

  /**
   * measureStartIndex
   */
  protected int measureStartIndex;

  /**
   * aggTypes
   */
  protected String[] aggTypes;

  /**
   * msrMinValue
   */
  protected Object[] msrMinValue;

  /**
   * isFunctionQuery
   */
  protected boolean isFunctionQuery;

  /**
   * aggExpressionStartIndex
   */
  protected int aggExpressionStartIndex;

  /**
   * sortDimIndexex
   */
  protected byte[] sortDimIndexes;

  /**
   * array of sql datatypes of mesaures and dimensions
   */
  protected SqlStatement.Type[] dataTypes;

}
