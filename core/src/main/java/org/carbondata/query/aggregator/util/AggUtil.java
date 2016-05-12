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
package org.carbondata.query.aggregator.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.SqlStatement;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.query.aggregator.CustomCarbonAggregateExpression;
import org.carbondata.query.aggregator.CustomMeasureAggregator;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.aggregator.impl.*;

/**
 * Class Description : AggUtil class for aggregate classes It will return
 * aggregator instance for measures
 */
public final class AggUtil {

  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER = LogServiceFactory.getLogService(AggUtil.class.getName());
  /**
   * measure ordinal number
   */
  public static int[] measureOrdinal;
  /**
   * all measures
   */
  public static SqlStatement.Type[] allMeasures;
  /**
   * type of Measure
   */
  private static HashMap<Integer, SqlStatement.Type> queryMeasureType = new HashMap<>();

  private AggUtil() {

  }

  /**
   * This method will return aggregate instance based on aggregate name
   *
   * @param aggregatorType aggregator Type
   * @param hasFactCount   whether table has fact count or not
   * @param generator      key generator
   * @return MeasureAggregator
   */
  public static MeasureAggregator getAggregator(String aggregatorType, boolean isNoDictionary,
      boolean hasFactCount, KeyGenerator generator, boolean isSurrogateBasedDistinctCountRequired,
      Object minValue, SqlStatement.Type dataType) {
    // this will be used for aggregate table because aggregate tables will
    // have one of the measure as fact count
    return getAggregator(aggregatorType, isNoDictionary, generator,
        isSurrogateBasedDistinctCountRequired, minValue, dataType);
  }

  /**
   * get Aggregator by dataType and aggregatorType
   *
   * @param aggregatorType
   * @param isNoDictionary
   * @param generator
   * @param isSurrogateGeneratedDistinctCount
   * @param minValue
   * @param dataType
   * @return
   */
  private static MeasureAggregator getAggregator(String aggregatorType, boolean isNoDictionary,
      KeyGenerator generator, boolean isSurrogateGeneratedDistinctCount, Object minValue,
      SqlStatement.Type dataType) {
    // get the MeasureAggregator based on aggregate type
    if (CarbonCommonConstants.MIN.equalsIgnoreCase(aggregatorType)) {
      return new MinAggregator();
    } else if (CarbonCommonConstants.COUNT.equalsIgnoreCase(aggregatorType)) {
      return new CountAggregator();
    }
    //
    else if (CarbonCommonConstants.MAX.equalsIgnoreCase(aggregatorType)) {
      return new MaxAggregator();
    }
    //
    else if (CarbonCommonConstants.AVERAGE.equalsIgnoreCase(aggregatorType)) {
      switch (dataType) {
        case LONG:

          return new AvgLongAggregator();
        case DECIMAL:

          return new AvgBigDecimalAggregator();
        default:

          return new AvgDoubleAggregator();
      }
    }
    //
    else if (CarbonCommonConstants.DISTINCT_COUNT.equalsIgnoreCase(aggregatorType)) {
      if (isNoDictionary) {
        return new DistinctStringCountAggregator();
      }
      return new DistinctCountAggregator(minValue);
    } else if (CarbonCommonConstants.SUM.equalsIgnoreCase(aggregatorType)) {
      switch (dataType) {
        case LONG:

          return new SumLongAggregator();
        case DECIMAL:

          return new SumBigDecimalAggregator();
        default:

          return new SumDoubleAggregator();
      }
    } else if (CarbonCommonConstants.SUM_DISTINCT.equalsIgnoreCase(aggregatorType)) {
      switch (dataType) {
        case LONG:

          return new SumDistinctLongAggregator();
        case DECIMAL:

          return new SumDistinctBigDecimalAggregator();
        default:

          return new SumDistinctDoubleAggregator();
      }
    } else if (CarbonCommonConstants.DUMMY.equalsIgnoreCase(aggregatorType)) {
      switch (dataType) {
        case LONG:

          return new DummyLongAggregator();
        case DECIMAL:

          return new DummyBigDecimalAggregator();
        default:

          return new DummyDoubleAggregator();
      }
    } else {
      return null;
    }
  }

  private static MeasureAggregator getCustomAggregator(String aggregatorType,
      String aggregatorClassName, KeyGenerator generator, String cubeUniqueName) {
    //
    try {
      Class customAggregatorClass = Class.forName(aggregatorClassName);
      Constructor declaredConstructor =
          customAggregatorClass.getDeclaredConstructor(KeyGenerator.class, String.class);
      return (MeasureAggregator) declaredConstructor.newInstance(generator, cubeUniqueName);
    } catch (ClassNotFoundException e) {
      LOGGER.error(e,
          "No custom class named " + aggregatorClassName + " was found");
    }
    //
    catch (SecurityException e) {
      LOGGER.error(e,
          "Security Exception while loading custom class " + aggregatorClassName);
    }
    //
    catch (NoSuchMethodException e) {
      LOGGER.error(e,
          "Required constructor for custom class " + aggregatorClassName + " not found");
    } catch (IllegalArgumentException e) {
      LOGGER.error(e,
          "IllegalArgumentException while loading custom class " + aggregatorClassName);
    }
    //
    catch (InstantiationException e) {
      LOGGER.error(e,
          "InstantiationException while loading custom class " + aggregatorClassName);
    }
    //
    catch (IllegalAccessException e) {
      LOGGER.error(e,
          "IllegalAccessException while loading custom class " + aggregatorClassName);
    } catch (InvocationTargetException e) {
      LOGGER.error(e,
          "InvocationTargetException while loading custom class " + aggregatorClassName);
    }
    // return default sum aggregator in case something wrong happen and log it
    LOGGER.info("Custom aggregator could not be loaded, " + "returning the default Sum Aggregator");
    return null;
  }

  /**
   * This method determines what agg needs to be given for each aggregateNames
   *
   * @param aggregateNames list of MeasureAggregator name
   * @param hasFactCount   is fact count present
   * @param generator      key generator
   * @return MeasureAggregator array which will hold agrregator for each
   * aggregateNames
   */
  public static MeasureAggregator[] getAggregators(List<String> aggregateNames,
      List<String> aggregatorClassName, boolean hasFactCount, KeyGenerator generator,
      String cubeUniqueName, Object[] minValue, char[] type) {
    int valueSize = aggregateNames.size();
    MeasureAggregator[] aggregators = new MeasureAggregator[valueSize];
    for (int i = 0; i < valueSize; i++) {
      if (aggregateNames.get(i).equals(CarbonCommonConstants.CUSTOM)) {
        aggregators[i] =
            getCustomAggregator(aggregateNames.get(i), aggregatorClassName.get(i), generator,
                cubeUniqueName);
      } else {
        SqlStatement.Type dataType = null;
        switch (type[i]) {
          case 'l':
            dataType = SqlStatement.Type.LONG;
            break;
          case 'b':
            dataType = SqlStatement.Type.DECIMAL;
            break;
          default:
            dataType = SqlStatement.Type.DOUBLE;

        }
        aggregators[i] =
            getAggregator(aggregateNames.get(i), hasFactCount, generator, false, minValue[i],
                dataType);
      }
    }
    return aggregators;
  }

  public static MeasureAggregator[] getAggregators(String[] aggType, boolean hasFactCount,
      KeyGenerator generator, String cubeUniqueName, Object[] minValue, boolean[] noDictionaryTypes,
      SqlStatement.Type[] dataTypes) {
    MeasureAggregator[] aggregators = new MeasureAggregator[aggType.length];
    for (int i = 0; i < aggType.length; i++) {
      SqlStatement.Type dataType = dataTypes[i];
      if (null != noDictionaryTypes) {
        aggregators[i] =
            getAggregator(aggType[i], noDictionaryTypes[i], hasFactCount, generator, false,
                minValue[i], dataType);
      } else {
        aggregators[i] =
            getAggregator(aggType[i], false, hasFactCount, generator, false, minValue[i], dataType);
      }
    }
    return aggregators;

  }

  public static MeasureAggregator[] getAggregators(String[] aggType,
      List<CustomCarbonAggregateExpression> aggregateExpressions, boolean hasFactCount,
      KeyGenerator generator, String cubeUniqueName, Object[] minValue, boolean[] noDictionaryTypes,
      SqlStatement.Type[] dataTypes) {
    MeasureAggregator[] aggregators = new MeasureAggregator[aggType.length];
    int customIndex = 0;
    for (int i = 0; i < aggType.length; i++) {

      if (aggType[i].equalsIgnoreCase(CarbonCommonConstants.CUSTOM)) {
        CustomCarbonAggregateExpression current = aggregateExpressions.get(customIndex++);
        if (current != null && current.getAggregator() != null) {
          aggregators[i] = (CustomMeasureAggregator) current.getAggregator().getCopy();
        } else {
          LOGGER.error("Unable to load custom aggregator");
        }
      } else {
        SqlStatement.Type dataType = dataTypes[i];
        if (null != noDictionaryTypes) {
          aggregators[i] =
              getAggregator(aggType[i], noDictionaryTypes[i], hasFactCount, generator, false,
                  minValue[i], dataType);
        } else {
          aggregators[i] =
              getAggregator(aggType[i], false, hasFactCount, generator, false, minValue[i],
                  dataType);
        }
      }
    }
    return aggregators;

  }
}
