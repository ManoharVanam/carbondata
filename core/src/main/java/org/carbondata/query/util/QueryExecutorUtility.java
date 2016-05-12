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

package org.carbondata.query.util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.carbondata.core.carbon.SqlStatement;
import org.carbondata.core.carbon.SqlStatement.Type;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.columnar.impl.MultiDimKeyVarLengthEquiSplitGenerator;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.metadata.SliceMetaData;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.aggregator.dimension.DimensionAggregatorInfo;
import org.carbondata.query.complex.querytypes.ArrayQueryType;
import org.carbondata.query.complex.querytypes.GenericQueryType;
import org.carbondata.query.complex.querytypes.PrimitiveQueryType;
import org.carbondata.query.complex.querytypes.StructQueryType;
import org.carbondata.query.datastorage.InMemoryTable;
import org.carbondata.query.datastorage.Member;
import org.carbondata.query.datastorage.MemberStore;
import org.carbondata.query.datastorage.TableDataStore;
import org.carbondata.query.executer.exception.QueryExecutionException;
import org.carbondata.query.schema.metadata.SliceUniqueValueInfo;

public final class QueryExecutorUtility {

  private QueryExecutorUtility() {

  }

  public static Object[] getMinValueOfSlices(String factTable, boolean isAgg,
      List<InMemoryTable> slices, SqlStatement.Type[] dataTypes) {
    List<SliceUniqueValueInfo> sliceMinValueInfos =
        new ArrayList<SliceUniqueValueInfo>(null != slices ? slices.size() : 0);
    processUniqueAndMinValueInfo(factTable, sliceMinValueInfos, false, isAgg, slices);
    Object[] minValues = new Object[0];
    if (sliceMinValueInfos.size() > 0) {
      minValues = mergerSliceUniqueValueInfo(sliceMinValueInfos, dataTypes);
    }
    return minValues;
  }

  private static void processUniqueAndMinValueInfo(String factTable,
      List<SliceUniqueValueInfo> sliceUniqueValueInfos, boolean uniqueValue, boolean isAgg,
      List<InMemoryTable> slices) {
    SliceUniqueValueInfo sliceUniqueValueInfo = null;
    SliceMetaData sliceMataData = null;
    if (slices != null) {
      for (int i = 0; i < slices.size(); i++) {
        TableDataStore dataCache = slices.get(i).getDataCache(factTable);
        if (null != dataCache) {
          sliceMataData = slices.get(i).getRsStore().getSliceMetaCache(factTable);
          Object[] currentUniqueValue = null;
          SqlStatement.Type[] dataType;
          if (uniqueValue) {
            currentUniqueValue = slices.get(i).getDataCache(factTable).getUniqueValue();
          } else {
            if (isAgg) {
              currentUniqueValue = slices.get(i).getDataCache(factTable).getMinValueFactForAgg();
            } else {
              currentUniqueValue = slices.get(i).getDataCache(factTable).getMinValue();
            }
          }
          sliceUniqueValueInfo = new SliceUniqueValueInfo();
          sliceUniqueValueInfo.setCols(sliceMataData.getMeasures());
          sliceUniqueValueInfo.setUniqueValue(currentUniqueValue);
          sliceUniqueValueInfos.add(sliceUniqueValueInfo);
        }
      }
    }
  }

  /**
   * @param sliceUniqueValueInfos
   */
  private static Object[] mergerSliceUniqueValueInfo(
      List<SliceUniqueValueInfo> sliceUniqueValueInfos, SqlStatement.Type[] dataTypes) {
    int maxInfoIndex = 0;
    int lastMaxValue = 0;
    for (int i = 0; i < sliceUniqueValueInfos.size(); i++) {
      if (sliceUniqueValueInfos.get(i).getLength() > lastMaxValue) {
        lastMaxValue = sliceUniqueValueInfos.get(i).getLength();
        maxInfoIndex = i;
      }
    }
    SliceUniqueValueInfo sliceUniqueValueInfo = sliceUniqueValueInfos.get(maxInfoIndex);
    Object[] maxSliceUniqueValue = sliceUniqueValueInfo.getUniqueValue();
    String[] cols = null;
    Object[] currentUniqueValue = null;
    for (int i = 0; i < sliceUniqueValueInfos.size(); i++) {
      if (i == maxInfoIndex) {
        continue;
      }
      cols = sliceUniqueValueInfos.get(i).getCols();
      currentUniqueValue = sliceUniqueValueInfos.get(i).getUniqueValue();
      for (int j = 0; j < cols.length; j++) {
        switch (dataTypes[j]) {
          case LONG:
            maxSliceUniqueValue[j] = (long) maxSliceUniqueValue[j] > (long) currentUniqueValue[j] ?
                currentUniqueValue[j] :
                maxSliceUniqueValue[j];
            break;
          case DECIMAL:
            maxSliceUniqueValue[j] =
                ((BigDecimal) maxSliceUniqueValue[j]).compareTo((BigDecimal) currentUniqueValue[j])
                    > 0 ? currentUniqueValue[j] : maxSliceUniqueValue[j];
            break;
          default:
            maxSliceUniqueValue[j] =
                (double) maxSliceUniqueValue[j] > (double) currentUniqueValue[j] ?
                    currentUniqueValue[j] :
                    maxSliceUniqueValue[j];
        }
      }
    }
    return maxSliceUniqueValue;
  }

  public static int[][] getMaskedByteRangeForSorting(Dimension[] queryDimensions,
      KeyGenerator generator, int[] maskedRanges) {
    int[][] dimensionCompareIndex = new int[queryDimensions.length][];
    int index = 0;
    for (int i = 0; i < queryDimensions.length; i++) {
      Set<Integer> integers = new TreeSet<Integer>();
      if (queryDimensions[i].isNoDictionaryDim()) {
        continue;
      }
      int[] range = generator.getKeyByteOffsets(queryDimensions[i].getOrdinal());
      for (int j = range[0]; j <= range[1]; j++) {
        integers.add(j);
      }
      dimensionCompareIndex[index] = new int[integers.size()];
      int j = 0;
      for (Iterator<Integer> iterator = integers.iterator(); iterator.hasNext(); ) {
        Integer integer = (Integer) iterator.next();
        dimensionCompareIndex[index][j++] = integer.intValue();
      }
      index++;
    }

    for (int i = 0; i < dimensionCompareIndex.length; i++) {
      if (null == dimensionCompareIndex[i]) {
        continue;
      }
      int[] range = dimensionCompareIndex[i];
      if (null != range) {
        for (int j = 0; j < range.length; j++) {
          for (int k = 0; k < maskedRanges.length; k++) {
            if (range[j] == maskedRanges[k]) {
              range[j] = k;
              break;
            }
          }
        }
      }

    }

    return dimensionCompareIndex;
  }

  public static byte[][] getMaksedKeyForSorting(Dimension[] queryDimensions, KeyGenerator generator,
      int[][] dimensionCompareIndex, int[] maskedRanges) throws QueryExecutionException {
    byte[][] maskedKey = new byte[queryDimensions.length][];
    byte[] mdKey = null;
    long[] key = null;
    byte[] maskedMdKey = null;
    try {
      if (null != dimensionCompareIndex) {
        for (int i = 0; i < dimensionCompareIndex.length; i++) {
          if (null == dimensionCompareIndex[i]) {
            continue;
          }
          key = new long[generator.getDimCount()];
          maskedKey[i] = new byte[dimensionCompareIndex[i].length];
          key[queryDimensions[i].getOrdinal()] = Long.MAX_VALUE;
          mdKey = generator.generateKey(key);
          maskedMdKey = new byte[maskedRanges.length];
          for (int k = 0;
               k < maskedMdKey.length; k++) { // CHECKSTYLE:OFF Approval No:Approval-V1R2C10_001
            maskedMdKey[k] = mdKey[maskedRanges[k]];
          }
          for (int j = 0; j < dimensionCompareIndex[i].length; j++) {
            maskedKey[i][j] = maskedMdKey[dimensionCompareIndex[i][j]];
          }// CHECKSTYLE:ON

        }
      }
    } catch (KeyGenException e) {
      throw new QueryExecutionException(e);
    }
    return maskedKey;
  }

  //@TODO need to handle for restructuring scenario
  public static int[] getSelectedDimnesionIndex(Dimension[] queryDims) {
    // updated for block index size with complex types
    Set<Integer> allQueryDimension =
        new LinkedHashSet<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    Map<Integer, Integer> primitiveCols = new LinkedHashMap<Integer, Integer>();
    int index = 0;
    for (int i = 0; i < queryDims.length; i++) {
      if (queryDims[i].getAllApplicableDataBlockIndexs().length > 1) {
        for (int eachBlockIndex : queryDims[i].getAllApplicableDataBlockIndexs()) {
          allQueryDimension.add(eachBlockIndex);
          index++;
        }
      } else {
        allQueryDimension.add(queryDims[i].getOrdinal());
        primitiveCols.put(index++, queryDims[i].getOrdinal());
      }
    }
    int[] indexArray =
        convertIntegerArrayToInt(allQueryDimension.toArray(new Integer[allQueryDimension.size()]));

    //Sorting only primitives cols for MDKey Generation.
    Map<Integer, Integer> sortedPrimitiveCols = sortByComparator(primitiveCols);
    List<Entry<Integer, Integer>> unordered =
        new ArrayList<Entry<Integer, Integer>>(primitiveCols.entrySet());
    List<Entry<Integer, Integer>> ordered =
        new ArrayList<Entry<Integer, Integer>>(sortedPrimitiveCols.entrySet());

    for (int i = 0; i < unordered.size(); i++) {
      indexArray[unordered.get(i).getKey()] = ordered.get(i).getValue();
    }

    return indexArray;
  }

  private static Map<Integer, Integer> sortByComparator(Map<Integer, Integer> unsortMap) {

    List<Entry<Integer, Integer>> list =
        new ArrayList<Entry<Integer, Integer>>(unsortMap.entrySet());

    // Sorting the list based on values
    Collections.sort(list, new Comparator<Entry<Integer, Integer>>() {
      @Override public int compare(Entry<Integer, Integer> o1, Entry<Integer, Integer> o2) {
        return o1.getValue().compareTo(o2.getValue());
      }
    });

    // Maintaining insertion order with the help of LinkedList
    Map<Integer, Integer> sortedMap = new LinkedHashMap<Integer, Integer>();
    for (Entry<Integer, Integer> entry : list) {
      sortedMap.put(entry.getKey(), entry.getValue());
    }

    return sortedMap;
  }

  /**
   * @param cube
   * @return
   */
  public static Map<String, GenericQueryType> getComplexDimensionsMap(
      Dimension[] currentDimTables) {
    Map<String, GenericQueryType> complexTypeMap = new HashMap<String, GenericQueryType>();

    Map<String, ArrayList<Dimension>> complexDimensions =
        new HashMap<String, ArrayList<Dimension>>();
    for (int i = 0; i < currentDimTables.length; i++) {
      ArrayList<Dimension> dimensions = complexDimensions.get(currentDimTables[i].getHierName());
      if (dimensions != null) {
        dimensions.add(currentDimTables[i]);
      } else {
        dimensions = new ArrayList<Dimension>();
        dimensions.add(currentDimTables[i]);
      }
      complexDimensions.put(currentDimTables[i].getHierName(), dimensions);
    }
    for (Entry<String, ArrayList<Dimension>> entry : complexDimensions.entrySet()) {
      if (entry.getValue().size() > 1) {
        Dimension dimZero = entry.getValue().get(0);
        GenericQueryType g = dimZero.getDataType().equals(SqlStatement.Type.ARRAY) ?
            new ArrayQueryType(dimZero.getColName(), "", dimZero.getDataBlockIndex()) :
            new StructQueryType(dimZero.getColName(), "", dimZero.getDataBlockIndex());
        complexTypeMap.put(dimZero.getColName(), g);
        for (int i = 1; i < entry.getValue().size(); i++) {
          Dimension dim = entry.getValue().get(i);
          switch (dim.getDataType()) {
            case ARRAY:
              g.addChildren(new ArrayQueryType(dim.getColName(), dim.getParentName(),
                  dim.getDataBlockIndex()));
              break;
            case STRUCT:
              g.addChildren(new StructQueryType(dim.getColName(), dim.getParentName(),
                  dim.getDataBlockIndex()));
              break;
            default:
              g.addChildren(new PrimitiveQueryType(dim.getColName(), dim.getParentName(),
                  dim.getDataBlockIndex(), dim.getDataType()));
          }
        }
      }
    }

    return complexTypeMap;
  }

  public static void getComplexDimensionsKeySize(Map<String, GenericQueryType> complexDimensionsMap,
      int[] dimensionCardinality) {
    int[] keyBlockSize = new MultiDimKeyVarLengthEquiSplitGenerator(
        CarbonUtil.getIncrementedCardinalityFullyFilled(dimensionCardinality), (byte) 1)
        .getBlockKeySize();
    for (Entry<String, GenericQueryType> entry : complexDimensionsMap.entrySet()) {
      entry.getValue().setKeySize(keyBlockSize);
    }
  }

  public static Map<String, Integer> getComplexQueryIndexes(Dimension[] queryDims,
      Dimension[] currentDimTables) {
    Map<String, Integer> colToDataMap = new HashMap<String, Integer>();
    int index = 0;
    for (Dimension queryDim : queryDims) {
      for (int i = 0; i < currentDimTables.length; i++) {
        if (currentDimTables[i].getColName().equals(queryDim.getColName()) && (
            currentDimTables[i].getDataType() == Type.ARRAY
                || currentDimTables[i].getDataType() == Type.STRUCT)) {
          colToDataMap.put(currentDimTables[i].getColName(), index++);
          break;
        }
      }
    }
    return colToDataMap;
  }

  public static int[] getAllSelectedDiemnsion(Dimension[] queryDims,
      List<DimensionAggregatorInfo> dimAggInfo, List<Dimension> fromCustomExps) {
    //Updated to get multiple column blocks for complex types
    Set<Integer> allQueryDimension =
        new LinkedHashSet<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (int i = 0; i < queryDims.length; i++) {
      if (queryDims[i].getAllApplicableDataBlockIndexs().length > 1) {
        for (int eachBlockIndex : queryDims[i].getAllApplicableDataBlockIndexs()) {
          allQueryDimension.add(eachBlockIndex);
        }
      } else {
        allQueryDimension.add(queryDims[i].getOrdinal());
      }
    }
    for (int i = 0; i < dimAggInfo.size(); i++) {
      if (dimAggInfo.get(i).isDimensionPresentInCurrentSlice()) {
        if (dimAggInfo.get(i).getDim().getAllApplicableDataBlockIndexs().length > 1) {
          for (int eachBlockIndex : dimAggInfo.get(i).getDim().getAllApplicableDataBlockIndexs()) {
            allQueryDimension.add(eachBlockIndex);
          }
        } else {
          allQueryDimension.add(dimAggInfo.get(i).getDim().getOrdinal());
        }
      }
    }

    for (int i = 0; i < fromCustomExps.size(); i++) {
      if (fromCustomExps.get(i).getAllApplicableDataBlockIndexs().length > 1) {
        for (int eachBlockIndex : fromCustomExps.get(i).getAllApplicableDataBlockIndexs()) {
          allQueryDimension.add(eachBlockIndex);
        }
      } else {
        allQueryDimension.add(fromCustomExps.get(i).getOrdinal());
      }
    }
    return convertIntegerArrayToInt(
        allQueryDimension.toArray(new Integer[allQueryDimension.size()]));
  }

  public static int[] convertIntegerArrayToInt(Integer[] integerArray) {
    int[] intArray = new int[integerArray.length];

    for (int i = 0; i < integerArray.length; i++) {
      intArray[i] = integerArray[i];
    }
    return intArray;
  }

  public static byte[] getMaskedKey(byte[] data, byte[] maxKey, int[] maskByteRanges,
      int byteCount) {
    // check masked key is null or not
    byte[] maskedKey = new byte[byteCount];
    int counter = 0;
    int byteRange = 0;
    for (int i = 0; i < byteCount; i++) {
      byteRange = maskByteRanges[i];
      if (byteRange != -1) {
        maskedKey[counter++] = (byte) (data[byteRange] & maxKey[byteRange]);
      }
    }
    return maskedKey;
  }

  public static Member getMemberBySurrogateKey(Dimension columnName, int surrogate,
      List<InMemoryTable> slices) {
    MemberStore store = null;
    for (InMemoryTable slice : slices) {
      store = slice.getMemberCache(
          columnName.getTableName() + '_' + columnName.getColName() + '_' + columnName.getDimName()
              + '_' + columnName.getHierName());
      if (null != store) {
        Member member = store.getMemberByID(surrogate);
        if (member != null) {
          return member;
        }
      }
    }
    return null;
  }

  public static Member getActualMemberBySortedKey(Dimension columnName, int surrogate,
      List<InMemoryTable> slices) {
    MemberStore store = null;
    for (InMemoryTable slice : slices) {
      store = slice.getMemberCache(
          columnName.getTableName() + '_' + columnName.getColName() + '_' + columnName.getDimName()
              + '_' + columnName.getHierName());
      if (null != store) {
        Member member = store.getActualKeyFromSortedIndex(surrogate);
        if (member != null) {
          return member;
        }
      }
    }
    return null;
  }

  public static Member getMemberBySurrogateKey(Dimension columnName, int surrogate,
      List<InMemoryTable> slices, int currentSliceIndex) {
    MemberStore store = null;
    for (int i = 0; i <= currentSliceIndex; i++) {
      store = slices.get(i).getMemberCache(
          columnName.getTableName() + '_' + columnName.getColName() + '_' + columnName.getDimName()
              + '_' + columnName.getHierName());
      if (null != store) {
        Member member = store.getMemberByID(surrogate);
        if (member != null) {
          return member;
        }
      }
    }
    return null;
  }

}
