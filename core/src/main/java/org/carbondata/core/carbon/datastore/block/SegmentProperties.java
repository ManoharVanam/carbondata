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
package org.carbondata.core.carbon.datastore.block;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.columnar.ColumnarSplitter;
import org.carbondata.core.keygenerator.columnar.impl.MultiDimKeyVarLengthVariableSplitGenerator;
import org.carbondata.core.keygenerator.mdkey.MultiDimKeyVarLengthGenerator;
import org.carbondata.core.util.CarbonUtil;

import org.apache.commons.lang3.ArrayUtils;

/**
 * This class contains all the details about the restructuring information of
 * the block. This will be used during query execution to handle restructure
 * information
 */
public class SegmentProperties {

  /**
   * key generator of the block which was used to generate the mdkey for
   * normal dimension. this will be required to
   */
  private KeyGenerator dimensionKeyGenerator;

  /**
   * key generator used for complex dimension
   */
  private KeyGenerator complexDimensionKeyGenerator;

  /**
   * list of dimension present in the block
   */
  private List<CarbonDimension> dimensions;

  /**
   * list of dimension present in the block
   */
  private List<CarbonDimension> complexDimensions;

  /**
   * list of measure present in the block
   */
  private List<CarbonMeasure> measures;

  /**
   * cardinality of dimension columns participated in key generator
   */
  private int[] dimColumnsCardinality;

  /**
   * cardinality of complex dimension
   */
  private int[] complexDimColumnCardinality;

  /**
   * mapping of dimension column to block in a file this will be used for
   * reading the blocks from file
   */
  private Map<Integer, Integer> dimensionOrdinalToBlockMapping;

  /**
   * mapping of measure column to block to in file this will be used while
   * reading the block in a file
   */
  private Map<Integer, Integer> measuresOrdinalToBlockMapping;

  /**
   * size of the each dimension column value in a block this can be used when
   * we need to do copy a cell value to create a tuple.for no dictionary
   * column this value will be -1. for dictionary column we size of the value
   * will be fixed.
   */
  private int[] eachDimColumnValueSize;

  /**
   * size of the each dimension column value in a block this can be used when
   * we need to do copy a cell value to create a tuple.for no dictionary
   * column this value will be -1. for dictionary column we size of the value
   * will be fixed.
   */
  private int[] eachComplexDimColumnValueSize;

  /**
   * below mapping will have mapping of the column group to dimensions ordinal
   * for example if 3 dimension present in the columngroupid 0 and its ordinal in
   * 2,3,4 then map will contain 0,{2,3,4}
   */
  private Map<Integer, KeyGenerator> columnGroupAndItsKeygenartor;

  /**
   * this will be used to split the fixed length key
   * this will all the information about how key was created
   * and how to split the key based on group
   */
  private ColumnarSplitter fixedLengthKeySplitter;

  /**
   * to store the number of no dictionary dimension
   * this will be used during query execution for creating
   * start and end key. Purpose of storing this value here is
   * so during query execution no need to calculate every time
   */
  private int numberOfNoDictionaryDimension;

  public SegmentProperties(List<ColumnSchema> columnsInTable, int[] columnCardinality) {
    dimensions = new ArrayList<CarbonDimension>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    complexDimensions =
        new ArrayList<CarbonDimension>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    measures = new ArrayList<CarbonMeasure>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    fillDimensionAndMeasureDetails(columnsInTable, columnCardinality);
    dimensionOrdinalToBlockMapping =
        new HashMap<Integer, Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    measuresOrdinalToBlockMapping =
        new HashMap<Integer, Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    fillOrdinalToBlockMappingForDimension();
    fillOrdinalToBlockIndexMappingForMeasureColumns();
    fillColumnGroupAndItsCardinality(columnCardinality);
    fillKeyGeneratorDetails();
  }

  /**
   * below method is to fill the dimension and its mapping to file blocks all
   * the column will point to same column group
   */
  private void fillOrdinalToBlockMappingForDimension() {
    int blockOrdinal = -1;
    CarbonDimension dimension = null;
    int index = 0;
    int prvcolumnGroupId = -1;
    while (index < dimensions.size()) {
      dimension = dimensions.get(index);
      // if column id is same as previous one then block index will be
      // same
      if (dimension.isColumnar() || dimension.columnGroupId() != prvcolumnGroupId) {
        blockOrdinal++;
      }
      dimensionOrdinalToBlockMapping.put(dimension.getOrdinal(), blockOrdinal);
      prvcolumnGroupId = dimension.columnGroupId();
      index++;
    }
    index = 0;
    // complex dimension will be stored at last
    while (index < complexDimensions.size()) {
      dimension = complexDimensions.get(index);
      dimensionOrdinalToBlockMapping.put(dimension.getOrdinal(), ++blockOrdinal);
      index++;
    }
  }

  /**
   * Below method will be used to fill the mapping
   * of measure ordinal to its block index mapping in
   * file
   */
  private void fillOrdinalToBlockIndexMappingForMeasureColumns() {
    int blockOrdinal = 0;
    int index = 0;
    while (index < measures.size()) {
      measuresOrdinalToBlockMapping.put(measures.get(index).getOrdinal(), blockOrdinal);
      blockOrdinal++;
      index++;
    }
  }

  /**
   * below method will fill dimension and measure detail of the block.
   *
   * @param blockMetadata
   */
  private void fillDimensionAndMeasureDetails(List<ColumnSchema> columnsInTable,
      int[] columnCardinality) {
    ColumnSchema columnSchema = null;
    // ordinal will be required to read the data from file block
    int dimensonOrdinal = -1;
    int measureOrdinal = -1;
    // table ordinal is actually a schema ordinal this is required as
    // cardinality array
    // which is stored in segment info contains -1 if that particular column
    // is n
    int tableOrdinal = -1;
    // creating a list as we do not know how many dimension not participated
    // in the mdkey
    List<Integer> cardinalityIndexForNormalDimensionColumn =
        new ArrayList<Integer>(columnsInTable.size());
    // creating a list as we do not know how many dimension not participated
    // in the mdkey
    List<Integer> cardinalityIndexForComplexDimensionColumn =
        new ArrayList<Integer>(columnsInTable.size());
    boolean isComplexDimensionStarted = false;
    CarbonDimension carbonDimension = null;
    // to store the position of dimension in surrogate key array which is
    // participating in mdkey
    int keyOrdinal = 0;
    int previousColumnGroup = -1;
    // to store the ordinal of the column group ordinal
    int columnGroupOrdinal = 0;
    for (int i = 0; i < columnsInTable.size(); i++) {
      columnSchema = columnsInTable.get(i);
      if (columnSchema.isDimensionColumn()) {

        tableOrdinal++;
        // not adding the cardinality of the non dictionary
        // column as it was not the part of mdkey
        if (CarbonUtil.hasEncoding(columnSchema.getEncodingList(), Encoding.DICTIONARY)
            && !isComplexDimensionStarted && columnSchema.getNumberOfChild() == 0) {
          cardinalityIndexForNormalDimensionColumn.add(tableOrdinal);
          if (columnSchema.isColumnar()) {
            // if it is a columnar dimension participated in mdkey then added
            // key ordinal and dimension ordinal
            carbonDimension =
                new CarbonDimension(columnSchema, ++dimensonOrdinal, keyOrdinal++, -1);
          } else {
            // if not columnnar then it is a column group dimension

            // below code to handle first dimension of the column group
            // in this case ordinal of the column group will be 0
            if (previousColumnGroup != columnSchema.getColumnGroupId()) {
              columnGroupOrdinal = 0;
              carbonDimension = new CarbonDimension(columnSchema, ++dimensonOrdinal, keyOrdinal++,
                  columnGroupOrdinal++);
            }
            // if previous dimension  column group id is same as current then
            // then its belongs to same row group
            else {
              carbonDimension = new CarbonDimension(columnSchema, ++dimensonOrdinal, keyOrdinal++,
                  columnGroupOrdinal++);
            }
            previousColumnGroup = columnSchema.getColumnGroupId();
          }
        }
        // as complex type will be stored at last so once complex type started all the dimension
        // will be added to complex type
        else if (isComplexDimensionStarted || CarbonUtil.hasDataType(columnSchema.getDataType(),
            new DataType[] { DataType.ARRAY, DataType.STRUCT, DataType.MAP })) {
          cardinalityIndexForComplexDimensionColumn.add(tableOrdinal);
          carbonDimension = new CarbonDimension(columnSchema, ++dimensonOrdinal, -1, -1);
          complexDimensions.add(carbonDimension);
          isComplexDimensionStarted = true;
          continue;
        } else {
          // for no dictionary dimension
          carbonDimension = new CarbonDimension(columnSchema, ++dimensonOrdinal, -1, -1);
          numberOfNoDictionaryDimension++;
        }
        dimensions.add(carbonDimension);
      } else {
        measures.add(new CarbonMeasure(columnSchema, ++measureOrdinal));
      }
    }
    dimColumnsCardinality = new int[cardinalityIndexForNormalDimensionColumn.size()];
    complexDimColumnCardinality = new int[cardinalityIndexForComplexDimensionColumn.size()];
    int index = 0;
    // filling the cardinality of the dimension column to create the key
    // generator
    for (Integer cardinalityArrayIndex : cardinalityIndexForNormalDimensionColumn) {
      dimColumnsCardinality[index++] = columnCardinality[cardinalityArrayIndex];
    }
    index = 0;
    // filling the cardinality of the complex dimension column to create the
    // key generator
    for (Integer cardinalityArrayIndex : cardinalityIndexForComplexDimensionColumn) {
      complexDimColumnCardinality[index++] = columnCardinality[cardinalityArrayIndex];
    }
  }

  /**
   * Below method will fill the key generator detail of both the type of key
   * generator. This will be required for during both query execution and data
   * loading.
   */
  private void fillKeyGeneratorDetails() {
    // create a dimension partitioner list
    // this list will contain information about how dimension value are
    // stored
    // it is stored in group or individually
    List<Integer> dimensionPartitionList =
        new ArrayList<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    List<Boolean> isDictionaryColumn =
        new ArrayList<Boolean>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    int prvcolumnGroupId = -1;
    int counter = 0;
    while (counter < dimensions.size()) {
      CarbonDimension carbonDimension = dimensions.get(counter);
      // if dimension is not a part of mdkey then no need to add
      if (!carbonDimension.getEncoder().contains(Encoding.DICTIONARY)) {
        isDictionaryColumn.add(false);
        counter++;
        continue;
      }
      // columnar column is stored individually
      // so add one
      if (carbonDimension.isColumnar()) {
        dimensionPartitionList.add(1);
        isDictionaryColumn.add(true);
      }
      // if in a group then need to add how many columns a selected in
      // group
      if (!carbonDimension.isColumnar() && carbonDimension.columnGroupId() == prvcolumnGroupId) {
        // incrementing the previous value of the list as it is in same column group
        dimensionPartitionList.set(dimensionPartitionList.size() - 1,
            dimensionPartitionList.get(dimensionPartitionList.size() - 1) + 1);
      } else if (!carbonDimension.isColumnar()) {
        dimensionPartitionList.add(1);
        isDictionaryColumn.add(true);
      }
      prvcolumnGroupId = carbonDimension.columnGroupId();
      counter++;
    }
    // get the partitioner
    int[] dimensionPartitions = ArrayUtils
        .toPrimitive(dimensionPartitionList.toArray(new Integer[dimensionPartitionList.size()]));
    // get the bit length of each column
    int[] bitLength = CarbonUtil.getDimensionBitLength(dimColumnsCardinality, dimensionPartitions);
    // create a key generator
    this.dimensionKeyGenerator = new MultiDimKeyVarLengthGenerator(bitLength);
    this.fixedLengthKeySplitter =
        new MultiDimKeyVarLengthVariableSplitGenerator(bitLength, dimensionPartitions);
    // get the size of each value in file block
    int[] dictionayDimColumnValueSize = fixedLengthKeySplitter.getBlockKeySize();
    int index = -1;
    this.eachDimColumnValueSize = new int[isDictionaryColumn.size()];
    for (int i = 0; i < eachDimColumnValueSize.length; i++) {
      if (!isDictionaryColumn.get(i)) {
        eachDimColumnValueSize[i] = -1;
        continue;
      }
      eachDimColumnValueSize[i] = dictionayDimColumnValueSize[++index];
    }
    if (complexDimensions.size() > 0) {
      int[] complexDimesionParition = new int[complexDimensions.size()];
      // as complex dimension will be stored in column format add one
      Arrays.fill(complexDimesionParition, 1);
      int[] complexDimensionBitLength = new int[complexDimesionParition.length];
      // number of bits will be 64
      Arrays.fill(complexDimensionBitLength, 64);
      this.complexDimensionKeyGenerator =
          new MultiDimKeyVarLengthGenerator(complexDimensionBitLength);
      ColumnarSplitter keySplitter =
          new MultiDimKeyVarLengthVariableSplitGenerator(complexDimensionBitLength,
              complexDimesionParition);
      eachComplexDimColumnValueSize = keySplitter.getBlockKeySize();
    } else {
      eachComplexDimColumnValueSize = new int[0];
    }
  }

  /**
   * Below method will be used to create a mapping of column group and its column cardinality this
   * mapping will have column group id to cardinality of the dimension present in
   * the column group.This mapping will be used during query execution, to create
   * a mask key for the column group dimension which will be used in aggregation
   * and filter query as column group dimension will be stored at the bit level
   */
  private void fillColumnGroupAndItsCardinality(int[] cardinality) {
    // mapping of the column group and its ordinal
    Map<Integer, List<Integer>> columnGroupAndOrdinalMapping =
        new HashMap<Integer, List<Integer>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    // to store a column group
    List<Integer> currentColumnGroup = null;
    // current index
    int index = 0;
    // previous column group to check all the column of column id has bee selected
    int prvColumnGroupId = -1;
    while (index < dimensions.size()) {
      // if dimension group id is not zero and it is same as the previous
      // column id
      // then we need to add ordinal of that column as it belongs to same
      // column group
      if (!dimensions.get(index).isColumnar()
          && dimensions.get(index).columnGroupId() == prvColumnGroupId) {
        currentColumnGroup.add(index);
      }
      // if column is not a columnar then new column group has come
      // so we need to create a list of new column id group and add the
      // ordinal
      else if (!dimensions.get(index).isColumnar()) {
        currentColumnGroup = new ArrayList<Integer>();
        columnGroupAndOrdinalMapping.put(dimensions.get(index).columnGroupId(), currentColumnGroup);
        currentColumnGroup.add(index);
      }
      // update the column id every time,this is required to group the
      // columns
      // of the same column group
      prvColumnGroupId = dimensions.get(index).columnGroupId();
      index++;
    }
    // Initializing the map
    this.columnGroupAndItsKeygenartor =
        new HashMap<Integer, KeyGenerator>(columnGroupAndOrdinalMapping.size());
    int[] columnGroupCardinality = null;
    index = 0;
    Iterator<Entry<Integer, List<Integer>>> iterator =
        columnGroupAndOrdinalMapping.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<Integer, List<Integer>> next = iterator.next();
      List<Integer> currentGroupOrdinal = next.getValue();
      // create the cardinality array
      columnGroupCardinality = new int[currentGroupOrdinal.size()];
      for (int i = 0; i < columnGroupCardinality.length; i++) {
        // fill the cardinality
        columnGroupCardinality[i] = cardinality[currentGroupOrdinal.get(i)];
      }
      this.columnGroupAndItsKeygenartor.put(next.getKey(), new MultiDimKeyVarLengthGenerator(
          CarbonUtil.getDimensionBitLength(cardinality, new int[] { cardinality.length })));
    }
  }

  /**
   * Below method is to get the value of each dimension column. As this method
   * will be used only once so we can merge both the dimension and complex
   * dimension array. Complex dimension will be store at last so first copy
   * the normal dimension the copy the complex dimension size. If we store
   * this value as a class variable unnecessarily we will waste some space
   *
   * @return each dimension value size
   */
  public int[] getDimensionColumnsValueSize() {
    int[] dimensionValueSize =
        new int[eachDimColumnValueSize.length + eachComplexDimColumnValueSize.length];
    System
        .arraycopy(eachDimColumnValueSize, 0, dimensionValueSize, 0, eachDimColumnValueSize.length);
    System.arraycopy(eachComplexDimColumnValueSize, 0, dimensionValueSize,
        eachDimColumnValueSize.length, eachComplexDimColumnValueSize.length);
    return dimensionValueSize;
  }

  /**
   * @return the dimensionKeyGenerator
   */
  public KeyGenerator getDimensionKeyGenerator() {
    return dimensionKeyGenerator;
  }

  /**
   * @return the complexDimensionKeyGenerator
   */
  public KeyGenerator getComplexDimensionKeyGenerator() {
    return complexDimensionKeyGenerator;
  }

  /**
   * @return the dimensions
   */
  public List<CarbonDimension> getDimensions() {
    return dimensions;
  }

  /**
   * @return the complexDimensions
   */
  public List<CarbonDimension> getComplexDimensions() {
    return complexDimensions;
  }

  /**
   * @return the measures
   */
  public List<CarbonMeasure> getMeasures() {
    return measures;
  }

  /**
   * @return the dimColumnsCardinality
   */
  public int[] getDimColumnsCardinality() {
    return dimColumnsCardinality;
  }

  /**
   * @return the complexDimColumnCardinality
   */
  public int[] getComplexDimColumnCardinality() {
    return complexDimColumnCardinality;
  }

  /**
   * @return the dimensionOrdinalToBlockMapping
   */
  public Map<Integer, Integer> getDimensionOrdinalToBlockMapping() {
    return dimensionOrdinalToBlockMapping;
  }

  /**
   * @return the measuresOrdinalToBlockMapping
   */
  public Map<Integer, Integer> getMeasuresOrdinalToBlockMapping() {
    return measuresOrdinalToBlockMapping;
  }

  /**
   * @return the eachDimColumnValueSize
   */
  public int[] getEachDimColumnValueSize() {
    return eachDimColumnValueSize;
  }

  /**
   * @return the eachComplexDimColumnValueSize
   */
  public int[] getEachComplexDimColumnValueSize() {
    return eachComplexDimColumnValueSize;
  }

  /**
   * @return the fixedLengthKeySplitter
   */
  public ColumnarSplitter getFixedLengthKeySplitter() {
    return fixedLengthKeySplitter;
  }

  /**
   * @return the columnGroupAndItsKeygenartor
   */
  public Map<Integer, KeyGenerator> getColumnGroupAndItsKeygenartor() {
    return columnGroupAndItsKeygenartor;
  }

  /**
   * @return the numberOfNoDictionaryDimension
   */
  public int getNumberOfNoDictionaryDimension() {
    return numberOfNoDictionaryDimension;
  }

}
