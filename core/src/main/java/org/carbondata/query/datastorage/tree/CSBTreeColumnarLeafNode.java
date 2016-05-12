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

package org.carbondata.query.datastorage.tree;

import java.nio.ByteBuffer;

import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.carbon.CarbonDef.CubeDimension;
import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.MeasureDataWrapper;
import org.carbondata.core.datastorage.store.NodeMeasureDataStore;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStore;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.util.StoreFactory;
import org.carbondata.core.metadata.BlockletInfoColumnar;
import org.carbondata.core.metadata.CarbonMetadata.Cube;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.vo.ColumnGroupModel;
import org.carbondata.query.datastorage.storeinterface.KeyValue;
import org.carbondata.query.schema.metadata.Pair;

public class CSBTreeColumnarLeafNode extends CSBNode {

  /**
   * Number of keys in the node
   */
  private int nKeys;

  /**
   * Next node
   */
  private CSBNode nextNode;

  /**
   * Key array
   */
  private ColumnarKeyStore keyStore;

  /**
   * Value array
   */
  private NodeMeasureDataStore dataStore;

  /**
   * Fact file name
   */
  private String factFileName;

  /**
   * nodeNumber
   */
  private long nodeNumber;

  private byte[][] columnMinData;

  private byte[][] columnMaxData;

  public CSBTreeColumnarLeafNode(int maxKeys, int[] eachBlockSize, boolean isFileStore,
      FileHolder fileHolder, BlockletInfoColumnar blockletInfo,
      ValueCompressionModel compressionModel, long nodeNumber, Cube metaCube,
      ColumnGroupModel hybridStoreModel) {
    nKeys = blockletInfo.getNumberOfKeys();
    keyStore = StoreFactory.createColumnarKeyStore(
        CarbonUtil.getColumnarKeyStoreInfo(blockletInfo, eachBlockSize, hybridStoreModel),
        fileHolder, isFileStore);
    dataStore = StoreFactory
        .createDataStore(isFileStore, compressionModel, blockletInfo.getMeasureOffset(),
            blockletInfo.getMeasureLength(), blockletInfo.getFileName(), fileHolder);
    this.nodeNumber = nodeNumber;
    byte[][] columnMaxData = blockletInfo.getColumnMaxData();
    byte[][] columnMinData = blockletInfo.getColumnMinData();
    assert(columnMaxData.length == columnMinData.length);

    this.columnMinData = new byte[columnMinData.length][];
    this.columnMaxData = new byte[columnMaxData.length][];
    CarbonDef.Cube cubeXml = metaCube.getCube();
    CubeDimension[] cubeDimensions = cubeXml.dimensions;
    int NoDictionaryColsCount = 0;
    for (int i = 0; i < columnMaxData.length; i++) {

      //For high cardinality dimension engine has to ignore the length and store the min and
      // max value of dimension members.
      //Primitives types + high Card Cols + complex columns. Incrementing highcard cols &
      // used it to identify complex columns block size.
      if (cubeDimensions[i].noDictionary || i >= eachBlockSize.length) {
        NoDictionaryColsCount++;
        ByteBuffer minBuffer = ByteBuffer.allocate(columnMinData[i].length);
        minBuffer.put(columnMinData[i]);
        minBuffer.flip();
        short minLength = minBuffer.getShort();
        this.columnMinData[i] = new byte[minLength];
        minBuffer.get(this.columnMinData[i]);

        ByteBuffer maxBuffer = ByteBuffer.allocate(columnMaxData[i].length);
        maxBuffer.put(columnMaxData[i]);
        maxBuffer.flip();
        short maxLength = maxBuffer.getShort();
        this.columnMaxData[i] = new byte[maxLength];
        maxBuffer.get(this.columnMaxData[i]);
      } else {
        this.columnMinData[i] = new byte[eachBlockSize[i]];
        System.arraycopy(columnMaxData[i], 0, this.columnMinData[i], 0, eachBlockSize[i]);

        this.columnMaxData[i] = new byte[eachBlockSize[i]];
        System.arraycopy(columnMaxData[i], eachBlockSize[i], this.columnMaxData[i], 0,
            eachBlockSize[i]);
      }
    }

    this.factFileName = blockletInfo.getFileName();
  }

  /**
   * @return the nKeys
   */
  public int getnKeys() {
    return nKeys;
  }

  public byte[][] getBlockMinData() {
    return this.columnMinData;
  }

  public byte[][] getBlockMaxData() {
    return this.columnMaxData;
  }

  /**
   * @return the nextnode
   */
  public CSBNode getNext() {
    return nextNode;
  }

  public void setNext(CSBNode nextNode) {
    this.nextNode = nextNode;
  }

  /**
   * @see CSBNode#addEntry(Pair)
   */
  public void addEntry(Pair<byte[], double[]> entry) {

  }

  public boolean isLeafNode() {
    return true;
  }

  /**
   * @param keyindex the key number to set
   */
  public double[] getValue(int keyindex) {
    return null;
  }

  /**
   * @param childIndex index of the child to be returned
   * @return the child node
   */
  public CSBNode getChild(int childIndex) {
    return null;
  }

  public void setChildren(CSBNode[] children) {
    // We shouldn't ever be here
  }

  @Override public KeyValue getNextKeyValue(int index) {
    return null;
  }

  public void setNextNode(CSBNode nextNode) {
    this.nextNode = nextNode;
  }

  public void setKey(int keyindex, byte[] key) {

  }

  @Override public byte[] getBackKeyArray(FileHolder fileHolder) {
    return null;
  }

  @Override public short getValueSize() {
    return dataStore.getLength();
  }

  @Override public MeasureDataWrapper getNodeMsrDataWrapper(int[] cols, FileHolder fileHolder) {
    return dataStore.getBackData(cols, fileHolder);
  }

  @Override public byte[] getKey(int keyIndex, FileHolder fileHolder) {
    return null;
  }

  @Override
  public ColumnarKeyStoreDataHolder[] getColumnarKeyStore(FileHolder fileHolder, int[] blockIndex,
      boolean[] needCompressedData, int[] noDictionaryVals) {
    return keyStore
        .getUnCompressedKeyArray(fileHolder, blockIndex, needCompressedData, noDictionaryVals);
  }

  @Override public long getNodeNumber() {
    return nodeNumber;
  }

  @Override
  public ColumnarKeyStoreDataHolder getColumnarKeyStore(FileHolder fileHolder, int blockIndex,
      boolean needCompressedData, int[] noDictionaryVals) {
    return keyStore
        .getUnCompressedKeyArray(fileHolder, blockIndex, needCompressedData, noDictionaryVals);
  }

  @Override public MeasureDataWrapper getNodeMsrDataWrapper(int cols, FileHolder fileHolder) {
    return dataStore.getBackData(cols, fileHolder);
  }

}
