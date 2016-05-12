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
import java.util.List;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.MeasureDataWrapper;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.metadata.CarbonMetadata.Cube;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.query.datastorage.storeinterface.DataStore;
import org.carbondata.query.datastorage.storeinterface.DataStoreBlock;
import org.carbondata.query.datastorage.storeinterface.KeyValue;
import org.carbondata.query.datastorage.streams.DataInputStream;
import org.carbondata.query.datastorage.tree.CSBInternalNode;
import org.carbondata.query.datastorage.tree.CSBNode;
import org.carbondata.query.datastorage.tree.CSBTreeLeafNode;
import org.carbondata.query.scanner.Scanner;
import org.carbondata.query.schema.metadata.Pair;

public class HierarchyBTreeStore implements DataStore {

  /**
   *
   */
  static final int DEFAULT_PAGESIZE = 128;

  /**
   * Maximum number of entries in leaf nodes
   */
  private final int leafMaxEntry;

  /**
   * Maximum number of entries in upper nodes
   */
  private final int upperMaxEntry;

  /**
   * Maximum children for upper nodes (intermediate nodes)
   */
  private final int upperMaxChildren;
  /**
   *
   */
  private final KeyGenerator keyGenerator;
  /**
   * Number of leaf nodes
   */
  private int nLeaf;
  /**
   * Root of the tree
   */
  private CSBNode root;
  /**
   * Total number of entries in CSB-Tree
   */
  private long nTotalKeys;

  public HierarchyBTreeStore(KeyGenerator keyGenerator) {
    this.keyGenerator = keyGenerator;
    upperMaxEntry = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty("com.huawei.datastore.internalnodesize", DEFAULT_PAGESIZE + ""));
    upperMaxChildren = upperMaxEntry + 1;

    // TODO Need to account for page headers and other fields
    leafMaxEntry = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.HIERARCHY_BLOCKLET_SIZE,
            CarbonCommonConstants.HIERARCHY_BLOCKLET_SIZE_DEFAULT_VAL));
  }

  public void build(DataInputStream factStream) {
    build(factStream, false);
  }

  @Override public void build(DataInputStream factStream, boolean hasFactCount) {
    // Number of tuples
    int num = 0;
    Pair<byte[], double[]> entry;
    int groupCounter;
    int nInternal = 0;
    CSBNode curNode = null;
    CSBNode prevNode = null;

    ArrayList<CSBNode[]> nodeGroups =
        new ArrayList<CSBNode[]>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    CSBNode[] currentGroup = null;
    // Scan input stream until all tuples are read

    if (null != factStream) {
      while ((entry = factStream.getNextHierTuple()) != null) {
        num++;

        if ((num - 1) % leafMaxEntry == 0) {
          // Create a new leaf node
          curNode = new HierarchyTreeLeafNode(leafMaxEntry, keyGenerator.getKeySizeInBytes(), 0);
          nLeaf++;

          // Attach the new leaf node to previous node
          if (prevNode != null) {
            prevNode.setNextNode(curNode);
          }
          prevNode = curNode;

          // Add the new leaf node to currentGroup
          groupCounter = (nLeaf - 1) % (upperMaxChildren);
          if (groupCounter == 0) {
            // Create new node group if current group is full
            currentGroup = new CSBNode[upperMaxChildren];
            nodeGroups.add(currentGroup);
            nInternal++;
          }
          if (null != currentGroup) {
            currentGroup[groupCounter] = curNode;
          }
        }
        if (null != curNode) {
          curNode.addEntry(entry);
        }
      }
    }

    // Build internal nodes level by level. Each upper node can have
    // upperMaxEntry keys and upperMaxEntry+1 children
    int remainder;
    int nHigh;

    boolean bRootBuilt = false;
    ArrayList<CSBNode[]> childNodeGroups = nodeGroups;

    nHigh = nInternal;
    remainder = nLeaf % (upperMaxChildren);

    while (nHigh > 1 || !bRootBuilt) {

      ArrayList<CSBNode[]> internalNodeGroups =
          new ArrayList<CSBNode[]>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

      nInternal = 0;

      for (int i = 0; i < nHigh; i++) {
        // Create a new internal node
        curNode = new CSBInternalNode(upperMaxEntry, keyGenerator.getKeySizeInBytes(), null);

        // Allocate a new node group if current node group is full
        groupCounter = i % (upperMaxChildren);
        if (groupCounter == 0) {
          // Create new node group
          currentGroup = new CSBInternalNode[upperMaxChildren];
          internalNodeGroups.add(currentGroup);
          nInternal++;
        }

        // Add the new internal node to current group
        if (null != currentGroup) {
          currentGroup[groupCounter] = curNode;
        }

        int nNodes;

        if (i == nHigh - 1 && remainder != 0) {
          nNodes = remainder - 1;
        } else {
          nNodes = upperMaxEntry;
        }

        // Point the internal node to its children node group
        curNode.setChildren(childNodeGroups.get(i));

        // Fill the internal node with keys based on its child nodes
        for (int j = 0; j < nNodes; j++) {
          curNode.setKey(j, childNodeGroups.get(i)[j + 1].getKey(0, null));
        }
      }

      // If nHigh is 1, we have the root node
      if (nHigh == 1) {
        bRootBuilt = true;
      }

      remainder = nHigh % (upperMaxChildren);
      nHigh = nInternal;
      childNodeGroups = internalNodeGroups;

    }
    root = curNode;

    // Set the total number of keys in the tree
    nTotalKeys = num;

  }

  @Override public KeyValue get(byte[] key, Scanner scanner) {
    return search(key, false, scanner);
  }

  @Override public KeyValue getNext(byte[] key, Scanner scanner) {
    return search(key, true, scanner);
  }

  @Override public long size() {
    return nTotalKeys;
  }

  @Override public long[][] getRanges() {
    return null;
  }

  public KeyValue search(byte[] key, boolean bNext, Scanner scanner) {
    CSBNode node = root;
    while ((null != node) && (!node.isLeafNode())) {
      //added method for source monitor fix
      node = binarySearchOnKeys(key, node);
    }

    if (node == null) {
      return null;
    }
    // Do a binary search in the leaf node
    int lo = 0;
    int hi = node.getnKeys() - 1;
    int mid = 0;
    int k = 0;

    while (lo <= hi) {
      mid = (lo + hi) >>> 1;
      k = keyGenerator.compare(key, node.getKey(mid, null));
      if (k < 0) {
        hi = mid - 1;
      } else if (k > 0) {
        lo = mid + 1;
      } else {
        // Found a match. Return the entry
        KeyValue entry = new KeyValue();
        entry.setKeyLength(keyGenerator.getKeySizeInBytes());
        entry.setBlock(node, null);
        entry.setRow(mid);
        if (scanner != null) {
          scanner.setDataStore(this, node, mid);
        }

        return entry;
      }
    }

    // No match found. The entry at mid should be the next highest key
    if (bNext) {
      if (k > 0) {
        // The entry at mid is less than the input key. Advance it by
        // one
        mid++;
      }
      KeyValue keyValue;
      if (mid < node.getnKeys()) {
        if (scanner != null) {
          scanner.setDataStore(this, node, mid);
        }
        keyValue = new KeyValue();
        keyValue.setKeyLength(keyGenerator.getKeySizeInBytes());
        keyValue.setBlock(node, null);
        keyValue.setRow(mid);
        return (keyValue);
      } else {
        if (scanner != null) {
          scanner.setDataStore(this, node.getNext(), 0);
        }
        if (node.getNext() != null) {
          keyValue = new KeyValue();
          keyValue.setKeyLength(keyGenerator.getKeySizeInBytes());
          keyValue.setBlock(node, null);
          keyValue.setRow(mid);
          return (keyValue);
        }
      }
    }

    return null;
  }

  /**
   * @param key
   * @param node
   * @return
   */
  private CSBNode binarySearchOnKeys(byte[] key, CSBNode node) {
    int l;
    // Do a binary search till we narrow down the search to a set of
    // keys
    // that will fit in a cacheline
    // TODO cacheline is assumed to be 128 for now
    //  scanner.getFileHolder();
    int lo = 0;
    int hi = node.getnKeys() - 1;
    int mid = 0;
    int k = -1;

    while (lo <= hi) {
      mid = lo + (hi - lo) / 2;
      k = keyGenerator.compare(key, node.getKey(mid, null));
      if (k < 0) {
        hi = mid - 1;
      } else if (k > 0) {
        lo = mid + 1;
      } else {
        break;
      }
    }

    if (k < 0) {
      l = mid;
    } else {
      l = mid + 1;
    }
    node = node.getChild(l);
    return node;
  }

  @Override public long getRangeSplitValue() {
    return 0;
  }

  @Override public void build(List<DataInputStream> factStream, boolean hasFactCount) {
  }

  @Override public ValueCompressionModel getCompressionModel() {
    return null;
  }

  @Override
  public DataStoreBlock getBlock(byte[] startKey, FileHolder fileHolderImpl, boolean isFirst) {
    return null;
  }

  @Override
  public void buildColumnar(List<DataInputStream> factStream, boolean hasFactCount, Cube cube) {

  }

  private static class HierarchyTreeLeafNode extends CSBTreeLeafNode {

    /**
     * @param maxKeys
     * @param keySizeInBytes
     * @param valueCount
     */
    HierarchyTreeLeafNode(int maxKeys, int keySizeInBytes, int valueCount) {
      super(maxKeys, keySizeInBytes, valueCount, false, false);
    }

    /**
     * @see CSBTreeLeafNode#addEntry(Pair)
     */
    public void addEntry(Pair<byte[], double[]> entry) {
      keyStore.put(nKeys, entry.getKey());
      nKeys++;
    }

    @Override public MeasureDataWrapper getNodeMsrDataWrapper(int[] cols, FileHolder fileHolder) {
      return null;
    }

    @Override public short getValueSize() {
      return 0;
    }

  }
}
