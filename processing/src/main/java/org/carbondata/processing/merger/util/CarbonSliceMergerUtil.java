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

package org.carbondata.processing.merger.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.writer.ByteArrayHolder;
import org.carbondata.processing.merger.exeception.SliceMergerException;

public final class CarbonSliceMergerUtil {
  /**
   * Comment for <code>LOGGER</code>
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonSliceMergerUtil.class.getName());

  private CarbonSliceMergerUtil() {

  }

  /**
   * This metod copy the multiple level files and merge into single file.
   *
   * @param filesToMerge
   * @param destFile
   * @throws IOException
   */
  public static void copyMultipleFileToSingleFile(List<File> filesToMerge, File destFile)
      throws IOException {

    InputStream inputStream = null;
    OutputStream outputStream = null;
    try {
      outputStream = new BufferedOutputStream(new FileOutputStream(destFile, true));

      for (File toMerge : filesToMerge) {
        inputStream = new BufferedInputStream(new FileInputStream(toMerge));
        copyFileWithoutClosingOutputStream(inputStream, outputStream);
      }
    } finally {
      CarbonUtil.closeStreams(inputStream, outputStream);
    }

  }

  /**
   * This method reads the hierarchy file, sort the Mdkey and write into the destination
   * file.
   *
   * @param filesToMerge
   * @param destFile
   * @throws IOException
   */
  public static void mergeHierarchyFiles(List<File> filesToMerge, File destFile, int keySizeInBytes)
      throws IOException {
    List<ByteArrayHolder> holder =
        new ArrayList<ByteArrayHolder>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

    for (File hierFiles : filesToMerge) {
      readHierarchyFile(hierFiles, keySizeInBytes, holder);
    }

    Collections.sort(holder);

    FileOutputStream fos = null;
    FileChannel outPutFileChannel = null;

    try {

      boolean isFileCreated = false;
      if (!destFile.exists()) {
        isFileCreated = destFile.createNewFile();

        if (!isFileCreated) {
          throw new IOException("unable to create file" + destFile.getAbsolutePath());
        }
      }

      fos = new FileOutputStream(destFile);

      outPutFileChannel = fos.getChannel();
      for (ByteArrayHolder arrayHolder : holder) {
        try {
          writeIntoHierarchyFile(arrayHolder.getMdKey(), arrayHolder.getPrimaryKey(),
              outPutFileChannel);
        } catch (SliceMergerException e) {
          LOGGER.error("Unable to write hierarchy file");
          throw new IOException(e);
        }

      }

    } finally {
      CarbonUtil.closeStreams(outPutFileChannel, fos);
    }

  }

  private static void writeIntoHierarchyFile(byte[] bytes, int primaryKey,
      FileChannel outPutFileChannel) throws SliceMergerException {

    ByteBuffer byteBuffer = storeValueInCache(bytes, primaryKey);

    try {
      byteBuffer.flip();
      outPutFileChannel.write(byteBuffer);
    } catch (IOException e) {
      throw new SliceMergerException(
          "Error while writting in the hierarchy mapping file at the merge step", e);
    }
  }

  private static ByteBuffer storeValueInCache(byte[] bytes, int primaryKey) {

    // adding 4 to store the total length of the row at the beginning
    ByteBuffer buffer = ByteBuffer.allocate(bytes.length + 4);

    buffer.put(bytes);
    buffer.putInt(primaryKey);

    return buffer;
  }

  /**
   * setHeirAndKeySizeMap
   *
   * @param heirAndKeySize void
   */
  public static Map<String, Integer> getHeirAndKeySizeMap(String heirAndKeySize) {
    String[] split = heirAndKeySize.split(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
    String[] split2 = null;
    Map<String, Integer> heirAndKeySizeMap = new HashMap<String, Integer>(split.length);
    for (int i = 0; i < split.length; i++) {
      split2 = split[i].split(CarbonCommonConstants.COLON_SPC_CHARACTER);
      heirAndKeySizeMap.put(split2[0], Integer.parseInt(split2[1]));
    }

    return heirAndKeySizeMap;
  }

  private static void readHierarchyFile(File hierarchyFile, int keySizeInBytes,
      List<ByteArrayHolder> byteArrayHolder) throws IOException {
    int rowLength = keySizeInBytes + 4;
    FileInputStream inputStream = null;
    FileChannel fileChannel = null;

    inputStream = new FileInputStream(hierarchyFile);
    fileChannel = inputStream.getChannel();

    long size = fileChannel.size();
    ByteBuffer rowlengthToRead = ByteBuffer.allocate(rowLength);
    try {
      while (fileChannel.position() < size) {
        fileChannel.read(rowlengthToRead);
        rowlengthToRead.rewind();

        byte[] mdKey = new byte[keySizeInBytes];
        rowlengthToRead.get(mdKey);
        int primaryKey = rowlengthToRead.getInt();
        byteArrayHolder.add(new ByteArrayHolder(mdKey, primaryKey));
        rowlengthToRead.clear();
      }
    } finally {
      CarbonUtil.closeStreams(fileChannel, inputStream);
    }

  }

  /**
   * This method will copy input stream to output stream it will copy file to
   * destination
   *
   * @param stream    InputStream
   * @param outStream outStream
   * @throws IOException
   * @throws IOException IOException
   */
  private static void copyFile(InputStream stream, OutputStream outStream) throws IOException {
    int len;
    final byte[] buffer = new byte[CarbonCommonConstants.BYTEBUFFER_SIZE];
    try {
      for (; ; ) {
        len = stream.read(buffer);
        if (len == -1) {
          return;
        }
        outStream.write(buffer, 0, len);
      }
    } catch (IOException e) {
      throw e;
    } finally {
      CarbonUtil.closeStreams(stream, outStream);
    }
  }

  /**
   * This method will copy input stream to output stream it will copy file to
   * destination and will not close the outputStream.
   *
   * @param stream    InputStream
   * @param outStream outStream
   * @throws IOException
   * @throws IOException IOException
   */
  private static void copyFileWithoutClosingOutputStream(InputStream stream, OutputStream outStream)
      throws IOException {

    final byte[] buffer = new byte[CarbonCommonConstants.BYTEBUFFER_SIZE];
    int len;
    try {
      for (; ; ) {
        len = stream.read(buffer);
        if (len == -1) {
          return;
        }
        outStream.write(buffer, 0, len);
      }
    } catch (IOException e) {
      throw e;
    } finally {
      CarbonUtil.closeStreams(stream);
    }
  }
}
