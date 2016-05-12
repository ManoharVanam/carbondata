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

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.metadata.CarbonSchemaReader;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.datastorage.Member;

import org.apache.commons.codec.binary.Base64;


/**
 * Class Description : This class will be used for prepare member and
 * hierarchies cache , it will read member and hierarchies files to prepare
 * cache Dimension: Filename = DimensionName Columns = 2 Rows =
 * Value(String),SurrogateKey(Long) Hierarchy: Filename= HierachyName Content =
 * byte content Fact/Aggregate: Filename= TableName Columns = dimensions +
 * measures Content = ; separated values
 * Version 1.0
 */

public final class CacheUtil {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CacheUtil.class.getName());
  /**
   * Map intiaL capacity
   */
  private static final int SET_INTIAL_CAPECITY = 1000;
  /**
   * list initial capacity
   */
  private static final int LIST_INTIAL_CAPECITY = 10;

  private CacheUtil() {

  }

  /**
   * This method Read dimension files and prepare Int2ObjectMap for dimension
   * which will contain all the member of dimension
   * Dimension: Filename = DimensionName Columns = 2 Rows =
   * Value(String),SurrogateKey(Long)
   *
   * @param filesLocaton    dimension file location
   * @param namecolumnindex name column index
   * @return Int2ObjectMap<Member> it will contain all the members
   */
  public static Member[][] getMembersList(String filesLocaton, byte nameColumnIndex,
      String dataType) {
    LOGGER.debug("Reading members data from location: " + filesLocaton);
    //        File decryptedFile = decryptEncyptedFile(filesLocaton);
    Member[][] members = processMemberFile(filesLocaton, dataType);
    return members;
  }

  public static int getMinValue(String filesLocaton) {
    if (null == filesLocaton) {
      return 0;
    }
    DataInputStream fileChannel = null;
    try {
      if (!FileFactory.isFileExist(filesLocaton, FileFactory.getFileType(filesLocaton))) {
        return 0;
      }
      fileChannel = new DataInputStream(FileFactory
          .getDataInputStream(filesLocaton, FileFactory.getFileType(filesLocaton), 10240));
      fileChannel.readInt();
      return fileChannel.readInt();
    } catch (IOException e) {
      //            e.printStackTrace();
      LOGGER.error(e, e.getMessage());
    } finally {
      CarbonUtil.closeStreams(fileChannel);
    }
    return 0;
  }

  public static int getMinValueFromLevelFile(String filesLocaton) {
    if (null == filesLocaton) {
      return 0;
    }
    DataInputStream fileChannel = null;
    try {
      if (!FileFactory.isFileExist(filesLocaton, FileFactory.getFileType(filesLocaton))) {
        return 0;
      }
      fileChannel = new DataInputStream(FileFactory
          .getDataInputStream(filesLocaton, FileFactory.getFileType(filesLocaton), 10240));
      return fileChannel.readInt();

    } catch (IOException e) {
      //            e.printStackTrace();
      LOGGER.error(e, e.getMessage());
    } finally {
      CarbonUtil.closeStreams(fileChannel);
    }
    return 0;
  }

  public static int getMaxValueFromLevelFile(String filesLocaton) {
    if (null == filesLocaton) {
      return 0;
    }
    DataInputStream fileChannel = null;
    try {
      if (!FileFactory.isFileExist(filesLocaton, FileFactory.getFileType(filesLocaton))) {
        return 0;
      }
      fileChannel = new DataInputStream(FileFactory
          .getDataInputStream(filesLocaton, FileFactory.getFileType(filesLocaton), 10240));
      CarbonFile memberFile =
          FileFactory.getCarbonFile(filesLocaton, FileFactory.getFileType(filesLocaton));
      long size = memberFile.getSize() - 4;
      long skipSize = size;
      long actualSkipSize = 0;
      while (actualSkipSize != size) {
        actualSkipSize += fileChannel.skip(skipSize);
        skipSize = skipSize - actualSkipSize;
      }
      LOGGER.debug("Bytes skipped " + skipSize);
      int maxVal = fileChannel.readInt();
      return maxVal;

    } catch (IOException e) {
      //            e.printStackTrace();
      LOGGER.error(e, e.getMessage());
    } finally {
      CarbonUtil.closeStreams(fileChannel);
    }
    return 0;
  }

  public static int[] getGlobalSurrogateMapping(String filesLocaton) {
    int[] globalMapping = new int[0];
    if (null == filesLocaton) {
      return globalMapping;
    }
    DataInputStream fileChannel = null;
    try {
      if (!FileFactory.isFileExist(filesLocaton, FileFactory.getFileType(filesLocaton))) {
        return null;
      }
      fileChannel =
          FileFactory.getDataInputStream(filesLocaton, FileFactory.getFileType(filesLocaton));
      fileChannel.readInt();
      int minValue = fileChannel.readInt();
      int numberOfEntries = fileChannel.readInt();
      globalMapping = new int[numberOfEntries];
      int counter = 0;
      int index = 0;
      while (counter < numberOfEntries) {
        index = fileChannel.readInt();
        globalMapping[index - minValue] = fileChannel.readInt();
        counter++;
      }
    } catch (IOException e) {
      //            e.printStackTrace();
      LOGGER.error(e, e.getMessage());
    } finally {
      CarbonUtil.closeStreams(fileChannel);
    }
    return globalMapping;
  }

  /**
   *
   * @param memberFile
   * @param inProgressLoadFolder
   * @return
   * @throws KettleException
   *
   */
  //    private static File decryptEncyptedFile(String memberFile)
  //    {
  //        String decryptedFilePath = memberFile + CarbonCommonConstants.FILE_INPROGRESS_STATUS;
  //
  //        try
  //        {
  //            SimpleFileEncryptor.decryptFile(memberFile , decryptedFilePath);
  //        }
  //        catch(CipherException e)
  //        {
  //            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, "Error while decrypting
  // File");
  //        }
  //        catch(IOException e)
  //        {
  //            LOGGER.error(
  //                    CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
  //                    e, "Not able to encrypt File");
  //        }
  //
  //        return new File(decryptedFilePath);
  //    }

  /**
   * @param nameColumnIndex
   * @param filename
   * @param members
   * @throws IOException
   * @author A00902732
   * </br>     Reads the member file
   * </br>
   * Member file structure
   * </br>           |1|2|3|4|5|6|7|8....
   * </br>           |1|2|3|4|5|6|....
   * </br>
   * </br>            1#totalRecoredLength - exclusive - 4 bytes
   * </br>            2#valueLength - 4 bytes
   * </br>            3#Value - depends on previous length
   * </br>            4#surrogateKey - 4 bytes
   * </br>            5#property1Length - 4 bytes
   * </br>            6#property1Value - depends on prev length
   * </br>            7#property2Length - 4 bytes
   * </br>            8#property2Value - depends on prev length
   * </br>            ...
   * Reads accordingly
   */
  public static Member[][] processMemberFile(String filename, String dataType)

  {
    long startTime = System.currentTimeMillis();
    Member[][] members = null;
    // create an object of FileOutputStream
    DataInputStream fileChannel = null;
    try {
      try {
        if (!FileFactory.isFileExist(filename, FileFactory.getFileType(filename))) {
          return members;
        }
      } catch (IOException e) {
        return members;
      }
      //
      fileChannel = FileFactory.getDataInputStream(filename, FileFactory.getFileType(filename));
      FileType fileType = FileFactory.getFileType(filename);
      CarbonFile memberFile = FileFactory.getCarbonFile(filename, fileType);
      members = populateMemberCache(fileChannel, memberFile, filename, dataType);
    } catch (FileNotFoundException f) {
      LOGGER.error("@@@@@@  Member file is missing @@@@@@ : " + filename);
    } catch (IOException e) {
      LOGGER.error("@@@@@@  Error while reading Member the file @@@@@@ : " + filename);
    } finally {
      CarbonUtil.closeStreams(fileChannel);
    }

    LOGGER.debug("Time taken to process file " + filename + " is : " + (System.currentTimeMillis()
            - startTime));
    return members;
  }

  private static Member[][] populateMemberCache(DataInputStream fileChannel, CarbonFile memberFile,
      String fileName, String dataType) throws IOException {
    // ByteBuffer toltalLength, memberLength, surrogateKey, bf3;
    // subtracted 4 as last 4 bytes will have the max value for no of
    // records
    long currPositionIndex = 0;
    long size = memberFile.getSize() - 4;
    long skipSize = size;
    long actualSkipSize = 0;
    while (actualSkipSize != size) {
      actualSkipSize += fileChannel.skip(skipSize);
      skipSize = skipSize - actualSkipSize;
    }
    //        LOGGER.debug(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, "Bytes skipped " +
    // skipSize);
    int maxVal = fileChannel.readInt();
    CarbonUtil.closeStreams(fileChannel);
    fileChannel = FileFactory.getDataInputStream(fileName, FileFactory.getFileType(fileName));
    // CHECKSTYLE:OFF Approval No:Approval-V1R2C10_005
    ByteBuffer buffer = ByteBuffer.allocate((int) size);
    // CHECKSTYLE:OFF
    fileChannel.readFully(buffer.array());
    int minVal = buffer.getInt();
    int totalArraySize = maxVal - minVal + 1;
    Member[][] surogateKeyArrays = null;
    if (totalArraySize > CarbonCommonConstants.LEVEL_ARRAY_SIZE) {
      int div = totalArraySize / CarbonCommonConstants.LEVEL_ARRAY_SIZE;
      int rem = totalArraySize % CarbonCommonConstants.LEVEL_ARRAY_SIZE;
      if (rem > 0) {
        div++;
      }
      surogateKeyArrays = new Member[div][];

      for (int i = 0; i < div - 1; i++) {
        surogateKeyArrays[i] = new Member[CarbonCommonConstants.LEVEL_ARRAY_SIZE];
      }

      if (rem > 0) {
        surogateKeyArrays[surogateKeyArrays.length - 1] = new Member[rem];
      } else {
        surogateKeyArrays[surogateKeyArrays.length - 1] =
            new Member[CarbonCommonConstants.LEVEL_ARRAY_SIZE];
      }
    } else {
      surogateKeyArrays = new Member[1][totalArraySize];
    }
    //        Member[] surogateKeyArrays = new Member[maxVal-minVal+1];
    //        int surrogateKeyIndex = minVal;
    currPositionIndex += 4;
    //
    int current = 0;
    // CHECKSTYLE:OFF Approval No:Approval-V1R2C10_005
    boolean enableEncoding = Boolean.valueOf(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.ENABLE_BASE64_ENCODING,
            CarbonCommonConstants.ENABLE_BASE64_ENCODING_DEFAULT));
    // CHECKSTYLE:ON
    int index = 0;
    int prvArrayIndex = 0;
    while (currPositionIndex < size) {
      int len = buffer.getInt();
      // CHECKSTYLE:OFF Approval No:Approval-V1R2C10_005
      // CHECKSTYLE:ON
      currPositionIndex += 4;
      byte[] rowBytes = new byte[len];
      buffer.get(rowBytes);
      currPositionIndex += len;
      // No:Approval-361
      if (enableEncoding) {
        rowBytes = Base64.decodeBase64(rowBytes);
      }
      surogateKeyArrays[current / CarbonCommonConstants.LEVEL_ARRAY_SIZE][index] =
          new Member(rowBytes);
      current++;
      if (current / CarbonCommonConstants.LEVEL_ARRAY_SIZE > prvArrayIndex) {
        prvArrayIndex++;
        index = 0;
      } else {
        index++;
      }
    }
    return surogateKeyArrays;
  }

  /**
   * Below method is responsible for reading the sort index and sort reverse index
   *
   * @param levelFileName
   * @return List<int[][]>
   * @throws IOException
   */
  public static List<int[][]> getLevelSortOrderAndReverseIndex(String levelFileName)
      throws IOException {
    if (!FileFactory.isFileExist(levelFileName + CarbonCommonConstants.LEVEL_SORT_INDEX_FILE_EXT,
        FileFactory.getFileType(levelFileName + CarbonCommonConstants.LEVEL_SORT_INDEX_FILE_EXT))) {
      return null;
    }
    DataInputStream dataInputStream = null;
    List<int[][]> sortIndexAndReverseIndexArray =
        new ArrayList<int[][]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    long size = 0;
    ByteBuffer buffer = null;
    try {
      dataInputStream = FileFactory
          .getDataInputStream(levelFileName + CarbonCommonConstants.LEVEL_SORT_INDEX_FILE_EXT,
              FileFactory.getFileType(levelFileName));
      size = FileFactory
          .getCarbonFile(levelFileName + CarbonCommonConstants.LEVEL_SORT_INDEX_FILE_EXT,
              FileFactory.getFileType(levelFileName)).getSize();
      // CHECKSTYLE:OFF Approval No:Approval-V1R2C10_005
      buffer = ByteBuffer.allocate((int) size);
      // CHECKSTYLE:ON
      dataInputStream.readFully(buffer.array());
      sortIndexAndReverseIndexArray.add(getIndexArray(buffer));
      sortIndexAndReverseIndexArray.add(getIndexArray(buffer));
    } catch (IOException e) {
      LOGGER.error(e);
      throw e;
    } finally {
      CarbonUtil.closeStreams(dataInputStream);
    }
    return sortIndexAndReverseIndexArray;
  }

  private static int[][] getIndexArray(ByteBuffer buffer) {
    int arraySize = buffer.getInt();
    int[][] sortorderIndexArray = new int[1][arraySize];
    if (arraySize > CarbonCommonConstants.LEVEL_ARRAY_SIZE) {
      int div = arraySize / CarbonCommonConstants.LEVEL_ARRAY_SIZE;
      int rem = arraySize % CarbonCommonConstants.LEVEL_ARRAY_SIZE;
      if (rem > 0) {
        div++;
      }
      sortorderIndexArray = new int[div][];
      for (int i = 0; i < div - 1; i++) {
        sortorderIndexArray[i] = new int[CarbonCommonConstants.LEVEL_ARRAY_SIZE];
      }

      if (rem > 0) {
        sortorderIndexArray[sortorderIndexArray.length - 1] = new int[rem];
      } else {
        sortorderIndexArray[sortorderIndexArray.length - 1] =
            new int[CarbonCommonConstants.LEVEL_ARRAY_SIZE];
      }
    }
    int index = 0;
    int prvArrayIndex = 0;
    int current = 0;
    for (int i = 0; i < arraySize; i++) {
      sortorderIndexArray[current / CarbonCommonConstants.LEVEL_ARRAY_SIZE][index] =
          buffer.getInt();
      current++;
      if (current / CarbonCommonConstants.LEVEL_ARRAY_SIZE > prvArrayIndex) {
        prvArrayIndex++;
        index = 0;
      } else {
        index++;
      }
    }
    return sortorderIndexArray;
  }

  /**
   * This method will return the size of a given file
   *
   * @param fileName
   * @return
   */
  public static long getMemberFileSize(String fileName) {
    if (isFileExists(fileName)) {
      FileType fileType = FileFactory.getFileType(fileName);
      CarbonFile memberFile = FileFactory.getCarbonFile(fileName, fileType);
      return memberFile.getSize();
    }
    return 0;
  }

  /**
   * @param fileName
   * @param fileType
   */
  public static boolean isFileExists(String fileName) {
    try {
      FileType fileType = FileFactory.getFileType(fileName);
      if (FileFactory.isFileExist(fileName, fileType)) {
        return true;
      }
    } catch (IOException e) {
      LOGGER.error("@@@@@@  Member file not found for size to be calculated @@@@@@ : " + fileName);
    }
    return false;
  }

  /**
   * This method will return the actual level name based on the dimension
   *
   * @param schema
   * @param dimension
   * @return
   */
  public static String getLevelActualName(CarbonDef.Schema schema,
      CarbonDef.CubeDimension dimension) {
    String levelActualName = null;
    CarbonDef.Hierarchy[] extractHierarchies =
        CarbonSchemaReader.extractHierarchies(schema, dimension);
    if (null != extractHierarchies) {
      for (CarbonDef.Hierarchy hierarchy : extractHierarchies) {
        for (CarbonDef.Level level : hierarchy.levels) {
          levelActualName = level.column;
          break;
        }
      }
    }
    return levelActualName;
  }

}
