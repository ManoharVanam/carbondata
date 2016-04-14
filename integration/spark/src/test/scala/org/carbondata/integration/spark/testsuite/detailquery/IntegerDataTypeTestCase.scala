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

package org.carbondata.integration.spark.testsuite.detailquery

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for detailed query on Integer datatypes
 * @author N00902756
 *
 */
class IntegerDataTypeTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
//    sql("CREATE CUBE integertypecube DIMENSIONS (empno Integer, workgroupcategory Integer, deptno Integer, projectcode Integer) MEASURES (attendance Integer) OPTIONS (PARTITIONER [PARTITION_COUNT=1])")
//    sql("LOAD DATA fact from './src/test/resources/data.csv' INTO CUBE integertypecube PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')")
  sql("CREATE CUBE cube_restructure444 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
    sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure444 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"')")
    sql("create schema myschema")
    sql("create schema myschema1")
    sql("create schema drug")

  }

  
  override def afterAll {
//    sql("drop cube integertypecube")
       sql("drop cube cube_restructure444")
      sql("drop schema myschema")
      sql("drop schema myschema1")
      sql("drop schema drug")

  }
  
/*  test("select empno from integertypecube") {
    checkAnswer(
      sql("select empno from integertypecube"),
      Seq(Row(11), Row(12), Row(13), Row(14), Row(15), Row(16), Row(17), Row(18), Row(19), Row(20)))
  }*/
  
  
 //TC_898
    test("select * from cube_restructure444") {     
   
      sql("select * from cube_restructure444")      
    
  }

    //TC_1059
test("TC_1059") {
sql("create cube  cube1 dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string)  measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("drop cube cube1")
}

//TC_1060
test("TC_1060") {
sql("create cube  myschema.cube2 dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string)  measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("drop cube myschema.cube2")
}

//TC_1061
test("TC_1061") {
sql("CREATE CUBE cube3 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("drop cube cube3")
}

//TC_1062
test("TC_1062") {
sql("CREATE CUBE myschema.cube4 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("drop cube myschema.cube4")
}

//TC_1063
test("TC_1063") {
sql("CREATE CUBE cube5 DIMENSIONS (AMSize STRING as col1) MEASURES (Latest_Day INTEGER as col2) OPTIONS (AGGREGATION [col2 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (col1) ,PARTITION_COUNT=1] )")
sql("drop cube cube5")
}

//TC_1064
test("TC_1064") {
sql("CREATE CUBE myschema.cube6 DIMENSIONS (AMSize STRING as col1) MEASURES (Latest_Day INTEGER as col2) OPTIONS (AGGREGATION [col2 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (col1) ,PARTITION_COUNT=1] )")
sql("drop cube myschema.cube6")
}

//TC_1065
test("TC_1065") {
sql("CREATE CUBE cube7 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count])")
sql("drop cube cube7")
}

//TC_1066
test("TC_1066") {
sql("CREATE CUBE myschema.cube8 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count])")
sql("drop cube myschema.cube8")
}

//TC_1067
test("TC_1067") {
sql("CREATE CUBE cube9 DIMENSIONS (AMSize STRING as col1) MEASURES (Latest_Day INTEGER as col2) OPTIONS (AGGREGATION [Latest_Day = count])")
sql("drop cube cube9")
}

//TC_1068
test("TC_1068") {
sql("CREATE CUBE myschema.cube10 DIMENSIONS (AMSize STRING as col1) MEASURES (Latest_Day INTEGER as col2) OPTIONS (AGGREGATION [Latest_Day = count])")
sql("drop cube myschema.cube10")
}

//TC_1069
test("TC_1069") {
sql("CREATE CUBE cube27 DIMENSIONS (AMSize STRING) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("drop cube cube27")
}

//TC_1070
test("TC_1070") {
sql("CREATE CUBE myschema.cube28 DIMENSIONS (AMSize STRING) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("drop cube myschema.cube28")
}

//TC_1071
test("TC_1071") {
sql("CREATE CUBE myschema.cube29 DIMENSIONS (AMSize STRING as col1) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (col1) ,PARTITION_COUNT=1] )")
sql("drop cube myschema.cube29")
}

//TC_1072
test("TC_1072") {
sql("CREATE CUBE cube30 DIMENSIONS (AMSize STRING as col1) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (col1) ,PARTITION_COUNT=1] )")
sql("drop cube cube30")
}

//TC_1073
test("TC_1073") {
try
{
sql("CREATE CUBE cube31 MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (Latest_Day), PARTITION_COUNT=1] )")
}
catch
{
  case ex: Throwable => 
}
}

//TC_1074
test("TC_1074") {
  
try
{
sql("CREATE CUBE cube32 MEASURES (Latest_Day INTEGER as col2) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (col2), PARTITION_COUNT=1] )")
}
catch
{
  case ex: Throwable => 
}
}

//TC_1075
test("TC_1075") {
sql("create cube cube33 DIMENSIONS (imei string,deviceInformationId integer,MAC string,deviceColor string, device_backColor string,modelId string, marketName string, AMSize string, ROMSize string, CUPAudit string, CPIClocked string, series string, productionDate string, bomCode string, internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince  string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, contractNumber numeric, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict  string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR  integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) MEASURES (gamePointId numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (imei) ,PARTITION_COUNT=2] )")
sql("drop cube cube33")
}

//TC_1076
test("TC_1076") {
sql("CREATE CUBE cube34 DIMENSIONS (AMSize STRING,deviceInformationId STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize,deviceInformationId) ,PARTITION_COUNT=1] )")
sql("drop cube cube34")
}

//TC_1077
test("TC_1077") {
sql("CREATE CUBE myschema.cube35 DIMENSIONS (AMSize STRING,deviceInformationId STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize,deviceInformationId) ,PARTITION_COUNT=1] )")
sql("drop cube myschema.cube35")
}

//TC_1078
test("TC_1078") {
sql("CREATE CUBE cube36 DIMENSIONS (AMSize STRING as col1,deviceInformationId STRING as col2) MEASURES (Latest_Day INTEGER as col3) OPTIONS (AGGREGATION [col3 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (col1,col2) ,PARTITION_COUNT=1] )")
sql("drop cube cube36")
}

//TC_1079
test("TC_1079") {
sql("CREATE CUBE myschema.cube37 DIMENSIONS (AMSize STRING as col1,deviceInformationId STRING as col2) MEASURES (Latest_Day INTEGER as col3) OPTIONS (AGGREGATION [col3 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (col1,col2) ,PARTITION_COUNT=1] )")
sql("drop cube myschema.cube37")
}

//TC_1080
test("TC_1080") {
sql("CREATE CUBE myschema.cube38 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl',COLUMNS= (AMSize) ,PARTITION_COUNT=10] )")
sql("drop cube myschema.cube38")
}

//TC_1081
test("TC_1081") {
sql("CREATE CUBE cube39 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl',COLUMNS= (AMSize) ,PARTITION_COUNT=10] )")
sql("drop cube cube39")
}

//TC_1082
test("TC_1082") {
sql("CREATE CUBE myschema.cube40 DIMENSIONS (bomCode integer) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count])")
sql("drop cube myschema.cube40")
}

//TC_1083
test("TC_1083") {
sql("CREATE CUBE cube41 DIMENSIONS (bomCode integer) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count])")
sql("drop cube cube41")
}

//TC_1084
test("TC_1084") {
sql("CREATE CUBE myschema.cube42 DIMENSIONS (bomCode integer as col1) MEASURES (Latest_Day INTEGER as col2) OPTIONS (AGGREGATION [col2 = count])")
sql("drop cube myschema.cube42")
}

//TC_1085
test("TC_1085") {
sql("CREATE CUBE cube43 DIMENSIONS (bomCode integer as col1) MEASURES (Latest_Day INTEGER as col2) OPTIONS (AGGREGATION [col2 = count])")
sql("drop cube cube43")
}

//TC_1086
test("TC_1086") {
  try
  {
    

    sql("drop cube cube44")
    fail("Unexpected behavior")
  }
catch
{
  case ex: Throwable => 
}
}

//TC_1087
test("TC_1087") {
sql("CREATE CUBE myschema.cube45 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day numeric) OPTIONS (AGGREGATION [bomCode = count])")
sql("drop cube myschema.cube45")
}

//TC_1088
test("TC_1088") {
sql("CREATE CUBE cube46 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day numeric) OPTIONS (AGGREGATION [bomCode = count])")
sql("drop cube cube46")
}

//TC_1089
test("TC_1089") {
sql("CREATE CUBE myschema.cube47 DIMENSIONS (AMSize STRING as col1) MEASURES (Latest_Day numeric as col2) OPTIONS (AGGREGATION [bomCode = count])")
sql("drop cube myschema.cube47")
}

//TC_1090
test("TC_1090") {
sql("CREATE CUBE cube48 DIMENSIONS (AMSize STRING as col1) MEASURES (Latest_Day numeric as col2) OPTIONS (AGGREGATION [bomCode = count])")
sql("drop cube cube48")
}

//TC_1091
test("TC_1091") {
  try
  {

        sql("drop cube cube49")
        fail("Unexpected behavior")
  }
catch
{
  case ex: Throwable => 
}
}

//TC_1092
test("TC_1092") {
sql("CREATE CUBE myschema.cube50 DIMENSIONS (AMSize numeric) MEASURES (Latest_Day numeric) OPTIONS (AGGREGATION [Latest_Day = count])")
sql("drop cube myschema.cube50")
}

//TC_1093
test("TC_1093") {
sql("CREATE CUBE cube51 DIMENSIONS (AMSize numeric) MEASURES (Latest_Day numeric) OPTIONS (AGGREGATION [Latest_Day = count])")
sql("drop cube cube51")
}

//TC_1094
test("TC_1094") {
sql("CREATE CUBE myschema.cube52 DIMENSIONS (AMSize numeric as col1) MEASURES (Latest_Day numeric as col2) OPTIONS (AGGREGATION [Latest_Day = count])")
sql("drop cube myschema.cube52")
}

//TC_1095
test("TC_1095") {
sql("CREATE CUBE cube53 DIMENSIONS (col1 numeric as col1) MEASURES (Latest_Day numeric as col2) OPTIONS (AGGREGATION [Latest_Day = count])")
sql("drop cube cube53")
}

//TC_1096
test("TC_1096") {
  try
  {

     sql("drop cube cube54")
        fail("Unexpected behavior")
  }
catch
{
  case ex: Throwable => 
}
}

//TC_1097
test("TC_1097") {
  try
  {

     sql("drop cube cube55")
        fail("Unexpected behavior")
  }
catch
{
  case ex: Throwable => 
}
}

//TC_1098
test("TC_1098") {
sql("CREATE CUBE cube106 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = sum] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("drop cube cube106")
}

//TC_1099
test("TC_1099") {
sql("CREATE CUBE cube107 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = avg] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("drop cube cube107")
}

//TC_1100
test("TC_1100") {
sql("CREATE CUBE cube108 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = min] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("drop cube cube108")
}

//TC_1101
test("TC_1101") {
sql("CREATE CUBE cube109 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = max] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("drop cube cube109")
}

//TC_1102
test("TC_1102") {
sql("CREATE CUBE cube110 DIMENSIONS(imei String, test integer, key String, name String) MEASURES(gamepointid Numeric, price integer) WITH dimFile2 RELATION (FACT.deviceid=key) INCLUDE ( key, name)")
sql("drop cube cube110")
}

//TC_1103
test("TC_1103") {
  try
  {

     sql("drop cube cube111")
        fail("Unexpected behavior")
  }
catch
{
  case ex: Throwable => 
}
}

//TC_1104
test("TC_1104") {
  try
  {
    

     sql("drop cube cube112")
        fail("Unexpected behavior")
  }
catch
{
  case ex: Throwable => 
}
}

//TC_1105
test("TC_1105") {
sql("CREATE CUBE cube113 dimensions(imei String, test integer, key String, name String) measures(gamepointid Numeric, price integer) with dimFile1 relation (FACT.deviceid=key) include ( key, name)")
sql("drop cube cube113")
}

//TC_1106
test("TC_1106") {
sql("create cube myschema.cube114 dimensions(imei String, test integer,key String, name String) measures(gamepointid Numeric, price integer) with dimFile relation (FACT.deviceid=key) include ( key, name)")
sql("drop cube myschema.cube114")
}

//TC_1107
test("TC_1107") {
  
  try
  {

     sql("drop cube cube115")
        fail("Unexpected behavior")
  }
catch
{
  case ex: Throwable => 
}
}

//TC_1108
test("TC_1108") {

  try
  {
    sql("CREATE CUBE cube116 DIMENSIONS(imei String, test integer) MEASURES(gamepointid Numeric, price integer) WITH hivetest RELATION (FACT.deviceid=key1) INCLUDE ( key, name)")
     sql("drop cube cube116")
        fail("Unexpected behavior")
  }
catch
{
  case ex: Throwable => 
}

}

//TC_1109
test("TC_1109") {

  try
  {
   sql("CREATE CUBE cube117 DIMENSIONS(imei String, test integer) MEASURES(gamepointid Numeric, price integer) WITH dimFile:hivetest RELATION () INCLUDE ()")
     sql("drop cube cube117")
        fail("Unexpected behavior")
  }
catch
{
  case ex: Throwable => 
}


}

//TC_1110
test("TC_1110") {
sql("create cube cube118 dimensions (AMSize STRING) measures (Latest_Day INTEGER) options (aggregation [Latest_Day = count] partitioner [class = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', columns= (AMSize) ,partition_count=1] )")
sql("drop cube cube118")
}

//TC_1111
test("TC_1111") {

  try
  {
    sql("create cube cube119 dimensions () measures () options (aggregation [] partitioner [class = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', columns= () ,partition_count=1] )")
     sql("drop cube cube119")
        fail("Unexpected behavior")
  }
catch
{
  case ex: Throwable => 
}


}

//TC_1112
test("TC_1112") {
sql("create cube cube120 dimensions(key string,name string) measures(gamepointid numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)")
sql("drop cube cube120")
}

//TC_1113
test("TC_1113") {
sql("create cube myschema.cube121 dimensions(key string,name string) measures(gamepointid numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)")
sql("drop cube myschema.cube121")
}

//DTS2015101203724
test("DTS2015101203724") {
  try
  {
   sql("CREATE CUBE myschema.include8 FROM './src/test/resources/100_hive.csv' INCLUDE (imei, imei),'./src/test/resources/101_hive.csv' RELATION (fact.gamepointid = gamepointid) EXCLUDE (MAC, bomcode)")
     sql("drop cube myschema.include8")
        fail("Unexpected behavior")
  }
catch
{
  case ex: Throwable => 
}


}

//DTS2015102804520
test("DTS2015102804520") {
sql("CREATE CUBE default.cube431 DIMENSIONS (bomCode integer as col1) MEASURES (Latest_Day INTEGER as col2)")
sql("drop cube default.cube431")
}

//DTS2015103009506
test("DTS2015103009506") {
sql("CREATE CUBE cube_restructure1 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("drop cube cube_restructure1")
}

//DTS2015103007487
test("DTS2015103007487") {

    sql("create schema abc");
    sql("create cube abc.babu8 dimensions(a string) measures(b INTEGER)")
     sql("drop cube  abc.babu8")
      sql("drop schema abc");
}
  
  //TC_1114
test("TC_1114") {
sql("create cube  cube1_drop dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string)  measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("drop cube cube1_drop")
}

//TC_1115
test("TC_1115") {
sql("create cube  myschema.cube2_drop dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string)  measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("drop cube myschema.cube2_drop")
}

//TC_1116
test("TC_1116") {
sql("CREATE CUBE cube3_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("drop cube cube3_drop")
}

//TC_1117
test("TC_1117") {
sql("CREATE CUBE myschema.cube4_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("drop cube myschema.cube4_drop")
}

//TC_1118
test("TC_1118") {
sql("CREATE CUBE cube5_drop DIMENSIONS (AMSize STRING as col1) MEASURES (Latest_Day INTEGER as col2) OPTIONS (AGGREGATION [col2 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (col1) ,PARTITION_COUNT=1] )")
sql("drop cube cube5_drop")
}

//TC_1119
test("TC_1119") {
sql("CREATE CUBE myschema.cube6_drop DIMENSIONS (AMSize STRING as col1) MEASURES (Latest_Day INTEGER as col2) OPTIONS (AGGREGATION [col2 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (col1) ,PARTITION_COUNT=1] )")
sql("drop cube myschema.cube6_drop")
}

//TC_1120
test("TC_1120") {
sql("CREATE CUBE cube7_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count])")
sql("drop cube cube7_drop")
}

//TC_1121
test("TC_1121") {
sql("CREATE CUBE myschema.cube8_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count])")
sql("drop cube myschema.cube8_drop")
}

//TC_1122
test("TC_1122") {
sql("CREATE CUBE cube9_drop DIMENSIONS (AMSize STRING as col1) MEASURES (Latest_Day INTEGER as col2) OPTIONS (AGGREGATION [Latest_Day = count])")
sql("drop cube cube9_drop")
}

//TC_1123
test("TC_1123") {
sql("CREATE CUBE myschema.cube10_drop DIMENSIONS (AMSize STRING as col1) MEASURES (Latest_Day INTEGER as col2) OPTIONS (AGGREGATION [Latest_Day = count])")
sql("drop cube myschema.cube10_drop")
}

//TC_1124
test("TC_1124") {
sql("CREATE CUBE cube27_drop DIMENSIONS (AMSize STRING) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("drop cube cube27_drop")
}

//TC_1125
test("TC_1125") {
sql("CREATE CUBE myschema.cube28_drop DIMENSIONS (AMSize STRING) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("drop cube myschema.cube28_drop")
}

//TC_1126
test("TC_1126") {
sql("CREATE CUBE myschema.cube29_drop DIMENSIONS (AMSize STRING as col1) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (col1) ,PARTITION_COUNT=1] )")
sql("drop cube myschema.cube29_drop")
}

//TC_1127
test("TC_1127") {
sql("CREATE CUBE cube30_drop DIMENSIONS (AMSize STRING as col1) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (col1) ,PARTITION_COUNT=1] )")
sql("drop cube cube30_drop")
}

//TC_1128
test("TC_1128") {
sql("create cube cube33_drop DIMENSIONS (imei string,deviceInformationId integer,MAC string,deviceColor string, device_backColor string,modelId string, marketName string, AMSize string, ROMSize string, CUPAudit string, CPIClocked string, series string, productionDate string, bomCode string, internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince  string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, contractNumber numeric, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict  string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR  integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) MEASURES (gamePointId numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (imei) ,PARTITION_COUNT=2] )")
sql("drop cube cube33_drop")
}

//TC_1129
test("TC_1129") {
sql("CREATE CUBE cube34_drop DIMENSIONS (AMSize STRING,deviceInformationId STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize,deviceInformationId) ,PARTITION_COUNT=1] )")
sql("drop cube cube34_drop")
}

//TC_1130
test("TC_1130") {
sql("CREATE CUBE myschema.cube35_drop DIMENSIONS (AMSize STRING,deviceInformationId STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize,deviceInformationId) ,PARTITION_COUNT=1] )")
sql("drop cube myschema.cube35_drop")
}

//TC_1131
test("TC_1131") {
sql("CREATE CUBE cube36_drop DIMENSIONS (AMSize STRING as col1,deviceInformationId STRING as col2) MEASURES (Latest_Day INTEGER as col3) OPTIONS (AGGREGATION [col3 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (col1,col2) ,PARTITION_COUNT=1] )")
sql("drop cube cube36_drop")
}

//TC_1132
test("TC_1132") {
sql("CREATE CUBE myschema.cube37_drop DIMENSIONS (AMSize STRING as col1,deviceInformationId STRING as col2) MEASURES (Latest_Day INTEGER as col3) OPTIONS (AGGREGATION [col3 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (col1,col2) ,PARTITION_COUNT=1] )")
sql("drop cube myschema.cube37_drop")
}

//TC_1133
test("TC_1133") {
sql("CREATE CUBE myschema.cube38_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl',COLUMNS= (AMSize) ,PARTITION_COUNT=10] )")
sql("drop cube myschema.cube38_drop")
}

//TC_1134
test("TC_1134") {
sql("CREATE CUBE cube39_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl',COLUMNS= (AMSize) ,PARTITION_COUNT=10] )")
sql("drop cube cube39_drop")
}

//TC_1135
test("TC_1135") {
sql("CREATE CUBE myschema.cube45_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day numeric) OPTIONS (AGGREGATION [bomCode = count])")
sql("drop cube myschema.cube45_drop")
}

//TC_1136
test("TC_1136") {
sql("CREATE CUBE cube46_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day numeric) OPTIONS (AGGREGATION [bomCode = count])")
sql("drop cube cube46_drop")
}

//TC_1137
test("TC_1137") {
sql("CREATE CUBE myschema.cube47_drop DIMENSIONS (AMSize STRING as col1) MEASURES (Latest_Day numeric as col2) OPTIONS (AGGREGATION [bomCode = count])")
sql("drop cube myschema.cube47_drop")
}

//TC_1138
test("TC_1138") {
sql("CREATE CUBE cube48_drop DIMENSIONS (AMSize STRING as col1) MEASURES (Latest_Day numeric as col2) OPTIONS (AGGREGATION [bomCode = count])")
sql("drop cube cube48_drop")
}

//TC_1139
test("TC_1139") {
sql("CREATE CUBE myschema.cube50_drop DIMENSIONS (AMSize numeric) MEASURES (Latest_Day numeric) OPTIONS (AGGREGATION [Latest_Day = count])")
sql("drop cube myschema.cube50_drop")
}

//TC_1140
test("TC_1140") {
sql("CREATE CUBE cube51_drop DIMENSIONS (AMSize numeric) MEASURES (Latest_Day numeric) OPTIONS (AGGREGATION [Latest_Day = count])")
sql("drop cube cube51_drop")
}

//TC_1141
test("TC_1141") {
sql("CREATE CUBE myschema.cube52_drop DIMENSIONS (AMSize numeric as col1) MEASURES (Latest_Day numeric as col2) OPTIONS (AGGREGATION [Latest_Day = count])")
sql("drop cube myschema.cube52_drop")
}

//TC_1142
test("TC_1142") {
sql("CREATE CUBE cube53_drop DIMENSIONS (col1 numeric as col1) MEASURES (Latest_Day numeric as col2) OPTIONS (AGGREGATION [Latest_Day = count])")
sql("drop cube cube53_drop")
}

//TC_1143
test("TC_1143") {
sql("CREATE CUBE cube106_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = sum] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("drop cube cube106_drop")
}

//TC_1144
test("TC_1144") {
sql("CREATE CUBE cube107_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = avg] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("drop cube cube107_drop")
}

//TC_1145
test("TC_1145") {
sql("CREATE CUBE cube108_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = min] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("drop cube cube108_drop")
}

//TC_1146
test("TC_1146") {
sql("CREATE CUBE cube109_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = max] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("drop cube cube109_drop")
}

//TC_1147
test("TC_1147") {
sql("CREATE CUBE cube110_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = max] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("drop cube cube110_drop")
}

//TC_1148
test("TC_1148") {
sql("CREATE CUBE cube111_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = max] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("drop cube cube111_drop")
}

//TC_1149
test("TC_1149") {
sql("CREATE CUBE cube113_drop dimensions(imei String, test integer, key String, name String) measures(gamepointid Numeric, price integer) with dimFile1 relation (FACT.deviceid=key) include ( key, name)")
sql("drop cube cube113_drop")
}

//TC_1150
test("TC_1150") {
sql("create cube myschema.cube114_drop dimensions(imei String, test integer,key String, name String) measures(gamepointid Numeric, price integer) with dimFile relation (FACT.deviceid=key) include ( key, name)")
sql("drop cube myschema.cube114_drop")
}

//TC_1151
test("TC_1151") {
sql("create cube cube118_drop dimensions (AMSize STRING) measures (Latest_Day INTEGER) options (aggregation [Latest_Day = count] partitioner [class = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', columns= (AMSize) ,partition_count=1] )")
sql("drop cube cube118_drop")
}

//TC_1152
test("TC_1152") {
  try
  {
//sql("create cube cube118_drop dimensions (AMSize STRING) measures (Latest_Day INTEGER) options (aggregation [Latest_Day = count] partitioner [class = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', columns= (AMSize) ,partition_count=1] )")

    fail("Unexpected behavior")
  }
  catch
{
  case ex: Throwable => 
}
}


//TC_1153
test("TC_1153") {
  
  try
  {
    
//sql("create cube cube118_drop dimensions (AMSize STRING) measures (Latest_Day INTEGER) options (aggregation [Latest_Day = count] partitioner [class = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', columns= (AMSize) ,partition_count=1] )")

    fail("Unexpected behavior")
  }
catch
{
  case ex: Throwable => 
}
}

//TC_1154
test("TC_1154") {
sql("create cube cube119_drop dimensions (AMSize STRING) measures (Latest_Day INTEGER) options (aggregation [Latest_Day = count] partitioner [class = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', columns= (AMSize) ,partition_count=1] )")
sql("drop cube cube119_drop")
}

//TC_1155
test("TC_1155") {
sql("create cube cube120_drop dimensions(key string,name string) measures(gamepointid numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)")
sql("drop cube cube120_drop")
}

//DTS2015112610913_03
test("DTS2015112610913_03") {
sql("CREATE CUBE cube_restructure61 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructuRE61 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"', FILEHEADER 'a0,b0')");
sql("alter cube CUBE_restructuRE61 add dimensions(a12 string) measures(b11 integer)");
sql("drop cube cube_restructure61")
}

//DTS2015113006662
test("DTS2015113006662") {
sql("CREATE CUBE default.t3 DIMENSIONS (imei String, productdate String, updatetime String, gamePointId Integer, contractNumber Integer) MEASURES (deviceInformationId Integer) WITH table4 RELATION (FACT.imei=imei) INCLUDE (imei, productdate, updatetime, gamePointId, contractNumber)OPTIONS ( AGGREGATION[ deviceInformationId=count ] , PARTITIONER [ CLASS='org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS=(imei), PARTITION_COUNT=5 ] )")
sql("select * from t3")
sql("drop cube default.t3")
}
  
  
//TC_1156
test("TC_1156") {
sql("create cube vardhan dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan"),
Seq(Row(100)))
sql("drop cube vardhan")
}

//TC_1157
test("TC_1157") {
try
{
sql("create cube vardhan12 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan12 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,AMSize,channelsId,ActiveCountry,Activecity')")
checkAnswer(
sql("select count(*) from vardhan12"),
Seq())
sql("drop cube vardhan12")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable => 
}
}

//TC_1158
test("TC_1158") {
try
{
sql("create cube vardhan13 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan13 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'deviceInformationId,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan13"),
Seq())
sql("drop cube vardhan13")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable => 
}
}

//TC_1159
test("TC_1159") {
try
{
sql("create cube vardhan3 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan3 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'deviceInformationId,AMSize,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan3"),
Seq())
sql("drop cube vardhan3")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable => 
}
}

//TC_1163
test("TC_1163") {
sql("create cube thebigdealisadealofdealneverdealadealtodealdealaphonetodealofdealkingisakingofkingdon dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube thebigdealisadealofdealneverdealadealtodealdealaphonetodealofdealkingisakingofkingdon partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from thebigdealisadealofdealneverdealadealtodealdealaphonetodealofdealkingisakingofkingdon"),
Seq(Row(100)))
sql("drop cube thebigdealisadealofdealneverdealadealtodealdealaphonetodealofdealkingisakingofkingdon")
}

//TC_1166
test("TC_1166") {
sql("create cube vardhan15 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan15 partitionData(DELIMITER ';' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan15"),
Seq(Row(0)))
sql("drop cube vardhan15")
}

//TC_1167
test("TC_1167") {
sql("create cube vardhan11 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan11 partitionData(DELIMITER ',' ,FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan11"),
Seq(Row(100)))
sql("drop cube vardhan11")
}

//TC_1168
test("TC_1168") {
sql("create cube vardhan2 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan2 partitionData(DELIMITER ',' ,QUOTECHAR '/', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan2"),
Seq(Row(100)))
sql("drop cube vardhan2")
}

//TC_1169
test("TC_1169") {
try
{
sql("create cube vardhan100 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan100 partitionData(DELIMITER ',' ,QUOTECHAR '\"')")
checkAnswer(
sql("select count(*) from vardhan100"),
Seq())
sql("drop cube vardhan100")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable => 
}
}

//TC_1170
test("TC_1170") {
sql("create cube vardhan200 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei,AMSize) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan200 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan200"),
Seq(Row(100)))
sql("drop cube vardhan200")
}

//TC_1171
test("TC_1171") {
sql("create cube vardhan500 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string, productionDate TIMESTAMP)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData6.csv' INTO Cube vardhan500 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
checkAnswer(
sql("select count(*) from vardhan500"),
Seq(Row(100)))
sql("drop cube vardhan500")
}

//TC_1172
test("TC_1172") {
sql("create cube vardhan1000 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData5.csv' INTO Cube vardhan1000 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan1000"),
Seq(Row(97)))
sql("drop cube vardhan1000")
}

//TC_1173
test("TC_1173") {
sql("create cube vardhan9 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan9 partitionData(DELIMITER ',' ,QUOTECHAR '/', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan9"),
Seq(Row(100)))
sql("drop cube vardhan9")
}

//TC_1174
test("TC_1174") {
sql("create cube vardhan16 dimensions(AMSize STRING) measures(deviceInformationId integer) OPTIONS (AGGREGATION [deviceInformationId=count])")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan16 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan16"),
Seq(Row(100)))
sql("drop cube vardhan16")
}

//TC_1175
test("TC_1175") {
  sql("create schema IF NOT EXISTS myschema1")
sql("create cube myschema1.vardhan17 DIMENSIONS (AMSize STRING) MEASURES (deviceInformationId integer) OPTIONS (AGGREGATION [deviceInformationId = count])")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube myschema1.vardhan17 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*)  from myschema1.vardhan17"),
Seq(Row(100)))
sql("drop cube myschema1.vardhan17")
sql("drop schema myschema1")
}

//TC_1176
test("TC_1176") {
sql("create cube vardhan dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan"),
Seq(Row(100)))
sql("drop cube vardhan")
}

//TC_1177
test("TC_1177") {
try
{
sql("create cube vardhan12 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan12 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,AMSize,channelsId,ActiveCountry,Activecity')")
checkAnswer(
sql("select count(*) from vardhan12"),
Seq())
sql("drop cube vardhan12")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable => 
}
}

//TC_1178
test("TC_1178") {
try
{
sql("create cube vardhan13 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan13 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'deviceInformationId,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan13"),
Seq())
sql("drop cube vardhan13")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable => 
}
}

//TC_1179
test("TC_1179") {
try
{
sql("create cube vardhan3 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan3 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'deviceInformationId,AMSize,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan3"),
Seq())
sql("drop cube vardhan3")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable => 
}
}

//TC_1183
test("TC_1183") {
sql("create cube thebigdealisadealofdealneverdealadealtodealdealaphonetodealofdealkingisakingofkingdon dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube thebigdealisadealofdealneverdealadealtodealdealaphonetodealofdealkingisakingofkingdon partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from thebigdealisadealofdealneverdealadealtodealdealaphonetodealofdealkingisakingofkingdon"),
Seq(Row(100)))
sql("drop cube thebigdealisadealofdealneverdealadealtodealdealaphonetodealofdealkingisakingofkingdon")
}

//TC_1186
test("TC_1186") {
sql("create cube vardhan15 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan15 partitionData(DELIMITER ';' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan15"),
Seq(Row(0)))
sql("drop cube vardhan15")
}

//TC_1187
test("TC_1187") {
sql("create cube vardhan11 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan11 partitionData(DELIMITER ',' ,FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan11"),
Seq(Row(100)))
sql("drop cube vardhan11")
}

//TC_1188
test("TC_1188") {
sql("create cube vardhan2 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan2 partitionData(DELIMITER ',' ,QUOTECHAR '/', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan2"),
Seq(Row(100)))
sql("drop cube vardhan2")
}

//TC_1189
test("TC_1189") {
try
{
sql("create cube vardhan100 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan100 partitionData(DELIMITER ',' ,QUOTECHAR '\"')")
checkAnswer(
sql("select count(*) from vardhan100"),
Seq())
sql("drop cube vardhan100")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable => 
}
}

//TC_1190
test("TC_1190") {
sql("create cube vardhan200 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei,AMSize) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan200 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan200"),
Seq(Row(100)))
sql("drop cube vardhan200")
}

//TC_1191
test("TC_1191") {
sql("create cube vardhan500 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string, productionDate TIMESTAMP)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData6.csv' INTO Cube vardhan500 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
checkAnswer(
sql("select count(*) from vardhan500"),
Seq(Row(100)))
sql("drop cube vardhan500")
}

//TC_1192
test("TC_1192") {
sql("create cube vardhan1000 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData5.csv' INTO Cube vardhan1000 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan1000"),
Seq(Row(97)))
sql("drop cube vardhan1000")
}

//TC_1193
test("TC_1193") {
sql("create cube vardhan9 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan9 partitionData(DELIMITER ',' ,QUOTECHAR '/', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan9"),
Seq(Row(100)))
sql("drop cube vardhan9")
}

//TC_1194
test("TC_1194") {
sql("create cube vardhan16 dimensions(AMSize STRING) measures(deviceInformationId integer) OPTIONS (AGGREGATION [deviceInformationId=count])")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan16 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan16"),
Seq(Row(100)))
sql("drop cube vardhan16")
}

//TC_1195
test("TC_1195") {
sql("create cube myschema.vardhan17 DIMENSIONS (AMSize STRING) MEASURES (deviceInformationId integer) OPTIONS (AGGREGATION [deviceInformationId = count])")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube myschema.vardhan17 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*)  from myschema.vardhan17"),
Seq(Row(100)))
sql("drop cube myschema.vardhan17")
}

//DTS2015111808892
test("DTS2015111808892") {
sql("CREATE CUBE cube_restructure DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"')")
checkAnswer(
sql("select count(*)  from cube_restructure"),
Seq(Row(100)))
sql("drop cube cube_restructure")
}

//DTS2015111809054
test("DTS2015111809054") {
sql("create cube cubeDTS2015111809054 dimensions(key string,name string) measures(gamepointid numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)")
sql("LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE cubeDTS2015111809054 PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')")
checkAnswer(
sql("select count(*)  from cubeDTS2015111809054"),
Seq(Row(21)))
sql("drop cube cubeDTS2015111809054")
}

//DTS2015112006803_01
test("DTS2015112006803_01") {
sql("CREATE CUBE incloading_DTS2015112006803_01 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE incloading_DTS2015112006803_01 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"')")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE incloading_DTS2015112006803_01 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"')")
checkAnswer(
sql("select count(*) from incloading_DTS2015112006803_01"),
Seq(Row(200)))
sql("drop cube incloading_DTS2015112006803_01")
}


//DTS2015112710336
test("DTS2015112710336") {
sql("create cube rock dimensions(key string as col1,name string as col3) measures(gamepointid numeric,price numeric) with dimFile RELATION (FACT.deviceid=col1) INCLUDE ( col1,col3)")
sql("LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE rock OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER '')")
checkAnswer(
sql("select count(*)  from rock"),
Seq(Row(21)))
sql("drop cube rock")
}

//DTS2015111810813
test("DTS2015111810813") {
sql("create cube single dimensions(imei string,deviceInformationId integer,mac string,productdate timestamp,updatetime timestamp) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA fact from './src/test/resources/vmallFact_headr.csv' INTO CUBE single PARTITIONDATA(DELIMITER '\001', QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,mac,productdate,updatetime,gamePointId,contractNumber')")
checkAnswer(
sql("select count(*) from single"),
Seq(Row(10)))
sql("drop cube single")
}

//DTS2015101504861
test("DTS2015101504861") {
sql("create cube vard970 dimensions(imei string,productionDate timestamp,AMSize string,channelsId string,ActiveCountry string, Activecity string) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData2.csv' INTO CUBE vard970 OPTIONS(DELIMITER ',', QUOTECHAR '\"', FILEHEADER 'imei,productionDate,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select imei from vard970 where productionDate='2015-07-06 12:07:00'"),
Seq())
sql("drop cube vard970")
}

//DTS2015101209623
test("DTS2015101209623") {
sql("create cube vard971 dimensions(imei string,productionDate timestamp,AMSize string,channelsId string,ActiveCountry string, Activecity string) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData2.csv' INTO CUBE vard971 OPTIONS(DELIMITER ',', QUOTECHAR '\"', FILEHEADER 'imei,productionDate,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select imei from vard971 WHERE gamepointId is NULL"),
Seq())
sql("drop cube vard971")
}

//TC_1326
test("TC_1326") {
sql("create cube vardhanincomp dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData4.csv' INTO Cube vardhanincomp OPTIONS(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select channelsId from vardhanincomp order by imei ASC limit 0"),
Seq())
sql("drop cube vardhanincomp")
}

//TC_1327
test("TC_1327") {
sql("create cube vardhan01 dimensions(key string,name string) measures(gamepointid numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)")
sql("LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')")
checkAnswer(
sql("select count(*) from vardhan01"),
Seq(Row(21)))
sql("drop cube vardhan01")
}

//TC_1328
test("TC_1328") {
sql("create cube vardhan01 dimensions(key string,name string as col1) measures(gamepointid numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,col1)")
sql("LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')")
checkAnswer(
sql("select count(*) from vardhan01"),
Seq(Row(21)))
sql("drop cube vardhan01")
}

  
  //DTS2015112009008
test("DTS2015112009008") {
sql("CREATE CUBE cube_restructure60 DIMENSIONS (AMSize STRING) MEASURES (Latest_DAY INTEGER) OPTIONS (AGGREGATION [Latest_DAY = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/create_cube.csv' INTO CUBE cube_restructure60 PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"\"')")
sql("alter cube cube_restructure60 add dimensions(a1 string)")
sql("select count(*) from cube_restructure60")
sql("alter cube cube_restructure60 drop(a1)")
checkAnswer(
sql("select count(*) from cube_restructure60"),
Seq(Row(200)))
sql("drop cube cube_restructure60")
}

  //DTS2015120304016
test("DTS2015120304016") {
sql("CREATE CUBE incloading1 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE incloading1 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("drop cube incloading1")
sql("CREATE CUBE incloading1 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE incloading1 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
checkAnswer(
sql("select count(*) from incloading1"),
Seq(Row(100)))
sql("drop cube incloading1")
}

  //DTS2015110311277
test("DTS2015110311277") {
sql("create cube vardhan dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData1.csv' INTO Cube vardhan OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("alter cube vardhan add dimensions(alreadID)")
sql("alter cube vardhan drop (alreadID)")
sql("LOAD DATA FACT FROM './src/test/resources/TestData1.csv' INTO Cube vardhan OPTIONS(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("select count(*) from vardhan"),
Seq(Row(200)))
sql("drop cube vardhan")
}

  //DTS2015121511752
test("DTS2015121511752") {
sql("CREATE CUBE cube_restructure68 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure68 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure68 add dimensions(a10 string) measures(b9 integer) options (AGGREGATION [b9 = MAX])")
sql(" drop cube cube_restructure68")
sql("CREATE CUBE cube_restructure68 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure68 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"')")
checkAnswer(
sql("select * from cube_restructure68 order by a0 ASC limit 3"),
Seq(Row("1AA1",2738.0),Row("1AA10",1714.0),Row("1AA100",1271.0)))
sql("drop cube cube_restructure68")
}

  //TC_1329
test("TC_1329") {
sql("create cube vardhan01 dimensions(key string,name string) measures(gamepointid numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)")
sql("alter cube vardhan01 add dimensions(productiondate timestamp) options (AGGREGATION [b = SUM])")
sql("LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')")
checkAnswer(
sql("select key from vardhan01 order by key ASC limit 1"),
Seq(Row("1")))
sql("drop cube vardhan01")
}

  //TC_1330
test("TC_1330") {
sql("create cube vardhan01 dimensions(key string,name string) measures(gamepointid numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)")
sql("LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')")
sql("alter cube vardhan01 add dimensions(productiondate timestamp) options (AGGREGATION [b = SUM])")
sql("LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')")
checkAnswer(
sql("select count(*) from vardhan01"),
Seq(Row(42)))
sql("drop cube vardhan01")
}

  //TC_1331
test("TC_1331") {
sql("create cube vardhan01 dimensions(key string,name string) measures(gamepointid numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)")
sql("LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')")
sql("alter cube vardhan01 add dimensions(productiondate timestamp as col5) options (AGGREGATION [b = SUM])")
sql("LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')")
checkAnswer(
sql("select count(*) from vardhan01"),
Seq(Row(42)))
sql("drop cube vardhan01")
}


  //TC_1332
test("TC_1332") {
  
  try
  {
    sql("create cube vardhan01 dimensions(key string,name string) measures(gamepointid numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)")
sql("alter cube vardhan01 drop (name)")
sql("alter cube vardhan01 add dimensions(name as name) options (AGGREGATION [b = SUM])")
sql("LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')")
sql("drop cube vardhan01")
fail("Unexpected behavior")
  }
catch
{
  
  case ex: Throwable => sql("drop cube vardhan01")
}

}

  
//TC_1160
test("TC_1160") {
try
{
sql("create cube vardhan4 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA './src/test/resources/TestData1.csv' INTO Cube vardhan4 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhan4")
 
}
}

//TC_1161
test("TC_1161") {
try
{
sql("create cube vardhan5 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  INTO Cube vardhan5 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhan5")
 
}
}

//TC_1162
test("TC_1162") {
try
{
sql("create cube vardhan1 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube  partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhan1")
 
}
}

//TC_1164
test("TC_1164") {
try
{
sql("create cube vardhan1 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("\"LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan1 (DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhan1")
 
}
}

//TC_1165
test("TC_1165") {
try
{
sql("create cube vardhan10 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan10 partitionData(QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhan10")
 
}
}

//TC_1180
test("TC_1180") {
try
{
sql("create cube vardhan4 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA './src/test/resources/TestData1.csv' INTO Cube vardhan4 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhan4")
 
}
}

//TC_1181
test("TC_1181") {
try
{
sql("create cube vardhan5 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  INTO Cube vardhan5 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhan5")
 
}
}

//TC_1182
test("TC_1182") {
try
{
sql("create cube vardhan1 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube  partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhan1")
 
}
}

//TC_1184
test("TC_1184") {
try
{
sql("create cube vardhan1 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("\"LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan1 (DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhan1")
 
}
}

//TC_1185
test("TC_1185") {
try
{
sql("create cube vardhan10 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan10 partitionData(QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhan10")
 
}
}

//DTS2015111900020
test("DTS2015111900020") {
try
{
sql("CREATE CUBE testwithout_measure DIMENSIONS (AMSize STRING) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/create_cube.csv' INTO CUBE testwithout_measure PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"')")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube testwithout_measure")
 
}
}

  
//TC_1196
test("TC_1196") {
sql("CREATE CUBE cube_restructure1 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure1 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure1 add dimensions(a string) measures(b integer)")
checkAnswer(
sql("select a,b from cube_restructure1 limit 1"),
Seq(Row(null,0.0)))
sql("drop cube cube_restructure1")
}

//TC_1197
test("TC_1197") {
sql("CREATE CUBE cube_restructure2 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure2 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure2 add dimensions(a39 string)")
checkAnswer(
sql("select distinct(a39) from cube_restructure2"),
Seq(Row(null)))
sql("drop cube cube_restructure2")
}

//TC_1198
test("TC_1198") {
sql("CREATE CUBE cube_restructure3 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure3 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure3 add measures(b1 integer)")
checkAnswer(
sql("select distinct(b1) from cube_restructure3"),
Seq(Row(0.0)))
sql("drop cube cube_restructure3")
}

//TC_1200
test("TC_1200") {
sql("CREATE CUBE cube_restructure5 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure5 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure5 add measures(b2 integer) options (AGGREGATION [b2 = SUM])")
checkAnswer(
sql("select distinct(b2) from cube_restructure5"),
Seq(Row(0.0)))
sql("drop cube cube_restructure5")
}

//TC_1204
test("TC_1204") {
sql("CREATE CUBE cube_restructure9 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure9 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure9 add dimensions(a8 string) measures(b7 integer) options (AGGREGATION [b7 = COUNT])")
checkAnswer(
sql("select a8,b7 from cube_restructure9 limit 1"),
Seq(Row(null,0.0)))
sql("drop cube cube_restructure9")
}

//TC_1205
test("TC_1205") {
sql("CREATE CUBE cube_restructure10 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure10 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure10 add dimensions(a9 string) measures(b8 integer) options (AGGREGATION [b8 = MIN])")
checkAnswer(
sql("select a9,b8 from cube_restructure10 limit 1"),
Seq(Row(null,0.0)))
sql("drop cube cube_restructure10")
}

//TC_1206
test("TC_1206") {
sql("CREATE CUBE cube_restructure11 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure11 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure11 add dimensions(a10 string) measures(b9 integer) options (AGGREGATION [b9 = MAX])")
checkAnswer(
sql("select a10,b9 from cube_restructure11 limit 1"),
Seq(Row(null,0.0)))
sql("drop cube cube_restructure11")
}

//TC_1207
test("TC_1207") {
sql("CREATE CUBE cube_restructure12 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure12 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure12 add dimensions(a11 string) measures(b10 integer) options (AGGREGATION [b10 = AVG])")
checkAnswer(
sql("select a11,b10 from cube_restructure12 limit 1"),
Seq(Row(null,0.0)))
sql("drop cube cube_restructure12")
}

//TC_1208
test("TC_1208") {
sql("CREATE CUBE cube_restructure13 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure13 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure13 add dimensions(a12 string) measures(b11 integer) options (defaults [a12=test])")
checkAnswer(
sql("select a12,b11 from cube_restructure13 limit 1"),
Seq(Row("test",0.0)))
sql("drop cube cube_restructure13")
}

//TC_1209
test("TC_1209") {
sql("CREATE CUBE cube_restructure14 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure14 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure14 add dimensions(a13 string) measures(b12 integer) options (defaults [thisisalongnamethisisalongnamethisisalongnamethisisalongnamethisisalongnamethisisalongnamethisisalongname=test])")
checkAnswer(
sql("select a13,b12 from cube_restructure14 limit 1"),
Seq(Row(null,0.0)))
sql("drop cube cube_restructure14")
}

//TC_1210
test("TC_1210") {
sql("CREATE CUBE cube_restructure15 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure15 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure15 add dimensions(a14 string) measures(b13 integer) options (defaults [a13=人转])")
checkAnswer(
sql("select a14,b13 from cube_restructure15 limit 1"),
Seq(Row(null,0.0)))
sql("drop cube cube_restructure15")
}

//TC_1211
test("TC_1211") {
sql("CREATE CUBE cube_restructure16 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure16 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure16 add dimensions(a15 string) measures(b14 integer) options (defaults [b14=10])")
checkAnswer(
sql("select a15,b14 from cube_restructure16 limit 1"),
Seq(Row(null,10.0)))
sql("drop cube cube_restructure16")
}

//TC_1212
test("TC_1212") {
sql("CREATE CUBE cube_restructure17 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure17 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure17 add dimensions(a16 string) measures(b13 integer) options (defaults [a16=10])")
checkAnswer(
sql("select a16,b13 from cube_restructure17 limit 1"),
Seq(Row("10",0.0)))
sql("drop cube cube_restructure17")
}

//TC_1213
test("TC_1213") {
sql("CREATE CUBE cube_restructure18 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure18 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure18 add dimensions(a17 string) measures(b14 integer) options (defaults [b14=test])")
checkAnswer(
sql("select a17,b14 from cube_restructure18 limit 1"),
Seq(Row(null,0.0)))
sql("drop cube cube_restructure18")
}

//TC_1214
test("TC_1214") {
sql("CREATE CUBE cube_restructure19 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure19 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure19 add dimensions(a18 string) measures(b15 integer) options (defaults [a18=test,b15=10])")
checkAnswer(
sql("select a18,b15 from cube_restructure19 limit 1"),
Seq(Row("test",10.0)))
sql("drop cube cube_restructure19")
}

//TC_1217
test("TC_1217") {
sql("CREATE CUBE cube_restructure22 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure22 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure22 add dimensions(a40 string) measures(b40 integer) options (AGGREGATION [b40 = AVG] defaults [a40=test])")
checkAnswer(
sql("select a40,b40 from cube_restructure22 limit 1"),
Seq(Row("test",0.0)))
sql("drop cube cube_restructure22")
}

//TC_1218
test("TC_1218") {
sql("CREATE CUBE cube_restructure23 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure23 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure23 add dimensions(a40 string) measures(b40 integer) options (AGGREGATION [b40 = AVG] defaults [a40=test])")
sql("alter cube cube_restructure23 drop (b40)")
checkAnswer(
sql("select * from cube_restructure23 limit 1"),
Seq(Row("1AA1","test",2738.0)))
sql("drop cube cube_restructure23")
}

//TC_1219
test("TC_1219") {
sql("CREATE CUBE cube_restructure24 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure24 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure24 add dimensions(a40 string) measures(b40 integer) options (AGGREGATION [b40 = AVG] defaults [a40=test])")
sql("alter cube cube_restructure24 drop (a40)")
checkAnswer(
sql("select * from cube_restructure24 limit 1"),
Seq(Row("1AA1",2738.0,0.0)))
sql("drop cube cube_restructure24")
}

//TC_1220
test("TC_1220") {
sql("CREATE CUBE cube_restructure25 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure25 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure25 add dimensions(a40 string) measures(b40 integer) options (AGGREGATION [b40 = AVG] defaults [a40=test])")
sql("alter cube cube_restructure25 drop (a40,b40)")
checkAnswer(
sql("select * from cube_restructure25 limit 1"),
Seq(Row("1AA1",2738.0)))
sql("drop cube cube_restructure25")
}

//TC_1223
test("TC_1223") {
sql("CREATE schema IF NOT EXISTS  res")
sql("CREATE CUBE res.cube_restructure27 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE res.cube_restructure27 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube res.cube_restructure27 add dimensions(a40 string) measures(b40 integer) options (AGGREGATION [b40 = AVG] defaults [a40=test])")
checkAnswer(
sql("select a40,b40 from res.cube_restructure27 limit 1"),
Seq(Row("test",0.0)))
sql("drop cube res.cube_restructure27")
}

//TC_1224
test("TC_1224") {
sql("CREATE CUBE res.cube_restructure29 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE res.cube_restructure29 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube res.cube_restructure29 add dimensions(a40 string) measures(b40 integer) options (AGGREGATION [b40 = AVG] defaults [a40=test])")
sql("alter cube res.cube_restructure29 drop (a40,b40)")
checkAnswer(
sql("select a0,b0 from res.cube_restructure29 limit 1"),
Seq(Row("1AA1",2738.0)))
sql("drop cube res.cube_restructure29")
}

//TC_1225
test("TC_1225") {
sql("CREATE CUBE cube_restructure_alias30 DIMENSIONS (a0 STRING as alias1) MEASURES (b0 INTEGER as alias2) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (alias1) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure_alias30 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure_alias30 add dimensions(a string as alias3) measures(b integer as alias4) options (AGGREGATION [alias4 = SUM])")
checkAnswer(
sql("select * from cube_restructure_alias30 limit 1"),
Seq(Row("1AA1",null,2738.0,0.0)))
sql("drop cube cube_restructure_alias30")
}

//TC_1226
test("TC_1226") {
sql("CREATE CUBE cube_restructure_alias31 DIMENSIONS (a0 STRING as alias1) MEASURES (b0 INTEGER as alias2) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (alias1) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure_alias31 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure_alias31 add dimensions(a1 string as alias5) measures(b integer as alias6) options (AGGREGATION [alias6 = COUNT])")
checkAnswer(
sql("select * from cube_restructure_alias31 limit 1"),
Seq(Row("1AA1",null,2738.0,0.0)))
sql("drop cube cube_restructure_alias31")
}

//TC_1227
test("TC_1227") {
sql("CREATE CUBE cube_restructure_alias32 DIMENSIONS (a0 STRING as alias1) MEASURES (b0 INTEGER as alias2) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (alias1) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure_alias32 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure_alias32 add dimensions(a1 string as alias7) measures(b integer as alias8) options (AGGREGATION [alias8 = MIN])")
checkAnswer(
sql("select * from cube_restructure_alias32 limit 1"),
Seq(Row("1AA1",null,2738.0,0.0)))
sql("drop cube cube_restructure_alias32")
}

//TC_1228
test("TC_1228") {
sql("CREATE CUBE cube_restructure_alias33 DIMENSIONS (a0 STRING as alias1) MEASURES (b0 INTEGER as alias2) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (alias1) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure_alias33 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure_alias33 add dimensions(a1 string as alias9) measures(b integer as alias10) options (AGGREGATION [alias10 = MAX])")
checkAnswer(
sql("select * from cube_restructure_alias33 limit 1"),
Seq(Row("1AA1",null,2738.0,0.0)))
sql("drop cube cube_restructure_alias33")
}

//TC_1229
test("TC_1229") {
sql("CREATE CUBE cube_restructure_alias34 DIMENSIONS (a0 STRING as alias1) MEASURES (b0 INTEGER as alias2) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (alias1) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure_alias34 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure_alias34 add dimensions(a1 string as alias11) measures(b integer as alias12) options (AGGREGATION [alias12 = AVG])")
checkAnswer(
sql("select * from cube_restructure_alias34 limit 1"),
Seq(Row("1AA1",null,2738.0,0.0)))
sql("drop cube cube_restructure_alias34")
}

//TC_1230
test("TC_1230") {
sql("CREATE CUBE cube_restructure_alias35 DIMENSIONS (a0 STRING as alias1) MEASURES (b0 INTEGER as alias2) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (alias1) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure_alias35 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure_alias35 add dimensions(a1 string as alias13) measures(b1 integer as alias14) options (defaults [alias14=test])")
checkAnswer(
sql("select * from cube_restructure_alias35 limit 1"),
Seq(Row("1AA1",null,2738.0,0.0)))
sql("drop cube cube_restructure_alias35")
}

//TC_1231
test("TC_1231") {
sql("CREATE CUBE cube_restructure_alias36 DIMENSIONS (a0 STRING as alias1) MEASURES (b0 INTEGER as alias2) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (alias1) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure_alias36 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure_alias36 add dimensions(a1 string as alias15) measures(b1 integer as alias16) options (defaults [alias15=test])")
checkAnswer(
sql("select * from cube_restructure_alias36 limit 1"),
Seq(Row("1AA1","test",2738.0,0.0)))
sql("drop cube cube_restructure_alias36")
}

//TC_1232
test("TC_1232") {
sql("CREATE CUBE cube_restructure_alias37 DIMENSIONS (a0 STRING as alias1) MEASURES (b0 INTEGER as alias2) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (alias1) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure_alias37 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure_alias37 add dimensions(a1 string as alias15) measures(b1 integer as alias16) options (defaults [alias15=test])")
sql("alter cube cube_restructure_alias37 drop (alias15)")
checkAnswer(
sql("select * from cube_restructure_alias37 limit 1"),
Seq(Row("1AA1",2738.0,0.0)))
sql("drop cube cube_restructure_alias37")
}

//TC_1233
test("TC_1233") {
sql("CREATE CUBE cube_restructure_alias38 DIMENSIONS (a0 STRING as alias1) MEASURES (b0 INTEGER as alias2) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (alias1) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure_alias38 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure_alias38 add dimensions(a1 string as alias15) measures(b1 integer as alias16) options (defaults [alias15=test])")
sql("alter cube cube_restructure_alias38 drop (alias16)")
checkAnswer(
sql("select * from cube_restructure_alias38 limit 1"),
Seq(Row("1AA1","test",2738.0)))
sql("drop cube cube_restructure_alias38")
}

//TC_1234
test("TC_1234") {
sql("CREATE CUBE cube_restructure_alias39 DIMENSIONS (a0 STRING as alias1) MEASURES (b0 INTEGER as alias2) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (alias1) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure_alias39 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure_alias39 add dimensions(a1 string as alias15) measures(b1 integer as alias16) options (defaults [alias15=test])")
sql("alter cube cube_restructure_alias39 drop (alias15,alias16)")
checkAnswer(
sql("select * from cube_restructure_alias39 limit 1"),
Seq(Row("1AA1",2738.0)))
sql("drop cube cube_restructure_alias39")
}

//TC_1235
test("TC_1235") {
sql("CREATE CUBE cube_restructure40 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure40 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure40 add dimensions(a42 timestamp)")
checkAnswer(
sql("select * from cube_restructure40 limit 1"),
Seq(Row("1AA1",null,2738.0)))
sql("drop cube cube_restructure40")
}

//TC_1236
test("TC_1236") {
sql("CREATE CUBE cube_restructure41 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure41 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure41 add dimensions(a43 integer)")
checkAnswer(
sql("select * from cube_restructure41 limit 1"),
Seq(Row("1AA1",null,2738.0)))
sql("drop cube cube_restructure41")
}

//TC_1237
test("TC_1237") {
sql("CREATE CUBE cube_restructure42 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure42 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure42 add measures(b42 numeric)")
checkAnswer(
sql("select * from cube_restructure42 limit 1"),
Seq(Row("1AA1",2738.0,0.0)))
sql("drop cube cube_restructure42")
}

//TC_1238
test("TC_1238") {
sql("CREATE CUBE cube_restructure43 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure43 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure43 add dimensions(a43 string) measures(b43 integer)")
sql("refresh table cube_restructure43")
checkAnswer(
sql("select * from cube_restructure43 limit 1"),
Seq(Row("1AA1",null,2738.0,0.0)))
sql("drop cube cube_restructure43")
}

//TC_1239
test("TC_1239") {
sql("CREATE CUBE cube_restructure44 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure44 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure44 add dimensions(a44 string) measures(b44 integer)")
sql("alter cube cube_restructure44 drop(a44,b44)")
sql("refresh table cube_restructure44")
checkAnswer(
sql("select * from cube_restructure44 limit 1"),
Seq(Row("1AA1",2738.0)))
sql("drop cube cube_restructure44")
}

//TC_1240
test("TC_1240") {
sql("CREATE CUBE cube_restructure45 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("alter cube cube_restructure45 add dimensions(a45 string) measures(b45 integer)")
checkAnswer(
sql("select * from cube_restructure45 limit 1"),
Seq())
sql("drop cube cube_restructure45")
}

//TC_1241
test("TC_1241") {
sql("CREATE CUBE cube_restructure46 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("alter cube cube_restructure46 add dimensions(a46 string) measures(b46 integer)")
sql("alter cube cube_restructure46 drop(a46,b46)")
checkAnswer(
sql("select * from cube_restructure46 limit 1"),
Seq())
sql("drop cube cube_restructure46")
}

//TC_1243
test("TC_1243") {
sql("CREATE CUBE cube_restructure48 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("alter cube cube_restructure48 add dimensions(a47) measures(b47)")
checkAnswer(
sql("select * from cube_restructure48 limit 1"),
Seq())
sql("drop cube cube_restructure48")
}

//DTS2015103009592
test("DTS2015103009592") {
sql("CREATE CUBE cube_restructure55 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure55 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("desc cube_restructure55")
sql("alter cube cube_restructure55 drop (b0)")
checkAnswer(
sql("select * from cube_restructure55 limit 1"),
Seq(Row("1AA1")))
sql("drop cube cube_restructure55")
}

//DTS2015111309941
test("DTS2015111309941") {
sql("CREATE CUBE cube_restructure56 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("alter cube cube_restructure56 add dimensions(a18 string) measures(b18 numeric) options (defaults [b18=10])")
checkAnswer(
sql("select * from cube_restructure56 limit 1"),
Seq())
sql("drop cube cube_restructure56")
}

//DTS2015112509895
test("DTS2015112509895") {
sql("CREATE CUBE cube_restructure57 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure57 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure57 add dimensions(a40)")
checkAnswer(
sql("select a0 from cube_restructure57 where a40 is NULL limit 1"),
Seq(Row("1AA1")))
sql("drop cube cube_restructure57")
}

//DTS2015112610913_01
test("DTS2015112610913_01") {
sql("CREATE CUBE cube_restructure58 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure58 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"', FILEHEADER 'a0,b0')")
sql("alter cube cube_restructure58 add dimensions(a12 string) measures(b11 integer)")
checkAnswer(
sql("select a0 from cube_restructure58 limit 1"),
Seq(Row("2738")))
sql("drop cube cube_restructure58")
}

//DTS2015112610913_02
test("DTS2015112610913_02") {
sql("CREATE CUBE cube_restructure59 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure59 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"', FILEHEADER 'a0,b0')")
sql("alter cube cube_restructure59 add dimensions(a12 string) measures(b11 integer)")
sql("alter cube cube_restructure59 add dimensions(a13 string) measures(b13 integer)")
checkAnswer(
sql("select a0 from cube_restructure59 order by a0 limit 1"),
Seq(Row("1015")))
sql("drop cube cube_restructure59")
}

//DTS2015120209187
test("DTS2015120209187") {
sql("CREATE cube t1 dimensions(imei string,deviceInformationId integer,mac string,age integer,productdate timestamp,updatetime timestamp) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("CREATE CUBE t15 DIMENSIONS (imei String, InformationId integer) MEASURES (pointid Numeric, contractnumber Numeric) WITH t1 RELATION (FACT.imei=imei) INCLUDE (imei)")
sql("ALTER CUBE t15 ADD DIMENSIONS (mac string,age integer) with t1 relation (FACT.imei = imei) include (imei, mac, age)")
checkAnswer(
sql("select imei from t15 limit 1"),
Seq())
sql("drop cube t1")
}

//DTS2015112509746
test("DTS2015112509746") {
sql("CREATE cube cube_restructure61 dimensions(AMSize STRING) measures(deviceInformationId integer) OPTIONS (AGGREGATION [deviceInformationId=count])")
sql("LOAD DATA FACT FROM './src/test/resources/TestData1.csv' INTO Cube cube_restructure61 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("alter cube cube_restructure61 add dimensions(a1 string) measures (b1 integer)")
checkAnswer(
sql("select AMSize from cube_restructure61 limit 1"),
Seq(Row("8RAM size")))
sql("drop cube cube_restructure61")
}

//DTS2015110209906
test("DTS2015110209906") {
sql("CREATE CUBE cube_restructure63 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure63 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("alter cube cube_restructure63 add dimensions(a10 string) measures(b9 integer) options (AGGREGATION [b9 = MAX])")
sql("alter cube cube_restructure63 add dimensions(a11 string) measures(b10 integer) options (AGGREGATION [b10 = AVG])")
sql("select * from cube_restructure63 limit 10")
sql("refresh table cube_restructure63")
sql("select * from cube_restructure63 limit 10")
sql("alter cube cube_restructure63 add dimensions(a12 string) measures(b11 integer) options (AGGREGATION [b10 = AVG])")
sql("drop cube cube_restructure63")
sql("CREATE CUBE cube_restructure63 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure63 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("refresh table cube_restructure63")
sql("select * from cube_restructure63 limit 10")
sql("alter cube cube_restructure63 add dimensions(a string)")
checkAnswer(
sql("select a0 from cube_restructure63 limit 1"),
Seq(Row("1AA1")))
sql("drop cube cube_restructure63")
}

//DTS2015110300006
test("DTS2015110300006") {
sql("CREATE CUBE cube_restructure64 DIMENSIONS (a0 STRING as alias1) MEASURES (b0 INTEGER as alias2) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (alias1) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure64 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')")
sql("select count(*) from cube_restructure64")
sql("alter cube cube_restructure64 add dimensions(a string as alias3) measures(b integer as alias4) options (AGGREGATION [alias4 = SUM])")
sql("select count(*) from cube_restructure64")
sql("alter cube cube_restructure64 add dimensions(a1 string as alias5) measures(b111 integer as alias6) options (AGGREGATION [alias6 = COUNT])")
checkAnswer(
sql("select count(*) from cube_restructure64"),
Seq(Row(100)))
sql("drop cube cube_restructure64")
}

//DTS2015120405916
test("DTS2015120405916") {
sql("CREATE cube vardhan1 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData1.csv' INTO Cube vardhan1 OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("alter cube vardhan1 drop (deviceInformationId)")
sql("alter cube vardhan1 add  measures(deviceInformationId integer) options ()")
checkAnswer(
sql("SELECT imei FROM vardhan1 order by imei LIMIT 1"),
Seq(Row("1AA1")))
sql("drop cube vardhan1")
}

//DTS2015120405916_1
test("DTS2015120405916_1") {
sql("CREATE cube vardhan1 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData1.csv' INTO Cube vardhan1 OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("alter cube vardhan1 drop (ActiveCountry)")
sql("alter cube vardhan1 add  dimensions(ActiveCountry string) options ()")
checkAnswer(
sql("SELECT channelsId FROM vardhan1 order by imei LIMIT 1"),
Seq(Row("4")))
sql("drop cube vardhan1")
}

//DTS2015110309063
test("DTS2015110309063") {
sql("CREATE cube vardhan dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData1.csv' INTO Cube vardhan OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("alter cube vardhan  add dimensions(alreadID)")
checkAnswer(
sql("select count(*) from vardhan where alreadID is null"),
Seq(Row(100)))
sql("drop cube vardhan")
}

//TC_1333
test("TC_1333") {
sql("CREATE cube vardhanretention01 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string as prsntcntry, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer as deviceid) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention01 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("alter cube vardhanretention01 drop (prsntcntry)")
checkAnswer(
sql("select imei from vardhanretention01 order by imei desc limit 1"),
Seq(Row("1AA100084")))
sql("drop cube vardhanretention01")
}

//TC_1334
test("TC_1334") {
sql("CREATE cube vardhanretention dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("alter cube vardhanretention drop(AMSize) add dimensions(AMSize string)")
checkAnswer(
sql("select count(*) from vardhanretention"),
Seq(Row(100)))
sql("drop cube vardhanretention")
}

//TC_1335
test("TC_1335") {
sql("CREATE cube vardhanretention dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("alter cube vardhanretention drop(AMSize) add dimensions(AMSize string)")
checkAnswer(
sql("select imei from vardhanretention where imei='1AA100'"),
Seq(Row("1AA1",0.0)))
sql("drop cube vardhanretention")
}

//TC_1199
test("TC_1199") {
try
{
sql("CREATE CUBE cube_restructure4 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure4 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')\"")
sql("alter cube cube_restructure4 add dimensions(a2 string) options (AGGREGATION [a2 = SUM])")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube_restructure4")
 
}
}

//TC_1201
test("TC_1201") {
try
{
sql("CREATE CUBE cube_restructure6 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure6 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')\"")
sql("alter cube cube_restructure6 add dimensions(a4 string, a5 integer, a3 timestamp) measures(b3 integer, b4 double) options (AGGREGATION [b3 = SUM])")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube_restructure6")
 
}
}

//TC_1202
test("TC_1202") {
try
{
sql("CREATE CUBE cube_restructure7 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure7 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')\"")
sql("alter cube cube_restructure7 add dimensions(a6 double,a7 long)")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube_restructure7")
 
}
}

//TC_1203
test("TC_1203") {
try
{
sql("CREATE CUBE cube_restructure8 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure8 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')\"")
sql("alter cube cube_restructure add measures(b5 string, b6 timestamp)")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube_restructure8")
 
}
}

//TC_1215
test("TC_1215") {
try
{
sql("CREATE CUBE cube_restructure20 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure20 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')\"")
sql("alter cube cube_restructure20 add dimensions(a22 string, a23 integer, a24 timestamp) measures(b18 integer, b19 double) options (defaults [\"a22=test\",\"a23=test1\",\"a24=27-04-1979\",\"b18=10\",\"b19=10\"])")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube_restructure20")
 
}
}

//TC_1216
test("TC_1216") {
try
{
sql("CREATE CUBE cube_restructure21 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure21 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')\"")
sql("alter cube cube_restructure21 add dimensions(a25 string, a26 integer, a27 timestamp) measures(b20 integer, b21 double) options (AGGREGATION [b20 = COUNT]) (defaults [a25=10])")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube_restructure21")
 
}
}

//TC_1221
test("TC_1221") {
try
{
sql("CREATE CUBE cube_restructure26 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure26 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')\"")
sql("alter cube add dimensions(a36 string, a37 integer, a38 timestamp) measures(b37 integer, b38 double) options (AGGREGATION [\"b37 = COUNT\"]) (defaults [\"a36=10\"])")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube_restructure26")
 
}
}

//TC_1222
test("TC_1222") {
try
{
sql("alter cube invalidcube add dimensions(a36 string, a37 integer, a38 timestamp) measures(b37 integer, b38 double) options (AGGREGATION [\"b37 = COUNT\"]) (defaults [\"a36=10\"])")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>
 
}
}

//TC_1242
test("TC_1242") {
try
{
sql("CREATE CUBE cube_restructure47 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("alter cube cube_restructure47 add dimensions(a0 string) measures(b0 integer)")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube_restructure47")
 
}
}

//TC_1244
test("TC_1244") {
try
{
sql("CREATE CUBE cube_restructure49 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("alter cube cube_restructure49 drop(a1,b1)")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube_restructure49")
 
}
}

//TC_1245
test("TC_1245") {
try
{
sql("CREATE CUBE cube_restructure50 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("alter cube cube_restructure50 drop()")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube_restructure50")
 
}
}

//DTS2015102211467_01
test("DTS2015102211467_01") {
try
{
sql("CREATE CUBE cube_restructure51 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("alter cube cube_restructure51 DROP (b0)")
sql("alter cube cube_restructure51 DROP (b0)")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube_restructure51")
 
}
}

//DTS2015102211467_02
test("DTS2015102211467_02") {
try
{
sql("CREATE CUBE cube_restructure52 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("alter cube cube_restructure52 DROP (b1)")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube_restructure52")
 
}
}

//DTS2015102211467_03
test("DTS2015102211467_03") {
try
{
sql("CREATE CUBE cube_restructure53 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("alter cube cube_restructure53 DROP ()")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube_restructure53")
 
}
}

//DTS2015102211545
test("DTS2015102211545") {
try
{
sql("CREATE CUBE cube_restructure54 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("alter cube cube_restructure54 DROP (a0)")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube_restructure54")
 
}
}

//DTS2015120502719_01
test("DTS2015120502719_01") {
try
{
sql("CREATE schema IF NOT EXISTS test1")
sql("create cube test1.t5 dimensions(imei string, productdate timestamp,updatetime timestamp) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("create cube t5 dimensions(imei string, productdate timestamp,updatetime timestamp) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )\"")
sql("alter cube t5 add dimensions (name string,age integer) measures(sales integer) with test1 relation (FACT.name = imei) include (mac)")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube test1")
 sql("drop cube t5")
 sql("drop schema test1")

 
}
}

//DTS2015120502719_02
test("DTS2015120502719_02") {
try
{
sql("CREATE schema IF NOT EXISTS test1")
sql("create cube test1.t6 dimensions(imei string, productdate timestamp,updatetime timestamp) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/test12.csv' INTO CUBE test1.t6 PARTITIONDATA (DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,productdate,updatetime,gamePointId,contractNumber')")
sql("create cube t6 dimensions(imei string, productdate timestamp,updatetime timestamp) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/test12.csv' INTO CUBE t6 PARTITIONDATA (DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,productdate,updatetime,gamePointId,contractNumber')\"")
sql("alter cube t6 add dimensions (name string,age integer) measures(sales integer) with test1 relation (FACT.name = imei) include (mac)")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>
 sql("drop cube  test1.t6")
 sql("drop cube t6")
 sql("drop schema test1")

 
}
}

//DTS2015120502719
test("DTS2015120502719") {
try
{
sql("CREATE schema test")
sql("create cube vardhan dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("create cube test.vardhan dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData1.csv' INTO Cube vardhan OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("LOAD DATA FACT FROM './src/test/resources/TestData1.csv' INTO Cube test.vardhan OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId') \"")
sql("alter cube vardhan add dimensions (name string,age integer) measures(sales integer) with test1 relation (FACT.name = imei) include (mac)")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhan")
 sql("drop cube test.vardhan")
 sql("drop schema test")

}
}

//DTS2015121710412
test("DTS2015121710412") {
try
{
sql("CREATE schema test")
sql("create cube vardhan dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("create cube test.vardhan dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData1.csv' INTO Cube vardhan OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("LOAD DATA FACT FROM './src/test/resources/TestData1.csv' INTO Cube test.vardhan OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId') \"")
sql("alter cube vardhan add dimensions (name string,age integer) measures(sales integer) with test1 relation (FACT.name = imei) include (name)")
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhan")
 sql("drop schema test")
 
}
}

  
//TC_1246
test("TC_1246") {
sql("create cube cube1 DIMENSIONS (imei string,deviceInformationId integer,MAC string,deviceColor string, device_backColor string,modelId string, marketName string, AMSize string, ROMSize string, CUPAudit string, CPIClocked string, series string, productionDate string, bomCode string, internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince  string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict  string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR  integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) MEASURES (gamePointId numeric,contractNumber numeric) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO CUBE cube1 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
checkAnswer(
sql("show loads for cube cube1"),
Seq(Row("0","Success","2015-11-05 14:27:10.0"," 2015-11-05 14:27:10.0")))
sql("drop cube cube1")
}

//TC_1247
test("TC_1247") {
  
  try
  {
sql("create schema IF NOT EXISTS myschema1")
sql("create cube myschema1.cube2 DIMENSIONS (imei string,deviceInformationId integer,MAC string,deviceColor string, device_backColor string,modelId string, marketName string, AMSize string, ROMSize string, CUPAudit string, CPIClocked string, series string, productionDate string, bomCode string, internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince  string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict  string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR  integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) MEASURES (gamePointId numeric,contractNumber numeric) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO CUBE myschema1.cube2 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
checkAnswer(
sql("SHOW LOADS for cube myschema1.cube2"),
Seq(Row("0","Success","2015-11-05 15:01:23.0"," 2015-11-05 15:01:26.0")))
}
catch
{
 case ex: Throwable =>sql("drop cube myschema1.cube2")
 
}

}

//TC_1248
test("TC_1248") {
sql("create cube cube3 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube3 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("show loads for cube cube3"),
Seq(Row("0","Success","2015-11-05 15:15:42.0"," 2015-11-05 15:15:43.0")))
sql("drop cube cube3")
}

//TC_1249
test("TC_1249") {
sql("create cube myschema1.cube4 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube myschema1.cube4 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("SHOW LOADS for cube myschema1.cube4"),
Seq(Row("0","Success","2015-11-05 15:23:02.0"," 2015-11-05 15:23:03.0")))
sql("drop cube myschema1.cube4")
}

//TC_1250
test("TC_1250") {
sql("create cube cube5 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei,AMSize) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube5 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("show loads for cube cube5"),
Seq(Row("0","Success","2015-11-05 15:23:02.0"," 2015-11-05 15:23:03.0")))
sql("drop cube cube5")
}

//TC_1251
test("TC_1251") {
sql("create cube cube6 dimensions(AMSize STRING) measures(deviceInformationId integer) OPTIONS (AGGREGATION [deviceInformationId=count])")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube6 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("show loads for cube cube6"),
Seq(Row("0","Success","2015-11-05 15:23:02.0"," 2015-11-05 15:23:03.0")))
sql("drop cube cube6")
}

//TC_1252
test("TC_1252") {
sql("create cube cube7 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
checkAnswer(
sql("show loads for cube cube7"),
Seq())
sql("drop cube cube7")
}

//TC_1253
test("TC_1253") {
sql("create cube cube123 DIMENSIONS (imei string,deviceInformationId integer,MAC string,deviceColor string, device_backColor string,modelId string, marketName string, AMSize string, ROMSize string, CUPAudit string, CPIClocked string, series string, productionDate string, bomCode string, internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince  string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict  string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR  integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) MEASURES (gamePointId numeric,contractNumber numeric) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (imei) ,PARTITION_COUNT=2] )")
checkAnswer(
sql("show loads for cube cube123"),
Seq())
sql("drop cube cube123")
}

//TC_1254
test("TC_1254") {
sql("create cube cube9 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube9 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube9 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("show loads for cube cube9"),
Seq(Row("0","Success","2015-11-05 17:43:21.0"," 2015-11-05 17:43:22.0"),Row("1","Success","2015-11-05 17:43:43.0"," 2015-11-05 17:43:44.0")))
sql("drop cube cube9")
}

//TC_1255
test("TC_1255") {
try
{
sql("create cube cube10 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
checkAnswer(
sql("show loads for cube cubedoesnotexist"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube10")
 
}
}

//TC_1256
test("TC_1256") {
try
{
sql("create cube cube11 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
checkAnswer(
sql("show loads for cube"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube11")
 
}
}

//TC_1257
test("TC_1257") {
sql("create cube cube12 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
checkAnswer(
sql("show loads for cube cube12"),
Seq())
sql("drop cube cube12")
}

//TC_1258
test("TC_1258") {
sql("create cube cube13 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube13 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("sHOw LoaDs for cube cube13"),
Seq(Row("0","Success","2015-11-05 18:09:40.0"," 2015-11-05 18:09:41.0")))
sql("drop cube cube13")
}

//DTS2015112006803_02
test("DTS2015112006803_02") {
sql("create cube cube14 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube14 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("select * from cube14")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube14 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("select * from cube14")
checkAnswer(
sql("show loads for cube cube14"),
Seq(Row("1","Success","2015-11-05 17:43:21.0"," 2015-11-05 17:43:22.0"),Row("0","Success","2015-11-05 17:43:43.0"," 2015-11-05 17:43:44.0")))
sql("drop cube cube14")
}

//DTS2015110901347
test("DTS2015110901347") {
sql("create cube cube15 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube15 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube15 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube15 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube15 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
checkAnswer(
sql("show loads for cube cube15"),
Seq(Row("3","Success","2015-11-05 17:43:21.0"," 2015-11-05 17:43:22.0"),Row("2","Success","2015-11-05 17:43:21.0"," 2015-11-05 17:43:22.0"),Row("1","Success","2015-11-05 17:43:21.0"," 2015-11-05 17:43:22.0"),Row("0","Success","2015-11-05 17:43:43.0"," 2015-11-05 17:43:44.0")))
sql("drop cube cube15")
}

//DTS2015121707872
test("DTS2015121707872") {
sql("create cube t202 dimensions(imei string,deviceInformationId integer,mac string,productdate timestamp,updatetime timestamp) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/test1t.csv' INTO CUBE t202 OPTIONS (DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,mac,productdate,updatetime,gamePointId,contractNumber')")
checkAnswer(
sql("show loads for cube t202"),
Seq())
sql("drop cube t202")
}

//TC_1336
test("TC_1336") {
sql("create cube vardhanretention123 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("alter cube vardhanretention123 drop(AMSize) add dimensions(AMSize string)")
checkAnswer(
sql("show loads for cube vardhanretention123"),
Seq())
sql("drop cube vardhanretention123")
}



  
  //TC_1271
test("TC_1271") {
sql("create cube cube123 DIMENSIONS (imei string,deviceInformationId integer,MAC string,deviceColor string, device_backColor string,modelId string, marketName string, AMSize string, ROMSize string, CUPAudit string, CPIClocked string, series string, productionDate string, bomCode string, internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince  string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict  string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR  integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) MEASURES (gamePointId numeric,contractNumber numeric) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO CUBE cube123 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
sql("create aggregatetable Latest_YEAR,count(contractNumber)from cube cube123")
checkAnswer(
sql("select contractNumber from cube123 where contractNumber=5281803.0"),
Seq(Row(5281803.0)))
sql("drop cube cube123")
}

//TC_1272
test("TC_1272") {
  try
  {
sql("create schema IF NOT EXISTS  myschema1")
sql("create cube myschema1.cube2 DIMENSIONS (imei string,deviceInformationId integer,MAC string,deviceColor string, device_backColor string,modelId string, marketName string, AMSize string, ROMSize string, CUPAudit string, CPIClocked string, series string, productionDate string, bomCode string, internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince  string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict  string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR  integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) MEASURES (gamePointId numeric,contractNumber numeric) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO CUBE myschema1.cube2 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
sql("create aggregatetable Latest_DAY,sum(contractNumber)from cube myschema1.cube2")
checkAnswer(
sql("select Latest_DAY from myschema1.cube2 limit 1"),
Seq(Row(1)))
  }
  catch
{
 case ex: Throwable =>sql("drop cube myschema1.cube2")
 
}

}

//TC_1273
test("TC_1273") {
try
{
sql("create cube cube3 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube3 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("create aggregatetable Latest_MONTH,sum(contractNumber)from cube cube3")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube3")
 
}
}

//TC_1274
test("TC_1274") {
try
{
sql("create cube myschema1.cube4 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube myschema1.cube4 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId') \"")
sql("create aggregatetable Latest_DAY,sum(contractNumber)from cube myschema1.cube4")

fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube myschema1.cube4")
 
}
}

//TC_1275
test("TC_1275") {
sql("create cube cube5 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei,AMSize) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube5 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("create aggregatetable imei,sum(deviceInformationId)from cube cube5")
checkAnswer(
sql("select imei from cube5 where imei=\"1AA1\""),
Seq(Row("1AA1")))
sql("drop cube cube5")
}

//TC_1276
test("TC_1276") {
sql("create cube cube6 dimensions(AMSize STRING) measures(deviceInformationId integer) OPTIONS (AGGREGATION [deviceInformationId=count])")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube6 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("create aggregatetable  AMSize,sum(deviceInformationId) from cube cube6")
checkAnswer(
sql("select AMSize from cube6 limit 1"),
Seq(Row("8RAM size")))
sql("drop cube cube6")
}

//TC_1277
test("TC_1277") {
sql("create cube cube7 dimensions(AMSize STRING) measures(deviceInformationId integer) OPTIONS (AGGREGATION [deviceInformationId=count])")
sql("create aggregatetable AMSize,sum(deviceInformationId) from cube cube7")
checkAnswer(
sql("select AMSize from cube7"),
Seq())
sql("drop cube cube7")
}

//TC_1278
test("TC_1278") {
sql("create cube cube8 dimensions(AMSize STRING) measures(deviceInformationId integer) OPTIONS (AGGREGATION [deviceInformationId=count])")
sql("alter cube cube8 add dimensions(a1 string) measures (b1 integer)")
sql("create aggregatetable AMSize,sum(deviceInformationId) from cube cube8")
checkAnswer(
sql("select deviceInformationId from cube8"),
Seq())
sql("drop cube cube8")
}

//TC_1279
test("TC_1279") {
sql("create cube cube9 dimensions(AMSize STRING) measures(deviceInformationId integer) OPTIONS (AGGREGATION [deviceInformationId=count])")
sql("alter cube cube9 add dimensions(a1 string) measures (b1 integer)")
sql("alter cube cube9 drop(a1,b1)")
sql("create aggregatetable AMSize,sum(deviceInformationId) from cube cube9")
checkAnswer(
sql("select AMSize from cube9"),
Seq())
sql("drop cube cube9")
}

//TC_1280
test("TC_1280") {
sql("create cube cube10 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube10 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("alter cube cube10 add dimensions(a1 string) measures (b1 integer)")
sql("alter cube cube10 drop(a1,b1)")
sql("create aggregatetable imei,sum(deviceInformationId)from cube cube10")
checkAnswer(
sql("select imei from cube10 where imei=\"1AA1\""),
Seq(Row("1AA1")))
sql("drop cube cube10")
}

//TC_1281
test("TC_1281") {
sql("create cube cube11 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube11 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("alter cube cube11 add dimensions(a1 string) measures (b1 integer)")
sql("alter cube cube11 drop(a1,b1)")
sql("create aggregatetable AMSize,sum(gamePointId)from cube cube11")
checkAnswer(
sql("select imei from cube11  where imei=\"1AA1\""),
Seq(Row("1AA1")))
sql("drop cube cube11")
}

//TC_1282
test("TC_1282") {
sql("create cube cube12 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube12 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube12 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("create aggregatetable channelsId,sum(gamePointId)from cube cube12")
checkAnswer(
sql("select channelsId from cube12  where channelsId=\"4\" limit 1"),
Seq(Row(4)))
sql("drop cube cube12")
}

//TC_1283
test("TC_1283") {
sql("create cube cube13 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube13 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube13 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("create aggregatetable imei,max(deviceInformationId)from cube cube13")
checkAnswer(
sql("select imei from cube13 where imei=\"1AA100001\" limit 1"),
Seq(Row("1AA100001")))
sql("drop cube cube13")
}

//TC_1284
test("TC_1284") {
sql("create cube cube14 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube14 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("alter cube cube14 add dimensions(a1 string) measures (b1 integer)")
sql("alter cube cube14 drop(a1,b1)")
sql("create aggregatetable channelsId,min(deviceInformationId)from cube cube14")
checkAnswer(
sql("select channelsId from cube14 where channelsId=\"4\" limit 1"),
Seq(Row(4)))
sql("drop cube cube14")
}

//TC_1285
test("TC_1285") {
sql("create cube cube15 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("alter cube cube15 add dimensions(a1 string) measures(b1 integer)")
sql("alter cube cube15 add dimensions(a2 string) measures(b2 integer)")
sql("create aggregatetable channelsId,max(deviceInformationId)from cube cube15")
checkAnswer(
sql("select channelsId from cube15"),
Seq())
sql("drop cube cube15")
}

//TC_1286
test("TC_1286") {
sql("create cube cube16 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube16 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube16 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("create aggregatetable channelsId,max(gamePointId)from cube cube16")
checkAnswer(
sql("select gamePointId from cube16 where gamePointId=2738.0 limit 1"),
Seq(Row(2738.0)))
sql("drop cube cube16")
}

//TC_1287
test("TC_1287") {
try
{
sql("create cube cube17 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube17 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("create aggregatetable Latest_Day,max(gamePointId)from cube cube17")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube17")
 
}
}

//TC_1288
test("TC_1288") {
sql("create cube cube18 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube18 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("create aggregatetable ActiveCountry,avg(gamePointId)from cube cube18")
checkAnswer(
sql("select ActiveCountry from cube18 limit 1"),
Seq(Row("Chinese")))
sql("drop cube cube18")
}

//TC_1289
test("TC_1289") {
sql("create cube cube19 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube19 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("CREATE AGGREGATETABLE ActiveCountry,max(deviceInformationId)FROM CUBE cube19")
checkAnswer(
sql("select imei from cube19 where imei=\"1AA1\""),
Seq(Row("1AA1")))
sql("drop cube cube19")
}

//TC_1290
test("TC_1290") {
try
{
sql("create cube cube20 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("create aggregatetable ActiveCountry,max(gamePointId)from cube20")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube20")
 
}
}

//DTS2015112608312
test("DTS2015112608312") {
sql("create cube cube21 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("CREATE AGGREGATETABLE ActiveCountry,max(deviceInformationId),sum(deviceInformationId) FROM CUBE cube21")
checkAnswer(
sql("select deviceInformationId from cube21 limit 1"),
Seq())
sql("drop cube cube21")
}

//DTS2015101505182
test("DTS2015101505182") {
sql("create cube cube22 dimensions(imei string, productdate timestamp) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("CREATE AGGREGATETABLE imei,productdate,max(gamePointId) FROM CUBE cube22")
checkAnswer(
sql("select productdate from cube22"),
Seq())
sql("drop cube cube22")
}

//DTS2015102211549
test("DTS2015102211549") {
sql("create cube cube23 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube23 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("CREATE AGGREGATETABLE imei,max(gamePointId) FROM CUBE cube23")
checkAnswer(
sql("select imei,max(gamePointId) FROM cube23 where imei=\"1AA10006\" group by imei"),
Seq(Row("1AA10006",2478.0)))
sql("drop cube cube23")
}

//DTS2015102309588_01
test("DTS2015102309588_01") {
sql("create cube cube24 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube24 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("CREATE AGGREGATETABLE imei,sum(distinct gamePointId) FROM CUBE cube24")
checkAnswer(
sql("select imei,sum(distinct gamePointId) FROM cube24 where imei=\"1AA10006\" group by imei limit 1"),
Seq(Row("1AA10006",2478.0)))
sql("drop cube cube24")
}

//DTS2015102309588_02
test("DTS2015102309588_02") {
sql("create cube cube25 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube25 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("CREATE AGGREGATETABLE imei,count(distinct gamePointId) FROM CUBE cube25")
checkAnswer(
sql("select count(imei),count(distinct gamePointId) FROM cube25 group by imei limit 1"),
Seq(Row(1,1)))
sql("drop cube cube25")
}

//DTS2015102309611
test("DTS2015102309611") {
sql("create cube cube26 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube26 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("create aggregatetable ActiveCountry,avg(gamePointId)from cube cube26")
checkAnswer(
sql("select avg(gamePointId)from cube26  limit 1"),
Seq(Row(1574.52)))
sql("drop cube cube26")
}

//DTS2015102400015
test("DTS2015102400015") {
sql("create cube cube27 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube27 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("create aggregatetable ActiveCountry,avg(gamePointId)from cube cube27")
sql("drop cube cube27")
sql("create cube cube27 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube27 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("create aggregatetable ActiveCountry,avg(gamePointId)from cube cube27")
checkAnswer(
sql("select avg(gamePointId)from cube27  limit 1"),
Seq(Row(1574.52)))
sql("drop cube cube27")
}

//DTS2015112707233
test("DTS2015112707233") {
sql("create cube cube28 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube28 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("delete load 0 from cube cube28")
sql("clean files for cube cube28")
sql("CREATE AGGREGATETABLE imei,sum(distinct gamePointId) FROM CUBE cube28")
checkAnswer(
sql("select imei,sum(distinct gamePointId) FROM cube28 group by imei limit 1"),
Seq())
sql("drop cube cube28")
}

//DTS2015113008797_01
test("DTS2015113008797_01") {
sql("create cube cube29 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube29 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("CREATE AGGREGATETABLE imei,sum(distinct gamePointId) FROM CUBE cube29")
checkAnswer(
sql("select imei,deviceInformationId,sum(distinct gamePointId) FROM cube29 where imei=\"1AA100080\" group by imei,deviceInformationId"),
Seq(Row("1AA100080",100080.0,954.0)))
sql("drop cube cube29")
}

//DTS2015113008797_02
test("DTS2015113008797_02") {
sql("create cube cube30 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube30 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("CREATE AGGREGATETABLE IMEI,sum(distinct gamePOIntId) FROM CUBE cube30")
checkAnswer(
sql("select count(imei),count(deviceInformationId),count(distinct gamePointId) FROM cube30 group by imei,deviceInformationId limit 1"),
Seq(Row(1,1,1)))
sql("drop cube cube30")
}

//TC_1338
test("TC_1338") {
sql("create cube vardhanretention dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("alter cube vardhanretention drop(AMSize) add dimensions(AMSize string)")
sql("CREATE AGGREGATETABLE AMSize,sum(gamePointId) FROM cube vardhanretention")
checkAnswer(
sql("select imei from  vardhanretention order by imei desc limit 1"),
Seq(Row("1AA100084")))
sql("drop cube vardhanretention")
}


  
  //TC_1291
test("TC_1291") {
sql("create cube cube1 DIMENSIONS (imei string,deviceInformationId integer,MAC string,deviceColor string, device_backColor string,modelId string, marketName string, AMSize string, ROMSize string, CUPAudit string, CPIClocked string, series string, productionDate string, bomCode string, internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince  string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict  string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR  integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) MEASURES (gamePointId numeric,contractNumber numeric) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO CUBE cube1 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
sql("delete load 0 from  cube cube1")
checkAnswer(
sql("select imei from cube1"),
Seq())
sql("drop cube cube1")
}

//TC_1292
test("TC_1292") {
  try
  {
sql("create cube myschema1.cube2 DIMENSIONS (imei string,deviceInformationId integer,MAC string,deviceColor string, device_backColor string,modelId string, marketName string, AMSize string, ROMSize string, CUPAudit string, CPIClocked string, series string, productionDate string, bomCode string, internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince  string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict  string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR  integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) MEASURES (gamePointId numeric,contractNumber numeric) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO CUBE myschema1.cube2 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
sql("delete load 0 from  cube myschema1.cube2")
checkAnswer(
sql("select deviceInformationId from myschema1.cube2"),
Seq())
  }
catch
{
 case ex: Throwable =>sql("drop cube myschema1.cube2")
 
}

}

//TC_1293
test("TC_1293") {
sql("create cube cube3 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube3 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("delete load 0 from  cube cube3")
checkAnswer(
sql("select gamePointId from cube3"),
Seq())
sql("drop cube cube3")
}

//TC_1294
test("TC_1294") {
sql("create cube myschema1.cube4 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube myschema1.cube4 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("delete load 0 from  cube myschema1.cube4")
checkAnswer(
sql("select channelsId from myschema1.cube4"),
Seq())
sql("drop cube myschema1.cube4")
}

//TC_1295
test("TC_1295") {
sql("create cube cube5 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei,AMSize) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube5 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("delete load 0 from cube cube5")
checkAnswer(
sql("select AMSize from cube5"),
Seq())
sql("drop cube cube5")
}

//TC_1296
test("TC_1296") {
sql("create cube cube6 dimensions(AMSize STRING) measures(deviceInformationId integer) OPTIONS (AGGREGATION [deviceInformationId=count])")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube6 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("delete load 0 from  cube cube6")
checkAnswer(
sql("select deviceInformationId from cube6"),
Seq())
sql("drop cube cube6")
}

//TC_1297
test("TC_1297") {
sql("create cube cube7 dimensions(AMSize STRING) measures(deviceInformationId integer) OPTIONS (AGGREGATION [deviceInformationId=count])")
sql("delete load 0 from cube cube7")
checkAnswer(
sql("select AMSize from cube7"),
Seq())
sql("drop cube cube7")
}

//TC_1298
test("TC_1298") {
sql("create cube cube8 dimensions(AMSize STRING) measures(deviceInformationId integer) OPTIONS (AGGREGATION [deviceInformationId=count])")
sql("alter cube cube8 add dimensions(a1 string) measures (b1 integer)")
sql("delete load 0 from  cube cube8")
checkAnswer(
sql("select a1,b1 from cube8"),
Seq())
sql("drop cube cube8")
}

//TC_1299
test("TC_1299") {
sql("create cube cube9 dimensions(AMSize STRING) measures(deviceInformationId integer) OPTIONS (AGGREGATION [deviceInformationId=count])")
sql("alter cube cube9 add dimensions(a1 string) measures (b1 integer)")
sql("alter cube cube9 drop(a1,b1)")
sql("delete load 0 from cube cube9")
checkAnswer(
sql("select deviceInformationId from cube9"),
Seq())
sql("drop cube cube9")
}

//TC_1300
test("TC_1300") {
sql("create cube cube10 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube10 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("alter cube cube10 add dimensions(a1 string) measures (b1 integer)")
sql("alter cube cube10 drop(a1,b1)")
sql("delete load 1 from  cube cube10")
checkAnswer(
sql("select deviceInformationId from cube10 limit 1"),
Seq(Row(1.0)))
sql("drop cube cube10")
}

//TC_1301
test("TC_1301") {
sql("create cube cube11 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube11 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("alter cube cube11 add dimensions(a1 string) measures (b1 integer)")
sql("alter cube cube11 drop(a1,b1)")
sql("delete load 0 from  cube cube11")
checkAnswer(
sql("select AMSize from cube11 limit 1"),
Seq())
sql("drop cube cube11")
}

//TC_1302
test("TC_1302") {
sql("create cube cube12 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube12 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube12 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("delete load 0,1 from  cube cube12")
checkAnswer(
sql("select gamePointId from cube12 limit 1"),
Seq())
sql("drop cube cube12")
}

//TC_1303
test("TC_1303") {
sql("create cube cube13 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube13 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube13 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("delete load 0 from  cube cube13")
checkAnswer(
sql("select imei from cube13 limit 1"),
Seq(Row("1AA1")))
sql("drop cube cube13")
}

//TC_1304
test("TC_1304") {
sql("create cube cube14 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube14 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("alter cube cube14 add dimensions(a1 string) measures (b1 integer)")
sql("alter cube cube14 drop(a1,b1)")
sql("delete load 0,1 from  cube cube14")
checkAnswer(
sql("select channelsId from cube14 limit 1"),
Seq())
sql("drop cube cube14")
}

//TC_1305
test("TC_1305") {
sql("create cube cube15 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("alter cube cube15 add dimensions(a1 string) measures(b1 integer)")
sql("alter cube cube15 add dimensions(a2 string) measures(b2 integer)")
sql("delete load 0 from  cube cube15")
sql("delete load 1 from  cube cube15")
checkAnswer(
sql("select ActiveCountry from cube15 limit 1"),
Seq())
sql("drop cube cube15")
}

//TC_1306
test("TC_1306") {
sql("create cube cube16 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube16 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube16 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("delete load 0 from  cube cube16")
sql("delete load 1 from  cube cube16")
checkAnswer(
sql("select gamePointId from cube16 limit 1"),
Seq())
sql("drop cube cube16")
}

//TC_1307
test("TC_1307") {
try
{
sql("create cube cube17 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube17 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("delete load 1 from  cube cube17")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube17")
 
}
}

//TC_1308
test("TC_1308") {
sql("create cube cube18 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube18 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("delete load 0,1 from  cube cube18")
checkAnswer(
sql("select ActiveCountry from cube18 limit 1"),
Seq())
sql("drop cube cube18")
}

//TC_1309
test("TC_1309") {
sql("create cube cube19 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube19 partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')")
sql("DELETE LOAD 0 FROM  CUBE cube19")
checkAnswer(
sql("select deviceInformationId from cube19"),
Seq())
sql("drop cube cube19")
}

//TC_1310
test("TC_1310") {
try
{
sql("create cube cube20 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )")
sql("delete load 1 from cube cube20")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube20")
 
}
}

//TC_1311
test("TC_1311") {
sql("create cube vardhanretention dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("delete load 0 from cube vardhanretention")
checkAnswer(
sql("select imei,deviceInformationId from vardhanretention"),
Seq())
sql("drop cube vardhanretention")
}

//TC_1312
test("TC_1312") {
sql("create cube vardhanretention1 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention1 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention1 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("delete load 0 from cube vardhanretention1")
checkAnswer(
sql("select imei,deviceInformationId from vardhanretention1  where imei='1AA1'"),
Seq(Row("1AA1",1.0)))
sql("drop cube vardhanretention1")
}

//TC_1313
test("TC_1313") {
try
{
sql("create cube vardhanretention2 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention2 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention2 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("delete load 2 from cube vardhanretention2")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhanretention2")
 
}
}

//TC_1314
test("TC_1314") {
sql("create cube vardhanretention3 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention3 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention3 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("delete load 0 from cube vardhanretention3")
checkAnswer(
sql("select imei,AMSize from vardhanretention3 where gamePointId=1407"),
Seq(Row("1AA100051" , "3RAM size"),Row("1AA100061" , "0RAM size")))
sql("drop cube vardhanretention3")
}

//TC_1315
test("TC_1315") {
try
{
sql("create cube vardhanretention4 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("delete load 0 from cube vardhanretention4")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhanretention4")
 
}
}

//TC_1316
test("TC_1316") {
sql("create cube vardhanretention5 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention5 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention5 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("delete load 0,1 from cube vardhanretention5")
checkAnswer(
sql("select imei,deviceInformationId from vardhanretention5"),
Seq())
sql("drop cube vardhanretention5")
}

//TC_1317
test("TC_1317") {
try
{
sql("create cube vardhanretention14 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention14 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("delete load 0 from cube vardhanretention14")
sql("delete load 0 from cube vardhanretention14")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhanretention14")
 
}
}

//TC_1318
test("TC_1318") {
sql("create cube vardhanretention6 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention6 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("delete from cube vardhanretention6 where productionDate before '2015-07-05 12:07:28'")
checkAnswer(
sql("select count(*)  from vardhanretention6"),
Seq(Row(54)))
sql("drop cube vardhanretention6")
}

//TC_1319
test("TC_1319") {
try
{
sql("create cube vardhanretention15 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention15 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("delete load 0,1 from cube vardhanretention15")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhanretention15")
 
}
}

//TC_1320
test("TC_1320") {
try
{
sql("create cube vardhanretention7 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention7 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("delete from cube vardhanretention7 where productionDate before '2015-07-05 '")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhanretention7")
 
}
}

//TC_1321
test("TC_1321") {
try
{
sql("create cube vardhanretention8 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("delete from cube vardhanretention8 where productionDate before '2015-07-05 '")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhanretention8")
 
}
}

//TC_1322
test("TC_1322") {
sql("create cube vardhanretention9 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention9 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("delete from cube vardhanretention9 where productionDate before '2015-10-06 12:07:28 '")
checkAnswer(
sql("select count(*) from vardhanretention9"),
Seq(Row(3)))
sql("drop cube vardhanretention9")
}

//TC_1323
test("TC_1323") {
try
{
sql("create cube vardhanretention10 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention10 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("delete from cube vardhanretention10 where productionDate before ''")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhanretention10")
 
}
}

//TC_1324
test("TC_1324") {
try
{
sql("create cube vardhanretention12 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention12 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("delete from cube vardhanretention12 where productionDate before '10-06-2015 12:07:28'")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhanretention12")
 
}
}

//TC_1325
test("TC_1325") {
try
{
sql("create cube vardhanretention13 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention13 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("delete from cube vardhanretention13 where productionDate before '2013-13-13 12:07:28 '")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhanretention13")
 
}
}

//DTS2015112610913_03
test("DTS2015112610913_031") {
sql("create CUBE cube_restructure60 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructuRE60 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"', FILEHEADER 'a0,b0')")
sql("alter cube CUBE_restructuRE60 add dimensions(a12 string) measures(b11 integer)")
sql("delete load 0 from cube CUBE_restructuRE60")
checkAnswer(
sql("select * from CUBE_restructuRE60 limit 1"),
Seq())
sql("drop cube cube_restructure60")
}

//DTS2015112608945
test("DTS2015112608945") {
try
{
sql("create cube babu_67 dimensions(imei string,deviceInformationId integer,mac string,productdate timestamp,updatetime timestamp) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("delete from cube babu_67 where productdate before '2015-01-10 19:59:00'")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube babu_67")
 
}
}

//DTS2015102804722
test("DTS2015102804722") {
try
{
sql("create CUBE cube_restructure62 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure62 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"', FILEHEADER 'a0,b0')")
sql("delete load 0 from cube cube_restructure62\"")
sql("delete load 0 from cube cube_restructure62")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube_restructure62")
 
}
}

//DTS2015110209900
test("DTS2015110209900") {
sql("create CUBE cube_restructure63 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure63 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"', FILEHEADER 'a0,b0')")
sql("delete load 0 from cube cube_RESTRUCTURE63")
checkAnswer(
sql("select * from cube_restructure63 limit 1"),
Seq())
sql("drop cube cube_restructure63")
}

//DTS2015111403600
test("DTS2015111403600") {
try
{
sql("create CUBE cube_restructure64 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure64 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"', FILEHEADER 'a0,b0')")
sql("delete load 0 from cube cube_restructure64")
sql("clean files for cube cube_restructure64\"")
sql("\"delete load 0 from cube cube_restructure64")
sql("delete load 1 from cube cube_restructure64\"")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube_restructure64")
 
}
}

//DTS2015112703910
test("DTS2015112703910") {
try
{
sql("CREATE CUBE cube_restructure65 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )")
sql("delete load 1 from cube cube_restructure65")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube cube_restructure65")
 
}
}

//DTS2015110209665
test("DTS2015110209665") {
try
{
sql("create cube vardhanretention13 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("delete from cube vardhanretention13 where productionDate before '2013-13-13 12:07:28 '")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhanretention13")
 
}
}

//DTS2015110209543
test("DTS2015110209543") {
sql("create cube vardhanretention13 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention13 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("delete from cube vardhanretention13 where productionDate before '2015-09-08 12:07:28'")
sql("delete from cube vardhanretention13 where productionDate before '2000-09-08 12:07:28'")
checkAnswer(
sql("select count(*) from vardhanretention13"),
Seq(Row(31)))
sql("drop cube vardhanretention13")
}

//DTS2015101506341
test("DTS2015101506341") {
sql("create cube vardhanretention13 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention13 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate') \"")
sql("delete from cube vardhanretention13 where productiondate before '2015-09-08 12:07:28'")
checkAnswer(
sql("select count(*) from vardhanretention13"),
Seq())
sql("drop cube vardhanretention13")
}

//DTS2015112611263
test("DTS2015112611263") {
sql("create cube makamraghuvardhan002 dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO Cube makamraghuvardhan002 OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO Cube makamraghuvardhan002 OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO Cube makamraghuvardhan002 OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
sql("delete load 0 from cube makamraghuvardhan002")
checkAnswer(
sql("select count(*) from makamraghuvardhan002 where series='8Series'"),
Seq(Row(11)))
sql("drop cube makamraghuvardhan002")
}

//csv doesnt exist
/*//DTS2015121509729
test("DTS2015121509729") {
sql("create cube vardhanretention002 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention002 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("alter cube vardhanretention002 add dimensions(deliveryDate string) measures(deliverycharge integer) options ()")
sql("alter cube vardhanretention002 drop (deliveryDate,deliverycharge)")
sql("alter cube vardhanretention002 add dimensions(deliveryDate timestamp) measures(deliverycharge integer) options ()")
sql("LOAD DATA FACT FROM './src/test/resources/vardhandaterestruct.csv' INTO CUBE vardhanretention002 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')")
sql("\"delete from cube vardhanretention002 where deliveryDate before '2015-09-12 12:07:28'")
checkAnswer(
sql("select count(*) from vardhanretention002"),
Seq())
sql("drop cube vardhanretention002")
}*/

//DTS2015122300770
test("DTS2015122300770") {
sql("create table test1(imei string,deviceInformationId int,mac string,productdate timestamp,updatetime timestamp,gamePointId double,contractNumber double) row format delimited fields terminated by ','")
sql("SHOW CREATE CUBE t22 FACT FROM test1 INCLUDE (deviceInformationId), DIMENSION FROM table4:test1 RELATION (FACT.imei = imei) EXCLUDE (mac,deviceInformationId) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (imei), PARTITION_COUNT=5] )")
sql("CREATE CUBE default.t22 DIMENSIONS (imei String, productdate Timestamp, updatetime Timestamp, gamepointid Numeric, contractnumber Numeric) MEASURES (deviceinformationid Integer) WITH table4 RELATION (FACT.imei=imei) INCLUDE (imei, productdate, updatetime, gamepointid, contractnumber) OPTIONS( PARTITIONER[ CLASS='org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS=(imei), PARTITION_COUNT=5 ] )")
sql("LOAD DATA FACT FROM './src/test/resources/test13.csv' DIMENSION FROM table4:'./src/test/resources/test14.csv' INTO CUBE t22 PARTITIONDATA (DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceinformationid')")
sql("delete from cube t22 where productdate before '2015-04-01 10:00:00'")
checkAnswer(
sql("select count(*) from t22"),
Seq())
sql("drop cube test1(imei")
}

//TC_1339
test("TC_1339") {
sql("create cube vardhan323 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhan323 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("delete from cube vardhan323  where productiondate before '2015-08-07 12:07:28'")
checkAnswer(
sql("select productiondate from vardhan323 order by imei ASC limit 3"),
Seq(Row(null,"2015-08-07 12:07:28.0","2015-08-08 12:07:28.0")))
sql("drop cube vardhan323")
}

//TC_1340
test("TC_1340") {
sql("create cube vardhan60 dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhan60 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhan60 OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("delete load 0 from cube vardhan60")
sql("delete from cube vardhan60 where productionDate before '2015-07-25 12:07:28'")
checkAnswer(
sql("select count(*)from vardhan60"),
Seq(Row(76)))
sql("drop cube vardhan60")
}

//TC_1341
test("TC_1341") {
sql("create cube vardhan01 dimensions(key string,name string) measures(gamepointid numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)")
sql("LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER '')")
sql("alter cube vardhan01 add dimensions(productiondate timestamp) options (AGGREGATION [b = SUM])")
sql("delete load 0 from cube vardhan01")
checkAnswer(
sql("select count(*) from vardhan01"),
Seq(Row(0)))
sql("drop cube vardhan01")
}

//TC_1342
test("TC_1342") {
sql("create cube vardhan01 dimensions(key string,name string) measures(gamepointid numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)")
sql("LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER '')")
sql("alter cube vardhan01 add dimensions(productiondate timestamp) options (AGGREGATION [b = SUM])")
sql("delete from cube vardhan01 where productiondate before '2015-08-10 19:59:00'")
checkAnswer(
sql("select count(*) from vardhan01"),
Seq(Row(21)))
sql("drop cube vardhan01")
}

//TC_1343
test("TC_1343") {
sql("create cube vardhan01 dimensions(key string,name string) measures(gamepointid numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)")
sql("LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER '')")
sql("alter cube vardhan01 add dimensions(productiondate timestamp) options (AGGREGATION [b = SUM])")
sql("LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER '')")
sql("delete from cube vardhan01 where productiondate before '2015-01-10 19:59:00'")
checkAnswer(
sql("select count(*) from vardhan01"),
Seq())
sql("drop cube vardhan01")
}

//TC_1344
test("TC_1344") {
sql("create schema drug")
sql("create cube vardhan01 dimensions(key string,name string) measures(gamepointid numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)")
sql("create cube drug.vardhan01 dimensions(key string,name string) measures(gamepointid numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)")
sql("LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER '')")
sql("LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE drug.vardhan01 PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER '')")
sql("delete load 0 from cube vardhan01")
checkAnswer(
sql("select count(*) from vardhan01"),
Seq())
sql("drop cube drug")
}

//TC_1345
test("TC_1345") {
sql("create cube vardhan01 dimensions(key string,name string ,productiondate timestamp) measures(gamepointid numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)")
sql("create cube drug.vardhan01 dimensions(key string,name string, productiondate timestamp) measures(gamepointid numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)")
sql("LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER '')")
sql("LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE drug.vardhan01 PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER '')")
sql("delete from cube vardhan01 where productiondate before '2015-01-12 00:00:00'")
checkAnswer(
sql("select count(*) from vardhan01"),
Seq())
sql("drop cube vardhan01")
}

//TC_1346
test("TC_1346") {
try
{
sql("create cube vardhanretention dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("delete load 0 from cube vardhanretention")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhanretention")
 
}
}

//TC_1347
test("TC_1347") {
sql("create cube vardhanretention dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("alter cube vardhanretention drop (AMSize)")
sql("delete from cube vardhanretention where productionDate before '2015-08-10 19:59:00'")
checkAnswer(
sql("select count(*) from vardhanretention"),
Seq(Row(59)))
sql("drop cube vardhanretention")
}

//TC_1348
test("TC_1348") {
try
{
sql("create cube vardhanretention dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("alter cube vardhanretention drop(AMSize) add dimensions(AMSize string)")
sql("delete load 0 from cube vardhanretention")
checkAnswer(
sql("NA"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube vardhanretention")
 
}
}

//TC_1349
test("TC_1349") {
sql("create cube vardhanretention dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("alter cube vardhanretention drop(AMSize) add dimensions(AMSize string)")
sql("delete load 0 from cube vardhanretention")
checkAnswer(
sql("select imei,deviceInformationId from vardhanretention where imei='1AA100'"),
Seq())
sql("drop cube vardhanretention")
}

//TC_1350
test("TC_1350") {
sql("create cube vardhanretention dimensions(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
sql("alter cube vardhanretention drop(AMSize) add dimensions(AMSize string)")
sql("delete load 0 from cube vardhanretention")
checkAnswer(
sql("select count(*) from vardhanretention"),
Seq(Row(0)))
sql("drop cube vardhanretention")
}


 //TC_1259
test("TC_1259") {
sql("CREATE DATABASE IF NOT EXISTS my")
sql("create cube my.Carbon01 dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO Cube my.Carbon01 OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
checkAnswer(
sql("SUGGEST AGGREGATE WITH SCRIPTS USING DATA_STATS FOR cube my.Carbon01"),
Seq(Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_BacVerNumber,sum(gamePointId),sum(contractNumber) from cube my.Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube my.Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube my.Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube my.Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube my.Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube my.Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube my.Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube my.Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube my.Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube my.Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube my.Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube my.Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube my.Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube my.Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  Latest_province,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube my.Carbon01  ")))
sql("drop cube my.Carbon01")
}

//TC_1260
test("TC_1260") {
sql("create cube Carbon01 dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO Cube Carbon01 OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
checkAnswer(
sql("SUGGEST AGGREGATE WITH SCRIPTS USING DATA_STATS FOR cube Carbon01"),
Seq(Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_BacVerNumber,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  Latest_province,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01")))
sql("drop cube Carbon01")
}

//TC_1261
test("TC_1261") {
sql("create cube Carbon01 dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO Cube Carbon01 OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
checkAnswer(
sql("SUGGEST AGGREGATE  USING DATA_STATS FOR cube Carbon01"),
Seq(Row("DATA_STATS"," ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_BacVerNumber,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","Latest_province,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)")))
sql("drop cube Carbon01")
}

//TC_1262
test("TC_1262") {
sql("create cube Carbon01 dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO Cube Carbon01 OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
sql("")
checkAnswer(
sql("SUGGEST AGGREGATE  USING DATA_STATS FOR cube Carbon01"),
Seq(Row("DATA_STATS"," ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_BacVerNumber,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","Latest_province,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)")))
sql("drop cube Carbon01")
}

//TC_1263
test("TC_1263") {
sql("create cube Carbon01 dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO Cube Carbon01 OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
sql("execute the query\"select ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_BacVerNumber,sum(gamePointId),sum(contractNumber) from Carbon01 group by ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_BacVerNumber\"")
checkAnswer(
sql("SUGGEST AGGREGATE  WITH SCRIPTS USING QUERY_STATS FOR cube Carbon01"),
Seq(Row("DATA_STATS","CREATE AGGREGATETABLE  MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveProvince,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_BacVerNumber,sum(gamePointId),sum(contractNumber) from cube Carbon01")))
sql("drop cube Carbon01")
}

//TC_1264
test("TC_1264") {
sql("create cube Carbon01 dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO Cube Carbon01 OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
checkAnswer(
sql("SUGGEST AGGREGATE  USING QUERY_STATS FOR cube Carbon01"),
Seq(Row("DATA_STATS","MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveProvince,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_BacVerNumber,sum(gamePointId),sum(contractNumber)")))
sql("drop cube Carbon01")
}

//TC_1265
test("TC_1265") {
sql("create cube Carbon01 dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO Cube Carbon01 OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
checkAnswer(
sql("SUGGEST AGGREGATE   FOR cube Carbon01"),
Seq(Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_BacVerNumber,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryTime,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,productionDate,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,MAC,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","ActiveProvince,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","Latest_province,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber)"),Row("DATA_STATS","MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveProvince,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_BacVerNumber,sum(gamePointId),sum(contractNumber) ")))
sql("drop cube Carbon01")
}

//TC_1266
test("TC_1266") {
sql("create cube Carbon01 dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO Cube Carbon01 OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
checkAnswer(
sql("SUGGEST AGGREGATE with scripts  FOR cube Carbon01"),
Seq(Row("DATA_STATS","   CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_BacVerNumber,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  Latest_province,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveProvince,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_BacVerNumber,sum(gamePointId),sum(contractNumber) from cube Carbon01 ")))
sql("drop cube Carbon01")
}

//TC_1267
test("TC_1267") {
try
{
sql("CREATE DATABASE IF NOT EXISTS my")
sql("create cube Carbon01 dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , PARTITION_COUNT=2] )")
checkAnswer(
sql("SUGGEST AGGREGATE with scripts for cube my.Carbon01"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube Carbon01")
 
}
}

//TC_1268
test("TC_1268") {
sql("create cube Carbon01 dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO Cube Carbon01 OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
checkAnswer(
sql("Create AGGREGATETABLE imei,sum(gamePointId) FROM cube Carbon05"),
Seq(Row("DATA_STATS","CREATE AGGREGATETABLE  imei,sum(gamepointid) From cube Carbon01")))
sql("drop cube Carbon01")
}

//TC_1269
test("TC_1269") {
try
{
sql("create cube Carbon01 dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO Cube Carbon01 OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
checkAnswer(
sql("SUGGEST AGGREGATE with scripts for cube Carbon01"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube Carbon01")
 
}
}

//TC_1270
test("TC_1270") {
sql("create cube Carbon01 dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/100.csv' INTO Cube Carbon01 OPTIONS(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
checkAnswer(
sql("SUGGEST AGGREGATE with scripts for cube Carbon01"),
Seq(Row("DATA_STATS"," CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_BacVerNumber,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_operaSysVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_EMUIVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryTime,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,productionDate,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,MAC,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  ActiveProvince,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  Latest_province,MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveCountry,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_country,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_phonePADPartitionedVersions,sum(gamePointId),sum(contractNumber) from cube Carbon01"),Row("DATA_STATS","CREATE AGGREGATETABLE  MAC,productionDate,deliveryTime,deliveryCountry,ActiveCheckTime,ActiveProvince,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_phonePADPartitionedVersions,Latest_MONTH,Latest_DAY,Latest_EMUIVersion,Latest_BacVerNumber,sum(gamePointId),sum(contractNumber) from cube Carbon01 ")))
sql("drop cube Carbon01")
}

//DTS2015112010240
test("DTS2015112010240") {
try
{
sql("create cube twinkles DIMENSIONS (imei String,uuid String,MAC String,device_color String,device_shell_color String,device_name String,product_name String,ram String,rom String,cpu_clock String,series String,check_date String,check_year String,check_month String,check_day String,check_hour String,bom String,inside_name String,packing_date String,packing_year String,packing_month String,packing_day String,packing_hour String,customer_name String,deliveryAreaId String,deliveryCountry String,deliveryProvince String,deliveryCity String,deliveryDistrict String,packing_list_no String,order_no String,Active_check_time String,Active_check_year String,Active_check_month String,Active_check_day String,Active_check_hour String,ActiveAreaId String,ActiveCountry String,ActiveProvince String,Activecity String,ActiveDistrict String,Active_network String,Active_firmware_version String,Active_emui_version String,Active_os_version String,Latest_check_time String,Latest_check_year String,Latest_check_month String,Latest_check_day String,Latest_check_hour String,Latest_areaId String,Latest_country String,Latest_province String,Latest_city String,Latest_district String,Latest_firmware_version String,Latest_emui_version String,Latest_os_version String,Latest_network String,site String,site_desc String,product String,product_desc String) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' COLUMNS= (imei) PARTITION_COUNT=1] )")
checkAnswer(
sql("SUGGEST AGGREGATE WITH SCRIPTS USING DATA_STATS FOR cube twinkles"),
Seq())
fail("Unexpected behavior")
}
catch
{
 case ex: Throwable =>sql("drop cube twinkles")
 
}
}

//TC_1337
test("TC_1337") {
sql("create cube makam05 DIMENSIONS (imei String,uuid String,MAC String,device_color String,device_shell_color String,device_name String,product_name String,ram String,rom String,cpu_clock String,series String,check_date String,check_year String,check_month String,check_day String,check_hour String,bom String,inside_name String,packing_date String,packing_year String,packing_month String,packing_day String,packing_hour String,customer_name String,deliveryAreaId String,deliveryCountry String,deliveryProvince String,deliveryCity String,deliveryDistrict String,packing_list_no String,order_no String,Active_check_time String,Active_check_year String,Active_check_month String,Active_check_day String,Active_check_hour String,ActiveAreaId String,ActiveCountry String,ActiveProvince String,Activecity String,ActiveDistrict String,Active_network String,Active_firmware_version String,Active_emui_version String,Active_os_version String,Latest_check_time String,Latest_check_year String,Latest_check_month String,Latest_check_day String,Latest_check_hour String,Latest_areaId String,Latest_country String,Latest_province String,Latest_city String,Latest_district String,Latest_firmware_version String,Latest_emui_version String,Latest_os_version String,Latest_network String,site String,site_desc String,product String,product_desc String)  OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' COLUMNS= (imei) PARTITION_COUNT=2] )")
sql("LOAD DATA FACT FROM './src/test/resources/bigdata.csv' INTO CUBE makam05 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"', FILEHEADER 'imei,uuid,MAC,device_color,device_shell_color,device_name,product_name,ram,rom,cpu_clock,series,check_date,check_year,check_month,check_day,check_hour,bom,inside_name,packing_date,packing_year,packing_month,packing_day,packing_hour,customer_name,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,packing_list_no,order_no,Active_check_time,Active_check_year,Active_check_month,Active_check_day,Active_check_hour,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,Active_network,Active_firmware_version,Active_emui_version,Active_os_version,Latest_check_time,Latest_check_year,Latest_check_month,Latest_check_day,Latest_check_hour,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_firmware_version,Latest_emui_version,Latest_os_version,Latest_network,site,site_desc,product,product_desc')")
sql("alter cube makam05 drop(uuid) add dimensions(uuid string)")
checkAnswer(
sql("suggest aggregate using data_stats for cube makam05"),
Seq())
sql("drop cube makam05")
}

  
  

}