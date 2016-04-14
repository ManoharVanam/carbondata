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

package org.carbondata.integration.spark.testsuite.aggquery

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for aggregate query on Integer datatypes
 * @author N00902756
 *
 */
class IntegerDataTypeTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
   // sql("CREATE CUBE integertypecube DIMENSIONS (empno Integer, workgroupcategory Integer, deptno Integer, projectcode Integer) MEASURES (attendance Integer) OPTIONS (PARTITIONER [PARTITION_COUNT=1])")
   // sql("LOAD DATA fact from './src/test/resources/data.csv' INTO CUBE integertypecube PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')")
    sql("create cube Carbon_automation_test dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string)  measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , PARTITION_COUNT=2] )");
    sql("LOAD DATA FACT FROM  './src/test/resources/100_olap.csv' INTO Cube Carbon_automation_test partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')");

  }
  override def afterAll {
   // sql("drop cube integertypecube")
    sql("drop cube Carbon_automation_test")

  }

/*  test("select empno from integertypecube") {
    checkAnswer(
      sql("select empno from integertypecube"),
      Seq(Row(11), Row(12), Row(13), Row(14), Row(15), Row(16), Row(17), Row(18), Row(19), Row(20)))
  }*/
  
  //TC_222
test("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY  LIKE Latest_areaId AND  Latest_DAY  LIKE Latest_HOUR") ({
checkAnswer(
sql("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY  LIKE Latest_areaId AND  Latest_DAY  LIKE Latest_HOUR"),
Seq())
})

//TC_224
test("select * from (select if( Latest_areaId=7,7,NULL) as babu,Latest_MONTH from Carbon_automation_test) qq where babu NOT LIKE   Latest_MONTH") ({
checkAnswer(
sql("select * from (select if( Latest_areaId=7,7,NULL) as babu,Latest_MONTH from Carbon_automation_test) qq where babu NOT LIKE   Latest_MONTH"),
Seq())
})

//TC_262
test("select count(imei) ,series from Carbon_automation_test group by series having sum (Latest_DAY) == 99") ({
checkAnswer(
sql("select count(imei) ,series from Carbon_automation_test group by series having sum (Latest_DAY) == 99"),
Seq())
})

//TC_264
test("SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY WHERE AMSize = \"\" GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC") ({
checkAnswer(
sql("SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY WHERE AMSize = \"\" GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC"),
Seq())
})

//TC_270
test("SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY WHERE AMSize < \"0RAM size\" GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC") ({
checkAnswer(
sql("SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY WHERE AMSize < \"0RAM size\" GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC"),
Seq())
})

//TC_320
test("SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY WHERE gamePointId > 1.0E9 GROUP BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC") ({
checkAnswer(
sql("SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY WHERE gamePointId > 1.0E9 GROUP BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC"),
Seq())
})

//TC_343
test("SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, deliverycity FROM (select * from Carbon_automation_test) SUB_QRY WHERE deliverycity IS NULL") ({
checkAnswer(
sql("SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, deliverycity FROM (select * from Carbon_automation_test) SUB_QRY WHERE deliverycity IS NULL"),
Seq())
})

//TC_409
test("select  gamePointId from Carbon_automation_test where  modelId is  null") ({
checkAnswer(
sql("select  gamePointId from Carbon_automation_test where  modelId is  null"),
Seq())
})

//TC_410
test("select  contractNumber from Carbon_automation_test where bomCode is  null") ({
checkAnswer(
sql("select  contractNumber from Carbon_automation_test where bomCode is  null"),
Seq())
})

//TC_411
test("select  imei from Carbon_automation_test where AMSIZE is  null") ({
checkAnswer(
sql("select  imei from Carbon_automation_test where AMSIZE is  null"),
Seq())
})

//TC_412
test("select  bomCode from Carbon_automation_test where contractnumber is  null") ({
checkAnswer(
sql("select  bomCode from Carbon_automation_test where contractnumber is  null"),
Seq())
})

//TC_413
test("select  latest_day from Carbon_automation_test where  modelId is  null") ({
checkAnswer(
sql("select  latest_day from Carbon_automation_test where  modelId is  null"),
Seq())
})

//TC_414
test("select  latest_day from Carbon_automation_test where  deviceinformationid is  null") ({
checkAnswer(
sql("select  latest_day from Carbon_automation_test where  deviceinformationid is  null"),
Seq())
})

//TC_415
test("select  deviceinformationid from Carbon_automation_test where  modelId is  null") ({
checkAnswer(
sql("select  deviceinformationid from Carbon_automation_test where  modelId is  null"),
Seq())
})

//TC_416
test("select  deviceinformationid from Carbon_automation_test where  deviceinformationid is  null") ({
checkAnswer(
sql("select  deviceinformationid from Carbon_automation_test where  deviceinformationid is  null"),
Seq())
})

//TC_417
test("select  imei from Carbon_automation_test where  modelId is  null") ({
checkAnswer(
sql("select  imei from Carbon_automation_test where  modelId is  null"),
Seq())
})

//TC_418
test("select  imei from Carbon_automation_test where  deviceinformationid is  null") ({
checkAnswer(
sql("select  imei from Carbon_automation_test where  deviceinformationid is  null"),
Seq())
})

//TC_112
test("select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a from Carbon_automation_test1") ({
validateResult(
sql("select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a from Carbon_automation_test"),
"TC_112.csv")
})

//TC_115
test("select percentile_approx(deviceInformationId,array(0.2,0.3,0.99))  as a from Carbon_automation_test") ({
validateResult(
sql("select percentile_approx(deviceInformationId,array(0.2,0.3,0.99))  as a from Carbon_automation_test"),
"TC_115.csv")
})

//TC_116
test("select percentile_approx(deviceInformationId,array(0.2,0.3,0.99),5) as a from Carbon_automation_test") ({
validateResult(
sql("select percentile_approx(deviceInformationId,array(0.2,0.3,0.99),5) as a from Carbon_automation_test"),
"TC_116.csv")
})

//TC_117
test("select histogram_numeric(deviceInformationId,2)  as a from Carbon_automation_test") ({
validateResult(
sql("select histogram_numeric(deviceInformationId,2)  as a from Carbon_automation_test"),
"TC_117.csv")
})

//TC_128
test("select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a from Carbon_automation_test") ({
validateResult(
sql("select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a from Carbon_automation_test"),
"TC_128.csv")
})

//TC_131
test("select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from Carbon_automation_test") ({
validateResult(
sql("select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from Carbon_automation_test"),
"TC_131.csv")
})

//TC_132
test("select percentile_approx(gamePointId,array(0.2,0.3,0.99),5) as a from Carbon_automation_test") ({
validateResult(
sql("select percentile_approx(gamePointId,array(0.2,0.3,0.99),5) as a from Carbon_automation_test"),
"TC_132.csv")
})

//TC_133
test("select histogram_numeric(gamePointId,2)  as a from Carbon_automation_test") ({
validateResult(
sql("select histogram_numeric(gamePointId,2)  as a from Carbon_automation_test"),
"TC_133.csv")
})

//TC_477
test("select percentile(1,array(1)) from Carbon_automation_test") ({
validateResult(
sql("select percentile(1,array(1)) from Carbon_automation_test"),
"TC_477.csv")
})

//TC_479
test("select percentile(1,array('0.5')) from Carbon_automation_test") ({
validateResult(
sql("select percentile(1,array('0.5')) from Carbon_automation_test"),
"TC_479.csv")
})

//TC_480
test("select percentile(1,array('1')) from Carbon_automation_test") ({
validateResult(
sql("select percentile(1,array('1')) from Carbon_automation_test"),
"TC_480.csv")
})

//TC_485
test("select percentile_approx(1,array(0.5),5000) from Carbon_automation_test11") ({
validateResult(
sql("select percentile_approx(1,array(0.5),5000) from Carbon_automation_test"),
"TC_485.csv")
})

//TC_486
test("select percentile_approx(1,array(0.5),5000) from Carbon_automation_test2") ({
validateResult(
sql("select percentile_approx(1,array(0.5),5000) from Carbon_automation_test"),
"TC_486.csv")
})

//TC_487
test("select histogram_numeric(1, 5000)from Carbon_automation_test") ({
validateResult(
sql("select histogram_numeric(1, 5000)from Carbon_automation_test"),
"TC_487.csv")
})

//TC_488
test("select histogram_numeric(1, 1000)from Carbon_automation_test") ({
validateResult(
sql("select histogram_numeric(1, 1000)from Carbon_automation_test"),
"TC_488.csv")
})

//TC_489
test("select histogram_numeric(1, 500)from Carbon_automation_test1") ({
validateResult(
sql("select histogram_numeric(1, 500)from Carbon_automation_test"),
"TC_489.csv")
})

//TC_490
test("select histogram_numeric(1, 500)from Carbon_automation_test") ({
validateResult(
sql("select histogram_numeric(1, 500)from Carbon_automation_test"),
"TC_490.csv")
})

//TC_491
test("select collect_set(gamePointId) from Carbon_automation_test") ({
validateResult(
sql("select collect_set(gamePointId) from Carbon_automation_test"),
"TC_491.csv")
})

//TC_492
test("select collect_set(AMSIZE) from Carbon_automation_test1") ({
validateResult(
sql("select collect_set(AMSIZE) from Carbon_automation_test"),
"TC_492.csv")
})

//TC_493
test("select collect_set(bomcode) from Carbon_automation_test") ({
validateResult(
sql("select collect_set(bomcode) from Carbon_automation_test"),
"TC_493.csv")
})

//TC_494
test("select collect_set(imei) from Carbon_automation_test") ({
validateResult(
sql("select collect_set(imei) from Carbon_automation_test"),
"TC_494.csv")
})

//TC_500
test("select percentile(1,array('0.5')) from Carbon_automation_test1") ({
validateResult(
sql("select percentile(1,array('0.5')) from Carbon_automation_test"),
"TC_500.csv")
})

//TC_501
test("select percentile_approx(1,array(0.5),5000) from Carbon_automation_test1") ({
validateResult(
sql("select percentile_approx(1,array(0.5),5000) from Carbon_automation_test"),
"TC_501.csv")
})

//TC_502
test("select collect_set(AMSIZE) from Carbon_automation_test") ({
validateResult(
sql("select collect_set(AMSIZE) from Carbon_automation_test"),
"TC_502.csv")
})
  
//TC_513
test("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC") ({
checkAnswer(
sql("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC"),
Seq())
})

//TC_563
test("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC") ({
checkAnswer(
sql("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC"),
Seq())
})

//TC_612
test("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IN (\"4RAM size\",\"8RAM size\") GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC") ({
validateResult(sql("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IN (\"4RAM size\",\"8RAM size\") GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC"), "TC_612.csv")
})

//TC_613
test("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC") ({
checkAnswer(
sql("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC"),
Seq())
})

//TC_663
test("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC") ({
checkAnswer(
sql("SELECT Carbon_automation_test.gamePointId AS gamePointId,Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.AMSize = Carbon_automation_test1.AMSize WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ,Carbon_automation_test.gamePointId ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC"),
Seq())
})

//TC_712
test("SELECT Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity, SUM(Carbon_automation_test.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.gamePointId = Carbon_automation_test1.gamePointId WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC") ({
checkAnswer(
sql("SELECT Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity, SUM(Carbon_automation_test.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.gamePointId = Carbon_automation_test1.gamePointId WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC"),
Seq())
})

//TC_760
test("SELECT Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity, SUM(Carbon_automation_test.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.gamePointId = Carbon_automation_test1.gamePointId WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC") ({
checkAnswer(
sql("SELECT Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity, SUM(Carbon_automation_test.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.gamePointId = Carbon_automation_test1.gamePointId WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC"),
Seq())
})

//TC_856
test("SELECT Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity, SUM(Carbon_automation_test.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.gamePointId = Carbon_automation_test1.gamePointId WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC") ({
checkAnswer(
sql("SELECT Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test.Activecity AS Activecity, SUM(Carbon_automation_test.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.gamePointId = Carbon_automation_test1.gamePointId WHERE Carbon_automation_test.AMSize IS NULL GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test.Activecity ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC, Carbon_automation_test.Activecity ASC"),
Seq())
})

/*
//VMALL_Per_TC_000
test("select count(*) from    myvmall") ({
checkAnswer(
sql("select count(*) from    myvmall"),
Seq(Row(1003)))
})

//VMALL_Per_TC_001
test("SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from myvmall) SUB_QRY GROUP BY product_name ORDER BY product_name ASC") ({
validateResult(sql("SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from myvmall) SUB_QRY GROUP BY product_name ORDER BY product_name ASC"), "VMALL_Per_TC_001.csv")

})

//VMALL_Per_TC_002
test("SELECT device_name, product, product_name, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from myvmall) SUB_QRY GROUP BY device_name, product, product_name ORDER BY device_name ASC, product ASC, product_name ASC") ({
validateResult(sql("SELECT device_name, product, product_name, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from myvmall) SUB_QRY GROUP BY device_name, product, product_name ORDER BY device_name ASC, product ASC, product_name ASC"), "VMALL_Per_TC_002.csv")
})

//VMALL_Per_TC_003
test("SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from myvmall) SUB_QRY where product_name='Huawei4009' GROUP BY product_name ORDER BY  product_name ASC") ({
checkAnswer(
sql("SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from myvmall) SUB_QRY where product_name='Huawei4009' GROUP BY product_name ORDER BY  product_name ASC"),
Seq(Row("Huawei4009",1)))
})

//VMALL_Per_TC_004
test("SELECT device_color FROM (select * from myvmall) SUB_QRY GROUP BY device_color ORDER BY device_color ASC") ({
validateResult(sql("SELECT device_color FROM (select * from myvmall) SUB_QRY GROUP BY device_color ORDER BY device_color ASC"), "VMALL_Per_TC_004.csv")
})

//VMALL_Per_TC_005
test("SELECT product_name  FROM (select * from myvmall) SUB_QRY GROUP BY product_name ORDER BY  product_name ASC") ({
validateResult(sql("SELECT product_name  FROM (select * from myvmall) SUB_QRY GROUP BY product_name ORDER BY  product_name ASC"), "VMALL_Per_TC_005.csv")
})

//VMALL_Per_TC_006
test("SELECT product, COUNT(DISTINCT packing_list_no) AS LONG_COL_0 FROM (select * from myvmall) SUB_QRY GROUP BY product ORDER BY product ASC") ({
validateResult(sql("SELECT product, COUNT(DISTINCT packing_list_no) AS LONG_COL_0 FROM (select * from myvmall) SUB_QRY GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_006.csv")
})

//VMALL_Per_TC_007
test("select count(distinct imei) DistinctCount_imei from myvmall") ({
checkAnswer(
sql("select count(distinct imei) DistinctCount_imei from myvmall"),
Seq(Row(1001)))
})

//VMALL_Per_TC_008
test("Select count(imei),deliveryCountry  from myvmall group by deliveryCountry order by deliveryCountry asc") ({
checkAnswer(
sql("Select count(imei),deliveryCountry  from myvmall group by deliveryCountry order by deliveryCountry asc"),
Seq())
})

//VMALL_Per_TC_009
test("select (t1.hnor6emui/t2.totalc)*100 from (select count (Active_emui_version)  as hnor6emui from myvmall where Active_emui_version=\"EmotionUI_2.1\")t1,(select count(Active_emui_version) as totalc from myvmall)t2") ({
checkAnswer(
sql("select (t1.hnor6emui/t2.totalc)*100 from (select count (Active_emui_version)  as hnor6emui from myvmall where Active_emui_version=\"EmotionUI_2.1\")t1,(select count(Active_emui_version) as totalc from myvmall)t2"),
Seq(Row(0.09970089730807577)))
})

//VMALL_Per_TC_010
test("select (t1.hnor4xi/t2.totalc)*100 from (select count (imei)  as hnor4xi from myvmall where device_name=\"Honor2\")t1,(select count (imei) as totalc from myvmall)t2") ({
checkAnswer(
sql("select (t1.hnor4xi/t2.totalc)*100 from (select count (imei)  as hnor4xi from myvmall where device_name=\"Honor2\")t1,(select count (imei) as totalc from myvmall)t2"),
Seq(Row(0.0)))
})

//VMALL_Per_TC_011
test("select count(imei) from (select DATEDIFF(from_unixtime(unix_timestamp()),packing_date) mydates,imei from myvmall) sub where mydates<1000") ({
checkAnswer(
sql("select count(imei) from (select DATEDIFF(from_unixtime(unix_timestamp()),packing_date) mydates,imei from myvmall) sub where mydates<1000"),
Seq(Row(1000)))
})

//VMALL_Per_TC_012
test("SELECT Active_os_version, count(distinct imei) DistinctCount_imei FROM (select * from myvmall) SUB_QRY GROUP BY Active_os_version ORDER BY Active_os_version ASC") ({
validateResult(sql("SELECT Active_os_version, count(distinct imei) DistinctCount_imei FROM (select * from myvmall) SUB_QRY GROUP BY Active_os_version ORDER BY Active_os_version ASC"), "VMALL_Per_TC_012.csv")
})

//VMALL_Per_TC_013
test("select count(imei)  DistinctCount_imei from myvmall where (Active_emui_version=\"EmotionUI_2.972\" and Latest_emui_version=\"EmotionUI_3.863972\") OR (Active_emui_version=\"EmotionUI_2.843\" and Latest_emui_version=\"EmotionUI_3.863843\")") ({
checkAnswer(
sql("select count(imei)  DistinctCount_imei from myvmall where (Active_emui_version=\"EmotionUI_2.972\" and Latest_emui_version=\"EmotionUI_3.863972\") OR (Active_emui_version=\"EmotionUI_2.843\" and Latest_emui_version=\"EmotionUI_3.863843\")"),
Seq(Row(2)))
})

//VMALL_Per_TC_014
test("select count(imei) as imeicount from myvmall where (Active_os_version='Android 4.4.3' and Active_emui_version='EmotionUI_2.3')or (Active_os_version='Android 4.4.2' and Active_emui_version='EmotionUI_2.2')") ({
checkAnswer(
sql("select count(imei) as imeicount from myvmall where (Active_os_version='Android 4.4.3' and Active_emui_version='EmotionUI_2.3')or (Active_os_version='Android 4.4.2' and Active_emui_version='EmotionUI_2.2')"),
Seq(Row(2)))
})

//VMALL_Per_TC_B015
test("SELECT product, count(distinct imei) DistinctCount_imei FROM myvmall GROUP BY product ORDER BY product ASC") ({
checkAnswer(
sql("SELECT product, count(distinct imei) DistinctCount_imei FROM myvmall GROUP BY product ORDER BY product ASC"),
Seq())
})

//VMALL_Per_TC_B016
test("SELECT Active_emui_version, product, product_desc, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from myvmall) SUB_QRY GROUP BY Active_emui_version, product, product_desc ORDER BY Active_emui_version ASC, product ASC, product_desc ASC") ({
validateResult(sql("SELECT Active_emui_version, product, product_desc, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from myvmall) SUB_QRY GROUP BY Active_emui_version, product, product_desc ORDER BY Active_emui_version ASC, product ASC, product_desc ASC"), "VMALL_Per_TC_B016.csv")
})

//VMALL_Per_TC_B017
test("SELECT product, count(distinct imei) DistinctCount_imei FROM (select * from myvmall) SUB_QRY where product='SmartPhone_3998' GROUP BY product ORDER BY product ASC") ({
checkAnswer(
sql("SELECT product, count(distinct imei) DistinctCount_imei FROM (select * from myvmall) SUB_QRY where product='SmartPhone_3998' GROUP BY product ORDER BY product ASC"),
Seq(Row("SmartPhone_3998",1)))
})

//VMALL_Per_TC_B018
test("SELECT Active_emui_version FROM (select * from myvmall) SUB_QRY GROUP BY Active_emui_version ORDER BY Active_emui_version ASC") ({
validateResult(sql("SELECT Active_emui_version FROM (select * from myvmall) SUB_QRY GROUP BY Active_emui_version ORDER BY Active_emui_version ASC"), "VMALL_Per_TC_B018.csv")
})

//VMALL_Per_TC_B019
test("SELECT product FROM (select * from myvmall) SUB_QRY GROUP BY product ORDER BY product ASC") ({
validateResult(sql("SELECT product FROM (select * from myvmall) SUB_QRY GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_B019.csv")
})

//VMALL_Per_TC_B020
test("SELECT product, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM (select * from myvmall) SUB_QRY GROUP BY product ORDER BY product ASC") ({
validateResult(sql("SELECT product, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM (select * from myvmall) SUB_QRY GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_B020.csv")
})

//VMALL_Per_TC_015
test("SELECT product, count(distinct imei) DistinctCount_imei FROM    myvmall    GROUP BY product ORDER BY product ASC") ({
checkAnswer(
sql("SELECT product, count(distinct imei) DistinctCount_imei FROM    myvmall    GROUP BY product ORDER BY product ASC"),
Seq())
})

//VMALL_Per_TC_016
test("SELECT Active_emui_version, product, product_desc, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from    myvmall   ) SUB_QRY GROUP BY Active_emui_version, product, product_desc ORDER BY Active_emui_version ASC, product ASC, product_desc ASC") ({
validateResult(sql("SELECT Active_emui_version, product, product_desc, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from    myvmall   ) SUB_QRY GROUP BY Active_emui_version, product, product_desc ORDER BY Active_emui_version ASC, product ASC, product_desc ASC"), "VMALL_Per_TC_016.csv")
})

//VMALL_Per_TC_017
test("SELECT product, count(distinct imei) DistinctCount_imei FROM (select * from    myvmall   ) SUB_QRY where product='SmartPhone_3998' GROUP BY product ORDER BY product ASC") ({
validateResult(sql("SELECT product, count(distinct imei) DistinctCount_imei FROM (select * from    myvmall   ) SUB_QRY where product='SmartPhone_3998' GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_017.csv")
})

//VMALL_Per_TC_018
test("SELECT Active_emui_version FROM (select * from    myvmall   ) SUB_QRY GROUP BY Active_emui_version ORDER BY Active_emui_version ASC") ({
validateResult(sql("SELECT Active_emui_version FROM (select * from    myvmall   ) SUB_QRY GROUP BY Active_emui_version ORDER BY Active_emui_version ASC"), "VMALL_Per_TC_018.csv")
})

//VMALL_Per_TC_019
test("SELECT product FROM (select * from    myvmall   ) SUB_QRY GROUP BY product ORDER BY product ASC") ({
validateResult(sql("SELECT product FROM (select * from    myvmall   ) SUB_QRY GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_019.csv")
})

//VMALL_Per_TC_020
test("SELECT product, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM (select * from    myvmall   ) SUB_QRY GROUP BY product ORDER BY product ASC") ({
validateResult(sql("SELECT product, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM (select * from    myvmall   ) SUB_QRY GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_020.csv")
})

//VMALL_Per_TC_021
test("SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmall   ) SUB_QRY where device_name='Honor63011'  and product_name='Huawei3011'") ({
checkAnswer(
sql("SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmall   ) SUB_QRY where device_name='Honor63011'  and product_name='Huawei3011'"),
Seq(Row("imeiA009863011","Honor63011")))
})

//VMALL_Per_TC_022
test("SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmall   ) SUB_QRY where imei='imeiA009863011' or imei='imeiA009863012'") ({
checkAnswer(
sql("SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmall   ) SUB_QRY where imei='imeiA009863011' or imei='imeiA009863012'"),
Seq(Row("imeiA009863011","Honor63011"),Row("imeiA009863012","Honor63012")))
})

//VMALL_Per_TC_023
test("SELECT  count(imei) as distinct_imei,series FROM (select * from    myvmall   ) SUB_QRY where series LIKE 'series1%' group by series") ({
validateResult(sql("SELECT  count(imei) as distinct_imei,series FROM (select * from    myvmall   ) SUB_QRY where series LIKE 'series1%' group by series"), "VMALL_Per_TC_023.csv")
})

//VMALL_Per_TC_024
test("select product_name, count(distinct imei)  as imei_number from     myvmall    where imei='imeiA009863017' group by product_name") ({
checkAnswer(
sql("select product_name, count(distinct imei)  as imei_number from     myvmall    where imei='imeiA009863017' group by product_name"),
Seq(Row("Huawei3017",1)))
})

//VMALL_Per_TC_025
test("select product_name, count(distinct imei)  as imei_number from     myvmall     where deliveryAreaId ='500280121000000_9863017' group by product_name order by imei_number desc") ({
checkAnswer(
sql("select product_name, count(distinct imei)  as imei_number from     myvmall     where deliveryAreaId ='500280121000000_9863017' group by product_name order by imei_number desc"),
Seq(Row("Huawei3017",1)))
})

//VMALL_Per_TC_026
test("select deliveryCity, count(distinct imei)  as imei_number from     myvmall     where deliveryCity='deliveryCity17' group by deliveryCity order by imei_number desc") ({
checkAnswer(
sql("select deliveryCity, count(distinct imei)  as imei_number from     myvmall     where deliveryCity='deliveryCity17' group by deliveryCity order by imei_number desc"),
Seq(Row("deliveryCity17",2)))
})

//VMALL_Per_TC_027
test("select device_color, count(distinct imei)  as imei_number from     myvmall     where bom='51090576_63017' group by device_color order by imei_number desc") ({
checkAnswer(
sql("select device_color, count(distinct imei)  as imei_number from     myvmall     where bom='51090576_63017' group by device_color order by imei_number desc"),
Seq(Row("black3017",1)))
})

//VMALL_Per_TC_028
test("select product_name, count(distinct imei)  as imei_number from     myvmall     where product_name='Huawei3017' group by product_name order by imei_number desc") ({
checkAnswer(
sql("select product_name, count(distinct imei)  as imei_number from     myvmall     where product_name='Huawei3017' group by product_name order by imei_number desc"),
Seq(Row("Huawei3017",1)))
})

//VMALL_Per_TC_029
test("select product_name, count(distinct imei)  as imei_number from     myvmall     where deliveryprovince='Province_17' group by product_name order by imei_number desc") ({
checkAnswer(
sql("select product_name, count(distinct imei)  as imei_number from     myvmall     where deliveryprovince='Province_17' group by product_name order by imei_number desc"),
Seq(Row("Huawei3017",1),Row("Huawei3517",1)))
})

//VMALL_Per_TC_030
test("select rom,cpu_clock, count(distinct imei)  as imei_number from     myvmall     where  deliveryprovince='Province_17' group by rom,cpu_clock order by imei_number desc") ({
checkAnswer(
sql("select rom,cpu_clock, count(distinct imei)  as imei_number from     myvmall     where  deliveryprovince='Province_17' group by rom,cpu_clock order by imei_number desc"),
Seq(Row("517_GB","cpu_clock517",1),Row("17_GB","cpu_clock17",1)))
})

//VMALL_Per_TC_031
test("select uuid,mac,device_color,count(distinct imei) from    myvmall    where  imei='imeiA009863017' and deliveryareaid='500280121000000_9863017' group by uuid,mac,device_color") ({
checkAnswer(
sql("select uuid,mac,device_color,count(distinct imei) from    myvmall    where  imei='imeiA009863017' and deliveryareaid='500280121000000_9863017' group by uuid,mac,device_color"),
Seq(Row("uuidA009863017","MAC09863017","black3017",1)))
})

//VMALL_Per_TC_032
test("select device_color,count(distinct imei)as imei_number  from     myvmall   where product_name='Huawei3987' and Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863987' group by device_color order by imei_number desc") ({
checkAnswer(
sql("select device_color,count(distinct imei)as imei_number  from     myvmall   where product_name='Huawei3987' and Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863987' group by device_color order by imei_number desc"),
Seq(Row("black3987",1)))
})

//VMALL_Per_TC_033
test("select product_name,device_color, count(distinct imei) as imei_number from  myvmall  where product_name='Huawei3993' and Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863993' group by product_name,device_color order by imei_number desc") ({
checkAnswer(
sql("select product_name,device_color, count(distinct imei) as imei_number from  myvmall  where product_name='Huawei3993' and Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863993' group by product_name,device_color order by imei_number desc"),
Seq(Row("Huawei3993","black3993",1)))
})

//VMALL_Per_TC_034
test("select device_color, count(distinct imei) as imei_number from  myvmall  where product_name='Huawei3972' and deliveryprovince='Province_472' group by device_color order by imei_number desc") ({
checkAnswer(
sql("select device_color, count(distinct imei) as imei_number from  myvmall  where product_name='Huawei3972' and deliveryprovince='Province_472' group by device_color order by imei_number desc"),
Seq(Row("black3972",1)))
})

//VMALL_Per_TC_035
test("select product_name,device_color, count(distinct imei) as imei_number from  myvmall  where product_name='Huawei3972' and deliveryprovince='Province_472' group by product_name,device_color order by imei_number desc") ({
checkAnswer(
sql("select product_name,device_color, count(distinct imei) as imei_number from  myvmall  where product_name='Huawei3972' and deliveryprovince='Province_472' group by product_name,device_color order by imei_number desc"),
Seq(Row("Huawei3972","black3972",1)))
})

//VMALL_Per_TC_036
test("select product_name,device_color, count(distinct imei) as imei_number from  myvmall  where product_name='Huawei3987' and deliveryprovince='Province_487' and deliverycity='deliveryCity487' group by product_name,device_color order by imei_number desc") ({
checkAnswer(
sql("select product_name,device_color, count(distinct imei) as imei_number from  myvmall  where product_name='Huawei3987' and deliveryprovince='Province_487' and deliverycity='deliveryCity487' group by product_name,device_color order by imei_number desc"),
Seq(Row("Huawei3987","black3987",1)))
})

//VMALL_Per_TC_037
test("select product_name,device_color, count(distinct imei) as imei_number from  myvmall  where product_name='Huawei3987' and deliveryprovince='Province_487' and deliverycity='deliveryCity487' and device_color='black3987' group by product_name,device_color order by imei_number desc") ({
checkAnswer(
sql("select product_name,device_color, count(distinct imei) as imei_number from  myvmall  where product_name='Huawei3987' and deliveryprovince='Province_487' and deliverycity='deliveryCity487' and device_color='black3987' group by product_name,device_color order by imei_number desc"),
Seq(Row("Huawei3987","black3987",1)))
})

//VMALL_Per_TC_038
test("select Latest_network, count(distinct imei) as imei_number from  myvmall  group by Latest_network") ({
validateResult(sql("select Latest_network, count(distinct imei) as imei_number from  myvmall  group by Latest_network"), "VMALL_Per_TC_038.csv")
})

//VMALL_Per_TC_039
test("select device_name, count(distinct imei) as imei_number from  myvmall  group by device_name") ({
validateResult(sql("select device_name, count(distinct imei) as imei_number from  myvmall  group by device_name"), "VMALL_Per_TC_039.csv")
})


//VMALL_Per_TC_040
test("select product_name, count(distinct imei) as imei_number from  myvmall  group by product_name") ({
validateResult(sql("select product_name, count(distinct imei) as imei_number from  myvmall  group by product_name"), "VMALL_Per_TC_040.csv")
})

//VMALL_Per_TC_041
test("select deliverycity, count(distinct imei) as imei_number from  myvmall  group by deliverycity") ({
validateResult(sql("select deliverycity, count(distinct imei) as imei_number from  myvmall  group by deliverycity"), "VMALL_Per_TC_041")
})

//VMALL_Per_TC_042
test("select device_name, deliverycity,count(distinct imei) as imei_number from  myvmall  group by device_name,deliverycity") ({
validateResult(sql("select device_name, deliverycity,count(distinct imei) as imei_number from  myvmall  group by device_name,deliverycity"), "VMALL_Per_TC_042")
})

//VMALL_Per_TC_043
test("select product_name, device_name, count(distinct imei) as imei_number from  myvmall  group by product_name,device_name") ({
validateResult(sql("select product_name, device_name, count(distinct imei) as imei_number from  myvmall  group by product_name,device_name"), "VMALL_Per_TC_043.csv")
})

//VMALL_Per_TC_044
test("select product_name,deliverycity, count(distinct imei) as imei_number from  myvmall  group by deliverycity,product_name1") ({
validateResult(sql("select product_name,deliverycity, count(distinct imei) as imei_number from  myvmall  group by deliverycity,product_name"), "VMALL_Per_TC_044.csv")
})

//VMALL_Per_TC_045
test("select product_name,deliverycity, count(distinct imei) as imei_number from  myvmall  group by deliverycity,product_name") ({
validateResult(sql("select product_name,deliverycity, count(distinct imei) as imei_number from  myvmall  group by deliverycity,product_name"), "VMALL_Per_TC_045.csv")
})

//VMALL_Per_TC_046
test("select check_day,check_hour, count(distinct imei) as imei_number from  myvmall  group by check_day,check_hour") ({
checkAnswer(
sql("select check_day,check_hour, count(distinct imei) as imei_number from  myvmall  group by check_day,check_hour"),
Seq(Row(15,-1,1000),Row(null,null,1)))
})

//VMALL_Per_TC_047
test("select device_color,product_name, count(distinct imei) as imei_number from  myvmall  group by device_color,product_name order by product_name limit 1000") ({
validateResult(sql("select device_color,product_name, count(distinct imei) as imei_number from  myvmall  group by device_color,product_name order by product_name limit 1000"), "VMALL_Per_TC_047.csv")
})

//VMALL_Per_TC_048
test("select packing_hour,deliveryCity,device_color,count(distinct imei) as imei_number from  myvmall  group by packing_hour,deliveryCity,device_color order by deliveryCity  limit 1000") ({
validateResult(sql("select packing_hour,deliveryCity,device_color,count(distinct imei) as imei_number from  myvmall  group by packing_hour,deliveryCity,device_color order by deliveryCity  limit 1000"), "VMALL_Per_TC_048.csv")
})

//VMALL_Per_TC_049
test("SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmall  GROUP BY product_name ORDER BY product_name ASC") ({
checkAnswer(
sql("SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmall  GROUP BY product_name ORDER BY product_name ASC"),
Seq())
})

//VMALL_Per_TC_050
test("SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmall  SUB_QRY where product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC") ({
checkAnswer(
sql("SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmall  SUB_QRY where product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC"),
Seq(Row("Huawei3987",1)))
})

//VMALL_Per_TC_051
test("SELECT device_color, product_name, COUNT(DISTINCT imei) AS DistinctCount_imei FROM  myvmall  GROUP BY device_color, product_name ORDER BY device_color ASC, product_name ASC") ({
checkAnswer(
sql("SELECT device_color, product_name, COUNT(DISTINCT imei) AS DistinctCount_imei FROM  myvmall  GROUP BY device_color, product_name ORDER BY device_color ASC, product_name ASC"),
Seq())
})

//VMALL_Per_TC_052
test("SELECT product_name, count(distinct imei) DistinctCount_imei from  myvmall  where product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC") ({
checkAnswer(
sql("SELECT product_name, count(distinct imei) DistinctCount_imei from  myvmall  where product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC"),
Seq(Row("Huawei3987",1)))
})

//VMALL_Per_TC_053
test("SELECT product_name FROM  myvmall  SUB_QRY GROUP BY product_name ORDER BY product_name ASC") ({
checkAnswer(
sql("SELECT product_name FROM  myvmall  SUB_QRY GROUP BY product_name ORDER BY product_name ASC"),
Seq())
})

//VMALL_Per_TC_054
test("SELECT product_name, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM  myvmall  GROUP BY product_name ORDER BY product_name ASC") ({
checkAnswer(
sql("SELECT product_name, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM  myvmall  GROUP BY product_name ORDER BY product_name ASC"),
Seq())
})*/


}