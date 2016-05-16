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

package org.carbondata.integration.spark.testsuite.allqueries

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.carbondata.core.util.CarbonProperties
import org.scalatest.BeforeAndAfterAll

/**
  * Test Class for all query on multiple datatypes
  * Manohar
  */
class AllDataTypesTestCase3 extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    CarbonProperties.getInstance().addProperty("carbon.direct.surrogate","false")

    sql("create cube Carbon_automation_test dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string)  measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , PARTITION_COUNT=2] )");
    sql("LOAD DATA FACT FROM'"+currentDirectory+"/src/test/resources/100_olap.csv' INTO Cube Carbon_automation_test partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')");
  }
  override def afterAll {
    sql("drop cube Carbon_automation_test")

  }
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
}