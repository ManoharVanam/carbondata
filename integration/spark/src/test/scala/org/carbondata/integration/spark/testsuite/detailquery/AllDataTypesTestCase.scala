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
 * Test Class for detailed query on multiple datatypes
 * @author N00902756
 *
 */

class AllDataTypesTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
//    sql("CREATE CUBE alldatatypescube DIMENSIONS (empno Integer, empname String, designation String, doj Timestamp, workgroupcategory Integer, workgroupcategoryname String, deptno Integer, deptname String, projectcode Integer, projectjoindate Timestamp, projectenddate Timestamp) MEASURES (attendance Integer,utilization Integer,salary Integer) OPTIONS (PARTITIONER [PARTITION_COUNT=1])")
//    sql("LOAD DATA fact from './src/test/resources/data.csv' INTO CUBE alldatatypescube PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')");
  
    sql("create cube Carbon_automation_test dimensions(imei string,deviceInformationId integer,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string)  measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , PARTITION_COUNT=2] )");
  sql("LOAD DATA FACT FROM  './src/test/resources/100_olap.csv' INTO Cube Carbon_automation_test partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')");

  }
  
    override def afterAll {
//    sql("drop cube alldatatypescube")
     sql("drop cube Carbon_automation_test")

  }

/*  test("select empno,empname,utilization,count(salary),sum(empno) from alldatatypescube where empname in ('arvind','ayushi') group by empno,empname,utilization") {
    checkAnswer(
      sql("select empno,empname,utilization,count(salary),sum(empno) from alldatatypescube where empname in ('arvind','ayushi') group by empno,empname,utilization"),
      Seq(Row(11, "arvind", 96.2, 1, 11), Row(15, "ayushi", 91.5, 1, 15)))
  }*/

    //Test-16
  test("select imei, Latest_DAY+ 10 as a  from Carbon_automation_test") {
   validateResult(sql("select imei, Latest_DAY+ 10 as a  from Carbon_automation_test"),"TC_016.csv");
  }
//Test-17
  test("select imei, gamePointId+ 10 as Total from Carbon_automation_test") {
   validateResult(sql("select imei, gamePointId+ 10 as Total from Carbon_automation_test"),"TC_017.csv");
  }
  //Test-18
    test("select imei, modelId+ 10 Total from Carbon_automation_test ") {
   validateResult(sql("select imei, modelId+ 10 Total from Carbon_automation_test "),"TC_018.csv");
  }
  //Test-19
    
        test("select imei, gamePointId+contractNumber as a  from Carbon_automation_test ") {
  validateResult(sql("select imei, gamePointId+contractNumber as a  from Carbon_automation_test "),"TC_019.csv");
  }
       
       //Test-20 
test("select imei, deviceInformationId+gamePointId as Total from Carbon_automation_test") {
    validateResult(sql("select imei, deviceInformationId+gamePointId as Total from Carbon_automation_test"),"TC_020.csv");
  }
  //Test-21
  test("select imei, deviceInformationId+deviceInformationId Total from Carbon_automation_test") {
  validateResult(sql("select imei, deviceInformationId+deviceInformationId Total from Carbon_automation_test"),"TC_021.csv");
  }
  
  //TC_477
test("select percentile(1,array(1)) from Carbon_automation_test") ({
  
  validateResult(sql("select percentile(1,array(1)) from Carbon_automation_test"),"TC_477.csv");

})
  
  
//TC_479
test("select percentile(1,array('0.5')) from Carbon_automation_test") ({
  validateResult(sql("select percentile(1,array('0.5')) from Carbon_automation_test"),"TC_479.csv");

})

//TC_480
test("select percentile(1,array('1')) from Carbon_automation_test") ({
  validateResult(sql("select percentile(1,array('1')) from Carbon_automation_test"),"TC_480.csv");

})

//TC_485
test("select percentile_approx(1,array(0.5),5000) from Carbon_automation_test") ({
  validateResult(sql("select percentile_approx(1,array(0.5),5000) from Carbon_automation_test"),"TC_485.csv");

})

//TC_486
test("select percentile_approx(1,array(0.5),5000) from Carbon_automation_test22") ({
  validateResult(sql("select percentile_approx(1,array(0.5),5000) from Carbon_automation_test"),"TC_486.csv");

})

//TC_487
test("select histogram_numeric(1, 5000)from Carbon_automation_test") ({
  validateResult(sql("select histogram_numeric(1, 5000)from Carbon_automation_test"),"TC_487.csv");

})

//TC_488
test("select histogram_numeric(1, 1000)from Carbon_automation_test") ({

})

//TC_489
test("select histogram_numeric(1, 500)from Carbon_automation_test2") ({
  validateResult(sql("select histogram_numeric(1, 500)from Carbon_automation_test"),"TC_489.csv");

})

//TC_490
test("select histogram_numeric(1, 500)from Carbon_automation_test3") ({
  validateResult(sql("select histogram_numeric(1, 500)from Carbon_automation_test"),"TC_490.csv");

})

    test("select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a from Carbon_automation_test") 
  {
    checkAnswer(
      sql("select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a from Carbon_automation_test"),
      Seq(Row(1.0, 100006.6, 100016.4, 1000000.0)))
      
//     validateResult(sql("select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a from Carbon_automation_test"),"TC_112.csv");
  }


}