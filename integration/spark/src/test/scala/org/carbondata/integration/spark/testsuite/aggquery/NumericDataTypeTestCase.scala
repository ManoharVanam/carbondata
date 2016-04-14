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
 * Test Class for aggregate query on Numeric datatypes
 * @author N00902756
 *
 */
class NumericDataTypeTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    //sql("CREATE CUBE doubletype DIMENSIONS (utilization Numeric,salary Numeric) OPTIONS (PARTITIONER [PARTITION_COUNT=1])")
    //sql("LOAD DATA fact from './src/test/resources/data.csv' INTO CUBE doubletype PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')")
  
 sql("create cube myvmallTest dimensions(imei String,uuid String,MAC String,device_color String,device_shell_color String,device_name String,product_name String,ram String,rom String,cpu_clock String,series String,check_date String,check_year Integer,check_month Integer ,check_day Integer,check_hour Integer,bom String,inside_name String,packing_date String,packing_year String,packing_month String,packing_day String,packing_hour String,customer_name String,deliveryAreaId String,deliveryCountry String,deliveryProvince String,deliveryCity String,deliveryDistrict String,packing_list_no String,order_no String,Active_check_time String,Active_check_year Integer,Active_check_month Integer,Active_check_day Integer,Active_check_hour Integer,ActiveAreaId String,ActiveCountry String,ActiveProvince String,Activecity String,ActiveDistrict String,Active_network String,Active_firmware_version String,Active_emui_version String,Active_os_version String,Latest_check_time String,Latest_check_year Integer,Latest_check_month Integer,Latest_check_day Integer,Latest_check_hour Integer,Latest_areaId String,Latest_country String,Latest_province String,Latest_city String,Latest_district String,Latest_firmware_version String,Latest_emui_version String,Latest_os_version String,Latest_network String,site String,site_desc String,product String,product_desc String) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=3] )")
    sql("LOAD DATA fact from './src/test/resources/100_VMALL_1_Day_DATA_2015-09-15.csv' INTO CUBE myvmallTest PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"', FILEHEADER 'imei,uuid,MAC,device_color,device_shell_color,device_name,product_name,ram,rom,cpu_clock,series,check_date,check_year,check_month,check_day,check_hour,bom,inside_name,packing_date,packing_year,packing_month,packing_day,packing_hour,customer_name,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,packing_list_no,order_no,Active_check_time,Active_check_year,Active_check_month,Active_check_day,Active_check_hour,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,Active_network,Active_firmware_version,Active_emui_version,Active_os_version,Latest_check_time,Latest_check_year,Latest_check_month,Latest_check_day,Latest_check_hour,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_firmware_version,Latest_emui_version,Latest_os_version,Latest_network,site,site_desc,product,product_desc')")

  }
  
    override def afterAll {
//    sql("drop cube doubletype")
     sql("drop cube myvmallTest")

  }

/*  test("select utilization from doubletype") {
    checkAnswer(
      sql("select utilization from doubletype"),
      Seq(Row(96.2), Row(95.1), Row(99.0), Row(92.2), Row(91.5),
        Row(93.0), Row(97.45), Row(98.23), Row(91.678), Row(94.22)))
  }*/
  
  //VMALL_Per_TC_000
test("select count(*) from    myvmallTest") ({
checkAnswer(
sql("select count(*) from    myvmallTest"),
Seq(Row(1003)))
})

//VMALL_Per_TC_001
test("SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY GROUP BY product_name ORDER BY product_name ASC") ({
validateResult(sql("SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY GROUP BY product_name ORDER BY product_name ASC"), "VMALL_Per_TC_001.csv")

})

//VMALL_Per_TC_002
test("SELECT device_name, product, product_name, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY GROUP BY device_name, product, product_name ORDER BY device_name ASC, product ASC, product_name ASC") ({
validateResult(sql("SELECT device_name, product, product_name, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY GROUP BY device_name, product, product_name ORDER BY device_name ASC, product ASC, product_name ASC"), "VMALL_Per_TC_002.csv")
})

//VMALL_Per_TC_003
test("SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY where product_name='Huawei4009' GROUP BY product_name ORDER BY  product_name ASC") ({
checkAnswer(
sql("SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY where product_name='Huawei4009' GROUP BY product_name ORDER BY  product_name ASC"),
Seq(Row("Huawei4009",1)))
})

//VMALL_Per_TC_004
test("SELECT device_color FROM (select * from myvmallTest) SUB_QRY GROUP BY device_color ORDER BY device_color ASC") ({
validateResult(sql("SELECT device_color FROM (select * from myvmallTest) SUB_QRY GROUP BY device_color ORDER BY device_color ASC"), "VMALL_Per_TC_004.csv")
})

//VMALL_Per_TC_005
test("SELECT product_name  FROM (select * from myvmallTest) SUB_QRY GROUP BY product_name ORDER BY  product_name ASC") ({
validateResult(sql("SELECT product_name  FROM (select * from myvmallTest) SUB_QRY GROUP BY product_name ORDER BY  product_name ASC"), "VMALL_Per_TC_005.csv")
})

//VMALL_Per_TC_006
test("SELECT product, COUNT(DISTINCT packing_list_no) AS LONG_COL_0 FROM (select * from myvmallTest) SUB_QRY GROUP BY product ORDER BY product ASC") ({
validateResult(sql("SELECT product, COUNT(DISTINCT packing_list_no) AS LONG_COL_0 FROM (select * from myvmallTest) SUB_QRY GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_006.csv")
})

//VMALL_Per_TC_007
test("select count(distinct imei) DistinctCount_imei from myvmallTest") ({
checkAnswer(
sql("select count(distinct imei) DistinctCount_imei from myvmallTest"),
Seq(Row(1001)))
})

//VMALL_Per_TC_008
test("Select count(imei),deliveryCountry  from myvmallTest group by deliveryCountry order by deliveryCountry asc") ({
validateResult(sql("Select count(imei),deliveryCountry  from myvmallTest group by deliveryCountry order by deliveryCountry asc"), "VMALL_Per_TC_008.csv")
})

//VMALL_Per_TC_009
test("select (t1.hnor6emui/t2.totalc)*100 from (select count (Active_emui_version)  as hnor6emui from myvmallTest where Active_emui_version=\"EmotionUI_2.1\")t1,(select count(Active_emui_version) as totalc from myvmallTest)t2") ({
checkAnswer(
sql("select (t1.hnor6emui/t2.totalc)*100 from (select count (Active_emui_version)  as hnor6emui from myvmallTest where Active_emui_version=\"EmotionUI_2.1\")t1,(select count(Active_emui_version) as totalc from myvmallTest)t2"),
Seq(Row(0.09970089730807577)))
})

//VMALL_Per_TC_010
test("select (t1.hnor4xi/t2.totalc)*100 from (select count (imei)  as hnor4xi from myvmallTest where device_name=\"Honor2\")t1,(select count (imei) as totalc from myvmallTest)t2") ({
checkAnswer(
sql("select (t1.hnor4xi/t2.totalc)*100 from (select count (imei)  as hnor4xi from myvmallTest where device_name=\"Honor2\")t1,(select count (imei) as totalc from myvmallTest)t2"),
Seq(Row(0.0)))
})

//VMALL_Per_TC_011
test("select count(imei) from (select DATEDIFF(from_unixtime(unix_timestamp()),packing_date) mydates,imei from myvmallTest) sub where mydates<1000") ({
checkAnswer(
sql("select count(imei) from (select DATEDIFF(from_unixtime(unix_timestamp()),packing_date) mydates,imei from myvmallTest) sub where mydates<1000"),
Seq(Row(1000)))
})

//VMALL_Per_TC_012
test("SELECT Active_os_version, count(distinct imei) DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY GROUP BY Active_os_version ORDER BY Active_os_version ASC") ({
validateResult(sql("SELECT Active_os_version, count(distinct imei) DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY GROUP BY Active_os_version ORDER BY Active_os_version ASC"), "VMALL_Per_TC_012.csv")
})

//VMALL_Per_TC_013
test("select count(imei)  DistinctCount_imei from myvmallTest where (Active_emui_version=\"EmotionUI_2.972\" and Latest_emui_version=\"EmotionUI_3.863972\") OR (Active_emui_version=\"EmotionUI_2.843\" and Latest_emui_version=\"EmotionUI_3.863843\")") ({
checkAnswer(
sql("select count(imei)  DistinctCount_imei from myvmallTest where (Active_emui_version=\"EmotionUI_2.972\" and Latest_emui_version=\"EmotionUI_3.863972\") OR (Active_emui_version=\"EmotionUI_2.843\" and Latest_emui_version=\"EmotionUI_3.863843\")"),
Seq(Row(2)))
})

//VMALL_Per_TC_014
test("select count(imei) as imeicount from myvmallTest where (Active_os_version='Android 4.4.3' and Active_emui_version='EmotionUI_2.3')or (Active_os_version='Android 4.4.2' and Active_emui_version='EmotionUI_2.2')") ({
checkAnswer(
sql("select count(imei) as imeicount from myvmallTest where (Active_os_version='Android 4.4.3' and Active_emui_version='EmotionUI_2.3')or (Active_os_version='Android 4.4.2' and Active_emui_version='EmotionUI_2.2')"),
Seq(Row(2)))
})

//VMALL_Per_TC_B015
test("SELECT product, count(distinct imei) DistinctCount_imei FROM myvmallTest GROUP BY product ORDER BY product ASC") ({
validateResult(sql("SELECT product, count(distinct imei) DistinctCount_imei FROM myvmallTest GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_B015.csv")
})

//VMALL_Per_TC_B016
test("SELECT Active_emui_version, product, product_desc, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY GROUP BY Active_emui_version, product, product_desc ORDER BY Active_emui_version ASC, product ASC, product_desc ASC") ({
validateResult(sql("SELECT Active_emui_version, product, product_desc, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY GROUP BY Active_emui_version, product, product_desc ORDER BY Active_emui_version ASC, product ASC, product_desc ASC"), "VMALL_Per_TC_B016.csv")
})

//VMALL_Per_TC_B017
test("SELECT product, count(distinct imei) DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY where product='SmartPhone_3998' GROUP BY product ORDER BY product ASC") ({
checkAnswer(
sql("SELECT product, count(distinct imei) DistinctCount_imei FROM (select * from myvmallTest) SUB_QRY where product='SmartPhone_3998' GROUP BY product ORDER BY product ASC"),
Seq(Row("SmartPhone_3998",1)))
})

//VMALL_Per_TC_B018
test("SELECT Active_emui_version FROM (select * from myvmallTest) SUB_QRY GROUP BY Active_emui_version ORDER BY Active_emui_version ASC") ({
validateResult(sql("SELECT Active_emui_version FROM (select * from myvmallTest) SUB_QRY GROUP BY Active_emui_version ORDER BY Active_emui_version ASC"), "VMALL_Per_TC_B018.csv")
})

//VMALL_Per_TC_B019
test("SELECT product FROM (select * from myvmallTest) SUB_QRY GROUP BY product ORDER BY product ASC") ({
validateResult(sql("SELECT product FROM (select * from myvmallTest) SUB_QRY GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_B019.csv")
})

//VMALL_Per_TC_B020
test("SELECT product, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM (select * from myvmallTest) SUB_QRY GROUP BY product ORDER BY product ASC") ({
validateResult(sql("SELECT product, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM (select * from myvmallTest) SUB_QRY GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_B020.csv")
})

//duplicate
/*//VMALL_Per_TC_015
test("SELECT product, count(distinct imei) DistinctCount_imei FROM    myvmallTest    GROUP BY product ORDER BY product ASC") ({
checkAnswer(
sql("SELECT product, count(distinct imei) DistinctCount_imei FROM    myvmallTest    GROUP BY product ORDER BY product ASC"),
Seq())
})

//VMALL_Per_TC_016
test("SELECT Active_emui_version, product, product_desc, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from    myvmallTest   ) SUB_QRY GROUP BY Active_emui_version, product, product_desc ORDER BY Active_emui_version ASC, product ASC, product_desc ASC") ({
validateResult(sql("SELECT Active_emui_version, product, product_desc, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from    myvmallTest   ) SUB_QRY GROUP BY Active_emui_version, product, product_desc ORDER BY Active_emui_version ASC, product ASC, product_desc ASC"), "VMALL_Per_TC_016.csv")
})

//VMALL_Per_TC_017
test("SELECT product, count(distinct imei) DistinctCount_imei FROM (select * from    myvmallTest   ) SUB_QRY where product='SmartPhone_3998' GROUP BY product ORDER BY product ASC") ({
validateResult(sql("SELECT product, count(distinct imei) DistinctCount_imei FROM (select * from    myvmallTest   ) SUB_QRY where product='SmartPhone_3998' GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_017.csv")
})

//VMALL_Per_TC_018
test("SELECT Active_emui_version FROM (select * from    myvmallTest   ) SUB_QRY GROUP BY Active_emui_version ORDER BY Active_emui_version ASC") ({
validateResult(sql("SELECT Active_emui_version FROM (select * from    myvmallTest   ) SUB_QRY GROUP BY Active_emui_version ORDER BY Active_emui_version ASC"), "VMALL_Per_TC_018.csv")
})

//VMALL_Per_TC_019
test("SELECT product FROM (select * from    myvmallTest   ) SUB_QRY GROUP BY product ORDER BY product ASC") ({
validateResult(sql("SELECT product FROM (select * from    myvmallTest   ) SUB_QRY GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_019.csv")
})

//VMALL_Per_TC_020
test("SELECT product, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM (select * from    myvmallTest   ) SUB_QRY GROUP BY product ORDER BY product ASC") ({
validateResult(sql("SELECT product, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM (select * from    myvmallTest   ) SUB_QRY GROUP BY product ORDER BY product ASC"), "VMALL_Per_TC_020.csv")
})*/

//VMALL_Per_TC_021
test("SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmallTest   ) SUB_QRY where device_name='Honor63011'  and product_name='Huawei3011'") ({
checkAnswer(
sql("SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmallTest   ) SUB_QRY where device_name='Honor63011'  and product_name='Huawei3011'"),
Seq(Row("imeiA009863011","Honor63011")))
})

//VMALL_Per_TC_022
test("SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmallTest   ) SUB_QRY where imei='imeiA009863011' or imei='imeiA009863012'") ({
checkAnswer(
sql("SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmallTest   ) SUB_QRY where imei='imeiA009863011' or imei='imeiA009863012'"),
Seq(Row("imeiA009863011","Honor63011"),Row("imeiA009863012","Honor63012")))
})

//VMALL_Per_TC_023
test("SELECT  count(imei) as distinct_imei,series FROM (select * from    myvmallTest   ) SUB_QRY where series LIKE 'series1%' group by series") ({
validateResult(sql("SELECT  count(imei) as distinct_imei,series FROM (select * from    myvmallTest   ) SUB_QRY where series LIKE 'series1%' group by series"), "VMALL_Per_TC_023.csv")
})

//VMALL_Per_TC_024
test("select product_name, count(distinct imei)  as imei_number from     myvmallTest    where imei='imeiA009863017' group by product_name") ({
checkAnswer(
sql("select product_name, count(distinct imei)  as imei_number from     myvmallTest    where imei='imeiA009863017' group by product_name"),
Seq(Row("Huawei3017",1)))
})

//VMALL_Per_TC_025
test("select product_name, count(distinct imei)  as imei_number from     myvmallTest     where deliveryAreaId ='500280121000000_9863017' group by product_name order by imei_number desc") ({
checkAnswer(
sql("select product_name, count(distinct imei)  as imei_number from     myvmallTest     where deliveryAreaId ='500280121000000_9863017' group by product_name order by imei_number desc"),
Seq(Row("Huawei3017",1)))
})

//VMALL_Per_TC_026
test("select deliveryCity, count(distinct imei)  as imei_number from     myvmallTest     where deliveryCity='deliveryCity17' group by deliveryCity order by imei_number desc") ({
checkAnswer(
sql("select deliveryCity, count(distinct imei)  as imei_number from     myvmallTest     where deliveryCity='deliveryCity17' group by deliveryCity order by imei_number desc"),
Seq(Row("deliveryCity17",2)))
})

//VMALL_Per_TC_027
test("select device_color, count(distinct imei)  as imei_number from     myvmallTest     where bom='51090576_63017' group by device_color order by imei_number desc") ({
checkAnswer(
sql("select device_color, count(distinct imei)  as imei_number from     myvmallTest     where bom='51090576_63017' group by device_color order by imei_number desc"),
Seq(Row("black3017",1)))
})

//VMALL_Per_TC_028
test("select product_name, count(distinct imei)  as imei_number from     myvmallTest     where product_name='Huawei3017' group by product_name order by imei_number desc") ({
checkAnswer(
sql("select product_name, count(distinct imei)  as imei_number from     myvmallTest     where product_name='Huawei3017' group by product_name order by imei_number desc"),
Seq(Row("Huawei3017",1)))
})

//VMALL_Per_TC_029
test("select product_name, count(distinct imei)  as imei_number from     myvmallTest     where deliveryprovince='Province_17' group by product_name order by imei_number desc") ({
checkAnswer(
sql("select product_name, count(distinct imei)  as imei_number from     myvmallTest     where deliveryprovince='Province_17' group by product_name order by imei_number desc"),
Seq(Row("Huawei3017",1),Row("Huawei3517",1)))
})

//VMALL_Per_TC_030
test("select rom,cpu_clock, count(distinct imei)  as imei_number from     myvmallTest     where  deliveryprovince='Province_17' group by rom,cpu_clock order by imei_number desc") ({
checkAnswer(
sql("select rom,cpu_clock, count(distinct imei)  as imei_number from     myvmallTest     where  deliveryprovince='Province_17' group by rom,cpu_clock order by imei_number desc"),
Seq(Row("517_GB","cpu_clock517",1),Row("17_GB","cpu_clock17",1)))
})

//VMALL_Per_TC_031
test("select uuid,mac,device_color,count(distinct imei) from    myvmallTest    where  imei='imeiA009863017' and deliveryareaid='500280121000000_9863017' group by uuid,mac,device_color") ({
checkAnswer(
sql("select uuid,mac,device_color,count(distinct imei) from    myvmallTest    where  imei='imeiA009863017' and deliveryareaid='500280121000000_9863017' group by uuid,mac,device_color"),
Seq(Row("uuidA009863017","MAC09863017","black3017",1)))
})

//VMALL_Per_TC_032
test("select device_color,count(distinct imei)as imei_number  from     myvmallTest   where product_name='Huawei3987' and Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863987' group by device_color order by imei_number desc") ({
checkAnswer(
sql("select device_color,count(distinct imei)as imei_number  from     myvmallTest   where product_name='Huawei3987' and Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863987' group by device_color order by imei_number desc"),
Seq(Row("black3987",1)))
})

//VMALL_Per_TC_033
test("select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3993' and Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863993' group by product_name,device_color order by imei_number desc") ({
checkAnswer(
sql("select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3993' and Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863993' group by product_name,device_color order by imei_number desc"),
Seq(Row("Huawei3993","black3993",1)))
})

//VMALL_Per_TC_034
test("select device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3972' and deliveryprovince='Province_472' group by device_color order by imei_number desc") ({
checkAnswer(
sql("select device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3972' and deliveryprovince='Province_472' group by device_color order by imei_number desc"),
Seq(Row("black3972",1)))
})

//VMALL_Per_TC_035
test("select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3972' and deliveryprovince='Province_472' group by product_name,device_color order by imei_number desc") ({
checkAnswer(
sql("select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3972' and deliveryprovince='Province_472' group by product_name,device_color order by imei_number desc"),
Seq(Row("Huawei3972","black3972",1)))
})

//VMALL_Per_TC_036
test("select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3987' and deliveryprovince='Province_487' and deliverycity='deliveryCity487' group by product_name,device_color order by imei_number desc") ({
checkAnswer(
sql("select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3987' and deliveryprovince='Province_487' and deliverycity='deliveryCity487' group by product_name,device_color order by imei_number desc"),
Seq(Row("Huawei3987","black3987",1)))
})

//VMALL_Per_TC_037
test("select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3987' and deliveryprovince='Province_487' and deliverycity='deliveryCity487' and device_color='black3987' group by product_name,device_color order by imei_number desc") ({
checkAnswer(
sql("select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  where product_name='Huawei3987' and deliveryprovince='Province_487' and deliverycity='deliveryCity487' and device_color='black3987' group by product_name,device_color order by imei_number desc"),
Seq(Row("Huawei3987","black3987",1)))
})

//VMALL_Per_TC_038
test("select Latest_network, count(distinct imei) as imei_number from  myvmallTest  group by Latest_network") ({
validateResult(sql("select Latest_network, count(distinct imei) as imei_number from  myvmallTest  group by Latest_network"), "VMALL_Per_TC_038.csv")
})

//VMALL_Per_TC_039
test("select device_name, count(distinct imei) as imei_number from  myvmallTest  group by device_name") ({
validateResult(sql("select device_name, count(distinct imei) as imei_number from  myvmallTest  group by device_name"), "VMALL_Per_TC_039.csv")
})


//VMALL_Per_TC_040
test("select product_name, count(distinct imei) as imei_number from  myvmallTest  group by product_name") ({
validateResult(sql("select product_name, count(distinct imei) as imei_number from  myvmallTest  group by product_name"), "VMALL_Per_TC_040.csv")
})

//VMALL_Per_TC_041
test("select deliverycity, count(distinct imei) as imei_number from  myvmallTest  group by deliverycity") ({
validateResult(sql("select deliverycity, count(distinct imei) as imei_number from  myvmallTest  group by deliverycity"), "VMALL_Per_TC_041.csv")
})

//VMALL_Per_TC_042
test("select device_name, deliverycity,count(distinct imei) as imei_number from  myvmallTest  group by device_name,deliverycity") ({
validateResult(sql("select device_name, deliverycity,count(distinct imei) as imei_number from  myvmallTest  group by device_name,deliverycity"), "VMALL_Per_TC_042.csv")
})

//VMALL_Per_TC_043
test("select product_name, device_name, count(distinct imei) as imei_number from  myvmallTest  group by product_name,device_name") ({
validateResult(sql("select product_name, device_name, count(distinct imei) as imei_number from  myvmallTest  group by product_name,device_name"), "VMALL_Per_TC_043.csv")
})

//VMALL_Per_TC_044
test("select product_name,deliverycity, count(distinct imei) as imei_number from  myvmallTest  group by deliverycity,product_name1") ({
validateResult(sql("select product_name,deliverycity, count(distinct imei) as imei_number from  myvmallTest  group by deliverycity,product_name"), "VMALL_Per_TC_044.csv")
})

//VMALL_Per_TC_045
test("select product_name,deliverycity, count(distinct imei) as imei_number from  myvmallTest  group by deliverycity,product_name") ({
validateResult(sql("select product_name,deliverycity, count(distinct imei) as imei_number from  myvmallTest  group by deliverycity,product_name"), "VMALL_Per_TC_045.csv")
})

//VMALL_Per_TC_046
test("select check_day,check_hour, count(distinct imei) as imei_number from  myvmallTest  group by check_day,check_hour") ({
checkAnswer(
sql("select check_day,check_hour, count(distinct imei) as imei_number from  myvmallTest  group by check_day,check_hour"),
Seq(Row(15,-1,1000),Row(null,null,1)))
})

//VMALL_Per_TC_047
test("select device_color,product_name, count(distinct imei) as imei_number from  myvmallTest  group by device_color,product_name order by product_name limit 1000") ({
validateResult(sql("select device_color,product_name, count(distinct imei) as imei_number from  myvmallTest  group by device_color,product_name order by product_name limit 1000"), "VMALL_Per_TC_047.csv")
})

//VMALL_Per_TC_048
test("select packing_hour,deliveryCity,device_color,count(distinct imei) as imei_number from  myvmallTest  group by packing_hour,deliveryCity,device_color order by deliveryCity  limit 1000") ({
validateResult(sql("select packing_hour,deliveryCity,device_color,count(distinct imei) as imei_number from  myvmallTest  group by packing_hour,deliveryCity,device_color order by deliveryCity  limit 1000"), "VMALL_Per_TC_048.csv")
})

//VMALL_Per_TC_049
test("SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmallTest  GROUP BY product_name ORDER BY product_name ASC") ({
validateResult(sql("SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmallTest  GROUP BY product_name ORDER BY product_name ASC"), "VMALL_Per_TC_049.csv")
})

//VMALL_Per_TC_050
test("SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmallTest  SUB_QRY where product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC") ({
checkAnswer(
sql("SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmallTest  SUB_QRY where product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC"),
Seq(Row("Huawei3987",1)))
})

//VMALL_Per_TC_051
test("SELECT device_color, product_name, COUNT(DISTINCT imei) AS DistinctCount_imei FROM  myvmallTest  GROUP BY device_color, product_name ORDER BY device_color ASC, product_name ASC") ({
validateResult(sql("SELECT device_color, product_name, COUNT(DISTINCT imei) AS DistinctCount_imei FROM  myvmallTest  GROUP BY device_color, product_name ORDER BY device_color ASC, product_name ASC"), "VMALL_Per_TC_051.csv")
})

//VMALL_Per_TC_052
test("SELECT product_name, count(distinct imei) DistinctCount_imei from  myvmallTest  where product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC") ({
checkAnswer(
sql("SELECT product_name, count(distinct imei) DistinctCount_imei from  myvmallTest  where product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC"),
Seq(Row("Huawei3987",1)))
})

//VMALL_Per_TC_053
test("SELECT product_name FROM  myvmallTest  SUB_QRY GROUP BY product_name ORDER BY product_name ASC") ({
validateResult(sql("SELECT product_name FROM  myvmallTest  SUB_QRY GROUP BY product_name ORDER BY product_name ASC"), "VMALL_Per_TC_053.csv")
})

//VMALL_Per_TC_054
test("SELECT product_name, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM  myvmallTest  GROUP BY product_name ORDER BY product_name ASC") ({
validateResult(sql("SELECT product_name, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM  myvmallTest  GROUP BY product_name ORDER BY product_name ASC"), "VMALL_Per_TC_054.csv")
})



}