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

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for aggregate query on String datatypes
 * @author N00902756
 *
 */
class StringDataTypeTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
//    sql("CREATE CUBE stringtypecube DIMENSIONS (empname String, designation String, workgroupcategoryname String, deptname String) OPTIONS (PARTITIONER [PARTITION_COUNT=1])");
//    sql("LOAD DATA fact from './src/test/resources/data.csv' INTO CUBE stringtypecube PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')");
    sql("create cube IF NOT EXISTS traffic_2g_3g_4g dimensions(SOURCE_INFO String ,APP_CATEGORY_ID String ,APP_CATEGORY_NAME String ,APP_SUB_CATEGORY_ID String ,APP_SUB_CATEGORY_NAME String ,RAT_NAME String ,IMSI String ,OFFER_MSISDN String ,OFFER_ID String ,OFFER_OPTION_1 String ,OFFER_OPTION_2 String ,OFFER_OPTION_3 String ,MSISDN String ,PACKAGE_TYPE String ,PACKAGE_PRICE String ,TAG_IMSI String ,TAG_MSISDN String ,PROVINCE String ,CITY String ,AREA_CODE String ,TAC String ,IMEI String ,TERMINAL_TYPE String ,TERMINAL_BRAND String ,TERMINAL_MODEL String ,PRICE_LEVEL String ,NETWORK String ,SHIPPED_OS String ,WIFI String ,WIFI_HOTSPOT String ,GSM String ,WCDMA String ,TD_SCDMA String ,LTE_FDD String ,LTE_TDD String ,CDMA String ,SCREEN_SIZE String ,SCREEN_RESOLUTION String ,HOST_NAME String ,WEBSITE_NAME String ,OPERATOR String ,SRV_TYPE_NAME String ,TAG_HOST String ,CGI String ,CELL_NAME String ,COVERITY_TYPE1 String ,COVERITY_TYPE2 String ,COVERITY_TYPE3 String ,COVERITY_TYPE4 String ,COVERITY_TYPE5 String ,LATITUDE String ,LONGITUDE String ,AZIMUTH String ,TAG_CGI String ,APN String ,USER_AGENT String ,DAY String ,HOUR String ,`MIN` String ,IS_DEFAULT_BEAR integer ,EPS_BEARER_ID String ,QCI integer ,USER_FILTER String ,ANALYSIS_PERIOD String ) measures(UP_THROUGHPUT numeric,DOWN_THROUGHPUT numeric,UP_PKT_NUM numeric,DOWN_PKT_NUM numeric,APP_REQUEST_NUM numeric,PKT_NUM_LEN_1_64 numeric,PKT_NUM_LEN_64_128 numeric,PKT_NUM_LEN_128_256 numeric,PKT_NUM_LEN_256_512 numeric,PKT_NUM_LEN_512_768 numeric,PKT_NUM_LEN_768_1024 numeric,PKT_NUM_LEN_1024_ALL numeric,IP_FLOW_MARK numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (MSISDN) ,PARTITION_COUNT=3] )");
    sql("LOAD DATA fact from'" + currentDirectory + "/src/test/resources/FACT_UNITED_DATA_INFO_sample_cube.csv' INTO CUBE traffic_2g_3g_4g PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')");

  }

  override def afterAll {
//    sql("drop cube stringtypecube")
    sql("drop cube traffic_2g_3g_4g")

  }

/*  test("select empname from stringtypecube") {
    checkAnswer(
      sql("select empname from stringtypecube"),
      Seq(Row("arvind"), Row("krithin"), Row("madhan"), Row("anandh"), Row("ayushi"),
        Row("pramod"), Row("gawrav"), Row("sibi"), Row("shivani"), Row("bill")))
  }*/

  //SmartPCC_Perf_TC_001
  test("select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where TERMINAL_BRAND='HTC' and APP_CATEGORY_NAME='Web_Browsing' group by MSISDN")({
    checkAnswer(
      sql("select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where TERMINAL_BRAND='HTC' and APP_CATEGORY_NAME='Web_Browsing' group by MSISDN"),
      Seq(Row("8613649905753", 2381)))
  })

  //SmartPCC_Perf_TC_002
  test("select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where RAT_NAME='GERAN' group by MSISDN having total < 1073741824 order by total desc")({
    checkAnswer(
      sql("select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where RAT_NAME='GERAN' group by MSISDN having total < 1073741824 order by total desc"),
      Seq(Row("8613893462639", 2874640), Row("8613993676885", 73783), Row("8618394185970", 23865), Row("8618793100458", 15112), Row("8618794812876", 14411), Row("8615120474362", 6936), Row("8613893853351", 6486), Row("8618393012284", 5700), Row("8613993800024", 5044), Row("8618794965341", 4840), Row("8613993899110", 4364), Row("8613519003078", 2485), Row("8613649905753", 2381), Row("8613893600602", 2346), Row("8615117035070", 1310), Row("8618700943475", 1185), Row("8613919791668", 928), Row("8615209309657", 290), Row("8613993104233", 280)))
  })

  //SmartPCC_Perf_TC_003
  test("select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where RAT_NAME='GERAN' group by MSISDN having total > 23865 order by total desc")({
    checkAnswer(
      sql("select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where RAT_NAME='GERAN' group by MSISDN having total > 23865 order by total desc"),
      Seq(Row("8613893462639", 2874640), Row("8613993676885", 73783)))
  })

  //SmartPCC_Perf_TC_004
  test("select APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number, sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by APP_CATEGORY_NAME")({
    checkAnswer(
      sql("select APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number, sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by APP_CATEGORY_NAME"),
      Seq(Row("Network_Admin", 5, 12402), Row("Web_Browsing", 6, 2972886), Row("IM", 2, 29565), Row("Tunnelling", 1, 4364), Row("Game", 1, 2485), Row("", 4, 24684)))
  })

  //SmartPCC_Perf_TC_005
  test("select APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where APP_CATEGORY_NAME='Web_Browsing' group by APP_CATEGORY_NAME order by msidn_number desc")({
    checkAnswer(
      sql("select APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where APP_CATEGORY_NAME='Web_Browsing' group by APP_CATEGORY_NAME order by msidn_number desc"),
      Seq(Row("Web_Browsing", 6, 2972886)))
  })

  //SmartPCC_Perf_TC_006
  test("select APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by APP_SUB_CATEGORY_NAME")({
    checkAnswer(
      sql("select APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by APP_SUB_CATEGORY_NAME"),
      Seq(Row("QQ_Media", 1, 5700), Row("DNS", 5, 12402), Row("QQ_IM", 1, 23865), Row("HTTP", 4, 2896722), Row("XiaYiDao", 1, 2485), Row("HTTP_Browsing", 1, 2381), Row("HTTPS", 1, 73783), Row("", 4, 24684), Row("SSL", 1, 4364)))
  })

  //SmartPCC_Perf_TC_007
  test("select APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where APP_SUB_CATEGORY_NAME='HTTP' and TERMINAL_BRAND='MARCONI' group by APP_SUB_CATEGORY_NAME order by msidn_number desc")({
    checkAnswer(
      sql("select APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where APP_SUB_CATEGORY_NAME='HTTP' and TERMINAL_BRAND='MARCONI' group by APP_SUB_CATEGORY_NAME order by msidn_number desc"),
      Seq(Row("HTTP", 1, 2874640)))
  })

  //SmartPCC_Perf_TC_008
  test("select TERMINAL_BRAND,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by TERMINAL_BRAND")({
    checkAnswer(
      sql("select TERMINAL_BRAND,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by TERMINAL_BRAND"),
      Seq(Row("OPPO", 1, 928), Row("HTC", 2, 2661), Row("美奇", 1, 2485), Row("NOKIA", 1, 14411), Row("MARCONI", 1, 2874640), Row("SUNUP", 1, 290), Row("TIANYU", 1, 23865), Row("LANBOXING", 1, 4364), Row("BBK", 1, 6936), Row("SECURE", 1, 1185), Row("MOTOROLA", 3, 80137), Row("DAXIAN", 1, 6486), Row("LENOVO", 1, 2346), Row("", 1, 4840), Row("山水", 1, 5700), Row("SANGFEI", 1, 15112)))
  })

  //SmartPCC_Perf_TC_009
  test("select TERMINAL_BRAND,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where TERMINAL_BRAND='MARCONI' group by TERMINAL_BRAND order by msidn_number desc")({
    checkAnswer(
      sql("select TERMINAL_BRAND,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where TERMINAL_BRAND='MARCONI' group by TERMINAL_BRAND order by msidn_number desc"),
      Seq(Row("MARCONI", 1, 2874640)))
  })

  //SmartPCC_Perf_TC_010
  test("select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by TERMINAL_TYPE")({
    checkAnswer(
      sql("select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by TERMINAL_TYPE"),
      Seq(Row(" ", 2, 2875825), Row("SMARTPHONE", 8, 123420), Row("", 1, 4840), Row("FEATURE PHONE", 8, 42301)))
  })

  //SmartPCC_Perf_TC_011
  test("select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where RAT_NAME='GERAN' group by TERMINAL_TYPE order by total desc")({
    checkAnswer(
      sql("select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where RAT_NAME='GERAN' group by TERMINAL_TYPE order by total desc"),
      Seq(Row(" ", 2, 2875825), Row("SMARTPHONE", 8, 123420), Row("FEATURE PHONE", 8, 42301), Row("", 1, 4840)))
  })

  //SmartPCC_Perf_TC_012
  test("select CGI,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by CGI")({
    checkAnswer(
      sql("select CGI,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by CGI"),
      Seq(Row("460003772902063", 1, 73783), Row("460003788311323", 1, 5700), Row("460003777109392", 1, 6486), Row("460003787211338", 1, 1310), Row("460003776440020", 1, 5044), Row("460003773401611", 1, 2381), Row("460003767804016", 1, 4840), Row("460003784806621", 1, 1185), Row("460003787360026", 1, 14411), Row("460003785041401", 1, 6936), Row("460003766033446", 1, 15112), Row("460003776906411", 1, 4364), Row("460003782800719", 1, 2874640), Row("460003764930757", 1, 928), Row("460003788410098", 1, 23865), Row("460003763202233", 1, 2485), Row("460003763606253", 1, 290), Row("460003788100762", 1, 280), Row("460003784118872", 1, 2346)))
  })

  //SmartPCC_Perf_TC_013
  test("select CGI,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where CGI='460003772902063' group by CGI order by total desc")({
    checkAnswer(
      sql("select CGI,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where CGI='460003772902063' group by CGI order by total desc"),
      Seq(Row("460003772902063", 1, 73783)))
  })

  //SmartPCC_Perf_TC_014
  test("select RAT_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by RAT_NAME")({
    checkAnswer(
      sql("select RAT_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by RAT_NAME"),
      Seq(Row("GERAN", 19, 3046386)))
  })

  //SmartPCC_Perf_TC_015
  test("select DAY,HOUR,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by DAY,HOUR")({
    checkAnswer(
      sql("select DAY,HOUR,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by DAY,HOUR"),
      Seq(Row("8-1", "23", 19, 3046386)))
  })

  //SmartPCC_Perf_TC_016
  test("select DAY,HOUR,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where hour between 20 and 24 group by DAY,HOUR order by total desc")({
    checkAnswer(
      sql("select DAY,HOUR,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where hour between 20 and 24 group by DAY,HOUR order by total desc"),
      Seq(Row("8-1", "23", 19, 3046386)))
  })

  //SmartPCC_Perf_TC_017
  test("select APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME")({
    checkAnswer(
      sql("select APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME"),
      Seq(Row("HTTPS", "Web_Browsing", 1, 73783), Row("QQ_IM", "IM", 1, 23865), Row("HTTP_Browsing", "Web_Browsing", 1, 2381), Row("XiaYiDao", "Game", 1, 2485), Row("", "", 4, 24684), Row("SSL", "Tunnelling", 1, 4364), Row("HTTP", "Web_Browsing", 4, 2896722), Row("QQ_Media", "IM", 1, 5700), Row("DNS", "Network_Admin", 5, 12402)))
  })

  //SmartPCC_Perf_TC_018
  test("select APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  APP_CATEGORY_NAME='Web_Browsing' group by APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME order by total desc")({
    checkAnswer(
      sql("select APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  APP_CATEGORY_NAME='Web_Browsing' group by APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME order by total desc"),
      Seq(Row("HTTP", "Web_Browsing", 4, 2896722), Row("HTTPS", "Web_Browsing", 1, 73783), Row("HTTP_Browsing", "Web_Browsing", 1, 2381)))
  })

  //SmartPCC_Perf_TC_019
  test("select TERMINAL_BRAND,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by APP_SUB_CATEGORY_NAME,TERMINAL_BRAND")({
    checkAnswer(
      sql("select TERMINAL_BRAND,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by APP_SUB_CATEGORY_NAME,TERMINAL_BRAND"),
      Seq(Row("SECURE", "HTTP", 1, 1185), Row("HTC", "HTTP_Browsing", 1, 2381), Row("TIANYU", "QQ_IM", 1, 23865), Row("DAXIAN", "HTTP", 1, 6486), Row("BBK", "", 1, 6936), Row("山水", "QQ_Media", 1, 5700), Row("LENOVO", "", 1, 2346), Row("LANBOXING", "SSL", 1, 4364), Row("MOTOROLA", "DNS", 2, 6354), Row("MOTOROLA", "HTTPS", 1, 73783), Row("SANGFEI", "", 1, 15112), Row("美奇", "XiaYiDao", 1, 2485), Row("NOKIA", "HTTP", 1, 14411), Row("", "DNS", 1, 4840), Row("MARCONI", "HTTP", 1, 2874640), Row("OPPO", "DNS", 1, 928), Row("HTC", "DNS", 1, 280), Row("SUNUP", "", 1, 290)))
  })

  //SmartPCC_Perf_TC_020
  test("select TERMINAL_BRAND,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  RAT_NAME='GERAN' and TERMINAL_BRAND='HTC' group by TERMINAL_BRAND,APP_SUB_CATEGORY_NAME order by total desc")({
    checkAnswer(
      sql("select TERMINAL_BRAND,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  RAT_NAME='GERAN' and TERMINAL_BRAND='HTC' group by TERMINAL_BRAND,APP_SUB_CATEGORY_NAME order by total desc"),
      Seq(Row("HTC", "HTTP_Browsing", 1, 2381), Row("HTC", "DNS", 1, 280)))
  })

  //SmartPCC_Perf_TC_021
  test("select RAT_NAME,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g   group by APP_SUB_CATEGORY_NAME,RAT_NAME")({
    checkAnswer(
      sql("select RAT_NAME,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g   group by APP_SUB_CATEGORY_NAME,RAT_NAME"),
      Seq(Row("GERAN", "QQ_Media", 1, 5700), Row("GERAN", "HTTP", 4, 2896722), Row("GERAN", "XiaYiDao", 1, 2485), Row("GERAN", "", 4, 24684), Row("GERAN", "QQ_IM", 1, 23865), Row("GERAN", "DNS", 5, 12402), Row("GERAN", "HTTPS", 1, 73783), Row("GERAN", "SSL", 1, 4364), Row("GERAN", "HTTP_Browsing", 1, 2381)))
  })

  //SmartPCC_Perf_TC_022
  test("select RAT_NAME,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  RAT_NAME='GERAN' group by APP_SUB_CATEGORY_NAME,RAT_NAME order by total desc")({
    checkAnswer(
      sql("select RAT_NAME,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  RAT_NAME='GERAN' group by APP_SUB_CATEGORY_NAME,RAT_NAME order by total desc"),
      Seq(Row("GERAN", "HTTP", 4, 2896722), Row("GERAN", "HTTPS", 1, 73783), Row("GERAN", "", 4, 24684), Row("GERAN", "QQ_IM", 1, 23865), Row("GERAN", "DNS", 5, 12402), Row("GERAN", "QQ_Media", 1, 5700), Row("GERAN", "SSL", 1, 4364), Row("GERAN", "XiaYiDao", 1, 2485), Row("GERAN", "HTTP_Browsing", 1, 2381)))
  })

  //SmartPCC_Perf_TC_023
  test("select TERMINAL_TYPE,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g   group by APP_SUB_CATEGORY_NAME,TERMINAL_TYPE")({
    checkAnswer(
      sql("select TERMINAL_TYPE,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g   group by APP_SUB_CATEGORY_NAME,TERMINAL_TYPE"),
      Seq(Row("SMARTPHONE", "", 1, 2346), Row("SMARTPHONE", "QQ_IM", 1, 23865), Row("FEATURE PHONE", "QQ_Media", 1, 5700), Row("SMARTPHONE", "DNS", 3, 6634), Row("FEATURE PHONE", "HTTP", 1, 6486), Row("SMARTPHONE", "HTTPS", 1, 73783), Row("FEATURE PHONE", "XiaYiDao", 1, 2485), Row("SMARTPHONE", "HTTP_Browsing", 1, 2381), Row(" ", "HTTP", 2, 2875825), Row("FEATURE PHONE", "", 3, 22338), Row("", "DNS", 1, 4840), Row("FEATURE PHONE", "DNS", 1, 928), Row("FEATURE PHONE", "SSL", 1, 4364), Row("SMARTPHONE", "HTTP", 1, 14411)))
  })

  //SmartPCC_Perf_TC_024
  test("select TERMINAL_TYPE,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  CGI in('460003772902063','460003773401611') group by APP_SUB_CATEGORY_NAME,TERMINAL_TYPE order by total desc")({
    checkAnswer(
      sql("select TERMINAL_TYPE,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  CGI in('460003772902063','460003773401611') group by APP_SUB_CATEGORY_NAME,TERMINAL_TYPE order by total desc"),
      Seq(Row("SMARTPHONE", "HTTPS", 1, 73783), Row("SMARTPHONE", "HTTP_Browsing", 1, 2381)))
  })

  //SmartPCC_Perf_TC_025
  test("select HOUR,cgi,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT+down_THROUGHPUT) as total from  traffic_2g_3g_4g  group by HOUR,cgi,APP_SUB_CATEGORY_NAME")({
    checkAnswer(
      sql("select HOUR,cgi,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT+down_THROUGHPUT) as total from  traffic_2g_3g_4g  group by HOUR,cgi,APP_SUB_CATEGORY_NAME"),
      Seq(Row("23", "460003788311323", "QQ_Media", 1, 5700), Row("23", "460003763606253", "", 1, 290), Row("23", "460003784806621", "HTTP", 1, 1185), Row("23", "460003776440020", "DNS", 1, 5044), Row("23", "460003772902063", "HTTPS", 1, 73783), Row("23", "460003782800719", "HTTP", 1, 2874640), Row("23", "460003776906411", "SSL", 1, 4364), Row("23", "460003788410098", "QQ_IM", 1, 23865), Row("23", "460003766033446", "", 1, 15112), Row("23", "460003763202233", "XiaYiDao", 1, 2485), Row("23", "460003764930757", "DNS", 1, 928), Row("23", "460003787211338", "DNS", 1, 1310), Row("23", "460003767804016", "DNS", 1, 4840), Row("23", "460003773401611", "HTTP_Browsing", 1, 2381), Row("23", "460003784118872", "", 1, 2346), Row("23", "460003785041401", "", 1, 6936), Row("23", "460003777109392", "HTTP", 1, 6486), Row("23", "460003788100762", "DNS", 1, 280), Row("23", "460003787360026", "HTTP", 1, 14411)))
  })

  //SmartPCC_Perf_TC_026
  test("select RAT_NAME,APP_SUB_CATEGORY_NAME,TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by RAT_NAME,APP_SUB_CATEGORY_NAME,TERMINAL_TYPE")({
    checkAnswer(
      sql("select RAT_NAME,APP_SUB_CATEGORY_NAME,TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  group by RAT_NAME,APP_SUB_CATEGORY_NAME,TERMINAL_TYPE"),
      Seq(Row("GERAN", "SSL", "FEATURE PHONE", 1, 4364), Row("GERAN", "HTTP", "SMARTPHONE", 1, 14411), Row("GERAN", "", "SMARTPHONE", 1, 2346), Row("GERAN", "QQ_IM", "SMARTPHONE", 1, 23865), Row("GERAN", "QQ_Media", "FEATURE PHONE", 1, 5700), Row("GERAN", "DNS", "SMARTPHONE", 3, 6634), Row("GERAN", "HTTPS", "SMARTPHONE", 1, 73783), Row("GERAN", "HTTP", "FEATURE PHONE", 1, 6486), Row("GERAN", "XiaYiDao", "FEATURE PHONE", 1, 2485), Row("GERAN", "HTTP_Browsing", "SMARTPHONE", 1, 2381), Row("GERAN", "HTTP", " ", 2, 2875825), Row("GERAN", "", "FEATURE PHONE", 3, 22338), Row("GERAN", "DNS", "", 1, 4840), Row("GERAN", "DNS", "FEATURE PHONE", 1, 928)))
  })

  //SmartPCC_Perf_TC_027
  test("select t2.MSISDN,t1.APP_SUB_CATEGORY_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT)+sum(t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  t1, viptable t2 where t1.MSISDN=t2.MSISDN group by t1.APP_SUB_CATEGORY_NAME,t2.MSISDN")({
    checkAnswer(
      sql("select t2.MSISDN,t1.APP_SUB_CATEGORY_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT)+sum(t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  t1, viptable t2 where t1.MSISDN=t2.MSISDN group by t1.APP_SUB_CATEGORY_NAME,t2.MSISDN"),
      Seq(Row("8613993676885", "HTTPS", 1, 73783), Row("8618394185970", "QQ_IM", 1, 23865), Row("8613993800024", "DNS", 1, 5044), Row("8613893600602", "", 1, 2346), Row("8613919791668", "DNS", 1, 928), Row("8618793100458", "", 1, 15112), Row("8618794812876", "HTTP", 1, 14411), Row("8618700943475", "HTTP", 1, 1185), Row("8613993104233", "DNS", 1, 280), Row("8615120474362", "", 1, 6936), Row("8615209309657", "", 1, 290), Row("8613893462639", "HTTP", 1, 2874640), Row("8615117035070", "DNS", 1, 1310), Row("8613519003078", "XiaYiDao", 1, 2485), Row("8613893853351", "HTTP", 1, 6486), Row("8613649905753", "HTTP_Browsing", 1, 2381), Row("8618794965341", "DNS", 1, 4840), Row("8613993899110", "SSL", 1, 4364), Row("8618393012284", "QQ_Media", 1, 5700)))
  })

  //SmartPCC_Perf_TC_028
  test("select t2.MSISDN,t1.APP_SUB_CATEGORY_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  t1, viptable t2 where t1.TERMINAL_BRAND='HTC' and t1.MSISDN=t2.MSISDN group by t1.APP_SUB_CATEGORY_NAME,t2.MSISDN")({
    checkAnswer(
      sql("select t2.MSISDN,t1.APP_SUB_CATEGORY_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  t1, viptable t2 where t1.TERMINAL_BRAND='HTC' and t1.MSISDN=t2.MSISDN group by t1.APP_SUB_CATEGORY_NAME,t2.MSISDN"),
      Seq(Row("8613993104233", "DNS", 1, 280), Row("8613649905753", "HTTP_Browsing", 1, 2381)))
  })

  //SmartPCC_Perf_TC_029
  test("select t2.MSISDN,t1.APP_SUB_CATEGORY_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT)+sum(t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  t1, viptable t2 where t1.TERMINAL_BRAND='HTC' and t1.MSISDN=t2.MSISDN group by t1.APP_SUB_CATEGORY_NAME,t2.MSISDN order by total desc")({
    checkAnswer(
      sql("select t2.MSISDN,t1.APP_SUB_CATEGORY_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT)+sum(t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  t1, viptable t2 where t1.TERMINAL_BRAND='HTC' and t1.MSISDN=t2.MSISDN group by t1.APP_SUB_CATEGORY_NAME,t2.MSISDN order by total desc"),
      Seq(Row("8613649905753", "HTTP_Browsing", 1, 2381), Row("8613993104233", "DNS", 1, 280)))
  })

  //SmartPCC_Perf_TC_030
  test("select t2.MSISDN,t1.RAT_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT)+sum(t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  t1, viptable t2 where t1.MSISDN=t2.MSISDN group by t1.RAT_NAME,t2.MSISDN")({
    checkAnswer(
      sql("select t2.MSISDN,t1.RAT_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT)+sum(t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  t1, viptable t2 where t1.MSISDN=t2.MSISDN group by t1.RAT_NAME,t2.MSISDN"),
      Seq(Row("8618794965341", "GERAN", 1, 4840), Row("8613993676885", "GERAN", 1, 73783), Row("8613893462639", "GERAN", 1, 2874640), Row("8613993800024", "GERAN", 1, 5044), Row("8618394185970", "GERAN", 1, 23865), Row("8613893853351", "GERAN", 1, 6486), Row("8613919791668", "GERAN", 1, 928), Row("8613993104233", "GERAN", 1, 280), Row("8613893600602", "GERAN", 1, 2346), Row("8618393012284", "GERAN", 1, 5700), Row("8613519003078", "GERAN", 1, 2485), Row("8618793100458", "GERAN", 1, 15112), Row("8615117035070", "GERAN", 1, 1310), Row("8615120474362", "GERAN", 1, 6936), Row("8613649905753", "GERAN", 1, 2381), Row("8615209309657", "GERAN", 1, 290), Row("8613993899110", "GERAN", 1, 4364), Row("8618794812876", "GERAN", 1, 14411), Row("8618700943475", "GERAN", 1, 1185)))
  })

  //SmartPCC_Perf_TC_031
  test("select level, sum(sumUPdown) as total, count(distinct MSISDN) as MSISDN_count from (select MSISDN, t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT as sumUPdown, if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>52428800, '>50M', if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>10485760,'50M~10M',if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>1048576, '10M~1M','<1m'))) as level from  traffic_2g_3g_4g  t1) t2 group by level")({
    checkAnswer(
      sql("select level, sum(sumUPdown) as total, count(distinct MSISDN) as MSISDN_count from (select MSISDN, t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT as sumUPdown, if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>52428800, '>50M', if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>10485760,'50M~10M',if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>1048576, '10M~1M','<1m'))) as level from  traffic_2g_3g_4g  t1) t2 group by level"),
      Seq(Row("<1m", 171746, 18), Row("10M~1M", 2874640, 1)))
  })

  //SmartPCC_Perf_TC_032
  test("select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+ sum(DOWN_THROUGHPUT) as total from  traffic_2G_3G_4G where MSISDN='8613993800024' group by TERMINAL_TYPE")({
    checkAnswer(
      sql("select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+ sum(DOWN_THROUGHPUT) as total from  traffic_2G_3G_4G where MSISDN='8613993800024' group by TERMINAL_TYPE"),
      Seq(Row("SMARTPHONE", 1, 5044)))
  })

  //SmartPCC_Perf_TC_033
  test("select TERMINAL_TYPE,sum(UP_THROUGHPUT)+ sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g where MSISDN='8613519003078' group by TERMINAL_TYPE")({
    checkAnswer(
      sql("select TERMINAL_TYPE,sum(UP_THROUGHPUT)+ sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g where MSISDN='8613519003078' group by TERMINAL_TYPE"),
      Seq(Row("FEATURE PHONE", 2485)))
  })

  //SmartPCC_Perf_TC_034
  test("select TERMINAL_TYPE,UP_THROUGHPUT, DOWN_THROUGHPUT as total from  Traffic_2G_3G_4G where MSISDN='8613993104233'")({
    checkAnswer(
      sql("select TERMINAL_TYPE,UP_THROUGHPUT, DOWN_THROUGHPUT as total from  Traffic_2G_3G_4G where MSISDN='8613993104233'"),
      Seq(Row("SMARTPHONE", 134, 146)))
  })

  //SmartPCC_Perf_TC_035
  test("select SOURCE_INFO,APP_CATEGORY_ID,RAT_NAME,TERMINAL_TYPE,UP_THROUGHPUT, DOWN_THROUGHPUT from  Traffic_2G_3G_4G where MSISDN='8618394185970' and APP_CATEGORY_ID='2'")({
    checkAnswer(
      sql("select SOURCE_INFO,APP_CATEGORY_ID,RAT_NAME,TERMINAL_TYPE,UP_THROUGHPUT, DOWN_THROUGHPUT from  Traffic_2G_3G_4G where MSISDN='8618394185970' and APP_CATEGORY_ID='2'"),
      Seq(Row("GN", "2", "GERAN", "SMARTPHONE", 13934, 9931)))
  })

  //SmartPCC_Perf_TC_036
  test("select SOURCE_INFO,APP_CATEGORY_ID,APP_CATEGORY_NAME,AREA_CODE,CITY,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G where MSISDN='8615209309657' and APP_CATEGORY_ID='-1'")({
    checkAnswer(
      sql("select SOURCE_INFO,APP_CATEGORY_ID,APP_CATEGORY_NAME,AREA_CODE,CITY,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G where MSISDN='8615209309657' and APP_CATEGORY_ID='-1'"),
      Seq(Row("GN", "-1", "", "0930", "临夏", 200, 90)))
  })

  //SmartPCC_Perf_TC_037
  test("select SOURCE_INFO,APP_CATEGORY_ID from Traffic_2G_3G_4G where MSISDN='8615120474362' and APP_CATEGORY_ID='-1'")({
    checkAnswer(
      sql("select SOURCE_INFO,APP_CATEGORY_ID from Traffic_2G_3G_4G where MSISDN='8615120474362' and APP_CATEGORY_ID='-1'"),
      Seq(Row("GN", "-1")))
  })

  //SmartPCC_Perf_TC_038
  test("select SOURCE_INFO,APP_CATEGORY_ID,TERMINAL_BRAND,TERMINAL_MODEL,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G where MSISDN='8613893600602' and APP_CATEGORY_ID='-1'")({
    checkAnswer(
      sql("select SOURCE_INFO,APP_CATEGORY_ID,TERMINAL_BRAND,TERMINAL_MODEL,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G where MSISDN='8613893600602' and APP_CATEGORY_ID='-1'"),
      Seq(Row("GN", "-1", "LENOVO", "LENOVO A60", 1662, 684)))
  })

  //SmartPCC_Perf_TC_039
  test("select SOURCE_INFO,APP_CATEGORY_ID,CGI,DAY,HOUR,MIN,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G where  APP_CATEGORY_ID='16' and MSISDN='8613993899110' and CGI='460003776906411' and DAY='8-1' and HOUR='23'")({
    checkAnswer(
      sql("select SOURCE_INFO,APP_CATEGORY_ID,CGI,DAY,HOUR,MIN,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G where  APP_CATEGORY_ID='16' and MSISDN='8613993899110' and CGI='460003776906411' and DAY='8-1' and HOUR='23'"),
      Seq(Row("GN", "16", "460003776906411", "8-1", "23", "0", 1647, 2717)))
  })

}