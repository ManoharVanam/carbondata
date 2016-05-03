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

package org.carbondata.integration.spark.testsuite.sortexpr

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.carbondata.core.util.CarbonProperties
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for sort expression query on multiple datatypes
 * @author N00902756
 *
 */

class AllDataTypesTestCaseSort extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    CarbonProperties.getInstance().addProperty("carbon.direct.surrogate","abc")
    sql("CREATE CUBE alldatatypescubeSort DIMENSIONS (empno Integer, empname String, designation String, doj Timestamp, workgroupcategory Integer, workgroupcategoryname String, deptno Integer, deptname String, projectcode Integer, projectjoindate Timestamp, projectenddate Timestamp) MEASURES (attendance Integer,utilization Integer,salary Integer) OPTIONS (PARTITIONER [PARTITION_COUNT=1])")
    sql("LOAD DATA fact from'"+currentDirectory+"/src/test/resources/data.csv' INTO CUBE alldatatypescubeSort PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')");
  }

  test("select empno,empname,utilization,count(salary),sum(empno) from alldatatypescubeSort where empname in ('arvind','ayushi') group by empno,empname,utilization order by empno") {
    checkAnswer(
      sql("select empno,empname,utilization,count(salary),sum(empno) from alldatatypescubeSort where empname in ('arvind','ayushi') group by empno,empname,utilization order by empno"),
      Seq(Row(11, "arvind", 96.2, 1, 11), Row(15, "ayushi", 91.5, 1, 15)))
  }

  override def afterAll {
    sql("drop cube alldatatypescube")
  }
}