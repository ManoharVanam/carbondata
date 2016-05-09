package org.carbondata.integration.spark.testsuite.dataretention

import java.io.File
import java.text.SimpleDateFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.scalatest.BeforeAndAfterAll

/**
  * Created by Manohar on 5/9/2016.
  */
class DataRetentionTestCase extends QueryTest with BeforeAndAfterAll {

  val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
    .getCanonicalPath

  val resource = currentDirectory + "/src/test/resources/"
  var time = ""

  override def beforeAll {
    CarbonProperties.getInstance.addProperty(CarbonCommonConstants.MAX_QUERY_EXECUTION_TIME, "1")

    sql(
      "CREATE CUBE DataRetentionTable DIMENSIONS (ID Integer, date String, country String, name " +
        "String," +
        "phonetype String, serialname String) MEASURES (salary Integer)" +
        "OPTIONS (PARTITIONER [PARTITION_COUNT=3])"
    )

    sql("LOAD DATA FACT FROM'" + resource + "dataretention1.csv' INTO CUBE DataRetentionTable " +
      "OPTIONS(DELIMITER ',')")
    sql("LOAD DATA FACT FROM'" + resource + "dataretention2.csv' INTO CUBE DataRetentionTable " +
      "OPTIONS(DELIMITER ',')")

    Thread.sleep(1000)
    time = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .format(System.currentTimeMillis())
    Thread.sleep(1000)

    sql("LOAD DATA FACT FROM'" + resource + "dataretention3.csv' INTO CUBE DataRetentionTable " +
      "OPTIONS(DELIMITER ',')")



    sql("SELECT country, count(salary) AS amount FROM DataRetentionTable WHERE country" +
      " IN ('china','ind','aus','eng') GROUP BY country"
    ).show()

    //cc.sql("DROP CUBE DataRetentionTable")
    //sql("SHOW LOADS FOR TABLE DataRetentionTable").show()

    //Thread.sleep(1000*60)
    //cc.sql("clean files for TABLE DataRetentionTable")
    //cc.sql("DELETE SEGMENTS FROM TABLE DataRetentionTable where STARTTIME before '2016-05-09
    // 22:22:00'")
  }

  override def afterAll {
    sql("drop cube DataRetentionTable")
  }

  test("RetentionTest1") {
    checkAnswer(
      sql("SELECT country, count(salary) AS amount FROM DataRetentionTable WHERE country" +
        " IN ('china','ind','aus','eng') GROUP BY country"
      ),
      Seq(Row("aus", 9), Row("eng", 9), Row("ind", 9))
    )
  }

  test("RetentionTest2") {
    // delete ind, aus segments
    sql("SHOW LOADS FOR TABLE DataRetentionTable").show()
    sql("DELETE SEGMENTS FROM TABLE DataRetentionTable where STARTTIME before '" + time + "'")
    sql("SHOW LOADS FOR TABLE DataRetentionTable").show()
    checkAnswer(
      sql("SELECT country, count(salary) AS amount FROM DataRetentionTable WHERE country" +
        " IN ('china','ind','aus','eng') GROUP BY country"
      ),
      Seq(Row("eng", 9))
    )
  }

  test("RetentionTest3") {
    // delete load 2 and load ind segment
    sql("SHOW LOADS FOR TABLE DataRetentionTable").show()
    sql("DELETE LOAD 2 FROM TABLE DataRetentionTable")
    sql("LOAD DATA FACT FROM'" + resource + "dataretention1.csv' INTO CUBE DataRetentionTable " +
      "OPTIONS(DELIMITER ',')")
    sql("SHOW LOADS FOR TABLE DataRetentionTable").show()
    checkAnswer(
      sql("SELECT country, count(salary) AS amount FROM DataRetentionTable WHERE country" +
        " IN ('china','ind','aus','eng') GROUP BY country"
      ),
      Seq(Row("ind", 9))
    )
  }


}
