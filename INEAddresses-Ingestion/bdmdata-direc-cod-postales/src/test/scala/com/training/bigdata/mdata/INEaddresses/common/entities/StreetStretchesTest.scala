package com.training.bigdata.mdata.INEaddresses.common.entities

import java.sql.Timestamp

import com.training.bigdata.mdata.INEaddresses.common.util.tag.TagTest.UnitTestTag
import com.training.bigdata.mdata.INEaddresses.INEFilesIngestion.entities.StreetStretches
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}

class StreetStretchesTest extends FlatSpec with Matchers with DatasetSuiteBase {

  "parse" must "parse all rows given to street stretches dataframe" taggedAs UnitTestTag in {

    val fileContent: RDD[String] = spark.sparkContext.parallelize(Seq("0100101001   00017010100100000            0124010001 9999    20190630 01001   0001701" +
      "                          ALEGRIADULANTZI          ALEGRIADULANTZI         01001TORROÑDOA                00000" +
      "                                                              0124010001 9999 "))

    val expected = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("01", "001", "01", "001", null, null, "0001701", 1001, 0, null, "01240", "1", 1, null, 9999, null, null, null,
        new Timestamp(1561845600000L), null, "01", "001", null, null, "0001701", null, "ALEGRIADULANTZI", "ALEGRIADULANTZI", 1001, "TORROÑDOA", 0, null, null, "01240", "1", 1, null, 9999, null))),
      StreetStretches.FILE_STRUCT_TYPE
    )

    val result = StreetStretches.parse(fileContent)(spark)

    result.count shouldBe 1
    assertDataFrameEquals(expected, result)

  }

}
