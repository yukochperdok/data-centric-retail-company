package com.training.bigdata.mdata.INEaddresses.common.entities

import java.sql.Timestamp

import com.training.bigdata.mdata.INEaddresses.common.util.tag.TagTest.UnitTestTag
import com.training.bigdata.mdata.INEaddresses.INEFilesIngestion.entities.StreetTypes
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}

class StreetTypesTest extends FlatSpec with Matchers with DatasetSuiteBase {

  "parse" must "parse all rows given to street stretches dataframe" taggedAs UnitTestTag in {

    val fileContent: RDD[String] = spark.sparkContext.parallelize(Seq("0100101001   20190630 01001KALE 0TORRONDOA" +
      "                                         TORRONDOA                "))

    val expected = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("01", "001", 1001, null, null, new Timestamp(1561845600000L),
        null, 1001, "KALE", 0, "TORRONDOA", "TORRONDOA"))),
      StreetTypes.FILE_STRUCT_TYPE
    )

    val result = StreetTypes.parse(fileContent)(spark)

    result.count shouldBe 1
    assertDataFrameEquals(expected, result)

  }

}
