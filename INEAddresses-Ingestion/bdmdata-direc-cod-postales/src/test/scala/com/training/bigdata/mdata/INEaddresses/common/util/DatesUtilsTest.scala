package com.training.bigdata.mdata.INEaddresses.common.util

import java.sql.Timestamp

import com.training.bigdata.mdata.INEaddresses.common.util.tag.TagTest.UnitTestTag
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.{FlatSpec, Matchers}


class DatesUtilsTest extends FlatSpec with Matchers with DatasetSuiteBase {

  "stringToTimestamp" must "parse string to timestamp" taggedAs UnitTestTag in {
    val expected = new Timestamp(1561845600000L)

    DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER) shouldBe expected
  }

  it must "throw an error when string could not be parsed" taggedAs UnitTestTag in {

    assertThrows[IllegalArgumentException] {
      DatesUtils.stringToTimestamp("2019-06-30", DatesUtils.yyyyMMdd_FORMATTER)
    }

    assertThrows[IllegalArgumentException] {
      DatesUtils.stringToTimestamp("20190630 00:00:00", DatesUtils.yyyyMMdd_FORMATTER)
    }

    assertThrows[IllegalArgumentException] {
      DatesUtils.stringToTimestamp("ggfgfh", DatesUtils.yyyyMMdd_FORMATTER)
    }

  }


}