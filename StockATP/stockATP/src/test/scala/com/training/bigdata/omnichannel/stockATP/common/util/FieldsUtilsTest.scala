package com.training.bigdata.omnichannel.stockATP.common.util

import com.training.bigdata.omnichannel.stockATP.common.util.tag.TagTest.UnitTestTag
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}

class FieldsUtilsTest extends FlatSpec with Matchers with DatasetSuiteBase{

  "generateNFieldsSequence" must " return n strings with the number suffix starting with 0" taggedAs (UnitTestTag) in {
    val tag = "testTag"
    val days = 20
    val result = FieldsUtils.generateNFieldsSequence(tag, days)
    val listExpected: List[String] = List(
      "testTag",
      "testTag1",
      "testTag2",
      "testTag3",
      "testTag4",
      "testTag5",
      "testTag6",
      "testTag7",
      "testTag8",
      "testTag9",
      "testTag10",
      "testTag11",
      "testTag12",
      "testTag13",
      "testTag14",
      "testTag15",
      "testTag16",
      "testTag17",
      "testTag18",
      "testTag19",
      "testTag20"
    )
    result.length shouldBe (days + 1)
    result shouldBe listExpected

  }

  it must " return n strings with the number suffix not starting with 0" taggedAs (UnitTestTag) in {
    val tag = "testTag"
    val days = 20

    val result = FieldsUtils.generateNFieldsSequence(tag, 25, days+1)
    val listExpected: List[String] = List(
      "testTag21",
      "testTag22",
      "testTag23",
      "testTag24",
      "testTag25"
    )
    result.length shouldBe (5)
    result shouldBe listExpected
  }

  it must " return the parameter tag when days is 0" taggedAs (UnitTestTag) in {
    val tag = "testTag"
    val days = 0
    val result = FieldsUtils.generateNFieldsSequence(tag, days)
    result.length shouldBe 1
    result.head shouldBe "testTag"
  }

  it must " return an exception with negative days" taggedAs (UnitTestTag) in {
    val tag = "testTag"
    val days = -2
    val thrown = intercept[IllegalArgumentException] {
      FieldsUtils.generateNFieldsSequence(tag, days)
    }
    assert(thrown.getMessage === "requirement failed: last index must be non negative")
  }

  "zip5" must " zip 3 lists" taggedAs(UnitTestTag) in {
    val seq1 = Seq("1", "2", "3", "4")
    val seq2 = Seq("a", "b", "c", "d")
    val seq3 = Seq(("-1", "0"), ("0", "1"), ("1", "2"), ("2", "3"))
    val seqExpected = Seq(
      ("1", "a", "-1", "0"),
      ("2", "b",  "0", "1"),
      ("3", "c",  "1", "2"),
      ("4", "d",  "2", "3")
    )
    FieldsUtils.zip3(seq1, seq2, seq3) shouldBe seqExpected
  }

  it must " zip 3 lists with one of them has less registries" taggedAs(UnitTestTag) in {
    val seq1 = Seq("1", "2", "3", "4")
    val seq2 = Seq("a", "b", "c")
    val seq3 = Seq(("-1", "0"), ("0", "1"), ("1", "2"), ("2", "3"))
    val seqExpected = Seq(
      ("1", "a", "-1", "0"),
      ("2", "b",  "0", "1"),
      ("3", "c",  "1", "2")
    )
    FieldsUtils.zip3(seq1, seq2, seq3) shouldBe seqExpected
  }

  it must " zip 3 lists with the first seq with less registries" taggedAs(UnitTestTag) in {
    val seq1 = Seq("1", "2", "3")
    val seq2 = Seq("a", "b", "c", "d")
    val seq3 = Seq(("-1", "0"), ("0", "1"), ("1", "2"), ("2", "3"))
    val seqExpected = Seq(
      ("1", "a", "-1", "0"),
      ("2", "b",  "0", "1"),
      ("3", "c",  "1", "2")
    )
    FieldsUtils.zip3(seq1, seq2, seq3) shouldBe seqExpected
  }

  "adjustPivotedColumns " must " rename a number of pivoted columns of a DF " taggedAs (UnitTestTag) in {

    val today: String = "2018-09-06"
    val tomorrow: String = "2018-09-07"
    val afterTomorrow: String = "2018-09-08"
    val forecastDateMap: Map[String, String] = Map(
      today         -> "",
      tomorrow      -> "1",
      afterTomorrow -> "2")

    val pivotedDF : DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("1", "11", 50.5, 60.6, 0.5),
      Row("2", "22", 50.5, null, null),
      Row("3", "33", null, 50.5, null)
    )), StructType(List(
      StructField("idArticle",   StringType),
      StructField("idStore",     StringType),
      StructField(today,         DoubleType),
      StructField(tomorrow,      DoubleType),
      StructField(afterTomorrow, DoubleType)
    )))

    val expectedDF: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("1", "11", 50.5, 60.6,  0.5),
      Row("2", "22", 50.5, null, null),
      Row("3", "33", null, 50.5, null)
    )), StructType(List(
      StructField("idArticle",       StringType),
      StructField("idStore",         StringType),
      StructField("salesForecastN",  DoubleType),
      StructField("salesForecastN1", DoubleType),
      StructField("salesForecastN2", DoubleType)
    )))

    val resultDF: DataFrame = pivotedDF.transform(FieldsUtils.renamePivotedColumns(forecastDateMap, "salesForecastN"))
      .select(expectedDF.columns.head, expectedDF.columns.tail: _*)
    assertDataFrameEquals(expectedDF, resultDF)

  }

  "generateNFieldsSequenceGroupedByPreviousAndActual"  must " return an exception with negative days" taggedAs (UnitTestTag) in {
    val tag = "testTag"
    val days = -2
    val thrown = intercept[IllegalArgumentException] {
      FieldsUtils.generateNFieldsSequenceGroupedByPreviousAndActual(tag, days)
    }
    assert(thrown.getMessage === "requirement failed: last index must be non negative")
  }

  it must " return n strings with the number suffix grouped with the previous one" taggedAs (UnitTestTag) in {
    val tag = "testTag"
    val days = 5
    val result = FieldsUtils.generateNFieldsSequenceGroupedByPreviousAndActual(tag, days, -1)
    val listExpected: List[(String, String)] = List(
      ( "testTag-1",  "testTag"),
      (   "testTag", "testTag1"),
      (  "testTag1", "testTag2"),
      (  "testTag2", "testTag3"),
      (  "testTag3", "testTag4"),
      (  "testTag4", "testTag5")
    )
    result.length shouldBe (days + 1)
    result shouldBe listExpected
  }
}
