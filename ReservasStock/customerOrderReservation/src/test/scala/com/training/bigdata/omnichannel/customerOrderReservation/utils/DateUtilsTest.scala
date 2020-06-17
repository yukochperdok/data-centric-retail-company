package com.training.bigdata.omnichannel.customerOrderReservation.utils

import java.sql.Timestamp
import java.time.format.{DateTimeFormatter, DateTimeParseException}

import com.training.bigdata.omnichannel.customerOrderReservation.utils.tag.TagTest.UnitTestTag
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FlatSpecLike, Matchers}

class DateUtilsTest extends FlatSpecLike with Matchers with SparkLocalTest {

  "stringToTimestamp " must "return the timestamp corresponding to the string given" taggedAs UnitTestTag in {
    DateUtils.stringToTimestamp("2018-08-25 05:06:07.000+02:00") shouldBe new Timestamp(1535166367000L)
    DateUtils.stringToTimestamp("2018-08-25T05:06:07.000+02:00") shouldBe new Timestamp(1535166367000L)
    DateUtils.stringToTimestamp("2018-08-25 05:06:07", formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) shouldBe new Timestamp(1535166367000L)
    DateUtils.stringToTimestamp("20180825 05:06:07", formatter = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss")) shouldBe new Timestamp(1535166367000L)
    DateUtils.stringToTimestamp("20180825 16:06:07", "Europe/London", DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss")) shouldBe new Timestamp(1535209567000L)
    DateUtils.stringToTimestamp("2018-08-25 04:06:07.000+02:00", "Europe/London") shouldBe new Timestamp(1535166367000L)
  }

  it must "throw a DateTimeParseException when the format does not match " taggedAs UnitTestTag in {
    intercept[DateTimeParseException] {
      DateUtils.stringToTimestamp("20180825 05:06:07")
    }
  }

  "castTimestampColumn" must "cast string column to Timestamp" taggedAs UnitTestTag in {
    val inputDF: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(List[Row](
        Row("2018-08-25 05:06:07.000+02:00"),
        Row("2018-08-25T05:06:07.000+02:00"),
        Row("20180825 05:06:07.000"),
        Row(null)
      )),
      StructType(List(StructField("field1", StringType)))
    )

    val expectedDF: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(List[Row](
        Row(new Timestamp(1535166367000L)),
        Row(new Timestamp(1535166367000L)),
        Row(null),
        Row(null)
      )),
      StructType(List(StructField("field1", TimestampType)))
    )

    val resultDF = DateUtils.castTimestampColumn("field1")(inputDF)
    assertDataFrameEquals(resultDF, expectedDF)

  }
}
