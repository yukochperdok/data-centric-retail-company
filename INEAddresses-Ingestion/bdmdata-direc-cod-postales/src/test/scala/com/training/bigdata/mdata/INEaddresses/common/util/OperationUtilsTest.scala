package com.training.bigdata.mdata.INEaddresses.common.util

import java.sql.Timestamp

import com.training.bigdata.mdata.INEaddresses.common.entities.interfaces.Audit
import com.training.bigdata.mdata.INEaddresses.common.util.tag.TagTest.UnitTestTag
import com.training.bigdata.mdata.INEaddresses.logging.CustomAppender
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}


class OperationUtilsTest extends FlatSpec with Matchers with DatasetSuiteBase with BeforeAndAfter {

  override protected implicit def enableHiveSupport: Boolean = false

  after {
    CustomAppender.clear()
  }

  trait DefaultConf {
    val defaultUser = "user"
    val defaultTimestamp = new Timestamp(System.currentTimeMillis)
    val inputStructType = StructType(List(
      StructField("field1", StringType),
      StructField("field2", StringType),
      StructField("field3", IntegerType)
    ))
    val inputDF: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row("row11", "row12", 1),
        Row("row21", "row22", 2)
      )),
      inputStructType
    )
    val couldNotBeJoinedMessage = "Not found ccaa"
  }

  "addAuditFields" must "add user and timestamp of insertion and update" taggedAs UnitTestTag in new DefaultConf {

    val expectedStructType = StructType(List(
      StructField("field1", StringType, true),
      StructField("field2", StringType, true),
      StructField("field3", IntegerType, true),
      StructField(Audit.tsInsertDlk, TimestampType, false),
      StructField(Audit.userInsertDlk, StringType, false),
      StructField(Audit.tsUpdateDlk, TimestampType, false),
      StructField(Audit.userUpdateDlk, StringType, false)
    ))
    val expectedDF: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row("row11", "row12", 1, defaultTimestamp, defaultUser, defaultTimestamp, defaultUser),
        Row("row21", "row22", 2, defaultTimestamp, defaultUser, defaultTimestamp, defaultUser)
      )),
      expectedStructType
    )

    val resultDF = inputDF.transform(OperationsUtils.addAuditFields(defaultUser, defaultTimestamp))

    assertDataFrameEquals(expectedDF, resultDF)

  }

  "addPartitionFields" must "add year, month and day fields based on column name" taggedAs UnitTestTag in {

    val inputStructType = StructType(List(
      StructField("field1", StringType),
      StructField("field2", TimestampType)
    ))
    val inputDF: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row("row11", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("row21", DatesUtils.stringToTimestamp("20180503", DatesUtils.yyyyMMdd_FORMATTER))
      )),
      inputStructType
    )

    val expectedStructType = StructType(List(
      StructField("field1", StringType, true),
      StructField("field2", TimestampType, true),
      StructField("year", StringType, true),
      StructField("month", StringType, true)
    ))
    val expectedDF: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row("row11", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER), "2019", "06"),
        Row("row21", DatesUtils.stringToTimestamp("20180503", DatesUtils.yyyyMMdd_FORMATTER), "2018", "05")
      )),
      expectedStructType
    )

    val resultDF = inputDF.transform(OperationsUtils.addPartitionFields("field2"))

    assertDataFrameEquals(expectedDF, resultDF)

  }

  "groupByAndGetFirstValue" must "group df based on params sequence and return df with the selected fields sequence" taggedAs UnitTestTag in {

    val inputStructType = StructType(
      List(
        StructField("field1", StringType),
        StructField("field2", StringType),
        StructField("field3", StringType),
        StructField("field4", StringType),
        StructField("field5", StringType)
      )
    )
    val inputDF: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row("row11", "row12", "row13", "row14", "row15"),
        Row("row21", "row12", "row13", "row24", "row25"),
        Row("row31", "row12", "row33", "row34", "row35"),
        Row("row41", "row42", "row43", "row44", "row45"),
        Row("row51", "row42", "row53", "row54", "row55"),
        Row("row61", "row62", "row63", "row64", "row65")
      )),
      inputStructType
    )

    val expectedStructType = StructType(List(
      StructField("field2", StringType),
      StructField("field3", StringType),
      StructField("field1", StringType),
      StructField("field4", StringType)
    ))
    // remove field5 and mix two rows
    val expectedDF: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row("row12", "row13", "row11", "row14"),
        Row("row12", "row33", "row31", "row34"),
        Row("row42", "row43", "row41", "row44"),
        Row("row42", "row53", "row51", "row54"),
        Row("row62", "row63", "row61", "row64")
      )),
      expectedStructType
    )

    val resultDF = inputDF.transform(OperationsUtils.groupByAndGetFirstValue(Seq("field2", "field3"), Seq("field1", "field4")))

    assertDataFrameEquals(expectedDF.orderBy("field2", "field3"), resultDF.orderBy("field2", "field3"))
  }

  "innerJoinAndTraceLogsForRest" must "return inner join between two DFs and trace logs with the rest of the records that do not match" taggedAs UnitTestTag in new DefaultConf {

    val dfToJoinStructType = StructType(List(
      StructField("field3", IntegerType),
      StructField("field4", StringType),
      StructField("field5", BooleanType),
      StructField("field6", IntegerType)
    ))
    val dfToJoin: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row(20, "row11", true, 3),
        Row(20, "row41", false, 4)
      )),
      dfToJoinStructType
    )

    val expectedStructType = StructType(List(
      StructField("field1", StringType, true),
      StructField("field2", StringType, true),
      StructField("field3", IntegerType, true),
      StructField("field3", IntegerType, true),
      StructField("field5", BooleanType, true),
      StructField("field6", IntegerType, true)
    ))
    val expectedDF: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row("row11", "row12", 1, 20, true, 3)
      )),
      expectedStructType
    )

    val resultDF = inputDF.transform(OperationsUtils.innerJoinAndTraceLogsForRest(dfToJoin, "field1", "field4", couldNotBeJoinedMessage))

    assertDataFrameEquals(expectedDF, resultDF)
    CustomAppender.listOfEvents.find(event => event.getMessage.toString.contains(couldNotBeJoinedMessage)) shouldBe defined
  }

  it must "return inner join between two DFs, remove duplicated columns and trace logs with the rest of the records that do not match" taggedAs UnitTestTag in new DefaultConf {

    val dfToJoinStructType = StructType(List(
      StructField("field3", IntegerType),
      StructField("field4", StringType),
      StructField("field5", BooleanType),
      StructField("field6", IntegerType)
    ))
    val dfToJoin: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row(20, "row11", true, 3),
        Row(20, "row41", false, 4)
      )),
      dfToJoinStructType
    )

    val expectedStructType = StructType(List(
      StructField("field1", StringType, true),
      StructField("field2", StringType, true),
      StructField("field3", IntegerType, true),
      StructField("field5", BooleanType, true),
      StructField("field6", IntegerType, true)
    ))
    val expectedDF: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row("row11", "row12", 1, true, 3)
      )),
      expectedStructType
    )

    val resultDF = inputDF.transform(OperationsUtils.innerJoinAndTraceLogsForRest(dfToJoin, "field1", "field4", couldNotBeJoinedMessage, Seq("field3")))

    assertDataFrameEquals(expectedDF, resultDF)
    CustomAppender.listOfEvents.find(event => event.getMessage.toString.contains(couldNotBeJoinedMessage)) shouldBe defined
  }

  it must "return inner join between two DFs and not trace any log when all the records match" taggedAs UnitTestTag in new DefaultConf {

    val dfToJoinStructType = StructType(List(
      StructField("field4", StringType),
      StructField("field5", BooleanType),
      StructField("field6", IntegerType)
    ))
    val dfToJoin: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row("row11", true, 3),
        Row("row21", false, 4),
        Row("row31", false, 5)
      )),
      dfToJoinStructType
    )

    val expectedStructType = StructType(List(
      StructField("field1", StringType, true),
      StructField("field2", StringType, true),
      StructField("field3", IntegerType, true),
      StructField("field5", BooleanType, true),
      StructField("field6", IntegerType, true)
    ))
    val expectedDF: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row("row11", "row12", 1, true, 3),
        Row("row21", "row22", 2, false, 4)
      )),
      expectedStructType
    )

    val resultDF = inputDF.transform(OperationsUtils.innerJoinAndTraceLogsForRest(dfToJoin, "field1", "field4", couldNotBeJoinedMessage))

    assertDataFrameEquals(expectedDF, resultDF)
    CustomAppender.listOfEvents.find(event => event.getMessage.toString.contains(couldNotBeJoinedMessage)) shouldBe None
  }

  it must "return inner join between two DFs and trace logs with the records that do not match when both join fields have the same name" taggedAs UnitTestTag in new DefaultConf {

    val dfToJoinStructType = StructType(List(
      StructField("field1", StringType),
      StructField("field4", BooleanType),
      StructField("field5", IntegerType)
    ))
    val dfToJoin: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row("row11", true, 3),
        Row("row41", false, 4)
      )),
      dfToJoinStructType
    )

    val expectedStructType = StructType(List(
      StructField("field1", StringType, true),
      StructField("field2", StringType, true),
      StructField("field3", IntegerType, true),
      StructField("field4", BooleanType, true),
      StructField("field5", IntegerType, true)
    ))
    val expectedDF: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row("row11", "row12", 1, true, 3)
      )),
      expectedStructType
    )

    val resultDF = inputDF.transform(OperationsUtils.innerJoinAndTraceLogsForRest(dfToJoin, "field1", "field1", couldNotBeJoinedMessage))

    assertDataFrameEquals(expectedDF, resultDF)
    CustomAppender.listOfEvents.find(event => event.getMessage.toString.contains(couldNotBeJoinedMessage)) shouldBe defined

  }

  "isProperValidityOfStreetStretches" must
    "check the proper validity of the streetStretches entity when zipCodeStreetMap entity has several range of validity" taggedAs UnitTestTag in {

    val zipCodesStreetMapStructType = StructType(List(
      StructField("field1", StringType),
      StructField("field2", TimestampType)
    ))
    val zipCodesStreetMapDF: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row("row11", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("row21", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("row31", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("row41", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("row51", DatesUtils.stringToTimestamp("20180503", DatesUtils.yyyyMMdd_FORMATTER))
      )),
      zipCodesStreetMapStructType
    )

    val streetStretchesStructType = StructType(List(
      StructField("field1", StringType),
      StructField("field2", TimestampType)
    ))
    val streetStretchesDF: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row("row11", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("row21", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER))
      )),
      streetStretchesStructType
    )

    val result = OperationsUtils.isStreetStretchesOlderThanMaxStreetMapInfo(streetStretchesDF, zipCodesStreetMapDF, "field2")

    result should be (false)

  }

  it must
    "check the proper validity of the streetStretches entity when zipCodeStreetMap entity is empty" taggedAs UnitTestTag in {

    val zipCodesStreetMapStructType = StructType(List(
      StructField("field1", StringType),
      StructField("field2", TimestampType)
    ))
    val zipCodesStreetMapDF: DataFrame = spark.createDataFrame(
      sc.parallelize(List.empty[Row]),
      zipCodesStreetMapStructType
    )

    val streetStretchesStructType = StructType(List(
      StructField("field1", StringType),
      StructField("field2", TimestampType)
    ))
    val streetStretchesDF: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row("row11", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("row21", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER))
      )),
      streetStretchesStructType
    )

    val result = OperationsUtils.isStreetStretchesOlderThanMaxStreetMapInfo(streetStretchesDF, zipCodesStreetMapDF, "field2")

    result should be (false)

  }

  it must
    "check the proper validity of the streetStretches entity when this entity has some incorrect range of validity" taggedAs UnitTestTag in {

    val zipCodesStreetMapStructType = StructType(List(
      StructField("field1", StringType),
      StructField("field2", TimestampType)
    ))
    val zipCodesStreetMapDF: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row("row11", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("row21", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("row31", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("row41", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("row51", DatesUtils.stringToTimestamp("20180503", DatesUtils.yyyyMMdd_FORMATTER))
      )),
      zipCodesStreetMapStructType
    )

    val streetStretchesStructType = StructType(List(
      StructField("field1", StringType),
      StructField("field2", TimestampType)
    ))
    val streetStretchesDF: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row("row11", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("row12", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("row13", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("row14", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("oldRow1", DatesUtils.stringToTimestamp("20190430", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("oldRow2", DatesUtils.stringToTimestamp("20190530", DatesUtils.yyyyMMdd_FORMATTER))
      )),
      streetStretchesStructType
    )

    val result = OperationsUtils.isStreetStretchesOlderThanMaxStreetMapInfo(streetStretchesDF, zipCodesStreetMapDF, "field2")

    result should be (true)

  }

  it must
    "check the proper validity of the streetStretches entity when is empty" taggedAs UnitTestTag in {

    val zipCodesStreetMapStructType = StructType(List(
      StructField("field1", StringType),
      StructField("field2", TimestampType)
    ))
    val zipCodesStreetMapDF: DataFrame = spark.createDataFrame(
      sc.parallelize(List[Row](
        Row("row11", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("row21", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("row31", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("row41", DatesUtils.stringToTimestamp("20190630", DatesUtils.yyyyMMdd_FORMATTER)),
        Row("row51", DatesUtils.stringToTimestamp("20180503", DatesUtils.yyyyMMdd_FORMATTER))
      )),
      zipCodesStreetMapStructType
    )

    val streetStretchesStructType = StructType(List(
      StructField("field1", StringType),
      StructField("field2", TimestampType)
    ))
    val streetStretchesDF: DataFrame = spark.createDataFrame(
      sc.parallelize(List.empty[Row]),
      streetStretchesStructType
    )

    val result = OperationsUtils.isStreetStretchesOlderThanMaxStreetMapInfo(streetStretchesDF, zipCodesStreetMapDF, "field2")

    result should be (false)

  }

}