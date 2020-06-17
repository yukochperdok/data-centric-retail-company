package com.training.bigdata.omnichannel.customerOrderReservation.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.training.bigdata.omnichannel.customerOrderReservation.utils.tag.TagTest.UnitTestTag
import com.training.bigdata.omnichannel.customerOrderReservation.utils.Constants.Errors._
import com.training.bigdata.omnichannel.customerOrderReservation.entities.EcommerceOrder._
import com.training.bigdata.omnichannel.customerOrderReservation.entities.Audit._
import com.training.bigdata.omnichannel.customerOrderReservation.entities.{CustomerOrdersReservations => reservations}
import com.training.bigdata.omnichannel.customerOrderReservation.utils.log.CustomAppender
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}



class OperationUtilsTest extends FlatSpec with Matchers with BeforeAndAfter with SparkLocalTest {

  before {
    CustomAppender.listOfEvents.clear()
  }

  after {
    CustomAppender.listOfEvents.clear()
  }

  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val timestamp20191116 = new Timestamp(format.parse("2019-11-16 12:41:00").getTime)
  val timestamp20190516 = new Timestamp(format.parse("2019-05-16 12:30:00").getTime)
  val timestamp20191216 = new Timestamp(format.parse("2019-12-16 12:30:00").getTime)

  val inputStructType = StructType(List(
    StructField("field1", StringType),
    StructField("field2", StringType)
  ))

  "udfLogAndReturnTimestamp" must " return the default value passed as parameter" taggedAs UnitTestTag in {

    val structType = StructType(List(
      StructField("field1", StringType),
      StructField(  "hour", StringType),
      StructField("field2", TimestampType)
    ))

    val inputDF: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(List[Row](
        Row("row11", "2012", timestamp20191116),
        Row("row21",   null, timestamp20190516)
      )),
      structType
    )

    val expectedDF: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(List[Row](
        Row("row11", "2012", timestamp20191116),
        Row("row21",   null, timestamp20191216)
      )),
      structType
    )
    import org.apache.spark.sql.functions._

    val resultDF = inputDF.withColumn(
      "field2",
      when(col("hour").isNull, lit(timestamp20191216))
    .otherwise(col("field2")))

    assertDataFrameEquals(expectedDF, resultDF)
  }

  "filterNullsAndLog " must "filter rows if the value of the field is null" taggedAs UnitTestTag in {
    val inputDF: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(List[Row](
        Row("row11", "row12"),
        Row("row21", "row22"),
        Row(null, "row22"),
        Row("row21", null)
      )),
      inputStructType
    )

    val expectedDF: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(List[Row](
        Row("row11", "row12"),
        Row("row21", "row22"),
        Row(null, "row22")
      )),
      inputStructType
    )
    val resultDF = inputDF.transform(OperationsUtils.filterNullsAndLog("field2", "Null value in column"))
    expectedDF.count() shouldBe(resultDF.count())
    expectedDF.except(resultDF).count() shouldBe 0

  }

  "transcodArticleAndSite" must "transcode article and store" taggedAs UnitTestTag in {

    val articleCol = "article"
    val siteCol = "store"
    val inputStructType = StructType(List(
      StructField(articleCol, StringType),
      StructField(siteCol,    StringType),
      StructField("field3",   StringType)
    ))
    val inputDF: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(List[Row](
        Row("1", "11", "111"),
        Row("3", "11", "111"),
        Row("1", "33", "111")
      )),
      inputStructType
    )

    val articleTranscodMap: Map[String, String] = Map(
      "1" -> "0000000001",
      "2" -> "0000000002"
    )

    val siteTranscodMap: Map[String, String] = Map(
      "11" -> "0011",
      "22" -> "0022"
    )
    val articleTranscodBroadcast: Broadcast[Map[String, String]] = sparkSession.sparkContext.broadcast(articleTranscodMap)
    val siteTranscodBroadcast: Broadcast[Map[String, String]] = sparkSession.sparkContext.broadcast(siteTranscodMap)


    val expectedDF: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(List[Row](
        Row("0000000001", "0011", "111")
      )),
      inputStructType
    )

    val resultDF = inputDF.transform(OperationsUtils.transcodArticleAndSite(articleCol, siteCol, articleTranscodBroadcast, siteTranscodBroadcast))

    assertDataFrameEquals(expectedDF, resultDF)
  }

  it must "log alerts when article or store transcoding not found" taggedAs UnitTestTag in {

    val articleCol = "article"
    val siteCol = "store"
    val inputStructType = StructType(List(
      StructField(articleCol, StringType),
      StructField(siteCol,    StringType)
    ))
    val inputDF: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(List[Row](
        Row("1", "11"),
        Row("3", "11"),
        Row("1", "33")
      )),
      inputStructType
    )

    val articleTranscodMap: Map[String, String] = Map(
      "1" -> "0000000001",
      "2" -> "0000000002"
    )

    val siteTranscodMap: Map[String, String] = Map(
      "11" -> "0011",
      "22" -> "0022"
    )
    val articleTranscodBroadcast: Broadcast[Map[String, String]] = sparkSession.sparkContext.broadcast(articleTranscodMap)
    val siteTranscodBroadcast: Broadcast[Map[String, String]] = sparkSession.sparkContext.broadcast(siteTranscodMap)

    val expectedDF: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(List[Row](
        Row("0000000001", "0011")
      )),
      inputStructType
    )

    val resultDF = inputDF.transform(OperationsUtils.transcodArticleAndSite(articleCol, siteCol, articleTranscodBroadcast, siteTranscodBroadcast))

    assertDataFrameEquals(expectedDF, resultDF)
    CustomAppender.listOfEvents.find( event => event.getMessage.toString.contains(ARTICLE_NOT_IN_TRANSCOD)) shouldBe defined
    CustomAppender.listOfEvents.find( event => event.getMessage.toString.contains(STORE_NOT_IN_TRANSCOD)) shouldBe defined

  }

  "separateByAction" must "separate orders in two dataframes depending on its state" taggedAs UnitTestTag in {

    System.setProperty("config.file", getClass.getClassLoader.getResource(s"./${PropertiesUtil.DEFAULT_CONFIG_FILE_NAME}").getPath)
    val propertiesUtil = PropertiesUtil("user", getClass.getClassLoader.getResource(s"./${PropertiesUtil.DEFAULT_APPLICATION_FILE_NAME}").getPath)

    val inputStructType = StructType(List(
      StructField(    siteId, StringType),
      StructField(orderState, StringType),
      StructField(  "field1", StringType)
    ))
    val inputDF: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(List[Row](
        Row("site1", "creation", "value1"),
        Row("site2", "creation", "value2"),
        Row("site1", "deletion", "value3"),
        Row("site2", "canceled", "value4"),
        Row("site3", "canceled", "value5"),
        Row("site3", "CÁNCELED", "value6"),
        Row("site3", "cánceled", "value7"),
        Row("site3", "CANCELED", "value8"),
        Row("site4", "canceled", "value9"),
        Row(   null,       null, "value10")
      )),
      inputStructType
    )

    val ordersActions: Map[(String, String), (String, String)] = Map(
      ("site1", "CREATION") -> (    "alta_sherpa", "ALTA"),
      ("site2", "CREATION") -> (    "alta_sherpa", "ALTA"),
      ("site3", "CANCELED") -> (    "alta_sherpa", "ALTA"),
      ("site1", "DELETION") -> ("deletion_sherpa", "BAJA"),
      ("site2", "CANCELED") -> ("deletion_sherpa", "BAJA")
    )
    val ordersActionsBroadcast: Broadcast[Map[(String, String), (String, String)]] = sparkSession.sparkContext.broadcast(ordersActions)

    val toBeUpsertedExpectedStructType = StructType(List(
      StructField(siteId, StringType),
      StructField("field1", StringType),
      StructField(reservations.sherpaOrderStatus, StringType)
    ))
    val toBeUpsertedExpectedDF: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(List[Row](
        Row("site1", "value1", "alta_sherpa"),
        Row("site2", "value2", "alta_sherpa"),
        Row("site3", "value5", "alta_sherpa"),
        Row("site3", "value6", "alta_sherpa"),
        Row("site3", "value7", "alta_sherpa"),
        Row("site3", "value8", "alta_sherpa")
      )),
      toBeUpsertedExpectedStructType
    )

    val toBeDeletedExpectedStructType = StructType(List(
      StructField(siteId, StringType),
      StructField("field1", StringType),
      StructField(reservations.sherpaOrderStatus, StringType)
    ))
    val toBeDeletedExpectedDF: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(List[Row](
        Row("site1", "value3", "deletion_sherpa"),
        Row("site2", "value4", "deletion_sherpa")
      )),
      toBeDeletedExpectedStructType
    )

    val (toBeUpsertedResDF, toBeDeletedResDF) = OperationsUtils.separateByAction(ordersActionsBroadcast,
      propertiesUtil.getUpsertAction, propertiesUtil.getDeleteAction)(inputDF)

    assertDataFrameEquals(toBeUpsertedExpectedDF, toBeUpsertedResDF)
    assertDataFrameEquals(toBeDeletedExpectedDF, toBeDeletedResDF)

  }

  "calculateCorrectQuantityAndUnitOfMeasure" must "should take correct quantity and unit of measurement depending on avro column" taggedAs UnitTestTag in {

    System.setProperty("config.file", getClass.getClassLoader.getResource(s"./${PropertiesUtil.DEFAULT_CONFIG_FILE_NAME}").getPath)
    val propertiesUtil = PropertiesUtil("user", getClass.getClassLoader.getResource(s"./${PropertiesUtil.DEFAULT_APPLICATION_FILE_NAME}").getPath)

    val inputStructType = StructType(List(
      StructField(      "field1", StringType),
      StructField(variableWeight, BooleanType),
      StructField(      quantity, IntegerType),
      StructField(   grossWeight, IntegerType)
    ))
    val inputDF: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(List[Row](
        Row("1", false,    1, null),
        Row("2",  null,    2, null),
        Row("3",  true, null, 1000),
        Row("4", false, null, null),
        Row("5",  true, null, null)
      )),
      inputStructType
    )

    val expectedStructType = StructType(List(
      StructField(                "field1", StringType),
      StructField(     reservations.amount, DoubleType),
      StructField(reservations.unitMeasure, StringType, false)
    ))
    val expectedDF: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(List[Row](
        Row("1", 1.0, propertiesUtil.getEA),
        Row("2", 2.0, propertiesUtil.getEA),
        Row("3", 1.0, propertiesUtil.getKG)
      )),
      expectedStructType
    )

    val resultDF = OperationsUtils.calculateAmountAndUnitOfMeasurement(propertiesUtil)(inputDF)

    assertDataFrameEquals(expectedDF, resultDF)

  }

  "addArticleBasedOnSite " must "add a new column for article based on site and drop sms columns" taggedAs UnitTestTag in {
    val inputStructType = StructType(List(
      StructField(    "field1", StringType),
      StructField(smsSizeColor, StringType),
      StructField(       smsId, StringType),
      StructField(      siteId, StringType)
    ))
    val inputDF: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(List[Row](
        Row("1", "0000000001", "111111",  "bodegaSite"),
        Row("2", "0000000002", "222222",   "basicSite"),
        Row("3", "0000000003", "333333",   "c4nonfood"),
        Row("4", "0000000004", "444444", "anotherSite")
      )),
      inputStructType
    )


    val expectedStructType = StructType(List(
      StructField("field1", StringType),
      StructField(  siteId, StringType),
      StructField( article, StringType)
    ))
    val expectedDF: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(List[Row](
        Row("1",  "bodegaSite", "1111110000"),
        Row("2",   "basicSite", "2222220000"),
        Row("3",   "c4nonfood", "0000000003"),
        Row("4", "anotherSite", "0000000004")
      )),
      expectedStructType
    )
    val resultDF = inputDF.transform(OperationsUtils.addArticleBasedOnSite)
    expectedDF.count() shouldBe(resultDF.count())
    expectedDF.except(resultDF).count() shouldBe 0

  }

}