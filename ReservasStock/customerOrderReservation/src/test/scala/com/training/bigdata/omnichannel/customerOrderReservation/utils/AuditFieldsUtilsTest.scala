package com.training.bigdata.omnichannel.customerOrderReservation.utils

import java.sql.Timestamp

import com.training.bigdata.omnichannel.customerOrderReservation.entities.Audit._
import com.training.bigdata.omnichannel.customerOrderReservation.utils.tag.TagTest.UnitTestTag
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}


class AuditFieldsUtilsTest extends FlatSpec with Matchers with BeforeAndAfter with SparkLocalTest {

  val inputStructType: StructType = StructType(List(
    StructField("field1", StringType),
    StructField("field2", StringType)
  ))

  "addAuditFields" must "add user and timestamp of insertion and update" taggedAs UnitTestTag in {

    val defaultUser = "user"
    val defaultTimestamp = new Timestamp(System.currentTimeMillis)

    val inputDF: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(List[Row](
        Row("row11", "row12"),
        Row("row21", "row22")
      )),
      inputStructType
    )

    val expectedStructType = StructType(List(
      StructField("field1", StringType, true),
      StructField("field2", StringType, true),
      StructField(tsInsertDlk, TimestampType, false),
      StructField(userInsertDlk, StringType, false),
      StructField(tsUpdateDlk, TimestampType, false),
      StructField(userUpdateDlk, StringType, false)
    ))
    val expectedDF: DataFrame = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(List[Row](
        Row("row11", "row12", defaultTimestamp, defaultUser, defaultTimestamp, defaultUser),
        Row("row21", "row22", defaultTimestamp, defaultUser, defaultTimestamp, defaultUser)
      )),
      expectedStructType
    )

    val resultDF = inputDF.transform(AuditFieldsUtils.addAuditFields(defaultUser, defaultTimestamp))

    expectedDF.except(resultDF).count() shouldBe 0

  }

}