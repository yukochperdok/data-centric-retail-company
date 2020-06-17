package com.training.bigdata.omnichannel.stockATP.common.util

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{Audit, InsertInterface, UpdateInterface}
import com.training.bigdata.omnichannel.stockATP.common.util.tag.TagTest.UnitTestTag
import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}

case class AuditTest(
  param: String,
  tsInsertDlk: Timestamp,
  tsUpdateDlk: Timestamp) extends InsertInterface

case class AuditTest2(
  param: String,
  tsUpdateDlk: Timestamp) extends UpdateInterface


class AuditUtilsTest extends FlatSpec with Matchers with DatasetSuiteBase {
  import spark.implicits._
  override protected implicit def enableHiveSupport: Boolean = false

  "parseUpdateInfo" must "return tsInsertDlk if tsUpdateDlk is null" taggedAs (UnitTestTag) in {
    val ts1 = new Timestamp(100)
    val ts2 = new Timestamp(101)
    val ts3 = new Timestamp(102)

    val df1: DataFrame = sc.parallelize(List[AuditTest](
      AuditTest("A", ts1, ts1),
      AuditTest("B", ts2, ts3),
      AuditTest("C", ts3, ts2),
      AuditTest("D", ts2, null),
      AuditTest("E", null, ts3),
      AuditTest("F", null, null)
    )).toDF

    val dfExpected: DataFrame = sc.parallelize(List[AuditTest2](
      AuditTest2("A", ts1),
      AuditTest2("B", ts3),
      AuditTest2("C", ts2),
      AuditTest2("D", ts2),
      AuditTest2("E", ts3),
      AuditTest2("F", null)
    )).toDF

    val dfResult: DataFrame = df1.transform(AuditUtils.parseUpdateInfo)

    assertDataFrameEquals(dfResult, dfExpected)
  }

  "getLastUpdateDlkInInnerOrLeftJoin" must " take the most recent ts_update_dlk after a join"  taggedAs (UnitTestTag) in {
    val ts1 = new Timestamp(100)
    val ts2 = new Timestamp(101)
    val ts3 = new Timestamp(102)

    val auditStructType = StructType(List(
      StructField("param",                      StringType),
      StructField(Audit.ccTags.tsUpdateDlkTag,  TimestampType)))

    val df1: DataFrame = sc.parallelize(List[AuditTest2](
      AuditTest2("A", ts1),
      AuditTest2("B", ts3),
      AuditTest2("C", ts2),
      AuditTest2("D", ts2)
    )).toDF

    val df2: DataFrame = sc.parallelize(List[AuditTest2](
      AuditTest2("A", ts1),
      AuditTest2("B", ts2),
      AuditTest2("C", ts3))).toDF

    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("A", ts1),
      Row("B", ts3),
      Row("C", ts3),
      Row("D", ts2))),
      auditStructType)
      .orderBy("param")

    val dfResult = df1
      .join(df2, Seq("param"), Constants.LEFT)
      .transform(AuditUtils.getLastUpdateDlkInInnerOrLeftJoin(df2(Audit.ccTags.tsUpdateDlkTag)))
      .orderBy("param")
    assertDataFrameEquals(dfExpected, dfResult)

  }
}

