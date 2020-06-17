package com.training.bigdata.omnichannel.stockATP.common.util

import java.sql.Timestamp

import Constants._
import com.training.bigdata.omnichannel.stockATP.common.entities.ControlEjecucionView
import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces._
import com.training.bigdata.omnichannel.stockATP.common.util.tag.TagTest.UnitTestTag
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.{FlatSpec, Matchers}
import com.training.bigdata.omnichannel.stockATP.common.util.Constants.{INNER, LEFT, OUTER}

case class SurtidoTest(
  idArticle: String,
  idStore: String,
  first: String) extends SurtidoInterface

case class SurtidoTest2(
  idArticle: String,
  idStore: String,
  second: Int) extends SurtidoInterface

case class StoreTest(
  idStore: String,
  first: String) extends StoreInterface

case class StoreTest2(
  idStore: String,
  second: Int) extends StoreInterface

case class ArticleTest(
  idArticle: String,
  first: String) extends ArticuloInterface

case class ArticleTest2(
  idArticle: String,
  second: Int) extends ArticuloInterface

case class SubfamilyTest(
  idSubfamily: String,
  first: String) extends SubfamilyInterface

case class SubfamilyTest2(
  idSubfamily: String,
  second: Int) extends SubfamilyInterface

case class OrderTest(
  idOrder: String,
  idOrderLine: String,
  first: String) extends OrderInterface

case class OrderTest2(
  idOrder: String,
  idOrderLine: String,
  second: Int) extends OrderInterface

case class MaterialTest(
  idListMaterial: String,
  first: String) extends MaterialInterface

case class MaterialTest2(
  idListMaterial: String,
  second: String) extends MaterialInterface

case class UDFTest(
  id: Int,
  timestamp: Timestamp)

case class UDFStringToTimestampTest(date: String)
case class UDFStringToTimestampTestTs(ts: Timestamp)

class CommonSqlLayerTest extends FlatSpec with Matchers with DatasetSuiteBase {
  import spark.implicits._
  override protected implicit def enableHiveSupport: Boolean = false
  val currentTimestamp: Timestamp = new Timestamp(System.currentTimeMillis)
  val dateStringBase = new Timestamp(1535166367000L)
  val dateStringPlus1 =  new Timestamp(dateStringBase.getTime + 3600000)

  trait SurtidoConf{
    val articleStoreSomethingStructType = StructType(List(
      StructField(Surtido.ccTags.idArticleTag, StringType),
      StructField(Surtido.ccTags.idStoreTag,   StringType),
      StructField("something",  StringType)
    ))

    val articleStoreAmountStructType = StructType(List(
      StructField(Surtido.ccTags.idArticleTag, StringType),
      StructField(Surtido.ccTags.idStoreTag,   StringType),
      StructField("amount",    IntegerType)
    ))

    val df1: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](Row("1", "11", "aaa"))),
      articleStoreSomethingStructType
    )

    val df2: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](Row("1", "11", 10))),
      articleStoreAmountStructType
    )

    val df3: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](Row("1", "22", 10))),
      articleStoreAmountStructType
    )

    val df4: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](Row("2", "11", 10))),
      articleStoreAmountStructType
    )

    val resultStructType = StructType(List(
      StructField(Surtido.ccTags.idArticleTag, StringType),
      StructField(Surtido.ccTags.idStoreTag,   StringType),
      StructField("something",  StringType),
      StructField("amount",    IntegerType)
    ))

    val dfExpected: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](Row("1", "11", "aaa", 10))),
        resultStructType
      )

    val dfExpectedEmpty: DataFrame =
      spark.createDataFrame(sc.parallelize(List.empty[Row]),
        resultStructType
      )
  }

  trait SurtidoConfWithAudit{

    val articleStoreSomethingWithAuditStructType = StructType(List(
      StructField(Surtido.ccTags.idArticleTag, StringType),
      StructField(Surtido.ccTags.idStoreTag,   StringType),
      StructField("something",  StringType),
      StructField(Audit.ccTags.tsUpdateDlkTag,  TimestampType)
    ))

    val articleStoreAmountWithAuditStructType = StructType(List(
      StructField(Surtido.ccTags.idArticleTag, StringType),
      StructField(Surtido.ccTags.idStoreTag,   StringType),
      StructField("amount",    IntegerType),
      StructField(Audit.ccTags.tsUpdateDlkTag,  TimestampType)
    ))

    val resultStructWithAuditType = StructType(List(
      StructField(Surtido.ccTags.idArticleTag, StringType, false),
      StructField(Surtido.ccTags.idStoreTag,   StringType, false),
      StructField("something",  StringType),
      StructField("amount",    IntegerType),
      StructField(Audit.ccTags.tsUpdateDlkTag,  TimestampType)
    ))

  }

  // Testing filterControlEjecucion

  "filterControlEjecucion " must " return the filtered execution control by process identification " taggedAs (UnitTestTag) in {

    val listProcesses: List[String] = "idProcess1" :: "idProcess2" :: Nil

    val ds: Dataset[ControlEjecucionView] = sc.parallelize(
      List[ControlEjecucionView](
        ControlEjecucionView("idProcess1", currentTimestamp, currentTimestamp, currentTimestamp),
        ControlEjecucionView("idProcess2", currentTimestamp, currentTimestamp, currentTimestamp),
        ControlEjecucionView("idProcess3", currentTimestamp, currentTimestamp, currentTimestamp),
        ControlEjecucionView("idProcess4", currentTimestamp, currentTimestamp, currentTimestamp)
      )
    ).toDS

    val dsExpected: Dataset[ControlEjecucionView] = sc.parallelize(
      List[ControlEjecucionView](
        ControlEjecucionView("idProcess1", currentTimestamp, currentTimestamp, currentTimestamp),
        ControlEjecucionView("idProcess2", currentTimestamp, currentTimestamp, currentTimestamp)
      )
    ).toDS

    val filterResult = ds.transform(CommonSqlLayer.filterControlEjecucion(listProcesses))
    assertDatasetEquals(filterResult, dsExpected)

  }

  it must " return empty dataset if there are not any target processes " taggedAs (UnitTestTag) in {

    val listProcesses: List[String] = "idProcess5" :: "idProcess6" :: Nil

    val ds: Dataset[ControlEjecucionView] = sc.parallelize(
      List[ControlEjecucionView](
        ControlEjecucionView("idProcess1", currentTimestamp, currentTimestamp, currentTimestamp),
        ControlEjecucionView("idProcess2", currentTimestamp, currentTimestamp, currentTimestamp),
        ControlEjecucionView("idProcess3", currentTimestamp, currentTimestamp, currentTimestamp),
        ControlEjecucionView("idProcess4", currentTimestamp, currentTimestamp, currentTimestamp)
      )
    ).toDS

    val dsExpected: Dataset[ControlEjecucionView] = sc.parallelize(List.empty[ControlEjecucionView]).toDS

    val filterResult = ds.transform(CommonSqlLayer.filterControlEjecucion(listProcesses))
    assertDatasetEquals(filterResult, dsExpected)

  }

  it must " return empty dataset if there are not any processes in control execution table" taggedAs (UnitTestTag) in {

    val listProcesses: List[String] = "idProcess1" :: "idProcess2" :: Nil

    val ds: Dataset[ControlEjecucionView] = sc.parallelize(List.empty[ControlEjecucionView]).toDS

    val dsExpected: Dataset[ControlEjecucionView] = sc.parallelize(List.empty[ControlEjecucionView]).toDS

    val filterResult = ds.transform(CommonSqlLayer.filterControlEjecucion(listProcesses))
    assertDatasetEquals(filterResult, dsExpected)

  }

  // Testing joinSurtidoByArticleAndLocation

  "outer joinSurtidoByArticleAndLocation " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "aaa"))).toDS
    val ds2: Dataset[SurtidoTest2] = sc.parallelize(List[SurtidoTest2](SurtidoTest2("1", "11", 10))).toDS

    val dsExpected: Dataset[(SurtidoTest, SurtidoTest2)] =
      sc.parallelize(List[(SurtidoTest, SurtidoTest2)]((SurtidoTest("1", "11", "aaa"), SurtidoTest2("1", "11", 10)))).toDS

    val joinResult = CommonSqlLayer.joinSurtidoByArticleAndLocation(ds1, ds2, OUTER)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "outer joinSurtidoByArticleAndLocation without the same keys " must
    " return the a dataset of a tuple with A and B with all registries" taggedAs (UnitTestTag) in {

    val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "aaa"))).toDS
    val ds2: Dataset[SurtidoTest2] = sc.parallelize(List[SurtidoTest2](SurtidoTest2("1", "22", 10))).toDS

    val dsExpected: Dataset[(SurtidoTest, SurtidoTest2)] =
      sc.parallelize(List[(SurtidoTest, SurtidoTest2)](
        (null, SurtidoTest2("1", "22", 10)),
        (SurtidoTest("1", "11", "aaa"), null)
      )).toDS

    val joinResult = CommonSqlLayer.joinSurtidoByArticleAndLocation(ds1, ds2, OUTER)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "inner joinSurtidoByArticleAndLocation " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "aaa"))).toDS
    val ds2: Dataset[SurtidoTest2] = sc.parallelize(List[SurtidoTest2](SurtidoTest2("1", "11", 10))).toDS

    val dsExpected: Dataset[(SurtidoTest, SurtidoTest2)] =
      sc.parallelize(List[(SurtidoTest, SurtidoTest2)]((SurtidoTest("1", "11", "aaa"), SurtidoTest2("1", "11", 10)))).toDS

    val joinResult = CommonSqlLayer.joinSurtidoByArticleAndLocation(ds1, ds2, INNER)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "default filter type as inner joinSurtidoByArticleAndLocation " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "aaa"))).toDS
    val ds2: Dataset[SurtidoTest2] = sc.parallelize(List[SurtidoTest2](SurtidoTest2("1", "11", 10))).toDS

    val dsExpected: Dataset[(SurtidoTest, SurtidoTest2)] =
      sc.parallelize(List[(SurtidoTest, SurtidoTest2)]((SurtidoTest("1", "11", "aaa"), SurtidoTest2("1", "11", 10)))).toDS

    val joinResult = CommonSqlLayer.joinSurtidoByArticleAndLocation(ds1, ds2)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "inner joinSurtidoByArticleAndLocation without the same keys " must
    " return the a dataset of a tuple with A and B without registries" taggedAs (UnitTestTag) in {

    val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "aaa"))).toDS
    val ds2: Dataset[SurtidoTest2] = sc.parallelize(List[SurtidoTest2](SurtidoTest2("1", "22", 10))).toDS

    val dsExpected: Dataset[(SurtidoTest, SurtidoTest2)] =
      sc.parallelize(List[(SurtidoTest, SurtidoTest2)]()).toDS

    val joinResult = CommonSqlLayer.joinSurtidoByArticleAndLocation(ds1, ds2, INNER)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "default filter type as inner joinSurtidoByArticleAndLocation without the same keys " must
    " return the a dataset of a tuple with A and B without registries" taggedAs (UnitTestTag) in {

    val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "aaa"))).toDS
    val ds2: Dataset[SurtidoTest2] = sc.parallelize(List[SurtidoTest2](SurtidoTest2("1", "22", 10))).toDS

    val dsExpected: Dataset[(SurtidoTest, SurtidoTest2)] =
      sc.parallelize(List[(SurtidoTest, SurtidoTest2)]()).toDS

    val joinResult = CommonSqlLayer.joinSurtidoByArticleAndLocation(ds1, ds2)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

    "left joinSurtidoByArticleAndLocation " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

      val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "aaa"))).toDS
      val ds2: Dataset[SurtidoTest2] = sc.parallelize(List[SurtidoTest2](SurtidoTest2("1", "11", 10))).toDS

      val dsExpected: Dataset[(SurtidoTest, SurtidoTest2)] =
        sc.parallelize(List[(SurtidoTest, SurtidoTest2)](
          (SurtidoTest("1", "11", "aaa"), SurtidoTest2("1", "11", 10)))).toDS

      val joinResult = CommonSqlLayer.joinSurtidoByArticleAndLocation(ds1, ds2, LEFT)(spark)
      assertDatasetEquals(joinResult, dsExpected)

    }

    "left joinSurtidoByArticleAndLocation without the same keys " must
      " return the a dataset of a tuple with A and B with one registry with null in the right part" taggedAs (UnitTestTag) in {

      val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "aaa"))).toDS
      val ds2: Dataset[SurtidoTest2] = sc.parallelize(List[SurtidoTest2](SurtidoTest2("1", "22", 10))).toDS

      val dsExpected: Dataset[(SurtidoTest, SurtidoTest2)] =
        sc.parallelize(List[(SurtidoTest, SurtidoTest2)](
          (SurtidoTest("1", "11", "aaa"), null))).toDS

      val joinResult = CommonSqlLayer.joinSurtidoByArticleAndLocation(ds1, ds2, LEFT)(spark)
      assertDatasetEquals(joinResult, dsExpected)

    }


  // Testing joinSurtidoBySubfamily
  "outer joinSurtidoBySubfamily " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[SubfamilyTest] = sc.parallelize(List[SubfamilyTest](SubfamilyTest("1", "aaa"))).toDS
    val ds2: Dataset[SubfamilyTest2] = sc.parallelize(List[SubfamilyTest2](SubfamilyTest2("1", 55))).toDS

    val dsExpected: Dataset[(SubfamilyTest, SubfamilyTest2)] =
      sc.parallelize(List[(SubfamilyTest, SubfamilyTest2)]((SubfamilyTest("1", "aaa"), SubfamilyTest2("1", 55)))).toDS

    val joinResult = CommonSqlLayer.joinSurtidoBySubfamily(ds1, ds2, OUTER)(spark)

    assertDatasetEquals(joinResult, dsExpected)
  }

  "outer joinSurtidoBySubfamily without the same keys " must
    " return the a dataset of a tuple with A and B with all registries" taggedAs (UnitTestTag) in {

    val ds1: Dataset[SubfamilyTest] = sc.parallelize(List[SubfamilyTest](SubfamilyTest("1", "aaa"))).toDS
    val ds2: Dataset[SubfamilyTest2] = sc.parallelize(List[SubfamilyTest2](SubfamilyTest2("3", 55))).toDS

    val joinResult = CommonSqlLayer.joinSurtidoBySubfamily(ds1, ds2, OUTER)(spark)

    val result: List[(SubfamilyTest, SubfamilyTest2)] = joinResult.collect().toList

    result should contain (SubfamilyTest("1", "aaa"), null)
    result should contain (null, SubfamilyTest2("3", 55))
    result.size shouldBe 2
  }

  "inner joinSurtidoBySubfamily " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[SubfamilyTest] = sc.parallelize(List[SubfamilyTest](SubfamilyTest("1", "aaa"))).toDS
    val ds2: Dataset[SubfamilyTest2] = sc.parallelize(List[SubfamilyTest2](SubfamilyTest2("1", 55))).toDS

    val dsExpected: Dataset[(SubfamilyTest, SubfamilyTest2)] =
      sc.parallelize(List[(SubfamilyTest, SubfamilyTest2)]((SubfamilyTest("1", "aaa"), SubfamilyTest2("1", 55)))).toDS

    val joinResult = CommonSqlLayer.joinSurtidoBySubfamily(ds1, ds2, INNER)(spark)

    assertDatasetEquals(joinResult, dsExpected)
  }

  "default filter type as inner joinSurtidoBySubfamily " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[SubfamilyTest] = sc.parallelize(List[SubfamilyTest](SubfamilyTest("1", "aaa"))).toDS
    val ds2: Dataset[SubfamilyTest2] = sc.parallelize(List[SubfamilyTest2](SubfamilyTest2("1", 55))).toDS

    val dsExpected: Dataset[(SubfamilyTest, SubfamilyTest2)] =
      sc.parallelize(List[(SubfamilyTest, SubfamilyTest2)]((SubfamilyTest("1", "aaa"), SubfamilyTest2("1", 55)))).toDS

    val joinResult = CommonSqlLayer.joinSurtidoBySubfamily(ds1, ds2)(spark)

    assertDatasetEquals(joinResult, dsExpected)
  }

  "inner joinSurtidoBySubfamily without the same keys " must
    " return the a dataset of a tuple with A and B without registries" taggedAs (UnitTestTag) in {

    val ds1: Dataset[SubfamilyTest] = sc.parallelize(List[SubfamilyTest](SubfamilyTest("1", "aaa"))).toDS
    val ds2: Dataset[SubfamilyTest2] = sc.parallelize(List[SubfamilyTest2](SubfamilyTest2("3", 55))).toDS

    val dsExpected: Dataset[(SubfamilyTest, SubfamilyTest2)] =
      sc.parallelize(List[(SubfamilyTest, SubfamilyTest2)]()).toDS

    val joinResult = CommonSqlLayer.joinSurtidoBySubfamily(ds1, ds2, INNER)(spark)

    assertDatasetEquals(joinResult, dsExpected)
  }

  "default filter type as inner joinSurtidoBySubfamily without the same keys " must
    " return the a dataset of a tuple with A and B without registries" taggedAs (UnitTestTag) in {

    val ds1: Dataset[SubfamilyTest] = sc.parallelize(List[SubfamilyTest](SubfamilyTest("1", "aaa"))).toDS
    val ds2: Dataset[SubfamilyTest2] = sc.parallelize(List[SubfamilyTest2](SubfamilyTest2("3", 55))).toDS

    val dsExpected: Dataset[(SubfamilyTest, SubfamilyTest2)] =
      sc.parallelize(List[(SubfamilyTest, SubfamilyTest2)]()).toDS

    val joinResult = CommonSqlLayer.joinSurtidoBySubfamily(ds1, ds2)(spark)

    assertDatasetEquals(joinResult, dsExpected)
  }

  "left joinSurtidoBySubfamily " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[SubfamilyTest] = sc.parallelize(List[SubfamilyTest](SubfamilyTest("1", "aaa"))).toDS
    val ds2: Dataset[SubfamilyTest2] = sc.parallelize(List[SubfamilyTest2](SubfamilyTest2("1", 55))).toDS

    val dsExpected: Dataset[(SubfamilyTest, SubfamilyTest2)] =
      sc.parallelize(List[(SubfamilyTest, SubfamilyTest2)]((SubfamilyTest("1", "aaa"), SubfamilyTest2("1", 55)))).toDS

    val joinResult = CommonSqlLayer.joinSurtidoBySubfamily(ds1, ds2, LEFT)(spark)

    assertDatasetEquals(joinResult, dsExpected)
  }

  "left joinSurtidoBySubfamily without the same keys " must
    " return the a dataset of a tuple with A and B with one registry with null in the right part" taggedAs (UnitTestTag) in {

    val ds1: Dataset[SubfamilyTest] = sc.parallelize(List[SubfamilyTest](SubfamilyTest("1", "aaa"))).toDS
    val ds2: Dataset[SubfamilyTest2] = sc.parallelize(List[SubfamilyTest2](SubfamilyTest2("3", 55))).toDS

    val dsExpected: Dataset[(SubfamilyTest, SubfamilyTest2)] =
      sc.parallelize(List[(SubfamilyTest, SubfamilyTest2)](
        (SubfamilyTest("1", "aaa"), null))).toDS

    val joinResult = CommonSqlLayer.joinSurtidoBySubfamily(ds1, ds2, LEFT)(spark)

    assertDatasetEquals(joinResult, dsExpected)
  }


       // Testing joinSurtidoByArticle

       "outer joinSurtidoByArticle " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

         val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "A"))).toDS
         val ds2: Dataset[ArticleTest2] = sc.parallelize(List[ArticleTest2](ArticleTest2("1", 5))).toDS

         val dsExpected: Dataset[(SurtidoTest, ArticleTest2)] =
           sc.parallelize(List[(SurtidoTest, ArticleTest2)]
             ((SurtidoTest("1", "11", "A"), ArticleTest2("1", 5)))).toDS

         val joinResult = CommonSqlLayer.joinSurtidoByArticle(ds1, ds2, OUTER)(spark)
         assertDatasetEquals(joinResult, dsExpected)

       }

       "outer joinSurtidoByArticle without the same keys " must
         " return the a dataset of a tuple with A and B with all registries" taggedAs (UnitTestTag) in {

         val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "A"))).toDS
         val ds2: Dataset[ArticleTest2] = sc.parallelize(List[ArticleTest2](ArticleTest2("3", 5))).toDS

         val dsExpected: Dataset[(SurtidoTest, ArticleTest2)] =
           sc.parallelize(List[(SurtidoTest, ArticleTest2)](
             (SurtidoTest("1", "11", "A"), null),
               (null, ArticleTest2("3", 5))
           )).toDS

         val joinResult = CommonSqlLayer.joinSurtidoByArticle(ds1, ds2, OUTER)(spark)
         assertDatasetEquals(joinResult, dsExpected)

       }

       "inner joinSurtidoByArticle " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

         val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "A"))).toDS
         val ds2: Dataset[ArticleTest2] = sc.parallelize(List[ArticleTest2](ArticleTest2("1", 5))).toDS

         val dsExpected: Dataset[(SurtidoTest, ArticleTest2)] =
           sc.parallelize(List[(SurtidoTest, ArticleTest2)]
             ((SurtidoTest("1", "11", "A"), ArticleTest2("1", 5)))).toDS

         val joinResult = CommonSqlLayer.joinSurtidoByArticle(ds1, ds2, INNER)(spark)
         assertDatasetEquals(joinResult, dsExpected)

       }

       "default filter type as inner joinSurtidoByArticle " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

         val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "A"))).toDS
         val ds2: Dataset[ArticleTest2] = sc.parallelize(List[ArticleTest2](ArticleTest2("1", 5))).toDS

         val dsExpected: Dataset[(SurtidoTest, ArticleTest2)] =
           sc.parallelize(List[(SurtidoTest, ArticleTest2)]
             ((SurtidoTest("1", "11", "A"), ArticleTest2("1", 5)))).toDS

         val joinResult = CommonSqlLayer.joinSurtidoByArticle(ds1, ds2)(spark)
         assertDatasetEquals(joinResult, dsExpected)

       }

       "inner joinSurtidoByArticle without the same keys " must
         " return the a dataset of a tuple with A and B without registries" taggedAs (UnitTestTag) in {

         val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "A"))).toDS
         val ds2: Dataset[ArticleTest2] = sc.parallelize(List[ArticleTest2](ArticleTest2("3", 5))).toDS

         val dsExpected: Dataset[(SurtidoTest, ArticleTest2)] = sc.parallelize(List[(SurtidoTest, ArticleTest2)]()).toDS

         val joinResult = CommonSqlLayer.joinSurtidoByArticle(ds1, ds2, INNER)(spark)
         assertDatasetEquals(joinResult, dsExpected)

       }

       "default filter type as inner joinSurtidoByArticle without the same keys " must
         " return the a dataset of a tuple with A and B without registries" taggedAs (UnitTestTag) in {

         val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "A"))).toDS
         val ds2: Dataset[ArticleTest2] = sc.parallelize(List[ArticleTest2](ArticleTest2("3", 5))).toDS

         val dsExpected: Dataset[(SurtidoTest, ArticleTest2)] =
           sc.parallelize(List[(SurtidoTest, ArticleTest2)]()).toDS

         val joinResult = CommonSqlLayer.joinSurtidoByArticle(ds1, ds2)(spark)
         assertDatasetEquals(joinResult, dsExpected)

       }

       "left joinSurtidoByArticle " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

         val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "A"))).toDS
         val ds2: Dataset[ArticleTest2] = sc.parallelize(List[ArticleTest2](ArticleTest2("1", 5))).toDS

         val dsExpected: Dataset[(SurtidoTest, ArticleTest2)] =
           sc.parallelize(List[(SurtidoTest, ArticleTest2)]
             ((SurtidoTest("1", "11", "A"), ArticleTest2("1", 5)))).toDS

         val joinResult = CommonSqlLayer.joinSurtidoByArticle(ds1, ds2, LEFT)(spark)
         assertDatasetEquals(joinResult, dsExpected)

       }

       "left joinSurtidoByArticle without the same keys " must
         " return the a dataset of a tuple with A and B with one registry with null in the right part" taggedAs (UnitTestTag) in {

         val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "A"))).toDS
         val ds2: Dataset[ArticleTest2] = sc.parallelize(List[ArticleTest2](ArticleTest2("3", 5))).toDS

         val dsExpected: Dataset[(SurtidoTest, ArticleTest2)] =
           sc.parallelize(List[(SurtidoTest, ArticleTest2)](
             (SurtidoTest("1", "11", "A"), null))).toDS

         val joinResult = CommonSqlLayer.joinSurtidoByArticle(ds1, ds2, LEFT)(spark)
         assertDatasetEquals(joinResult, dsExpected)

       }

  // Testing joinStore

  "outer joinStore " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[StoreTest] = sc.parallelize(List[StoreTest](StoreTest("1", "11"))).toDS
    val ds2: Dataset[StoreTest2] = sc.parallelize(List[StoreTest2](StoreTest2("1", 5))).toDS

    val dsExpected: Dataset[(StoreTest, StoreTest2)] =
      sc.parallelize(List[(StoreTest, StoreTest2)]
        ((StoreTest("1", "11"), StoreTest2("1", 5)))).toDS

    val joinResult = CommonSqlLayer.joinStore(ds1, ds2, OUTER)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "outer joinStore without the same keys " must
    " return the a dataset of a tuple with A and B with all registries" taggedAs (UnitTestTag) in {

    val ds1: Dataset[StoreTest] = sc.parallelize(List[StoreTest](StoreTest("1", "11"))).toDS
    val ds2: Dataset[StoreTest2] = sc.parallelize(List[StoreTest2](StoreTest2("3", 5))).toDS

    val dsExpected: Dataset[(StoreTest, StoreTest2)] =
      sc.parallelize(List[(StoreTest, StoreTest2)](
        (StoreTest("1", "11"), null),
          (null, StoreTest2("3", 5))
      )).toDS

    val joinResult = CommonSqlLayer.joinStore(ds1, ds2, OUTER)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "inner joinStore " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[StoreTest] = sc.parallelize(List[StoreTest](StoreTest("1", "11"))).toDS
    val ds2: Dataset[StoreTest2] = sc.parallelize(List[StoreTest2](StoreTest2("1", 5))).toDS

    val dsExpected: Dataset[(StoreTest, StoreTest2)] =
      sc.parallelize(List[(StoreTest, StoreTest2)]
        ((StoreTest("1", "11"), StoreTest2("1", 5)))).toDS

    val joinResult = CommonSqlLayer.joinStore(ds1, ds2, INNER)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "default filter type as inner joinStore " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[StoreTest] = sc.parallelize(List[StoreTest](StoreTest("1", "11"))).toDS
    val ds2: Dataset[StoreTest2] = sc.parallelize(List[StoreTest2](StoreTest2("1", 5))).toDS

    val dsExpected: Dataset[(StoreTest, StoreTest2)] =
      sc.parallelize(List[(StoreTest, StoreTest2)]
        ((StoreTest("1", "11"), StoreTest2("1", 5)))).toDS

    val joinResult = CommonSqlLayer.joinStore(ds1, ds2)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "inner joinStore without the same keys " must
    " return the a dataset of a tuple with A and B without registries" taggedAs (UnitTestTag) in {

    val ds1: Dataset[StoreTest] = sc.parallelize(List[StoreTest](StoreTest("1", "11"))).toDS
    val ds2: Dataset[StoreTest2] = sc.parallelize(List[StoreTest2](StoreTest2("3", 5))).toDS

    val dsExpected: Dataset[(StoreTest, StoreTest2)] = sc.parallelize(List[(StoreTest, StoreTest2)]()).toDS

    val joinResult = CommonSqlLayer.joinStore(ds1, ds2, INNER)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "default filter type as inner joinStore without the same keys " must
    " return the a dataset of a tuple with A and B without registries" taggedAs (UnitTestTag) in {

    val ds1: Dataset[StoreTest] = sc.parallelize(List[StoreTest](StoreTest("1", "11"))).toDS
    val ds2: Dataset[StoreTest2] = sc.parallelize(List[StoreTest2](StoreTest2("3", 5))).toDS

    val dsExpected: Dataset[(StoreTest, StoreTest2)] =
      sc.parallelize(List[(StoreTest, StoreTest2)]()).toDS

    val joinResult = CommonSqlLayer.joinStore(ds1, ds2)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "left joinStore " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[StoreTest] = sc.parallelize(List[StoreTest](StoreTest("1", "11"))).toDS
    val ds2: Dataset[StoreTest2] = sc.parallelize(List[StoreTest2](StoreTest2("1", 5))).toDS

    val dsExpected: Dataset[(StoreTest, StoreTest2)] =
      sc.parallelize(List[(StoreTest, StoreTest2)]
        ((StoreTest("1", "11"), StoreTest2("1", 5)))).toDS

    val joinResult = CommonSqlLayer.joinStore(ds1, ds2, LEFT)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "left joinStore without the same keys " must
    " return the a dataset of a tuple with A and B with one registry with null in the right part" taggedAs (UnitTestTag) in {

    val ds1: Dataset[StoreTest] = sc.parallelize(List[StoreTest](StoreTest("1", "11"))).toDS
    val ds2: Dataset[StoreTest2] = sc.parallelize(List[StoreTest2](StoreTest2("3", 5))).toDS

    val dsExpected: Dataset[(StoreTest, StoreTest2)] =
      sc.parallelize(List[(StoreTest, StoreTest2)](
        (StoreTest("1", "11"), null))).toDS

    val joinResult = CommonSqlLayer.joinStore(ds1, ds2, LEFT)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }


  // Testing joinMaterial

  "outer joinMaterial " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[MaterialTest] = sc.parallelize(List[MaterialTest](MaterialTest("1","11"))).toDS
    val ds2: Dataset[MaterialTest2] = sc.parallelize(List[MaterialTest2](MaterialTest2("1","22"))).toDS

    val dsExpected: Dataset[(MaterialTest, MaterialTest2)] =
      sc.parallelize(List[(MaterialTest, MaterialTest2)]
        ((MaterialTest("1", "11"), MaterialTest2("1", "22")))).toDS

    val joinResult = CommonSqlLayer.joinMaterial(ds1, ds2, OUTER)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "outer joinMaterial without the same keys " must
    " return the a dataset of a tuple with A and B with all registries" taggedAs (UnitTestTag) in {

    val ds1: Dataset[MaterialTest] = sc.parallelize(List[MaterialTest](MaterialTest("1","22"))).toDS
    val ds2: Dataset[MaterialTest2] = sc.parallelize(List[MaterialTest2](MaterialTest2("3","11"))).toDS

    val dsExpected: Dataset[(MaterialTest, MaterialTest2)] =
      sc.parallelize(List[(MaterialTest, MaterialTest2)](
        (MaterialTest("1", "22"), null),
        (null, MaterialTest2("3", "11"))
      )).toDS


    val joinResult = CommonSqlLayer.joinMaterial(ds1, ds2, OUTER)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "inner joinMaterial " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[MaterialTest] = sc.parallelize(List[MaterialTest](MaterialTest("1","11"))).toDS
    val ds2: Dataset[MaterialTest2] = sc.parallelize(List[MaterialTest2](MaterialTest2("1","22"))).toDS

    val dsExpected: Dataset[(MaterialTest, MaterialTest2)] =
      sc.parallelize(List[(MaterialTest, MaterialTest2)]
        ((MaterialTest("1", "11"), MaterialTest2("1", "22")))).toDS

    val joinResult = CommonSqlLayer.joinMaterial(ds1, ds2, INNER)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "default filter type as inner joinMaterial " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[MaterialTest] = sc.parallelize(List[MaterialTest](MaterialTest("1", "11"))).toDS
    val ds2: Dataset[MaterialTest2] = sc.parallelize(List[MaterialTest2](MaterialTest2("1", "22"))).toDS

    val dsExpected: Dataset[(MaterialTest, MaterialTest2)] =
      sc.parallelize(List[(MaterialTest, MaterialTest2)]
        ((MaterialTest("1", "11"), MaterialTest2("1", "22")))).toDS

    val joinResult = CommonSqlLayer.joinMaterial(ds1, ds2)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "inner joinMaterial without the same keys " must
    " return the a dataset of a tuple with A and B without registries" taggedAs (UnitTestTag) in {

    val ds1: Dataset[MaterialTest] = sc.parallelize(List[MaterialTest](MaterialTest("1", "11"))).toDS
    val ds2: Dataset[MaterialTest2] = sc.parallelize(List[MaterialTest2](MaterialTest2("3", "22"))).toDS

    val dsExpected: Dataset[(MaterialTest, MaterialTest2)] = sc.parallelize(List[(MaterialTest, MaterialTest2)]()).toDS

    val joinResult = CommonSqlLayer.joinMaterial(ds1, ds2, INNER)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "default filter type as inner joinMaterial without the same keys " must
    " return the a dataset of a tuple with A and B without registries" taggedAs (UnitTestTag) in {

    val ds1: Dataset[MaterialTest] = sc.parallelize(List[MaterialTest](MaterialTest("1", "11"))).toDS
    val ds2: Dataset[MaterialTest2] = sc.parallelize(List[MaterialTest2](MaterialTest2("3", "22"))).toDS

    val dsExpected: Dataset[(MaterialTest, MaterialTest2)] =
      sc.parallelize(List[(MaterialTest, MaterialTest2)]()).toDS

    val joinResult = CommonSqlLayer.joinMaterial(ds1, ds2)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "left joinMaterial " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[MaterialTest] = sc.parallelize(List[MaterialTest](MaterialTest("1", "11"))).toDS
    val ds2: Dataset[MaterialTest2] = sc.parallelize(List[MaterialTest2](MaterialTest2("1", "22"))).toDS

    val dsExpected: Dataset[(MaterialTest, MaterialTest2)] =
      sc.parallelize(List[(MaterialTest, MaterialTest2)]
        ((MaterialTest("1", "11"), MaterialTest2("1", "22")))).toDS

    val joinResult = CommonSqlLayer.joinMaterial(ds1, ds2, LEFT)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "left joinMaterial without the same keys " must
    " return the a dataset of a tuple with A and B with one registry with null in the right part" taggedAs (UnitTestTag) in {

    val ds1: Dataset[MaterialTest] = sc.parallelize(List[MaterialTest](MaterialTest("1", "11"))).toDS
    val ds2: Dataset[MaterialTest2] = sc.parallelize(List[MaterialTest2](MaterialTest2("3", "22"))).toDS

    val dsExpected: Dataset[(MaterialTest, MaterialTest2)] =
      sc.parallelize(List[(MaterialTest, MaterialTest2)](
        (MaterialTest("1", "11"), null))).toDS

    val joinResult = CommonSqlLayer.joinMaterial(ds1, ds2, LEFT)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  // Testing joinSurtidoWithStore

  "outer joinSurtidoWithStore " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "A"))).toDS
    val ds2: Dataset[StoreTest2] = sc.parallelize(List[StoreTest2](StoreTest2("11", 5))).toDS

    val dsExpected: Dataset[(SurtidoTest, StoreTest2)] =
      sc.parallelize(List[(SurtidoTest, StoreTest2)]
        ((SurtidoTest("1", "11", "A"), StoreTest2("11", 5)))).toDS

    val joinResult = CommonSqlLayer.joinSurtidoWithStore(ds1, ds2, OUTER)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "outer joinSurtidoWithStore without the same keys " must
    " return the a dataset of a tuple with A and B with all registries" taggedAs (UnitTestTag) in {

    val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "A"))).toDS
    val ds2: Dataset[StoreTest2] = sc.parallelize(List[StoreTest2](StoreTest2("33", 5))).toDS

    val joinResult = CommonSqlLayer.joinSurtidoWithStore(ds1, ds2, OUTER)(spark)
    val result = joinResult.collect()
    result.size shouldBe 2
    result should contain (null, StoreTest2("33", 5))
    result should contain (SurtidoTest("1", "11", "A"), null)

  }

  "inner joinSurtidoWithStore " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "A"))).toDS
    val ds2: Dataset[StoreTest2] = sc.parallelize(List[StoreTest2](StoreTest2("11", 5))).toDS

    val dsExpected: Dataset[(SurtidoTest, StoreTest2)] =
      sc.parallelize(List[(SurtidoTest, StoreTest2)]
        ((SurtidoTest("1", "11", "A"), StoreTest2("11", 5)))).toDS

    val joinResult = CommonSqlLayer.joinSurtidoWithStore(ds1, ds2, INNER)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "default filter type as inner joinSurtidoWithStore " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "A"))).toDS
    val ds2: Dataset[StoreTest2] = sc.parallelize(List[StoreTest2](StoreTest2("11", 5))).toDS

    val dsExpected: Dataset[(SurtidoTest, StoreTest2)] =
      sc.parallelize(List[(SurtidoTest, StoreTest2)]
        ((SurtidoTest("1", "11", "A"), StoreTest2("11", 5)))).toDS

    val joinResult = CommonSqlLayer.joinSurtidoWithStore(ds1, ds2)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "inner joinSurtidoWithStore without the same keys " must
    " return the a dataset of a tuple with A and B without registries" taggedAs (UnitTestTag) in {

    val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "A"))).toDS
    val ds2: Dataset[StoreTest2] = sc.parallelize(List[StoreTest2](StoreTest2("33", 5))).toDS

    val dsExpected: Dataset[(SurtidoTest, StoreTest2)] = sc.parallelize(List[(SurtidoTest, StoreTest2)]()).toDS

    val joinResult = CommonSqlLayer.joinSurtidoWithStore(ds1, ds2, INNER)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "default filter type as inner joinSurtidoWithStore without the same keys " must
    " return the a dataset of a tuple with A and B without registries" taggedAs (UnitTestTag) in {

    val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "A"))).toDS
    val ds2: Dataset[StoreTest2] = sc.parallelize(List[StoreTest2](StoreTest2("33", 5))).toDS

    val dsExpected: Dataset[(SurtidoTest, StoreTest2)] =
      sc.parallelize(List[(SurtidoTest, StoreTest2)]()).toDS

    val joinResult = CommonSqlLayer.joinSurtidoWithStore(ds1, ds2)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "left joinSurtidoWithStore " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "A"))).toDS
    val ds2: Dataset[StoreTest2] = sc.parallelize(List[StoreTest2](StoreTest2("11", 5))).toDS

    val dsExpected: Dataset[(SurtidoTest, StoreTest2)] =
      sc.parallelize(List[(SurtidoTest, StoreTest2)]
        ((SurtidoTest("1", "11", "A"), StoreTest2("11", 5)))).toDS

    val joinResult = CommonSqlLayer.joinSurtidoWithStore(ds1, ds2, LEFT)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "left joinSurtidoWithStore without the same keys " must
    " return the a dataset of a tuple with A and B with one registry with null in the right part" taggedAs (UnitTestTag) in {

    val ds1: Dataset[SurtidoTest] = sc.parallelize(List[SurtidoTest](SurtidoTest("1", "11", "A"))).toDS
    val ds2: Dataset[StoreTest2] = sc.parallelize(List[StoreTest2](StoreTest2("33", 5))).toDS

    val dsExpected: Dataset[(SurtidoTest, StoreTest2)] =
      sc.parallelize(List[(SurtidoTest, StoreTest2)](
        (SurtidoTest("1", "11", "A"), null))).toDS

    val joinResult = CommonSqlLayer.joinSurtidoWithStore(ds1, ds2, LEFT)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  // Testing joinPedidos

  "outer joinPedidos " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[OrderTest] = sc.parallelize(List[OrderTest](OrderTest("1", "11", "3"))).toDS
    val ds2: Dataset[OrderTest2] = sc.parallelize(List[OrderTest2](OrderTest2("1", "11", 5))).toDS

    val dsExpected: Dataset[(OrderTest, OrderTest2)] =
      sc.parallelize(List[(OrderTest, OrderTest2)]
        ((OrderTest("1", "11", "3"), OrderTest2("1", "11", 5)))).toDS

    val joinResult = CommonSqlLayer.joinPedidos(ds1, ds2, OUTER)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "outer joinPedidos without the same keys " must
    " return the a dataset of a tuple with A and B with all registries" taggedAs (UnitTestTag) in {

    val ds1: Dataset[OrderTest] = sc.parallelize(List[OrderTest](OrderTest("1", "11", "3"))).toDS
    val ds2: Dataset[OrderTest2] = sc.parallelize(List[OrderTest2](OrderTest2("3", "11", 5))).toDS

    val joinResult = CommonSqlLayer.joinPedidos(ds1, ds2, OUTER)(spark)
    val result = joinResult.collect()
    result.size shouldBe 2
    result should contain (null, OrderTest2("3", "11", 5))
    result should contain (OrderTest("1", "11", "3"), null)
  }

  "inner joinPedidos " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[OrderTest] = sc.parallelize(List[OrderTest](OrderTest("1", "11", "3"))).toDS
    val ds2: Dataset[OrderTest2] = sc.parallelize(List[OrderTest2](OrderTest2("1", "11", 5))).toDS

    val dsExpected: Dataset[(OrderTest, OrderTest2)] =
      sc.parallelize(List[(OrderTest, OrderTest2)]
        ((OrderTest("1", "11", "3"), OrderTest2("1", "11", 5)))).toDS

    val joinResult = CommonSqlLayer.joinPedidos(ds1, ds2, INNER)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "default filter type as inner joinPedidos " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[OrderTest] = sc.parallelize(List[OrderTest](OrderTest("1", "11", "3"))).toDS
    val ds2: Dataset[OrderTest2] = sc.parallelize(List[OrderTest2](OrderTest2("1", "11", 5))).toDS

    val dsExpected: Dataset[(OrderTest, OrderTest2)] =
      sc.parallelize(List[(OrderTest, OrderTest2)]
        ((OrderTest("1", "11", "3"), OrderTest2("1", "11", 5)))).toDS

    val joinResult = CommonSqlLayer.joinPedidos(ds1, ds2)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "inner joinPedidos without the same keys " must
    " return the a dataset of a tuple with A and B without registries" taggedAs (UnitTestTag) in {

    val ds1: Dataset[OrderTest] = sc.parallelize(List[OrderTest](OrderTest("1", "12", "3"))).toDS
    val ds2: Dataset[OrderTest2] = sc.parallelize(List[OrderTest2](OrderTest2("1", "11", 5))).toDS

    val dsExpected: Dataset[(OrderTest, OrderTest2)] =
      sc.parallelize(List[(OrderTest, OrderTest2)]()).toDS

    val joinResult = CommonSqlLayer.joinPedidos(ds1, ds2, INNER)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "default filter type as inner joinPedidos without the same keys " must
    " return the a dataset of a tuple with A and B without registries" taggedAs (UnitTestTag) in {

    val ds1: Dataset[OrderTest] = sc.parallelize(List[OrderTest](OrderTest("1", "12", "3"))).toDS
    val ds2: Dataset[OrderTest2] = sc.parallelize(List[OrderTest2](OrderTest2("1", "11", 5))).toDS

    val dsExpected: Dataset[(OrderTest, OrderTest2)] =
      sc.parallelize(List[(OrderTest, OrderTest2)]()).toDS

    val joinResult = CommonSqlLayer.joinPedidos(ds1, ds2)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "left joinPedidos " must " return the a dataset of a tuple with A and B " taggedAs (UnitTestTag) in {

    val ds1: Dataset[OrderTest] = sc.parallelize(List[OrderTest](OrderTest("1", "11", "3"))).toDS
    val ds2: Dataset[OrderTest2] = sc.parallelize(List[OrderTest2](OrderTest2("1", "11", 5))).toDS

    val dsExpected: Dataset[(OrderTest, OrderTest2)] =
      sc.parallelize(List[(OrderTest, OrderTest2)]
        ((OrderTest("1", "11", "3"), OrderTest2("1", "11", 5)))).toDS

    val joinResult = CommonSqlLayer.joinPedidos(ds1, ds2, LEFT)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "left joinPedidos without the same keys " must
    " return the a dataset of a tuple with A and B with one registry with null in the right part" taggedAs (UnitTestTag) in {

    val ds1: Dataset[OrderTest] = sc.parallelize(List[OrderTest](OrderTest("1", "11", "3"))).toDS
    val ds2: Dataset[OrderTest2] = sc.parallelize(List[OrderTest2](OrderTest2("1", "13", 5))).toDS

    val dsExpected: Dataset[(OrderTest, OrderTest2)] =
      sc.parallelize(List[(OrderTest, OrderTest2)](
        (OrderTest("1", "11", "3"), null))).toDS

    val joinResult = CommonSqlLayer.joinPedidos(ds1, ds2, LEFT)(spark)
    assertDatasetEquals(joinResult, dsExpected)

  }

  "UDF udfTimeStampToString " must "convert timestamp column to date column formatted to yyyy-mm-dd" taggedAs (UnitTestTag) in {

    val tsMidnight = Dates.stringToTimestamp("2018-08-25 00:06:07", "Europe/Paris")

    val listInputsTimestamps =
      List[UDFTest](
        UDFTest(1, new Timestamp(1535166367000L)),
        UDFTest(2, new Timestamp(1535546772000L)),
        UDFTest(3, tsMidnight)
      )
    val listOutputExpected = "2018-08-25" :: "2018-08-29" :: "2018-08-25" :: Nil

    val df1 = sc.parallelize(listInputsTimestamps).toDF("id","timestamp")

    import com.training.bigdata.omnichannel.stockATP.common.util.CommonSqlLayer.udfTimeStampToString
    import org.apache.spark.sql.functions._

    df1.select(udfTimeStampToString(col("timestamp"), lit(HYPHEN_SEPARATOR), lit("Europe/Paris")))
      .collect().map(_.get(0)) shouldBe (listOutputExpected)
  }

  it must "convert timestamp column to date column formatted to yyyy-mm-dd appropriate to a default timezone" taggedAs (UnitTestTag) in {

    val tsMidnight = Dates.stringToTimestamp("2018-08-25 00:06:07", "Europe/Paris")

    val listInputsTimestamps =
      List[UDFTest](
        UDFTest(1, new Timestamp(1535166367000L)),
        UDFTest(2, new Timestamp(1535546772000L)),
        UDFTest(3, tsMidnight)
      )
    val listOutputExpected = "2018-08-25" :: "2018-08-29" :: "2018-08-24" :: Nil

    val df1 = sc.parallelize(listInputsTimestamps).toDF("id", "timestamp")

    import com.training.bigdata.omnichannel.stockATP.common.util.CommonSqlLayer.udfTimeStampToString
    import org.apache.spark.sql.functions._

    df1.select(udfTimeStampToString(col("timestamp"), lit(HYPHEN_SEPARATOR), lit("Europe/London")))
      .collect().map(_.get(0)) shouldBe (listOutputExpected)
  }

  "UDF udfStringToTimestamp " must "convert string to timestamp" taggedAs (UnitTestTag) in new TestDatesVariables {
    val ds1 : Dataset[UDFStringToTimestampTest] = sc.parallelize(List[UDFStringToTimestampTest](
      UDFStringToTimestampTest("2018-08-25 05:06:07"))).toDS
    val dsExpected : Dataset[UDFStringToTimestampTestTs] = sc.parallelize(List[UDFStringToTimestampTestTs](
      UDFStringToTimestampTestTs(dateStringBase))).toDS

    import com.training.bigdata.omnichannel.stockATP.common.util.CommonSqlLayer.udfStringToTimestamp
    import org.apache.spark.sql.functions._

    val dsResult: Dataset[UDFStringToTimestampTestTs] = ds1.withColumn(
      "ts",
      udfStringToTimestamp(col("date"),lit("Europe/Paris"), lit(DEFAULT_DATETIME_FORMAT), lit(defaultTimestamp))
    )
      .drop(col("date"))
      .as[UDFStringToTimestampTestTs]

    assertDatasetEquals(dsExpected, dsResult)
  }

  it must "convert string to timestamp with another timezone" taggedAs (UnitTestTag) in new TestDatesVariables {
    val ds1 : Dataset[UDFStringToTimestampTest] = sc.parallelize(List[UDFStringToTimestampTest](
      UDFStringToTimestampTest("2018-08-25 05:06:07"))).toDS
    val dsExpected : Dataset[UDFStringToTimestampTestTs] = sc.parallelize(List[UDFStringToTimestampTestTs](
      UDFStringToTimestampTestTs(dateStringPlus1))).toDS

    import com.training.bigdata.omnichannel.stockATP.common.util.CommonSqlLayer.udfStringToTimestamp
    import org.apache.spark.sql.functions._

    val dsResult: Dataset[UDFStringToTimestampTestTs] = ds1.withColumn(
      "ts",
      udfStringToTimestamp(col("date"),lit("Europe/London"), lit(DEFAULT_DATETIME_FORMAT), lit(defaultTimestamp))
    )
      .drop(col("date"))
      .as[UDFStringToTimestampTestTs]

    assertDatasetEquals(dsExpected, dsResult)
  }

  it must "convert string to timestamp with another timezone and without hyphen formatter" taggedAs (UnitTestTag) in new TestDatesVariables {
    val ds1 : Dataset[UDFStringToTimestampTest] = sc.parallelize(List[UDFStringToTimestampTest](
      UDFStringToTimestampTest("20180825 05:06:07"),
      UDFStringToTimestampTest("2018-08-25 05:06:00"),
      UDFStringToTimestampTest(null)
    )).toDS
    val dsExpected : Dataset[UDFStringToTimestampTestTs] = sc.parallelize(List[UDFStringToTimestampTestTs](
      UDFStringToTimestampTestTs(dateStringPlus1),
      UDFStringToTimestampTestTs(defaultTimestamp),
      UDFStringToTimestampTestTs(defaultTimestamp)
    )).toDS

    import com.training.bigdata.omnichannel.stockATP.common.util.CommonSqlLayer.udfStringToTimestamp
    import org.apache.spark.sql.functions._

    val dsResult: Dataset[UDFStringToTimestampTestTs] = ds1.withColumn(
      "ts",
      udfStringToTimestamp(col("date"),lit("Europe/London"), lit(IN_PROCESS_ORDERS_DATETIME_FORMAT), lit(defaultTimestamp))
    )
      .drop(col("date"))
      .as[UDFStringToTimestampTestTs]

    assertDatasetEquals(dsExpected, dsResult)
  }

  "UDF udfStringToTimestamp " must "convert string to timestamp with wrong format" taggedAs (UnitTestTag) in new TestDatesVariables {
    val ds1 : Dataset[UDFStringToTimestampTest] = sc.parallelize(List[UDFStringToTimestampTest](
      UDFStringToTimestampTest("20180825 05:06:07"),
      UDFStringToTimestampTest("2018-08-25"),
      UDFStringToTimestampTest("2018-08-25 05:06:07"),
      UDFStringToTimestampTest(null)
    )).toDS
    val dsExpected : Dataset[UDFStringToTimestampTestTs] = sc.parallelize(List[UDFStringToTimestampTestTs](
      UDFStringToTimestampTestTs(defaultTimestamp),
      UDFStringToTimestampTestTs(defaultTimestamp),
      UDFStringToTimestampTestTs(dateStringBase),
      UDFStringToTimestampTestTs(defaultTimestamp)
    )).toDS

    import com.training.bigdata.omnichannel.stockATP.common.util.CommonSqlLayer.udfStringToTimestamp
    import org.apache.spark.sql.functions._

    val dsResult: Dataset[UDFStringToTimestampTestTs] = ds1.withColumn(
      "ts",
      udfStringToTimestamp(col("date"),lit("Europe/Paris"), lit(DEFAULT_DATETIME_FORMAT), lit(defaultTimestamp))
    )
      .drop(col("date"))
      .as[UDFStringToTimestampTestTs]

    assertDatasetEquals(dsExpected, dsResult)
  }

  "UDF udfCompleteDateString " must "complete date in format string if it had format customized" taggedAs (UnitTestTag) in {
    val regexOnlyDate = "(\\d{4}-\\d{2}-\\d{2})"
    val ds1 : Dataset[UDFStringToTimestampTest] = sc.parallelize(List[UDFStringToTimestampTest](
      UDFStringToTimestampTest("2018-08-25"),
      UDFStringToTimestampTest("2018-08-25 06:02:09"))).toDS
    val dsExpected : Dataset[UDFStringToTimestampTest] = sc.parallelize(List[UDFStringToTimestampTest](
      UDFStringToTimestampTest("2018-08-25 00:00:00"),
      UDFStringToTimestampTest("2018-08-25 06:02:09"))).toDS

    import com.training.bigdata.omnichannel.stockATP.common.util.CommonSqlLayer.udfCompleteDateString
    import org.apache.spark.sql.functions._

    val dsResult: Dataset[UDFStringToTimestampTest] = ds1.withColumn(
      "date",
      udfCompleteDateString(col("date"),lit(regexOnlyDate))
    ).as[UDFStringToTimestampTest]

    assertDatasetEquals(dsExpected, dsResult)
  }

  it must "complete date in format string if it had format customized using without hyphen" taggedAs (UnitTestTag) in {
    val regexOnlyDate = ONLY_DIGITS_DATE_REGEX
    val ds1 : Dataset[UDFStringToTimestampTest] = sc.parallelize(List[UDFStringToTimestampTest](
      UDFStringToTimestampTest("20180825"),
      UDFStringToTimestampTest("2018082500"),
      UDFStringToTimestampTest(null),
      UDFStringToTimestampTest("20180825 06:02:09"))).toDS
    val dsExpected : Dataset[UDFStringToTimestampTest] = sc.parallelize(List[UDFStringToTimestampTest](
      UDFStringToTimestampTest("20180825 00:00:00"),
      UDFStringToTimestampTest("2018082500"),
      UDFStringToTimestampTest(null),
      UDFStringToTimestampTest("20180825 06:02:09"))).toDS

    import com.training.bigdata.omnichannel.stockATP.common.util.CommonSqlLayer.udfCompleteDateString
    import org.apache.spark.sql.functions._

    val dsResult: Dataset[UDFStringToTimestampTest] = ds1.withColumn(
      "date",
      udfCompleteDateString(col("date"),lit(regexOnlyDate))
    ).as[UDFStringToTimestampTest]

    assertDatasetEquals(dsExpected, dsResult)
  }

  it must "complete date in format string on corner cases" taggedAs (UnitTestTag) in {
    val regexOnlyDate = ""
    val ds1 : Dataset[UDFStringToTimestampTest] = sc.parallelize(List[UDFStringToTimestampTest](
      UDFStringToTimestampTest(null),
      UDFStringToTimestampTest("2018-08-25 06:02:09"))).toDS
    val dsExpected : Dataset[UDFStringToTimestampTest] = sc.parallelize(List[UDFStringToTimestampTest](
      UDFStringToTimestampTest(null),
      UDFStringToTimestampTest("2018-08-25 06:02:09"))).toDS

    import com.training.bigdata.omnichannel.stockATP.common.util.CommonSqlLayer.udfCompleteDateString
    import org.apache.spark.sql.functions._

    val dsResult: Dataset[UDFStringToTimestampTest] = ds1.withColumn(
      "date",
      udfCompleteDateString(col("date"),lit(regexOnlyDate))
    ).as[UDFStringToTimestampTest]

    assertDatasetEquals(dsExpected, dsResult)
  }

  // Testing joinSurtidoByArticleAndLocationDF

  "outer joinSurtidoByArticleAndLocationDF " must " return a dataframe " taggedAs (UnitTestTag) in new SurtidoConf{

    val joinResult = CommonSqlLayer.joinSurtidoByArticleAndLocationDF(df1, df2, OUTER)(spark)
      .orderBy("idArticle").select(dfExpected.columns.head, dfExpected.columns.tail: _*)

    assertDataFrameEquals(dfExpected, joinResult)

  }

  "outer joinSurtidoByArticleAndLocationDF without the same keys " must
    " return a dataframe with all registries" taggedAs (UnitTestTag) in new SurtidoConf{

    val dfExpected2: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](Row("1", "11", "aaa", null), Row("2", "11", null, 10))),
        resultStructType
      )

    val joinResult = CommonSqlLayer.joinSurtidoByArticleAndLocationDF(df1, df4, OUTER)(spark)

    assertDataFrameEquals(dfExpected2, joinResult)

  }

  "inner joinSurtidoByArticleAndLocationDF " must " return a dataframe " taggedAs (UnitTestTag) in new SurtidoConf{

    val joinResult = CommonSqlLayer.joinSurtidoByArticleAndLocationDF(df1, df2, INNER)(spark)
    assertDataFrameEquals(dfExpected, joinResult)

  }

  "default filter type as inner joinSurtidoByArticleAndLocationDF " must " return a datasetframe " taggedAs (UnitTestTag) in new SurtidoConf{

    val joinResult = CommonSqlLayer.joinSurtidoByArticleAndLocationDF(df1, df2)(spark)
    assertDataFrameEquals(dfExpected, joinResult)

  }

  "inner joinSurtidoByArticleAndLocationDF without the same keys " must
    " return a dataframe without registries" taggedAs (UnitTestTag) in new SurtidoConf{

    val joinResult = CommonSqlLayer.joinSurtidoByArticleAndLocationDF(df1, df3, INNER)(spark)
    assertDataFrameEquals(dfExpectedEmpty, joinResult)

  }

  "default filter type as inner joinSurtidoByArticleAndLocationDF without the same keys " must
    " return a dataframe without registries" taggedAs (UnitTestTag) in new SurtidoConf{

    val joinResult = CommonSqlLayer.joinSurtidoByArticleAndLocationDF(df1, df3)(spark)
    assertDataFrameEquals(dfExpectedEmpty, joinResult)

  }

  "left joinSurtidoByArticleAndLocationDF " must " return a dataframe " taggedAs (UnitTestTag) in new SurtidoConf{

    val joinResult = CommonSqlLayer.joinSurtidoByArticleAndLocationDF(df1, df2, LEFT)(spark)
    assertDataFrameEquals(dfExpected, joinResult)

  }

  "left joinSurtidoByArticleAndLocationDF without the same keys " must
    " return a dataframe with one registry with null in the right part" taggedAs (UnitTestTag) in new SurtidoConf{

    val dfExpected2: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](Row("1", "11", "aaa", null))),
        resultStructType
      )

    val joinResult = CommonSqlLayer.joinSurtidoByArticleAndLocationDF(df1, df3, "left")(spark)
    assertDataFrameEquals(dfExpected2, joinResult)

  }

  "joinSurtidoWithStoreDF with default join" must " behave as inner" taggedAs (UnitTestTag) in new SurtidoConf {

    val dfLeft: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("1", "11", "aaa"),
      Row("2", "22", "bbb"),
      Row("3", "33", "ccc")
    )), articleStoreSomethingStructType
    )

    val dfRight: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("1", "11", 10),
      Row("2", "22", 11),
      Row("5", "22", 15),
      Row("4", "44", 14)
    )), articleStoreAmountStructType
    )

    val dfExpected1: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("11", "1", "aaa", "1", 10),
      Row("22", "2", "bbb", "2", 11),
      Row("22", "2", "bbb", "5", 15)
    )), StructType(List(
      StructField(Surtido.ccTags.idStoreTag,   StringType),
      StructField(Surtido.ccTags.idArticleTag, StringType),
      StructField("something",  StringType),
      StructField(Surtido.ccTags.idArticleTag, StringType),
      StructField("amount",    IntegerType)
    )))

    val dfResult: DataFrame = CommonSqlLayer.joinSurtidoWithStoreDF(dfLeft, dfRight)(spark)
    assertDataFrameEquals(dfExpected1, dfResult)

  }

  "joinSurtidoWithStoreDF with inner join" must " return a df joining by store " taggedAs (UnitTestTag) in new SurtidoConf {

    val dfLeft: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("1", "11", "aaa"),
      Row("2", "22", "bbb"),
      Row("3", "33", "ccc")
    )), articleStoreSomethingStructType
    )

    val dfRight: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("1", "11", 10),
      Row("2", "22", 11),
      Row("5", "22", 15),
      Row("4", "44", 14)
    )), articleStoreAmountStructType
    )

    val dfExpected1: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("11", "1", "aaa", "1", 10),
      Row("22", "2", "bbb", "2", 11),
      Row("22", "2", "bbb", "5", 15)
    )), StructType(List(
      StructField(Surtido.ccTags.idStoreTag,   StringType),
      StructField(Surtido.ccTags.idArticleTag, StringType),
      StructField("something",  StringType),
      StructField(Surtido.ccTags.idArticleTag, StringType),
      StructField("amount",    IntegerType)
    )))

    val dfResult: DataFrame = CommonSqlLayer.joinSurtidoWithStoreDF(dfLeft, dfRight, INNER)(spark)
    assertDataFrameEquals(dfExpected1, dfResult)

  }

  "joinSurtidoWithStoreDF with left join" must " return a df joining by store " taggedAs (UnitTestTag) in new SurtidoConf {

    val dfLeft: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("1", "11", "aaa"),
      Row("2", "22", "bbb"),
      Row("3", "33", "ccc")
    )), articleStoreSomethingStructType
    )

    val dfRight: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("1", "11", 10),
      Row("2", "22", 11),
      Row("5", "22", 15),
      Row("4", "44", 14)
    )), articleStoreAmountStructType
    )

    val dfExpected1: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("11", "1", "aaa",  "1",   10),
      Row("22", "2", "bbb",  "2",   11),
      Row("22", "2", "bbb",  "5",   15),
      Row("33", "3", "ccc", null, null)
    )), StructType(List(
      StructField(Surtido.ccTags.idStoreTag,   StringType),
      StructField(Surtido.ccTags.idArticleTag, StringType),
      StructField("something",  StringType),
      StructField(Surtido.ccTags.idArticleTag, StringType),
      StructField("amount",    IntegerType)
    ))).orderBy(Surtido.ccTags.idStoreTag)

    val dfResult: DataFrame = CommonSqlLayer.joinSurtidoWithStoreDF(dfLeft, dfRight, LEFT)(spark)
      .orderBy(Surtido.ccTags.idStoreTag)
    assertDataFrameEquals(dfExpected1, dfResult)

  }

  "joinSurtidoWithStoreDF with outer join" must " return a df joining by store " taggedAs (UnitTestTag) in new SurtidoConf {

    val dfLeft: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("1", "11", "aaa"),
      Row("2", "22", "bbb"),
      Row("3", "33", "ccc")
    )), articleStoreSomethingStructType
    )

    val dfRight: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("1", "11", 10),
      Row("2", "22", 11),
      Row("5", "22", 15),
      Row("4", "44", 14)
    )), articleStoreAmountStructType
    )

    val dfExpected1: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("11",  "1", "aaa",  "1",   10),
      Row("22",  "2", "bbb",  "2",   11),
      Row("22",  "2", "bbb",  "5",   15),
      Row("33",  "3", "ccc", null, null),
      Row("44", null,  null,  "4",   14)
    )), StructType(List(
      StructField(Surtido.ccTags.idStoreTag,   StringType),
      StructField(Surtido.ccTags.idArticleTag, StringType),
      StructField("something",  StringType),
      StructField(Surtido.ccTags.idArticleTag, StringType),
      StructField("amount",    IntegerType)
    ))).orderBy(Surtido.ccTags.idStoreTag)

    val dfResult: DataFrame = CommonSqlLayer.joinSurtidoWithStoreDF(dfLeft, dfRight, OUTER)(spark)
      .orderBy(Surtido.ccTags.idStoreTag)
    assertDataFrameEquals(dfExpected1, dfResult)

  }

  "addDFAndFillNullWithZeros" must
    " return a initial df with another added one by article and store if there is, otherwise fill with zeros " taggedAs (UnitTestTag) in new SurtidoConfWithAudit {

    val ts1 = new Timestamp(100)
    val ts2 = new Timestamp(101)
    val ts3 = new Timestamp(102)

    val dfLeft: DataFrame = spark.createDataFrame(
      sc.parallelize(
        List[Row](
          Row("1", "11", "aaa", ts1),
          Row("1", "22", "bbb", ts1),
          Row("1", "33", "ccc", ts1)
        )
      ),
      articleStoreSomethingWithAuditStructType
    )

    val dfRight: DataFrame = spark.createDataFrame(
      sc.parallelize(
        List[Row](
          Row("1", "11", 10, ts2),
          Row("1", "22", 20, ts1),
          Row("1", "44", 40, ts3)
        )
      ),
      articleStoreAmountWithAuditStructType
    )

    val dfExpected: DataFrame =
      spark.createDataFrame(
        sc.parallelize(
          List[Row](
            Row("1", "11", "aaa", 10, ts2),
            Row("1", "22", "bbb", 20, ts1),
            Row("1", "33", "ccc",  0, ts1)
          )
        ),
        resultStructWithAuditType
      )

    val dfResult: DataFrame =
      dfLeft.
        transform(
          CommonSqlLayer.addDFAndFillNullWithZeros(dfRight)(spark)
        )
        .orderBy(Surtido.ccTags.idArticleTag,Surtido.ccTags.idStoreTag)
        .select(dfExpected.columns.head, dfExpected.columns.tail:_*)

    assertDataFrameEquals(dfExpected, dfResult)

  }

}
