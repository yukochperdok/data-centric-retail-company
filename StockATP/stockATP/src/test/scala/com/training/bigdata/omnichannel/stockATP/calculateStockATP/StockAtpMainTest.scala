package com.training.bigdata.omnichannel.stockATP.calculateStockATP

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs.{PartialOrdersDeliveredView, _}
import com.training.bigdata.omnichannel.stockATP.calculateStockATP.util.Sql
import com.training.bigdata.omnichannel.stockATP.calculateStockATP.util.Sql.{filterOrderHeader, filterOrderLines}
import com.training.bigdata.omnichannel.stockATP.common.entities._
import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{Audit, Surtido}
import com.training.bigdata.omnichannel.stockATP.common.util.LensAppConfig._
import com.training.bigdata.omnichannel.stockATP.common.util.tag.TagTest.UnitTestTag
import com.training.bigdata.omnichannel.stockATP.common.util._
import com.training.bigdata.omnichannel.stockATP.common.util.Constants._
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.{FlatSpec, Matchers}


class StockAtpMainTest extends FlatSpec with Matchers with DatasetSuiteBase{
  import spark.implicits._
  override protected implicit def enableHiveSupport: Boolean = false

  trait StockATPConf extends DefaultConf {

    val appName = "stockATP"
    val appNameConsolidatedStock = "stockConsolidado"
    val appNameFixedData = "calculateDatosFijos"
    val appNameNoDependentProcess = "noDependentProcess"
    implicit val appConfig =
      defaultConfig
        .setControlEjecucionProcesses(appNameConsolidatedStock :: appNameFixedData :: appName :: Nil)
  }

  trait BaseTrait extends StockATPConf {
    val currentTimestamp: Timestamp = new Timestamp(System.currentTimeMillis)
    val todayAtMidnightTs: Timestamp  = Dates.getDateAtMidnight(currentTimestamp)._1
    val yesterdayAtMidnightTs: Timestamp = Dates.subtractDaysToTimestamp(todayAtMidnightTs, 1)
  }

  trait StockATPFor5ProjectionDays extends BaseTrait {

    def calculateProcessAndCompareResult
    (assert: (DataFrame, DataFrame) => Unit)
    (readEntities: ReadAtpEntities,
     stockConf: StockConf,
     currentDate: Timestamp,
     previousDate: Timestamp,
     executionControlRowsList: List[ControlEjecucionView],
     appName: String,
     orderByColumn: Seq[String],
     dfExpectedWithoutOrder: DataFrame) = {

      val dfExpected: DataFrame = dfExpectedWithoutOrder.orderBy(orderByColumn.map(col): _*)

      val dfFinal: DataFrame =
        StockATPMain.processCalculate(
          readEntities,
          stockConf,
          currentDate,
          previousDate,
          executionControlRowsList,
          appName,
          user)(spark, appConfig).orderBy(orderByColumn.map(col): _*)

      assert(dfFinal, dfExpected)
    }

    val assertDataFrameEqualsWithoutTsUpdateColumns: (DataFrame, DataFrame) => Unit = {
      (dfExpected: DataFrame, dfFinal: DataFrame) => {
        assertDataFrameEquals(
          dfExpected.drop(Audit.ccTags.tsUpdateDlkTag),
          dfFinal.drop(Audit.ccTags.tsUpdateDlkTag))
      }
    }

    def calculateProcessAndCompareResultWithoutTsUpdateColumns:
    (ReadAtpEntities, StockConf, Timestamp, Timestamp, List[ControlEjecucionView], String, Seq[String], DataFrame) => Unit = {
      calculateProcessAndCompareResult(assertDataFrameEqualsWithoutTsUpdateColumns)
    }


    /** Dates constants **/
    lazy val numberOfDaysToProject: Int = 4

    val yesterdayString: String = Dates.timestampToStringWithTZ(yesterdayAtMidnightTs, HYPHEN_SEPARATOR)
    val yesterdayWithoutSeparator: String = Dates.timestampToStringWithTZ(yesterdayAtMidnightTs, NO_SEPARATOR)

    val todayString: String = Dates.timestampToStringWithTZ(todayAtMidnightTs, HYPHEN_SEPARATOR)
    val todayWithoutSeparator: String = Dates.timestampToStringWithTZ(todayAtMidnightTs, NO_SEPARATOR)

    val dayN1: Timestamp = Dates.sumDaysToTimestamp(todayAtMidnightTs)
    val dayN2: Timestamp = Dates.sumDaysToTimestamp(dayN1)
    val dayN3: Timestamp = Dates.sumDaysToTimestamp(dayN2)
    val dayN4: Timestamp = Dates.sumDaysToTimestamp(dayN3)

    val lastProjectionDayTs: Timestamp = Dates.sumDaysToTimestamp(todayAtMidnightTs, numberOfDaysToProject + 1)
    val lastProjectionDayString: String = Dates.timestampToStringWithTZ(lastProjectionDayTs, HYPHEN_SEPARATOR)
    val lastProjectionDateWithoutSeparator: String = Dates.timestampToStringWithTZ(lastProjectionDayTs, NO_SEPARATOR)

    /** Article and stores constants **/
    val idArticle1 = "1"
    val idArticle2 = "2"
    val idArticle3 = "3"
    val idArticle4 = "4"
    val idArticle5 = "5"
    val idArticle6 = "6"
    val idArticle7 = "7"
    val idArticle8 = "8"
    val idArticle9 = "9"
    val idArticle10 = "10"

    val idStore1 = "11"
    val idStore2 = "22"
    val idStore3 = "33"
    val idStore4 = "44"
    val idStore5 = "55"
    val idStore6 = "66"
    val idStore7 = "77"

    /** Sectors constants **/
    val sector01 = "01"
    val freshProductsSector = "02"

    /** Unity measure constants **/
    val measureUnitEA = "EA"
    val measureUnitKG = "KG"
    val defaultUnityMeasure: String = appConfig.getCommonParameters.defaultUnidadMedida

    /** User constants **/
    val user = "user"

    /** Capacities constants **/
    val preparationCapacity5 = 5
    val preparationCapacity6 = 6
    val preparationCapacity7 = 7
    val preparationCapacity8 = 8
    val preparationCapacity9 = 9
    val notAPreparationCapacity = 0
    val isPreparationLocation: Boolean = true
    val notAPreparationLocation: Boolean = false
    val openStore = true
    val closedStore = false

    /** Orders constants **/
    val idOrder1 = "401"
    val idOrder2 = "402"
    val idOrder3 = "403"
    val idOrder4 = "404"
    val idOrder5 = "405"
    val idOrder6 = "406"
    val idOrderLine1 = "421"
    val idOrderLine2 = "422"
    val idOrderLine3 = "423"
    val idOrderLine4 = "424"
    val idOrderLine5 = "425"
    val idOrderLine6 = "426"
    val idOrderLine7 = "427"
    val idOrderLine8 = "428"
    val idOrderLine9 = "429"
    val idOrderLine10 = "430"
    val idOrderLine11 = "431"
    val idOrderLine12 = "432"
    val idOrderLine13 = "433"
    val idOrderLine14 = "434"
    val idNumPartialOrdersDelivered1="471"
    val idNumPartialOrdersDelivered2="472"
    val idNumPartialOrdersDelivered3="473"
    val idNumPartialOrdersDelivered4="474"
    val idNumPartialOrdersDelivered5="475"
    val idNumPartialOrdersDelivered6="476"
    val idNumPartialOrdersDelivered7="477"
    val idNumPartialOrdersDelivered8="478"
    val typeOrderHeaderOMS1: String = Constants.DEFAULT_LIST_TYPE_ORDERS(1)
    val typeOrderHeaderOMS2: String = Constants.DEFAULT_LIST_TYPE_ORDERS(2)
    val typeOrderHeaderOMS3: String = Constants.DEFAULT_LIST_TYPE_ORDERS(3)
    val typeOrderHeaderOMS4: String = Constants.DEFAULT_LIST_TYPE_ORDERS(4)
    val wrongTypeOrder = "ZZZZ"
    val stockTypeOMS: Int = appConfig.getStockTypeOMS
    val stockTypeAPRO = 2
    val returnedToProviderFlag: String = appConfig.getStockToBeDeliveredParameters.returnedToProviderFlag
    val alreadyDeliveredFlag: String = appConfig.getStockToBeDeliveredParameters.alreadyDeliveredFlag
    val canceledFlagList: List[String] = appConfig.getStockToBeDeliveredParameters.canceledFlagList
    val notCanceledFlag = "S"
    val notYetDeliveredFlag = "S"
    val notReturnedToProviderFlag = "0"

    /** Purchase articles constants **/
    val parentSaleArticle1 = "111"
    val parentSaleArticle2 = "222"
    val parentSaleArticle3 = "333"
    val purchaseWithUniqueSaleArticle1 = "111"
    val purchaseWithUniqueSaleArticle2 = "222"
    val purchaseWithUniqueSaleArticle3 = "333"
    val parentSaleArticleNull: Null = null
    val saleFlag = "S"
    val saleFlag2 = ""
    val purchaseFlag = "P"
    val purchaseFlag2 = "R"
    val purchaseArticleType1 = "ZVLG"
    val noPurchaseArticleType1 = "ZASD"
    val purchaseArticleCategory1 = "12"
    val noPurchaseArticleCategory1 = "55"
    val boxPrepackArticleType1 = "ZTEX"
    val boxPrepackArticleType2 = "ZBOX"
    val boxPrepackArticleCategory1 = "11"
    val boxPrepackArticleCategory2 = "12"
    val idListMaterial1 = "701"
    val idListMaterial2 = "702"
    val idListMaterial3 = "703"
    val idListMaterial4 = "704"
    val idListMaterial5 = "705"
    val idListMaterial6 = "706"
    val idListMaterial7 = "707"
    val idListMaterial8 = "708"
    val idListMaterial9 = "709"
    val idBoxPrepack1 = "801"
    val idBoxPrepack2 = "802"
    val idBoxPrepack3 = "803"
    val idBoxPrepack4 = "804"
    val idBoxPrepack5 = "805"
    val idBoxPrepack6 = "806"
    val idBoxPrepack7 = "807"
    val idBoxPrepack8 = "808"
    val idBoxPrepack9 = "809"
    val idBoxPrepack10 = "810"

    // This will be ignored as there is no modification rows control
    val controlExecutionListWithStockATPOnly = List(ControlEjecucionView(appName, currentTimestamp, currentTimestamp, currentTimestamp))

    val fixedDataStructType = StructType(List(
      StructField(   DatosFijos.ccTags.idArticleTag,    StringType),
      StructField(     DatosFijos.ccTags.idStoreTag,    StringType),
      StructField(    DatosFijos.ccTags.dateSaleTag,    StringType, false),
      StructField(      DatosFijos.ccTags.sectorTag,    StringType),
      StructField(DatosFijos.ccTags.unityMeasureTag,    StringType),
      StructField(         DatosFijos.ccTags.nspTag,    DoubleType),
      StructField(         DatosFijos.ccTags.nstTag,    DoubleType),
      StructField(    DatosFijos.ccTags.rotationTag,    StringType),
      StructField(      Audit.ccTags.tsUpdateDlkTag, TimestampType, false),
      StructField(    Audit.ccTags.userUpdateDlkTag,    StringType, false),
      StructField(                 "salesForecastN",    DoubleType),
      StructField(                "salesForecastN1",    DoubleType),
      StructField(                "salesForecastN2",    DoubleType),
      StructField(                "salesForecastN3",    DoubleType),
      StructField(                "salesForecastN4",    DoubleType),
      StructField(                "salesForecastN5",    DoubleType),
      StructField(                "salesForecastN6",    DoubleType),
      StructField(                "salesForecastN7",    DoubleType),
      StructField(                "salesForecastN8",    DoubleType),
      StructField(                "salesForecastN9",    DoubleType),
      StructField(               "salesForecastN10",    DoubleType)
    ))

    val fixedData100DaysStructType = StructType(List(
      StructField(   DatosFijos.ccTags.idArticleTag,    StringType),
      StructField(     DatosFijos.ccTags.idStoreTag,    StringType),
      StructField(    DatosFijos.ccTags.dateSaleTag,    StringType, false),
      StructField(      DatosFijos.ccTags.sectorTag,    StringType),
      StructField(DatosFijos.ccTags.unityMeasureTag,    StringType),
      StructField(         DatosFijos.ccTags.nspTag,    DoubleType),
      StructField(         DatosFijos.ccTags.nstTag,    DoubleType),
      StructField(    DatosFijos.ccTags.rotationTag,    StringType),
      StructField(      Audit.ccTags.tsUpdateDlkTag, TimestampType, false),
      StructField(    Audit.ccTags.userUpdateDlkTag,    StringType, false),
      StructField(                 "salesForecastN",    DoubleType),
      StructField(                "salesForecastN1",    DoubleType),
      StructField(                "salesForecastN2",    DoubleType),
      StructField(                "salesForecastN3",    DoubleType),
      StructField(                "salesForecastN4",    DoubleType),
      StructField(                "salesForecastN5",    DoubleType),
      StructField(                "salesForecastN6",    DoubleType),
      StructField(                "salesForecastN7",    DoubleType),
      StructField(                "salesForecastN8",    DoubleType),
      StructField(                "salesForecastN9",    DoubleType),
      StructField(               "salesForecastN10",    DoubleType),
      StructField(               "salesForecastN11",    DoubleType),
      StructField(               "salesForecastN12",    DoubleType),
      StructField(               "salesForecastN13",    DoubleType),
      StructField(               "salesForecastN14",    DoubleType),
      StructField(               "salesForecastN15",    DoubleType),
      StructField(               "salesForecastN16",    DoubleType),
      StructField(               "salesForecastN17",    DoubleType),
      StructField(               "salesForecastN18",    DoubleType),
      StructField(               "salesForecastN19",    DoubleType),
      StructField(               "salesForecastN20",    DoubleType),
      StructField(               "salesForecastN21",    DoubleType),
      StructField(               "salesForecastN22",    DoubleType),
      StructField(               "salesForecastN23",    DoubleType),
      StructField(               "salesForecastN24",    DoubleType),
      StructField(               "salesForecastN25",    DoubleType),
      StructField(               "salesForecastN26",    DoubleType),
      StructField(               "salesForecastN27",    DoubleType),
      StructField(               "salesForecastN28",    DoubleType),
      StructField(               "salesForecastN29",    DoubleType),
      StructField(               "salesForecastN30",    DoubleType),
      StructField(               "salesForecastN31",    DoubleType),
      StructField(               "salesForecastN32",    DoubleType),
      StructField(               "salesForecastN33",    DoubleType),
      StructField(               "salesForecastN34",    DoubleType),
      StructField(               "salesForecastN35",    DoubleType),
      StructField(               "salesForecastN36",    DoubleType),
      StructField(               "salesForecastN37",    DoubleType),
      StructField(               "salesForecastN38",    DoubleType),
      StructField(               "salesForecastN39",    DoubleType),
      StructField(               "salesForecastN40",    DoubleType),
      StructField(               "salesForecastN41",    DoubleType),
      StructField(               "salesForecastN42",    DoubleType),
      StructField(               "salesForecastN43",    DoubleType),
      StructField(               "salesForecastN44",    DoubleType),
      StructField(               "salesForecastN45",    DoubleType),
      StructField(               "salesForecastN46",    DoubleType),
      StructField(               "salesForecastN47",    DoubleType),
      StructField(               "salesForecastN48",    DoubleType),
      StructField(               "salesForecastN49",    DoubleType),
      StructField(               "salesForecastN50",    DoubleType),
      StructField(               "salesForecastN51",    DoubleType),
      StructField(               "salesForecastN52",    DoubleType),
      StructField(               "salesForecastN53",    DoubleType),
      StructField(               "salesForecastN54",    DoubleType),
      StructField(               "salesForecastN55",    DoubleType),
      StructField(               "salesForecastN56",    DoubleType),
      StructField(               "salesForecastN57",    DoubleType),
      StructField(               "salesForecastN58",    DoubleType),
      StructField(               "salesForecastN59",    DoubleType),
      StructField(               "salesForecastN60",    DoubleType),
      StructField(               "salesForecastN61",    DoubleType),
      StructField(               "salesForecastN62",    DoubleType),
      StructField(               "salesForecastN63",    DoubleType),
      StructField(               "salesForecastN64",    DoubleType),
      StructField(               "salesForecastN65",    DoubleType),
      StructField(               "salesForecastN66",    DoubleType),
      StructField(               "salesForecastN67",    DoubleType),
      StructField(               "salesForecastN68",    DoubleType),
      StructField(               "salesForecastN69",    DoubleType),
      StructField(               "salesForecastN70",    DoubleType),
      StructField(               "salesForecastN71",    DoubleType),
      StructField(               "salesForecastN72",    DoubleType),
      StructField(               "salesForecastN73",    DoubleType),
      StructField(               "salesForecastN74",    DoubleType),
      StructField(               "salesForecastN75",    DoubleType),
      StructField(               "salesForecastN76",    DoubleType),
      StructField(               "salesForecastN77",    DoubleType),
      StructField(               "salesForecastN78",    DoubleType),
      StructField(               "salesForecastN79",    DoubleType),
      StructField(               "salesForecastN80",    DoubleType),
      StructField(               "salesForecastN81",    DoubleType),
      StructField(               "salesForecastN82",    DoubleType),
      StructField(               "salesForecastN83",    DoubleType),
      StructField(               "salesForecastN84",    DoubleType),
      StructField(               "salesForecastN85",    DoubleType),
      StructField(               "salesForecastN86",    DoubleType),
      StructField(               "salesForecastN87",    DoubleType),
      StructField(               "salesForecastN88",    DoubleType),
      StructField(               "salesForecastN89",    DoubleType),
      StructField(               "salesForecastN90",    DoubleType),
      StructField(               "salesForecastN91",    DoubleType),
      StructField(               "salesForecastN92",    DoubleType),
      StructField(               "salesForecastN93",    DoubleType),
      StructField(               "salesForecastN94",    DoubleType),
      StructField(               "salesForecastN95",    DoubleType),
      StructField(               "salesForecastN96",    DoubleType),
      StructField(               "salesForecastN97",    DoubleType),
      StructField(               "salesForecastN98",    DoubleType),
      StructField(               "salesForecastN99",    DoubleType),
      StructField(              "salesForecastN100",    DoubleType),
      StructField(              "salesForecastN101",    DoubleType),
      StructField(              "salesForecastN102",    DoubleType)
    ))

    val fixedData0DaysStructType = StructType(List(
      StructField(   DatosFijos.ccTags.idArticleTag,    StringType),
      StructField(     DatosFijos.ccTags.idStoreTag,    StringType),
      StructField(    DatosFijos.ccTags.dateSaleTag,    StringType, false),
      StructField(      DatosFijos.ccTags.sectorTag,    StringType),
      StructField(DatosFijos.ccTags.unityMeasureTag,    StringType),
      StructField(         DatosFijos.ccTags.nspTag,    DoubleType),
      StructField(         DatosFijos.ccTags.nstTag,    DoubleType),
      StructField(    DatosFijos.ccTags.rotationTag,    StringType),
      StructField(      Audit.ccTags.tsUpdateDlkTag, TimestampType, false),
      StructField(    Audit.ccTags.userUpdateDlkTag,    StringType, false),
      StructField(                 "salesForecastN",    DoubleType),
      StructField(                "salesForecastN1",    DoubleType),
      StructField(                "salesForecastN2",    DoubleType)
    ))

    val stockAtpNXStructType = StructType(List(
      StructField(        DatosFijos.ccTags.idStoreTag,    StringType, false),
      StructField(      DatosFijos.ccTags.idArticleTag,    StringType, false),
      StructField(         DatosFijos.ccTags.sectorTag,    StringType),
      StructField(   DatosFijos.ccTags.unityMeasureTag,    StringType),
      StructField(         Audit.ccTags.tsUpdateDlkTag, TimestampType, false),
      StructField(                   AggPP.ccTags.isPP,   BooleanType),
      StructField(       Audit.ccTags.userUpdateDlkTag,    StringType, false),
      StructField(            StockATP.ccTags.nDateTag, TimestampType, false),
      StructField(                         "stockAtpN",    DoubleType),
      StructField(                        "stockAtpN1",    DoubleType),
      StructField(                        "stockAtpN2",    DoubleType),
      StructField(                        "stockAtpN3",    DoubleType),
      StructField(                        "stockAtpN4",    DoubleType),
      StructField(                        "stockAtpN5",    DoubleType),
      StructField(                        "stockAtpN6",    DoubleType),
      StructField(                        "stockAtpN7",    DoubleType),
      StructField(                        "stockAtpN8",    DoubleType),
      StructField(                        "stockAtpN9",    DoubleType),
      StructField(                        "stockAtpN10",    DoubleType),
      StructField(                        "stockAtpN11",    DoubleType),
      StructField(                        "stockAtpN12",    DoubleType),
      StructField(                        "stockAtpN13",    DoubleType),
      StructField(                        "stockAtpN14",    DoubleType),
      StructField(                        "stockAtpN15",    DoubleType),
      StructField(                        "stockAtpN16",    DoubleType),
      StructField(                        "stockAtpN17",    DoubleType),
      StructField(                        "stockAtpN18",    DoubleType),
      StructField(                        "stockAtpN19",    DoubleType),
      StructField(                        "stockAtpN20",    DoubleType),
      StructField(                        "stockAtpN21",    DoubleType),
      StructField(                        "stockAtpN22",    DoubleType),
      StructField(                        "stockAtpN23",    DoubleType),
      StructField(                        "stockAtpN24",    DoubleType),
      StructField(                        "stockAtpN25",    DoubleType),
      StructField(                        "stockAtpN26",    DoubleType),
      StructField(                        "stockAtpN27",    DoubleType),
      StructField(                        "stockAtpN28",    DoubleType),
      StructField(                        "stockAtpN29",    DoubleType),
      StructField(                        "stockAtpN30",    DoubleType),
      StructField(                        "stockAtpN31",    DoubleType),
      StructField(                        "stockAtpN32",    DoubleType),
      StructField(                        "stockAtpN33",    DoubleType),
      StructField(                        "stockAtpN34",    DoubleType),
      StructField(                        "stockAtpN35",    DoubleType),
      StructField(                        "stockAtpN36",    DoubleType),
      StructField(                        "stockAtpN37",    DoubleType),
      StructField(                        "stockAtpN38",    DoubleType),
      StructField(                        "stockAtpN39",    DoubleType),
      StructField(                        "stockAtpN40",    DoubleType),
      StructField(                        "stockAtpN41",    DoubleType),
      StructField(                        "stockAtpN42",    DoubleType),
      StructField(                        "stockAtpN43",    DoubleType),
      StructField(                        "stockAtpN44",    DoubleType),
      StructField(                        "stockAtpN45",    DoubleType),
      StructField(                        "stockAtpN46",    DoubleType),
      StructField(                        "stockAtpN47",    DoubleType),
      StructField(                        "stockAtpN48",    DoubleType),
      StructField(                        "stockAtpN49",    DoubleType),
      StructField(                        "stockAtpN50",    DoubleType),
      StructField(                        "stockAtpN51",    DoubleType),
      StructField(                        "stockAtpN52",    DoubleType),
      StructField(                        "stockAtpN53",    DoubleType),
      StructField(                        "stockAtpN54",    DoubleType),
      StructField(                        "stockAtpN55",    DoubleType),
      StructField(                        "stockAtpN56",    DoubleType),
      StructField(                        "stockAtpN57",    DoubleType),
      StructField(                        "stockAtpN58",    DoubleType),
      StructField(                        "stockAtpN59",    DoubleType),
      StructField(                        "stockAtpN60",    DoubleType),
      StructField(                        "stockAtpN61",    DoubleType),
      StructField(                        "stockAtpN62",    DoubleType),
      StructField(                        "stockAtpN63",    DoubleType),
      StructField(                        "stockAtpN64",    DoubleType),
      StructField(                        "stockAtpN65",    DoubleType),
      StructField(                        "stockAtpN66",    DoubleType),
      StructField(                        "stockAtpN67",    DoubleType),
      StructField(                        "stockAtpN68",    DoubleType),
      StructField(                        "stockAtpN69",    DoubleType),
      StructField(                        "stockAtpN70",    DoubleType),
      StructField(                        "stockAtpN71",    DoubleType),
      StructField(                        "stockAtpN72",    DoubleType),
      StructField(                        "stockAtpN73",    DoubleType),
      StructField(                        "stockAtpN74",    DoubleType),
      StructField(                        "stockAtpN75",    DoubleType),
      StructField(                        "stockAtpN76",    DoubleType),
      StructField(                        "stockAtpN77",    DoubleType),
      StructField(                        "stockAtpN78",    DoubleType),
      StructField(                        "stockAtpN79",    DoubleType),
      StructField(                        "stockAtpN80",    DoubleType),
      StructField(                        "stockAtpN81",    DoubleType),
      StructField(                        "stockAtpN82",    DoubleType),
      StructField(                        "stockAtpN83",    DoubleType),
      StructField(                        "stockAtpN84",    DoubleType),
      StructField(                        "stockAtpN85",    DoubleType),
      StructField(                        "stockAtpN86",    DoubleType),
      StructField(                        "stockAtpN87",    DoubleType),
      StructField(                        "stockAtpN88",    DoubleType),
      StructField(                        "stockAtpN89",    DoubleType),
      StructField(                        "stockAtpN90",    DoubleType),
      StructField(                        "stockAtpN91",    DoubleType),
      StructField(                        "stockAtpN92",    DoubleType),
      StructField(                        "stockAtpN93",    DoubleType),
      StructField(                        "stockAtpN94",    DoubleType),
      StructField(                        "stockAtpN95",    DoubleType),
      StructField(                        "stockAtpN96",    DoubleType),
      StructField(                        "stockAtpN97",    DoubleType),
      StructField(                        "stockAtpN98",    DoubleType),
      StructField(                        "stockAtpN99",    DoubleType),
      StructField(                        "stockAtpN100",   DoubleType),
      StructField(                        "stockAtpN101",   DoubleType)
    ))

  }

  "getDayNAndPreviousDayAtMidnight" must
    " return currentDay and its previous day when no data date nor default date is passed"  taggedAs UnitTestTag in new BaseTrait {
    StockATPMain.getDayNAndPreviousDayAtMidnight(None)(appConfig) should be (todayAtMidnightTs, yesterdayAtMidnightTs)
  }

  it must " return default date and its previous day when no data date is passed but default is set" taggedAs UnitTestTag  in new BaseTrait {
    val dateDataDefaultTs = new Timestamp(1535166367000L)
    val dateDataDefaultAtMidnightTs: Timestamp = Dates.getDateAtMidnight(dateDataDefaultTs)._1
    val previousDayDefaultAtMidnightTs: Timestamp = Dates.subtractDaysToTimestamp(dateDataDefaultAtMidnightTs, 1)

    StockATPMain.getDayNAndPreviousDayAtMidnight(None, dateDataDefaultTs)(appConfig) should be (dateDataDefaultAtMidnightTs, previousDayDefaultAtMidnightTs)
  }

  it must " return data day and its previous day when data date is passed" taggedAs UnitTestTag  in new BaseTrait {
    val optionalDateTs: Timestamp = Dates.stringToTimestamp("2018-01-02 00:00:00", appConfig.getTimezone)
    val previousDayDefaultAtMidnightTs: Timestamp = Dates.subtractDaysToTimestamp(optionalDateTs, 1)

    StockATPMain.getDayNAndPreviousDayAtMidnight(Option("2018-01-02"))(appConfig) should be (optionalDateTs, previousDayDefaultAtMidnightTs)
  }

  "processCalculate" must
    " calculate stock ATP for all articles in fixed data when there is only stock consolidated data"  taggedAs UnitTestTag in new StockATPFor5ProjectionDays {

    val stockConf = StockConf(
      maxNsp = 80d, minNsp = 10d,
      maxNst = 80d, minNst = 20d,
      rotationArtAA = 50d,
      rotationArtA = 60d,
      rotationArtB = 70d,
      rotationArtC = 80d,
      numberOfDaysToProject,
      tsUpdateDlk = currentTimestamp)

    /** Consolidated Stock **/
    val dsConsolidatedStock: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, 110d, currentTimestamp),
      StockConsolidadoView(idArticle2, idStore1, measureUnitEA, 120d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore2, measureUnitEA, 210d, currentTimestamp),
      StockConsolidadoView(idArticle2, idStore2, measureUnitEA, 220d, currentTimestamp),

      // This will be ignored: not in fixed data
      StockConsolidadoView(idArticle3, idStore2, measureUnitEA, 200d, currentTimestamp)

    )).toDS

    /** Fixed Data **/
    val dfFixedData: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row( idArticle1,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle2,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle1,  idStore2, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle2,  idStore2, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d)
      )), fixedDataStructType)
        .transform(Sql.filterDatosFijos(numberOfDaysToProject, todayAtMidnightTs, yesterdayAtMidnightTs)(spark, appConfig))

    /** Sales movements data : EMPTY **/
    val dsSaleMovements: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS

    /** Orders data : EMPTY **/
    val dsMaterials: Dataset[Materials] = sc.parallelize(List.empty[Materials]).toDS
    val dsMaterialsArticle: Dataset[MaterialsArticle] = sc.parallelize(List.empty[MaterialsArticle]).toDS
    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List.empty[CustomerReservations]).toDS
    val dsMaraWithArticleTypes: Dataset[MaraWithArticleTypes] = sc.parallelize(List.empty[MaraWithArticleTypes]).toDS
    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List.empty[OrderHeader]).toDS
    val dsOrderLine: Dataset[OrderLineView] = sc.parallelize(List.empty[OrderLineView]).toDS
    val dsPartialOrdersDelivered: Dataset[PartialOrdersDeliveredView] = sc.parallelize(List.empty[PartialOrdersDeliveredView]).toDS

    /** Capacities and store types: EMPTY --> Defaults are false and true **/
    val dsStoreCapacity: Dataset[StoreCapacityView] = sc.parallelize(List.empty[StoreCapacityView]).toDS
    val dsTypeStore: Dataset[StoreTypeView] = sc.parallelize(List.empty[StoreTypeView]).toDS


    /** ATP N Formula
      *
      * Store - Article  -> consolidated stock * rotation/100 - max(sales forecast - sales N, 0)*isOpen + stock to be delivered * nsp/100 * nst/100
      * 11    - 1        ->               110  * 70/100        - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 =  77
      * 11    - 2        ->               120  * 70/100        - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 =  84
      * 22    - 1        ->               210  * 70/100        - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 147
      * 22    - 2        ->               220  * 70/100        - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 154
      *
      * ATP NX Formula
      * Store - Article - Day -> ATPNX-1 - sales forecast + stock to be delivered * nsp/100 * nst/100
      *
      * As there is no other information apart from consolidated stock, ATP will be the same for each store-article every day
      *
      */
    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idStore1, idArticle1, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  77d,  77d,  77d,  77d,  77d,  77d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1, idArticle2, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  84d,  84d,  84d,  84d,  84d,  84d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, idArticle1, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 147d, 147d, 147d, 147d, 147d, 147d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, idArticle2, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 154d, 154d, 154d, 154d, 154d, 154d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null)
    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val readEntities =
      ReadAtpEntities(
        dsSaleMovements,
        dsConsolidatedStock,
        dfFixedData,
        dsOrderHeader,
        dsOrderLine,
        dsPartialOrdersDelivered,
        dsStoreCapacity,
        dsTypeStore,
        dsMaraWithArticleTypes,
        dsMaterials,
        dsMaterialsArticle,
        dsCustomerReservations)

    calculateProcessAndCompareResultWithoutTsUpdateColumns(
      readEntities,
      stockConf,
      todayAtMidnightTs,
      yesterdayAtMidnightTs,
      List.empty[ControlEjecucionView],
      appName,
      Seq(StockATP.ccTags.idArticleTag, StockATP.ccTags.idStoreTag),
      dfExpected)

  }

  it must
    " calculate stock ATP getting the correct rotation percentage"  taggedAs UnitTestTag in new StockATPFor5ProjectionDays {

    val stockConf = StockConf(
      maxNsp = 80d, minNsp = 10d,
      maxNst = 80d, minNst = 20d,
      rotationArtAA = 50d,
      rotationArtA = 60d,
      rotationArtB = 70d,
      rotationArtC = 80d,
      numberOfDaysToProject,
      tsUpdateDlk = currentTimestamp)

    /** Consolidated Stock **/
    val dsConsolidatedStock: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(  idArticle1, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(  idArticle2, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(  idArticle3, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(  idArticle4, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(  idArticle5, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(  idArticle6, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(  idArticle7, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(  idArticle8, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(  idArticle9, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView( idArticle10, idStore1, measureUnitEA, 100d, currentTimestamp)
    )).toDS

    /** Fixed Data **/
    val dfFixedData: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row(  idArticle1,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d,                     "A", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row(  idArticle2,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d,                     "a", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row(  idArticle3,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d,                     "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row(  idArticle4,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d,                     "b", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row(  idArticle5,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d,                     "C", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row(  idArticle6,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d,                     "c", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row(  idArticle7,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d,                    "A+", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row(  idArticle8,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d,                    "a+", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row(  idArticle9,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d, "Rotation Non Existent", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle10,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d,                    null, currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d)
      )), fixedDataStructType)
        .transform(Sql.filterDatosFijos(numberOfDaysToProject, todayAtMidnightTs, yesterdayAtMidnightTs)(spark, appConfig))

    /** Sales movements data : EMPTY **/
    val dsSaleMovements: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS

    /** Orders data : EMPTY **/
    val dsMaterials: Dataset[Materials] = sc.parallelize(List.empty[Materials]).toDS
    val dsMaterialsArticle: Dataset[MaterialsArticle] = sc.parallelize(List.empty[MaterialsArticle]).toDS
    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List.empty[CustomerReservations]).toDS
    val dsMaraWithArticleTypes: Dataset[MaraWithArticleTypes] = sc.parallelize(List.empty[MaraWithArticleTypes]).toDS
    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List.empty[OrderHeader]).toDS
    val dsOrderLine: Dataset[OrderLineView] = sc.parallelize(List.empty[OrderLineView]).toDS
    val dsPartialOrdersDelivered: Dataset[PartialOrdersDeliveredView] = sc.parallelize(List.empty[PartialOrdersDeliveredView]).toDS

    /** Capacities and store types: EMPTY --> Defaults are false and true **/
    val dsStoreCapacity: Dataset[StoreCapacityView] = sc.parallelize(List.empty[StoreCapacityView]).toDS
    val dsTypeStore: Dataset[StoreTypeView] = sc.parallelize(List.empty[StoreTypeView]).toDS


    /** ATP N Formula
      *
      * Store - Article   -> consolidated stock * rotation/100 - max(sales forecast - sales N, 0)*isOpen + stock to be delivered * nsp/100 * nst/100
      * 11    -  1        ->               100  * 60/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 60
      * 11    -  2        ->               100  * 60/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 60
      * 11    -  3        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 11    -  4        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 11    -  5        ->               100  * 80/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 80
      * 11    -  6        ->               100  * 80/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 80
      * 11    -  7        ->               100  * 50/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 50
      * 11    -  8        ->               100  * 50/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 50
      * 11    -  9        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 11    - 10        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      *
      * ATP NX Formula
      * Store - Article - Day -> ATPNX-1 - sales forecast + stock to be delivered * nsp/100 * nst/100
      *
      * As there is no other information apart from consolidated stock, ATP will be the same for each store-article every day
      *
      */
    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idStore1,  idArticle1, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  60d,  60d,  60d,  60d,  60d,  60d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1,  idArticle2, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  60d,  60d,  60d,  60d,  60d,  60d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1,  idArticle3, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  70d,  70d,  70d,  70d,  70d,  70d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1,  idArticle4, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  70d,  70d,  70d,  70d,  70d,  70d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1,  idArticle5, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  80d,  80d,  80d,  80d,  80d,  80d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1,  idArticle6, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  80d,  80d,  80d,  80d,  80d,  80d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1,  idArticle7, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  50d,  50d,  50d,  50d,  50d,  50d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1,  idArticle8, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  50d,  50d,  50d,  50d,  50d,  50d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1,  idArticle9, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  70d,  70d,  70d,  70d,  70d,  70d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1, idArticle10, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  70d,  70d,  70d,  70d,  70d,  70d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null)

    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val readEntities =
      ReadAtpEntities(
        dsSaleMovements,
        dsConsolidatedStock,
        dfFixedData,
        dsOrderHeader,
        dsOrderLine,
        dsPartialOrdersDelivered,
        dsStoreCapacity,
        dsTypeStore,
        dsMaraWithArticleTypes,
        dsMaterials,
        dsMaterialsArticle,
        dsCustomerReservations)

    calculateProcessAndCompareResultWithoutTsUpdateColumns(
      readEntities,
      stockConf,
      todayAtMidnightTs,
      yesterdayAtMidnightTs,
      List.empty[ControlEjecucionView],
      appName,
      Seq(StockATP.ccTags.idArticleTag, StockATP.ccTags.idStoreTag),
      dfExpected)

  }

  it must
    " calculate stock ATP for all articles in fixed data when there is stock consolidated data and sales forecast data calculated in day N"  taggedAs UnitTestTag in new StockATPFor5ProjectionDays {

    val stockConf = StockConf(
      maxNsp = 80d, minNsp = 10d,
      maxNst = 80d, minNst = 20d,
      rotationArtAA = 50d,
      rotationArtA = 60d,
      rotationArtB = 70d,
      rotationArtC = 80d,
      numberOfDaysToProject,
      tsUpdateDlk = currentTimestamp)

    /** Consolidated Stock **/
    val dsConsolidatedStock: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle2, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle2, idStore2, measureUnitEA, 100d, currentTimestamp),

      // This will be ignored: not in fixed data
      StockConsolidadoView(idArticle3, idStore2, measureUnitEA, 100d, currentTimestamp)

    )).toDS

    /** Fixed Data **/
    val dfFixedData: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row( idArticle1,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          11d, 12d, 13d, 14d, 15d, 16d, 17d, 18d, 19d, 20d, 21d),
        Row( idArticle2,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          21d, 22d, 23d, 24d, 25d, 26d, 27d, 28d, 29d, 30d, 31d),

        // This will be negative and rounded to 0: not in consolidated stock
        Row( idArticle3,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          31d, 32d, 33d, 34d, 35d, 36d, 37d, 38d, 39d, 40d, 41d),

        Row( idArticle1,  idStore2, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          41d, 42d, 43d, 44d, 45d, 46d, 47d, 48d, 49d, 50d, 51d),
        Row( idArticle2,  idStore2, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          51d, 52d, 53d, 54d, 55d, 56d, 57d, 58d, 59d, 60d, 61d)
      )), fixedDataStructType)
        .transform(Sql.filterDatosFijos(numberOfDaysToProject, todayAtMidnightTs, yesterdayAtMidnightTs)(spark, appConfig))

    /** Sales movements data : EMPTY **/
    val dsSaleMovements: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS

    /** Orders data : EMPTY **/
    val dsMaterials: Dataset[Materials] = sc.parallelize(List.empty[Materials]).toDS
    val dsMaterialsArticle: Dataset[MaterialsArticle] = sc.parallelize(List.empty[MaterialsArticle]).toDS
    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List.empty[CustomerReservations]).toDS
    val dsMaraWithArticleTypes: Dataset[MaraWithArticleTypes] = sc.parallelize(List.empty[MaraWithArticleTypes]).toDS
    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List.empty[OrderHeader]).toDS
    val dsOrderLine: Dataset[OrderLineView] = sc.parallelize(List.empty[OrderLineView]).toDS
    val dsPartialOrdersDelivered: Dataset[PartialOrdersDeliveredView] = sc.parallelize(List.empty[PartialOrdersDeliveredView]).toDS

    /** Capacities and store types: EMPTY --> Defaults are false and true **/
    val dsStoreCapacity: Dataset[StoreCapacityView] = sc.parallelize(List.empty[StoreCapacityView]).toDS
    val dsTypeStore: Dataset[StoreTypeView] = sc.parallelize(List.empty[StoreTypeView]).toDS


    /** ATP N Formula
      *
      * Store - Article  -> consolidated stock * rotation/100 - max(sales forecast - sales N, 0)*isOpen + stock to be delivered * nsp/100 * nst/100
      * 11    - 1        ->               100  * 70/100       - max(       11     -    0   , 0)*1      +           0           *  20/100 * 50/100 = 59
      * 11    - 2        ->               100  * 70/100       - max(       21     -    0   , 0)*1      +           0           *  20/100 * 50/100 = 49
      * 11    - 3        ->                 0  * 70/100       - max(       31     -    0   , 0)*1      +           0           *  20/100 * 50/100 =  0
      * 22    - 1        ->               100  * 70/100       - max(       41     -    0   , 0)*1      +           0           *  20/100 * 50/100 = 29
      * 22    - 2        ->               100  * 70/100       - max(       51     -    0   , 0)*1      +           0           *  20/100 * 50/100 = 19
      *
      * ATP NX Formula
      * Store - Article - Day -> ATPNX-1 - sales forecast + stock to be delivered * nsp/100 * nst/100
      * 11    - 1          1  ->    59   -         12     +           0           *  20/100 * 50/100 = 47
      * 11    - 2          1  ->    49   -         22     +           0           *  20/100 * 50/100 = 27
      * 11    - 3          1  ->     0   -         32     +           0           *  20/100 * 50/100 =  0
      * 22    - 1          1  ->    29   -         42     +           0           *  20/100 * 50/100 =  0
      * 22    - 2          1  ->    19   -         52     +           0           *  20/100 * 50/100 =  0
      *
      * 11    - 1          2  ->    47   -         13     +           0           *  20/100 * 50/100 = 34
      * 11    - 2          2  ->    27   -         23     +           0           *  20/100 * 50/100 =  4
      * 11    - 3          2  ->     0   -         33     +           0           *  20/100 * 50/100 =  0
      * 22    - 1          2  ->     0   -         43     +           0           *  20/100 * 50/100 =  0
      * 22    - 2          2  ->     0   -         53     +           0           *  20/100 * 50/100 =  0
      *
      * 11    - 1          3  ->    34   -         14     +           0           *  20/100 * 50/100 = 20
      * 11    - 2          3  ->     4   -         24     +           0           *  20/100 * 50/100 =  0
      * 11    - 3          3  ->     0   -         34     +           0           *  20/100 * 50/100 =  0
      * 22    - 1          3  ->     0   -         44     +           0           *  20/100 * 50/100 =  0
      * 22    - 2          3  ->     0   -         54     +           0           *  20/100 * 50/100 =  0
      *
      * 11    - 1          4  ->    20   -         15     +           0           *  20/100 * 50/100 =  5
      * 11    - 2          4  ->     0   -         25     +           0           *  20/100 * 50/100 =  0
      * 11    - 3          4  ->     0   -         35     +           0           *  20/100 * 50/100 =  0
      * 22    - 1          4  ->     0   -         45     +           0           *  20/100 * 50/100 =  0
      * 22    - 2          4  ->     0   -         55     +           0           *  20/100 * 50/100 =  0
      *
      * 11    - 1          5  ->     5   -         16     +           0           *  20/100 * 50/100 =  0
      * 11    - 2          5  ->     0   -         26     +           0           *  20/100 * 50/100 =  0
      * 11    - 3          5  ->     0   -         36     +           0           *  20/100 * 50/100 =  0
      * 22    - 1          5  ->     0   -         46     +           0           *  20/100 * 50/100 =  0
      * 22    - 2          5  ->     0   -         56     +           0           *  20/100 * 50/100 =  0
      *
      */
    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idStore1, idArticle1, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 59d, 47d, 34d, 20d, 5d, 0d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1, idArticle2, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 49d, 27d,  4d,  0d, 0d, 0d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1, idArticle3, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  0d,  0d,  0d,  0d, 0d, 0d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, idArticle1, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 29d,  0d,  0d,  0d, 0d, 0d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, idArticle2, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 19d,  0d,  0d,  0d, 0d, 0d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null)
    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val readEntities =
      ReadAtpEntities(
        dsSaleMovements,
        dsConsolidatedStock,
        dfFixedData,
        dsOrderHeader,
        dsOrderLine,
        dsPartialOrdersDelivered,
        dsStoreCapacity,
        dsTypeStore,
        dsMaraWithArticleTypes,
        dsMaterials,
        dsMaterialsArticle,
        dsCustomerReservations)

    calculateProcessAndCompareResultWithoutTsUpdateColumns(
      readEntities,
      stockConf,
      todayAtMidnightTs,
      yesterdayAtMidnightTs,
      List.empty[ControlEjecucionView],
      appName,
      Seq(StockATP.ccTags.idArticleTag, StockATP.ccTags.idStoreTag),
      dfExpected)

  }

  it must
    " calculate stock ATP when fixed data was calculated the previous day. " +
      "It should start taken into account sales forecast values from N1 column"  taggedAs UnitTestTag in new StockATPFor5ProjectionDays {

    val stockConf = StockConf(
      maxNsp = 80d, minNsp = 10d,
      maxNst = 80d, minNst = 20d,
      rotationArtAA = 50d,
      rotationArtA = 60d,
      rotationArtB = 70d,
      rotationArtC = 80d,
      numberOfDaysToProject,
      tsUpdateDlk = currentTimestamp)

    /** Consolidated Stock **/
    val dsConsolidatedStock: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle2, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle2, idStore2, measureUnitEA, 100d, currentTimestamp),

      // This will be ignored: not in fixed data
      StockConsolidadoView(idArticle3, idStore2, measureUnitEA, 100d, currentTimestamp)

    )).toDS

    /** Fixed Data **/
    val dfFixedData: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row( idArticle1,  idStore1, yesterdayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          11d, 12d, 13d, 14d, 15d, 16d, 17d, 18d, 19d, 20d, 21d),
        Row( idArticle2,  idStore1, yesterdayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          21d, 22d, 23d, 24d, 25d, 26d, 27d, 28d, 29d, 30d, 31d),

        // This will be negative and rounded to 0: not in consolidated stock
        Row( idArticle3,  idStore1, yesterdayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          31d, 32d, 33d, 34d, 35d, 36d, 37d, 38d, 39d, 40d, 41d),

        Row( idArticle1,  idStore2, yesterdayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          41d, 42d, 43d, 44d, 45d, 46d, 47d, 48d, 49d, 50d, 51d),
        Row( idArticle2,  idStore2, yesterdayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          51d, 52d, 53d, 54d, 55d, 56d, 57d, 58d, 59d, 60d, 61d)
      )), fixedDataStructType)
        .transform(Sql.filterDatosFijos(numberOfDaysToProject, todayAtMidnightTs, yesterdayAtMidnightTs)(spark, appConfig))

    /** Sales movements data : EMPTY **/
    val dsSaleMovements: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS

    /** Orders data : EMPTY **/
    val dsMaterials: Dataset[Materials] = sc.parallelize(List.empty[Materials]).toDS
    val dsMaterialsArticle: Dataset[MaterialsArticle] = sc.parallelize(List.empty[MaterialsArticle]).toDS
    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List.empty[CustomerReservations]).toDS
    val dsMaraWithArticleTypes: Dataset[MaraWithArticleTypes] = sc.parallelize(List.empty[MaraWithArticleTypes]).toDS
    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List.empty[OrderHeader]).toDS
    val dsOrderLine: Dataset[OrderLineView] = sc.parallelize(List.empty[OrderLineView]).toDS
    val dsPartialOrdersDelivered: Dataset[PartialOrdersDeliveredView] = sc.parallelize(List.empty[PartialOrdersDeliveredView]).toDS

    /** Capacities and store types: EMPTY --> Defaults are false and true **/
    val dsStoreCapacity: Dataset[StoreCapacityView] = sc.parallelize(List.empty[StoreCapacityView]).toDS
    val dsTypeStore: Dataset[StoreTypeView] = sc.parallelize(List.empty[StoreTypeView]).toDS


    /** ATP N Formula
      *
      * Store - Article  -> consolidated stock * rotation/100 - max(sales forecast - sales N, 0)*isOpen + stock to be delivered * nsp/100 * nst/100
      * 11    - 1        ->               100  * 70/100       - max(       12     -    0   , 0)*1      +           0           *  20/100 * 50/100 = 58
      * 11    - 2        ->               100  * 70/100       - max(       22     -    0   , 0)*1      +           0           *  20/100 * 50/100 = 48
      * 11    - 3        ->                 0  * 70/100       - max(       32     -    0   , 0)*1      +           0           *  20/100 * 50/100 =  0
      * 22    - 1        ->               100  * 70/100       - max(       42     -    0   , 0)*1      +           0           *  20/100 * 50/100 = 28
      * 22    - 2        ->               100  * 70/100       - max(       52     -    0   , 0)*1      +           0           *  20/100 * 50/100 = 18
      *
      * ATP NX Formula
      * Store - Article - Day -> ATPNX-1 - sales forecast + stock to be delivered * nsp/100 * nst/100
      * 11    - 1          1  ->    58   -         13     +           0           *  20/100 * 50/100 = 45
      * 11    - 2          1  ->    48   -         23     +           0           *  20/100 * 50/100 = 25
      * 11    - 3          1  ->     0   -         33     +           0           *  20/100 * 50/100 =  0
      * 22    - 1          1  ->    28   -         43     +           0           *  20/100 * 50/100 =  0
      * 22    - 2          1  ->    18   -         53     +           0           *  20/100 * 50/100 =  0
      *
      * 11    - 1          2  ->    45   -         14     +           0           *  20/100 * 50/100 = 31
      * 11    - 2          2  ->    25   -         24     +           0           *  20/100 * 50/100 =  1
      * 11    - 3          2  ->     0   -         34     +           0           *  20/100 * 50/100 =  0
      * 22    - 1          2  ->     0   -         44     +           0           *  20/100 * 50/100 =  0
      * 22    - 2          2  ->     0   -         54     +           0           *  20/100 * 50/100 =  0
      *
      * 11    - 1          3  ->    31   -         15     +           0           *  20/100 * 50/100 = 16
      * 11    - 2          3  ->     1   -         25     +           0           *  20/100 * 50/100 =  0
      * 11    - 3          3  ->     0   -         35     +           0           *  20/100 * 50/100 =  0
      * 22    - 1          3  ->     0   -         45     +           0           *  20/100 * 50/100 =  0
      * 22    - 2          3  ->     0   -         55     +           0           *  20/100 * 50/100 =  0
      *
      * 11    - 1          4  ->    16   -         16     +           0           *  20/100 * 50/100 =  0
      * 11    - 2          4  ->     0   -         26     +           0           *  20/100 * 50/100 =  0
      * 11    - 3          4  ->     0   -         36     +           0           *  20/100 * 50/100 =  0
      * 22    - 1          4  ->     0   -         46     +           0           *  20/100 * 50/100 =  0
      * 22    - 2          4  ->     0   -         56     +           0           *  20/100 * 50/100 =  0
      *
      * 11    - 1          5  ->     0   -         17     +           0           *  20/100 * 50/100 =  0
      * 11    - 2          5  ->     0   -         27     +           0           *  20/100 * 50/100 =  0
      * 11    - 3          5  ->     0   -         37     +           0           *  20/100 * 50/100 =  0
      * 22    - 1          5  ->     0   -         47     +           0           *  20/100 * 50/100 =  0
      * 22    - 2          5  ->     0   -         57     +           0           *  20/100 * 50/100 =  0
      *
      */
    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idStore1, idArticle1, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 58d, 45d, 31d, 16d, 0d, 0d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1, idArticle2, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 48d, 25d,  1d,  0d, 0d, 0d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1, idArticle3, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  0d,  0d,  0d,  0d, 0d, 0d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, idArticle1, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 28d,  0d,  0d,  0d, 0d, 0d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, idArticle2, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 18d,  0d,  0d,  0d, 0d, 0d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null)
    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val readEntities =
      ReadAtpEntities(
        dsSaleMovements,
        dsConsolidatedStock,
        dfFixedData,
        dsOrderHeader,
        dsOrderLine,
        dsPartialOrdersDelivered,
        dsStoreCapacity,
        dsTypeStore,
        dsMaraWithArticleTypes,
        dsMaterials,
        dsMaterialsArticle,
        dsCustomerReservations)

    calculateProcessAndCompareResultWithoutTsUpdateColumns(
      readEntities,
      stockConf,
      todayAtMidnightTs,
      yesterdayAtMidnightTs,
      List.empty[ControlEjecucionView],
      appName,
      Seq(StockATP.ccTags.idArticleTag, StockATP.ccTags.idStoreTag),
      dfExpected)

  }

  it must
    " calculate stock ATP correcting forecast sales with actual sales for day N"  taggedAs UnitTestTag in new StockATPFor5ProjectionDays {

    val stockConf = StockConf(
      maxNsp = 80d, minNsp = 10d,
      maxNst = 80d, minNst = 20d,
      rotationArtAA = 50d,
      rotationArtA = 60d,
      rotationArtB = 70d,
      rotationArtC = 80d,
      numberOfDaysToProject,
      tsUpdateDlk = currentTimestamp)

    /** Consolidated Stock **/
    val dsConsolidatedStock: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle2, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle2, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore3, measureUnitEA, 100d, currentTimestamp)
    )).toDS

    /** Fixed Data **/
    val dfFixedData: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row( idArticle1,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle2,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle1,  idStore2, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle2,  idStore2, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
           0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),

        // There will be no sales for this article
        Row( idArticle1,  idStore3, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d)
      )), fixedDataStructType)
        .transform(Sql.filterDatosFijos(numberOfDaysToProject, todayAtMidnightTs, yesterdayAtMidnightTs)(spark, appConfig))

    /** Sales movements data **/
    val dsSaleMovements: Dataset[SaleMovementsView] = sc.parallelize(List[SaleMovementsView](
      SaleMovementsView(idArticle1, idStore1, 100d, currentTimestamp),
      SaleMovementsView(idArticle2, idStore1,   5d, currentTimestamp),
      SaleMovementsView(idArticle1, idStore2,  10d, currentTimestamp),
      SaleMovementsView(idArticle2, idStore2,  10d, currentTimestamp),

      // This will be ignored: not in fixed data
      SaleMovementsView(idArticle3, idStore1, 50d, currentTimestamp)
    )).toDS

    /** Orders data : EMPTY **/
    val dsMaterials: Dataset[Materials] = sc.parallelize(List.empty[Materials]).toDS
    val dsMaterialsArticle: Dataset[MaterialsArticle] = sc.parallelize(List.empty[MaterialsArticle]).toDS
    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List.empty[CustomerReservations]).toDS
    val dsMaraWithArticleTypes: Dataset[MaraWithArticleTypes] = sc.parallelize(List.empty[MaraWithArticleTypes]).toDS
    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List.empty[OrderHeader]).toDS
    val dsOrderLine: Dataset[OrderLineView] = sc.parallelize(List.empty[OrderLineView]).toDS
    val dsPartialOrdersDelivered: Dataset[PartialOrdersDeliveredView] = sc.parallelize(List.empty[PartialOrdersDeliveredView]).toDS

    /** Capacities and store types: EMPTY --> Defaults are false and true **/
    val dsStoreCapacity: Dataset[StoreCapacityView] = sc.parallelize(List.empty[StoreCapacityView]).toDS
    val dsTypeStore: Dataset[StoreTypeView] = sc.parallelize(List.empty[StoreTypeView]).toDS


    /** ATP N Formula
      *
      * Store - Article  -> consolidated stock * rotation/100 - max(sales forecast - sales N, 0)*isOpen + stock to be delivered * nsp/100 * nst/100
      * 11    - 1        ->               100  * 70/100       - max(       10     -  100   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 11    - 2        ->               100  * 70/100       - max(       10     -    5   , 0)*1      +           0           *  20/100 * 50/100 = 65
      * 22    - 1        ->               100  * 70/100       - max(       10     -   10   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 22    - 2        ->               100  * 70/100       - max(        0     -   10   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 33    - 1        ->               100  * 70/100       - max(       10     -    0   , 0)*1      +           0           *  20/100 * 50/100 = 60
      *
      * ATP NX Formula
      * Store - Article - Day -> ATPNX-1 - sales forecast + stock to be delivered * nsp/100 * nst/100
      *
      * Sales forecast is 0 for every other day, so ATP will be the same for every day
      */
    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idStore1, idArticle1, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 70d, 70d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1, idArticle2, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 65d, 65d, 65d, 65d, 65d, 65d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, idArticle1, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 70d, 70d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, idArticle2, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 70d, 70d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore3, idArticle1, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 60d, 60d, 60d, 60d, 60d, 60d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null)
    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val readEntities =
      ReadAtpEntities(
        dsSaleMovements,
        dsConsolidatedStock,
        dfFixedData,
        dsOrderHeader,
        dsOrderLine,
        dsPartialOrdersDelivered,
        dsStoreCapacity,
        dsTypeStore,
        dsMaraWithArticleTypes,
        dsMaterials,
        dsMaterialsArticle,
        dsCustomerReservations)

    calculateProcessAndCompareResultWithoutTsUpdateColumns(
      readEntities,
      stockConf,
      todayAtMidnightTs,
      yesterdayAtMidnightTs,
      List.empty[ControlEjecucionView],
      appName,
      Seq(StockATP.ccTags.idArticleTag, StockATP.ccTags.idStoreTag),
      dfExpected)

  }

  it must
    " calculate stock ATP not take into account forecast sales when stores are closed, " +
      "but it should when store is open or not configured"  taggedAs UnitTestTag in new StockATPFor5ProjectionDays {

    val stockConf = StockConf(
      maxNsp = 80d, minNsp = 10d,
      maxNst = 80d, minNst = 20d,
      rotationArtAA = 50d,
      rotationArtA = 60d,
      rotationArtB = 70d,
      rotationArtC = 80d,
      numberOfDaysToProject,
      tsUpdateDlk = currentTimestamp)

    /** Consolidated Stock **/
    val dsConsolidatedStock: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore3, measureUnitEA, 100d, currentTimestamp)
    )).toDS

    /** Fixed Data **/
    val dfFixedData: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row( idArticle1,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle1,  idStore2, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle1,  idStore3, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d)
      )), fixedDataStructType)
        .transform(Sql.filterDatosFijos(numberOfDaysToProject, todayAtMidnightTs, yesterdayAtMidnightTs)(spark, appConfig))

    /** Sales movements data : EMPTY **/
    val dsSaleMovements: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS

    /** Orders data : EMPTY **/
    val dsMaterials: Dataset[Materials] = sc.parallelize(List.empty[Materials]).toDS
    val dsMaterialsArticle: Dataset[MaterialsArticle] = sc.parallelize(List.empty[MaterialsArticle]).toDS
    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List.empty[CustomerReservations]).toDS
    val dsMaraWithArticleTypes: Dataset[MaraWithArticleTypes] = sc.parallelize(List.empty[MaraWithArticleTypes]).toDS
    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List.empty[OrderHeader]).toDS
    val dsOrderLine: Dataset[OrderLineView] = sc.parallelize(List.empty[OrderLineView]).toDS
    val dsPartialOrdersDelivered: Dataset[PartialOrdersDeliveredView] = sc.parallelize(List.empty[PartialOrdersDeliveredView]).toDS

    /** Capacities and store types **/
    val dsStoreCapacity: Dataset[StoreCapacityView] = sc.parallelize(List.empty[StoreCapacityView]).toDS
    val dsTypeStore: Dataset[StoreTypeView] = sc.parallelize(List[StoreTypeView](
      StoreTypeView(idStore1, closedStore, currentTimestamp),
      StoreTypeView(idStore2, openStore, currentTimestamp),

      // This will be ignored: there is no data for this store
      StoreTypeView(idStore4, openStore, currentTimestamp)
    )).toDS


    /** ATP N Formula
      *
      * Store - Article  -> consolidated stock * rotation/100 - max(sales forecast - sales N, 0)*isOpen + stock to be delivered * nsp/100 * nst/100
      * 11    - 1        ->               100  * 70/100       - max(       10     -     0   , 0)*0      +           0           *  20/100 * 50/100 = 70
      * 22    - 1        ->               100  * 70/100       - max(       10     -     0   , 0)*1      +           0           *  20/100 * 50/100 = 60
      * 33    - 1        ->               100  * 70/100       - max(       10     -     0   , 0)*1      +           0           *  20/100 * 50/100 = 60
      *
      * ATP NX Formula
      * Store - Article - Day -> ATPNX-1 - sales forecast + stock to be delivered * nsp/100 * nst/100
      * 11    - 1          1  ->    70   -         10     +           0           *  20/100 * 50/100 = 60
      * 22    - 1          1  ->    60   -         10     +           0           *  20/100 * 50/100 = 50
      * 33    - 1          1  ->    60   -         10     +           0           *  20/100 * 50/100 = 50
      *
      * 11    - 1          2  ->    60   -         10     +           0           *  20/100 * 50/100 = 50
      * 22    - 1          2  ->    50   -         10     +           0           *  20/100 * 50/100 = 40
      * 33    - 1          2  ->    50   -         10     +           0           *  20/100 * 50/100 = 40
      *
      * 11    - 1          3  ->    50   -         10     +           0           *  20/100 * 50/100 = 40
      * 22    - 1          3  ->    40   -         10     +           0           *  20/100 * 50/100 = 30
      * 33    - 1          3  ->    40   -         10     +           0           *  20/100 * 50/100 = 30
      *
      * 11    - 1          4  ->    40   -         10     +           0           *  20/100 * 50/100 = 30
      * 22    - 1          4  ->    30   -         10     +           0           *  20/100 * 50/100 = 20
      * 33    - 1          4  ->    30   -         10     +           0           *  20/100 * 50/100 = 20
      *
      * 11    - 1          5  ->    30   -         10     +           0           *  20/100 * 50/100 = 20
      * 22    - 1          5  ->    20   -         10     +           0           *  20/100 * 50/100 = 10
      * 33    - 1          5  ->    20   -         10     +           0           *  20/100 * 50/100 = 10
      *
      */
    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idStore1, idArticle1, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 70d, 60d, 50d, 40d, 30d, 20d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, idArticle1, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 60d, 50d, 40d, 30d, 20d, 10d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore3, idArticle1, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 60d, 50d, 40d, 30d, 20d, 10d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null)
    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val readEntities =
      ReadAtpEntities(
        dsSaleMovements,
        dsConsolidatedStock,
        dfFixedData,
        dsOrderHeader,
        dsOrderLine,
        dsPartialOrdersDelivered,
        dsStoreCapacity,
        dsTypeStore,
        dsMaraWithArticleTypes,
        dsMaterials,
        dsMaterialsArticle,
        dsCustomerReservations)

    calculateProcessAndCompareResultWithoutTsUpdateColumns(
      readEntities,
      stockConf,
      todayAtMidnightTs,
      yesterdayAtMidnightTs,
      List.empty[ControlEjecucionView],
      appName,
      Seq(StockATP.ccTags.idArticleTag, StockATP.ccTags.idStoreTag),
      dfExpected)

  }

  it must
    " calculate stock ATP with stock to be delivered of only sale articles"  taggedAs UnitTestTag in new StockATPFor5ProjectionDays {

    val stockConf = StockConf(
      maxNsp = 80d, minNsp = 10d,
      maxNst = 80d, minNst = 20d,
      rotationArtAA = 50d,
      rotationArtA = 60d,
      rotationArtB = 70d,
      rotationArtC = 80d,
      numberOfDaysToProject,
      tsUpdateDlk = currentTimestamp)

    /** Consolidated Stock **/
    val dsConsolidatedStock: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle2, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle4, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle5, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle6, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle7, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle2, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore3, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle3, idStore3, measureUnitEA, 100d, currentTimestamp)
    )).toDS

    /** Fixed Data **/
    val dfFixedData: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row( idArticle1,  idStore1, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle2,  idStore1, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle4,  idStore1, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle5,  idStore1, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle6,  idStore1, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle7,  idStore1, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle1,  idStore2, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle2,  idStore2, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle1,  idStore3, todayString, freshProductsSector, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle3,  idStore3, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d)
      )), fixedDataStructType)
        .transform(Sql.filterDatosFijos(numberOfDaysToProject, todayAtMidnightTs, yesterdayAtMidnightTs)(spark, appConfig))

    /** Sales movements data : EMPTY **/
    val dsSaleMovements: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS

    /** Orders data : Only for sales articles **/
    val dsMaterials: Dataset[Materials] = sc.parallelize(List.empty[Materials]).toDS
    val dsMaterialsArticle: Dataset[MaterialsArticle] = sc.parallelize(List.empty[MaterialsArticle]).toDS
    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List.empty[CustomerReservations]).toDS

    val dsMaraWithArticleTypes: Dataset[MaraWithArticleTypes] = sc.parallelize(List[MaraWithArticleTypes](
      MaraWithArticleTypes(idArticle1, noPurchaseArticleType1, noPurchaseArticleCategory1, saleFlag,               null, currentTimestamp),
      MaraWithArticleTypes(idArticle2, noPurchaseArticleType1, noPurchaseArticleCategory1, saleFlag, parentSaleArticle2, currentTimestamp),
      MaraWithArticleTypes(idArticle4, noPurchaseArticleType1, noPurchaseArticleCategory1,       "",               null, currentTimestamp),
      MaraWithArticleTypes(idArticle5, noPurchaseArticleType1, noPurchaseArticleCategory1,       "", parentSaleArticle2, currentTimestamp),
      MaraWithArticleTypes(idArticle6, noPurchaseArticleType1, noPurchaseArticleCategory1,     null,               null, currentTimestamp),
      MaraWithArticleTypes(idArticle7, noPurchaseArticleType1, noPurchaseArticleCategory1,     null, parentSaleArticle2, currentTimestamp)
    )).toDS

    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List[OrderHeader](
      OrderHeader(idOrder1, typeOrderHeaderOMS1, currentTimestamp),
      OrderHeader(idOrder2, typeOrderHeaderOMS2, currentTimestamp),
      OrderHeader(idOrder3,      wrongTypeOrder, currentTimestamp),
      OrderHeader(idOrder4, typeOrderHeaderOMS3, currentTimestamp),
      OrderHeader(idOrder5, typeOrderHeaderOMS4, currentTimestamp),
      OrderHeader(idOrder6, typeOrderHeaderOMS2, currentTimestamp)
    )).toDS
      .transform(filterOrderHeader(spark, appConfig))

    val dsOrderLine: Dataset[OrderLineView] = sc.parallelize(List[OrderLine](
      OrderLine(idOrder1, idOrderLine1, idArticle1, idStore1,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder1, idOrderLine2, idArticle2, idStore1,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder2, idOrderLine1, idArticle1, idStore2,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder2, idOrderLine2, idArticle1, idStore2, canceledFlagList.head,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder2, idOrderLine3, idArticle1, idStore2,       notCanceledFlag,  notYetDeliveredFlag,    returnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder2, idOrderLine4, idArticle1, idStore2,       notCanceledFlag, alreadyDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder3, idOrderLine1, idArticle1, idStore2,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder4, idOrderLine1, idArticle1, idStore3,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder5, idOrderLine1, idArticle3, idStore3,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder5, idOrderLine2, idArticle1, idStore4,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder6, idOrderLine2, idArticle4, idStore1,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder6, idOrderLine3, idArticle5, idStore1,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder6, idOrderLine4, idArticle6, idStore1,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder6, idOrderLine1, idArticle7, idStore1,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp)
    )).toDS
      .transform(filterOrderLines(spark, appConfig))

    val dsPartialOrdersDelivered: Dataset[PartialOrdersDeliveredView] = sc.parallelize(List[PartialOrdersDeliveredView](
      // article 1 store 1 : partial orders for every day
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered1, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered2, 100d,  50d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered3, 200d, 100d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered4, 200d, 100d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered5, 200d, 100d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered6, 200d, 100d,                 dayN3, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered7, 200d, 100d,                 dayN4, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered8, 200d, 100d,   lastProjectionDayTs, currentTimestamp),

      // article 2 store 1 : partial orders for days in between
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered1, 200d, 10d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered2, 200d, 10d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered3, 200d, 10d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered4, 200d, 10d,                 dayN3, currentTimestamp),

      // article 4 store 1 : partial orders for every day
      PartialOrdersDeliveredView(idOrder6, idOrderLine2, idNumPartialOrdersDelivered1, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine2, idNumPartialOrdersDelivered2, 100d,  50d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine2, idNumPartialOrdersDelivered3, 200d, 100d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine2, idNumPartialOrdersDelivered4, 200d, 100d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine2, idNumPartialOrdersDelivered5, 200d, 100d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine2, idNumPartialOrdersDelivered6, 200d, 100d,                 dayN3, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine2, idNumPartialOrdersDelivered7, 200d, 100d,                 dayN4, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine2, idNumPartialOrdersDelivered8, 200d, 100d,   lastProjectionDayTs, currentTimestamp),

      // article 5 store 1 : partial orders for every day
      PartialOrdersDeliveredView(idOrder6, idOrderLine3, idNumPartialOrdersDelivered1, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine3, idNumPartialOrdersDelivered2, 100d,  50d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine3, idNumPartialOrdersDelivered3, 200d, 100d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine3, idNumPartialOrdersDelivered4, 200d, 100d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine3, idNumPartialOrdersDelivered5, 200d, 100d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine3, idNumPartialOrdersDelivered6, 200d, 100d,                 dayN3, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine3, idNumPartialOrdersDelivered7, 200d, 100d,                 dayN4, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine3, idNumPartialOrdersDelivered8, 200d, 100d,   lastProjectionDayTs, currentTimestamp),

      // article 6 store 1 : partial orders for every day
      PartialOrdersDeliveredView(idOrder6, idOrderLine4, idNumPartialOrdersDelivered1, 100d,   0d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine4, idNumPartialOrdersDelivered2,  50d,   0d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine4, idNumPartialOrdersDelivered3, 200d, 100d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine4, idNumPartialOrdersDelivered4, 200d, 100d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine4, idNumPartialOrdersDelivered5, 200d, 100d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine4, idNumPartialOrdersDelivered6, 200d, 100d,                 dayN3, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine4, idNumPartialOrdersDelivered7, 200d, 100d,                 dayN4, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine4, idNumPartialOrdersDelivered8, 200d, 100d,   lastProjectionDayTs, currentTimestamp),

      // article 7 store 1 : partial orders for every day
      PartialOrdersDeliveredView(idOrder6, idOrderLine1, idNumPartialOrdersDelivered1, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine1, idNumPartialOrdersDelivered2, 100d,  50d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine1, idNumPartialOrdersDelivered3, 200d, 100d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine1, idNumPartialOrdersDelivered4, 200d, 100d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine1, idNumPartialOrdersDelivered5, 200d, 100d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine1, idNumPartialOrdersDelivered6, 200d, 100d,                 dayN3, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine1, idNumPartialOrdersDelivered7, 200d, 100d,                 dayN4, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine1, idNumPartialOrdersDelivered8, 200d, 100d,   lastProjectionDayTs, currentTimestamp),

      // article 1 store 2 : all partial orders are either already delivered, or belong to canceled order lines, or order headers are not of OMS type
      PartialOrdersDeliveredView(idOrder2, idOrderLine1, idNumPartialOrdersDelivered1, 100d, 100d, yesterdayAtMidnightTs, currentTimestamp), // Already delivered
      PartialOrdersDeliveredView(idOrder2, idOrderLine1, idNumPartialOrdersDelivered2, 200d, 400d,     todayAtMidnightTs, currentTimestamp), // Already delivered (more than expected)
      PartialOrdersDeliveredView(idOrder2, idOrderLine2, idNumPartialOrdersDelivered3, 200d,  10d,                 dayN1, currentTimestamp), // Canceled Order Line
      PartialOrdersDeliveredView(idOrder2, idOrderLine3, idNumPartialOrdersDelivered4, 200d,  10d,                 dayN2, currentTimestamp), // Returned To Provider Order Line
      PartialOrdersDeliveredView(idOrder2, idOrderLine4, idNumPartialOrdersDelivered5, 200d,  10d,                 dayN3, currentTimestamp), // Already Delivered Order Line
      PartialOrdersDeliveredView(idOrder3, idOrderLine1, idNumPartialOrdersDelivered1, 200d,  20d,                 dayN3, currentTimestamp), // Order Header not of OMS type

      // article 2 store 2 : no partial orders for this article

      // article 1 store 3 : partial orders for every day, but it's from fresh products sector, so stock to be delivered will be taken from the same day
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered1, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered2, 100d,  50d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered3, 200d, 100d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered4, 200d, 100d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered5, 200d, 100d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered6, 200d, 100d,                 dayN3, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered7, 200d,  20d,                 dayN4, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered1, 200d, 100d,   lastProjectionDayTs, currentTimestamp),

      // article 3 store 3: not in mara, these orders will be ignored
      PartialOrdersDeliveredView(idOrder5, idOrderLine1, idNumPartialOrdersDelivered1, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp),

      // article 1 store 4: not fixed data
      PartialOrdersDeliveredView(idOrder5, idOrderLine2, idNumPartialOrdersDelivered2, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp)

    )).toDS

    /** Capacities and store types: EMPTY --> Defaults are false and true **/
    val dsStoreCapacity: Dataset[StoreCapacityView] = sc.parallelize(List.empty[StoreCapacityView]).toDS
    val dsTypeStore: Dataset[StoreTypeView] = sc.parallelize(List.empty[StoreTypeView]).toDS


    /** ATP N Formula
      *
      * Store - Article  -> consolidated stock * rotation/100 - max(sales forecast - sales N, 0)*isOpen + stock to be delivered * nsp/100 * nst/100
      * 11    - 1        ->               100  * 70/100       - max(       10      -    0   , 0)*1      + (200-100 + 100-50)    *  20/100 * 50/100 = 75
      * 11    - 2        ->               100  * 70/100       - max(       10      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 60
      * 11    - 4        ->               100  * 70/100       - max(       10      -    0   , 0)*1      + (200-100 + 100-50)    *  20/100 * 50/100 = 75
      * 11    - 5        ->               100  * 70/100       - max(       10      -    0   , 0)*1      + (200-100 + 100-50)    *  20/100 * 50/100 = 75
      * 11    - 6        ->               100  * 70/100       - max(       10      -    0   , 0)*1      + (100-  0 +  50- 0)    *  20/100 * 50/100 = 75
      * 11    - 7        ->               100  * 70/100       - max(       10      -    0   , 0)*1      + (200-100 + 100-50)    *  20/100 * 50/100 = 75
      * 22    - 1        ->               100  * 70/100       - max(       10      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 60
      * 22    - 2        ->               100  * 70/100       - max(       10      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 60
      * 33    - 1        ->               100  * 70/100       - max(       10      -    0   , 0)*1      + (200-100)             *  20/100 * 50/100 = 70
      * 33    - 3        ->               100  * 70/100       - max(       10      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 60
      *
      * ATP NX Formula
      * Store - Article - Day -> ATPNX-1 - sales forecast + stock to be delivered * nsp/100 * nst/100
      * 11    - 1          1  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 2          1  ->    60   -         10     +      (200-10)         *  20/100 * 50/100 = 69
      * 11    - 4          1  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 5          1  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 6          1  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 7          1  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 22    - 1          1  ->    60   -         10     +           0           *  20/100 * 50/100 = 50
      * 22    - 2          1  ->    60   -         10     +           0           *  20/100 * 50/100 = 50
      * 33    - 1          1  ->    70   -         10     +      (200-100)        *  20/100 * 50/100 = 70
      * 33    - 3          1  ->    60   -         10     +           0           *  20/100 * 50/100 = 50
      *
      * 11    - 1          2  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 2          2  ->    69   -         10     +      (200-10)         *  20/100 * 50/100 = 78
      * 11    - 4          2  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 5          2  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 6          2  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 7          2  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 22    - 1          2  ->    50   -         10     +           0           *  20/100 * 50/100 = 40
      * 22    - 2          2  ->    50   -         10     +           0           *  20/100 * 50/100 = 40
      * 33    - 1          2  ->    70   -         10     +      (200-100)        *  20/100 * 50/100 = 70
      * 33    - 3          2  ->    50   -         10     +           0           *  20/100 * 50/100 = 40
      *
      * 11    - 1          3  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 2          3  ->    78   -         10     +      (200-10)         *  20/100 * 50/100 = 87
      * 11    - 4          3  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 5          3  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 6          3  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 7          3  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 22    - 1          3  ->    40   -         10     +           0           *  20/100 * 50/100 = 30
      * 22    - 2          3  ->    40   -         10     +           0           *  20/100 * 50/100 = 30
      * 33    - 1          3  ->    70   -         10     +      (200-100)        *  20/100 * 50/100 = 70
      * 33    - 3          3  ->    40   -         10     +           0           *  20/100 * 50/100 = 30
      *
      * 11    - 1          4  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 2          4  ->    87   -         10     +      (200-10)         *  20/100 * 50/100 = 96
      * 11    - 4          4  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 5          4  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 6          4  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 7          4  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 22    - 1          4  ->    30   -         10     +           0           *  20/100 * 50/100 = 20
      * 22    - 2          4  ->    30   -         10     +           0           *  20/100 * 50/100 = 20
      * 33    - 1          4  ->    70   -         10     +      (200-20)         *  20/100 * 50/100 = 78
      * 33    - 3          4  ->    30   -         10     +           0           *  20/100 * 50/100 = 20
      *
      * 11    - 1          5  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 2          5  ->    96   -         10     +           0           *  20/100 * 50/100 = 86
      * 11    - 4          5  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 5          5  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 6          5  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 7          5  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 22    - 1          5  ->    20   -         10     +           0           *  20/100 * 50/100 = 10
      * 22    - 2          5  ->    20   -         10     +           0           *  20/100 * 50/100 = 10
      * 33    - 1          5  ->    78   -         10     +      (200-100)        *  20/100 * 50/100 = 78
      * 33    - 3          5  ->    20   -         10     +           0           *  20/100 * 50/100 = 10
      *
      */
    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idStore1, idArticle1,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 75d, 75d, 75d, 75d, 75d, 75d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1, idArticle2,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 60d, 69d, 78d, 87d, 96d, 86d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1, idArticle4,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 75d, 75d, 75d, 75d, 75d, 75d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1, idArticle5,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 75d, 75d, 75d, 75d, 75d, 75d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1, idArticle6,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 75d, 75d, 75d, 75d, 75d, 75d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1, idArticle7,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 75d, 75d, 75d, 75d, 75d, 75d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, idArticle1,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 60d, 50d, 40d, 30d, 20d, 10d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, idArticle2,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 60d, 50d, 40d, 30d, 20d, 10d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore3, idArticle1, freshProductsSector, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 78d, 78d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore3, idArticle3,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 60d, 50d, 40d, 30d, 20d, 10d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null)
    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val readEntities =
      ReadAtpEntities(
        dsSaleMovements,
        dsConsolidatedStock,
        dfFixedData,
        dsOrderHeader,
        dsOrderLine,
        dsPartialOrdersDelivered,
        dsStoreCapacity,
        dsTypeStore,
        dsMaraWithArticleTypes,
        dsMaterials,
        dsMaterialsArticle,
        dsCustomerReservations)

    calculateProcessAndCompareResultWithoutTsUpdateColumns(
      readEntities,
      stockConf,
      todayAtMidnightTs,
      yesterdayAtMidnightTs,
      List.empty[ControlEjecucionView],
      appName,
      Seq(StockATP.ccTags.idArticleTag, StockATP.ccTags.idStoreTag),
      dfExpected)

  }

  it must
    " calculate stock ATP with stock to be delivered of only purchase articles with unique parent sales article"  taggedAs UnitTestTag in new StockATPFor5ProjectionDays {

    val stockConf = StockConf(
      maxNsp = 80d, minNsp = 10d,
      maxNst = 80d, minNst = 20d,
      rotationArtAA = 50d,
      rotationArtA = 60d,
      rotationArtB = 70d,
      rotationArtC = 80d,
      numberOfDaysToProject,
      tsUpdateDlk = currentTimestamp)

    /** Consolidated Stock **/
    val dsConsolidatedStock: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(parentSaleArticle1, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(parentSaleArticle2, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(parentSaleArticle1, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(parentSaleArticle2, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(parentSaleArticle1, idStore3, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(parentSaleArticle3, idStore3, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(        idArticle4, idStore4, measureUnitEA, 100d, currentTimestamp)
    )).toDS

    /** Fixed Data **/
    val dfFixedData: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row( parentSaleArticle1,  idStore1, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( parentSaleArticle2,  idStore1, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( parentSaleArticle1,  idStore2, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( parentSaleArticle2,  idStore2, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( parentSaleArticle1,  idStore3, todayString, freshProductsSector, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( parentSaleArticle3,  idStore3, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row(         idArticle4,  idStore4, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d)
      )), fixedDataStructType)
        .transform(Sql.filterDatosFijos(numberOfDaysToProject, todayAtMidnightTs, yesterdayAtMidnightTs)(spark, appConfig))

    /** Sales movements data : EMPTY **/
    val dsSaleMovements: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS

    /** Orders data : Only for purchase articles with unique parent sale article **/
    val dsMaterials: Dataset[Materials] = sc.parallelize(List.empty[Materials]).toDS
    val dsMaterialsArticle: Dataset[MaterialsArticle] = sc.parallelize(List.empty[MaterialsArticle]).toDS
    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List.empty[CustomerReservations]).toDS

    val dsMaraWithArticleTypes: Dataset[MaraWithArticleTypes] = sc.parallelize(List[MaraWithArticleTypes](
      MaraWithArticleTypes(idArticle1, purchaseArticleType1, purchaseArticleCategory1, purchaseFlag, parentSaleArticle1, currentTimestamp),
      MaraWithArticleTypes(idArticle2, purchaseArticleType1, purchaseArticleCategory1, purchaseFlag, parentSaleArticle2, currentTimestamp),
      MaraWithArticleTypes(idArticle4, purchaseArticleType1, purchaseArticleCategory1, purchaseFlag, null, currentTimestamp)
    )).toDS

    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List[OrderHeader](
      OrderHeader(idOrder1, typeOrderHeaderOMS1, currentTimestamp),
      OrderHeader(idOrder2, typeOrderHeaderOMS2, currentTimestamp),
      OrderHeader(idOrder3,      wrongTypeOrder, currentTimestamp),
      OrderHeader(idOrder4, typeOrderHeaderOMS3, currentTimestamp),
      OrderHeader(idOrder5, typeOrderHeaderOMS4, currentTimestamp)
    )).toDS
      .transform(filterOrderHeader(spark, appConfig))

    val dsOrderLine: Dataset[OrderLineView] = sc.parallelize(List[OrderLine](
      OrderLine(idOrder1, idOrderLine1, idArticle1, idStore1,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder1, idOrderLine2, idArticle2, idStore1,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder2, idOrderLine1, idArticle1, idStore2,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder2, idOrderLine2, idArticle1, idStore2, canceledFlagList.head,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder2, idOrderLine3, idArticle1, idStore2,       notCanceledFlag,  notYetDeliveredFlag,    returnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder2, idOrderLine4, idArticle1, idStore2,       notCanceledFlag, alreadyDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder3, idOrderLine1, idArticle1, idStore2,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder4, idOrderLine1, idArticle1, idStore3,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder5, idOrderLine1, idArticle3, idStore3,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder5, idOrderLine2, idArticle2, idStore3,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder5, idOrderLine3, idArticle4, idStore4,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp)
    )).toDS
      .transform(filterOrderLines(spark, appConfig))

    val dsPartialOrdersDelivered: Dataset[PartialOrdersDeliveredView] = sc.parallelize(List[PartialOrdersDeliveredView](
      // article 111 store 1 : partial orders for every day
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered1, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered2, 100d,  50d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered3, 200d, 100d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered4, 200d, 100d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered5, 200d, 100d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered6, 200d, 100d,                 dayN3, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered7, 200d, 100d,                 dayN4, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered8, 200d, 100d,   lastProjectionDayTs, currentTimestamp),

      // article 222 store 1 : partial orders for days in between
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered1, 190d,  0d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered2, 190d,  0d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered3, 200d, 10d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered4, 200d, 10d,                 dayN3, currentTimestamp),

      // article 111 store 2 : all partial orders are either already delivered, or belong to canceled order lines, or order headers are not of OMS type
      PartialOrdersDeliveredView(idOrder2, idOrderLine1, idNumPartialOrdersDelivered1, 100d, 100d, yesterdayAtMidnightTs, currentTimestamp), // Already delivered
      PartialOrdersDeliveredView(idOrder2, idOrderLine1, idNumPartialOrdersDelivered2, 200d, 400d,     todayAtMidnightTs, currentTimestamp), // Already delivered (more than expected)
      PartialOrdersDeliveredView(idOrder2, idOrderLine2, idNumPartialOrdersDelivered3, 200d,  10d,                 dayN1, currentTimestamp), // Canceled Order Line
      PartialOrdersDeliveredView(idOrder2, idOrderLine3, idNumPartialOrdersDelivered4, 200d,  10d,                 dayN2, currentTimestamp), // Returned To Provider Order Line
      PartialOrdersDeliveredView(idOrder2, idOrderLine4, idNumPartialOrdersDelivered5, 200d,  10d,                 dayN3, currentTimestamp), // Already Delivered Order Line
      PartialOrdersDeliveredView(idOrder3, idOrderLine1, idNumPartialOrdersDelivered1, 200d,  20d,                 dayN3, currentTimestamp), // Order Header not of OMS type

      // article 222 store 2 : no partial orders for this article

      // article 111 store 3 : partial orders for every day, but it's from fresh products sector, so stock to be delivered will be taken from the same day
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered1, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered2, 100d,  50d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered3, 200d, 100d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered4, 200d, 100d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered5, 200d, 100d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered6, 200d, 100d,                 dayN3, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered7, 200d,  20d,                 dayN4, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered1, 200d, 100d,   lastProjectionDayTs, currentTimestamp),

      // article 333 store 3: not in mara, these orders will be ignored
      PartialOrdersDeliveredView(idOrder5, idOrderLine1, idNumPartialOrdersDelivered1, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp),

      // article 111 store 4: not fixed data
      PartialOrdersDeliveredView(idOrder5, idOrderLine2, idNumPartialOrdersDelivered1, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp),

      // article 444 store 4: article with no parent sale article
      PartialOrdersDeliveredView(idOrder5, idOrderLine3, idNumPartialOrdersDelivered1, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp)

    )).toDS

    /** Capacities and store types: EMPTY --> Defaults are false and true **/
    val dsStoreCapacity: Dataset[StoreCapacityView] = sc.parallelize(List.empty[StoreCapacityView]).toDS
    val dsTypeStore: Dataset[StoreTypeView] = sc.parallelize(List.empty[StoreTypeView]).toDS


    /** ATP N Formula
      *
      * Store - Article  -> consolidated stock * rotation/100 - max(sales forecast - sales N, 0)*isOpen + stock to be delivered * nsp/100 * nst/100
      * 11    - 111      ->               100  * 70/100       - max(       10      -    0   , 0)*1      + (200-100 + 100-50)    *  20/100 * 50/100 = 75
      * 11    - 222      ->               100  * 70/100       - max(       10      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 60
      * 22    - 111      ->               100  * 70/100       - max(       10      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 60
      * 22    - 222      ->               100  * 70/100       - max(       10      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 60
      * 33    - 111      ->               100  * 70/100       - max(       10      -    0   , 0)*1      + (200-100)             *  20/100 * 50/100 = 70
      * 33    - 333      ->               100  * 70/100       - max(       10      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 60
      * 44    -   4      ->               100  * 70/100       - max(       10      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 60
      *
      * ATP NX Formula
      * Store - Article - Day -> ATPNX-1 - sales forecast + stock to be delivered * nsp/100 * nst/100
      * 11    - 111        1  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 222        1  ->    60   -         10     +      (190-  0)        *  20/100 * 50/100 = 69
      * 22    - 111        1  ->    60   -         10     +           0           *  20/100 * 50/100 = 50
      * 22    - 222        1  ->    60   -         10     +           0           *  20/100 * 50/100 = 50
      * 33    - 111        1  ->    70   -         10     +      (200-100)        *  20/100 * 50/100 = 70
      * 33    - 333        1  ->    60   -         10     +           0           *  20/100 * 50/100 = 50
      * 44    -   4        1  ->    60   -         10     +           0           *  20/100 * 50/100 = 50
      *
      * 11    - 111        2  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 222        2  ->    69   -         10     +      (190-  0)        *  20/100 * 50/100 = 78
      * 22    - 111        2  ->    50   -         10     +           0           *  20/100 * 50/100 = 40
      * 22    - 222        2  ->    50   -         10     +           0           *  20/100 * 50/100 = 40
      * 33    - 111        2  ->    70   -         10     +      (200-100)        *  20/100 * 50/100 = 70
      * 33    - 333        2  ->    50   -         10     +           0           *  20/100 * 50/100 = 40
      * 44    -   4        2  ->    50   -         10     +           0           *  20/100 * 50/100 = 40
      *
      * 11    - 111        3  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 222        3  ->    78   -         10     +      (200-10)         *  20/100 * 50/100 = 87
      * 22    - 111        3  ->    40   -         10     +           0           *  20/100 * 50/100 = 30
      * 22    - 222        3  ->    40   -         10     +           0           *  20/100 * 50/100 = 30
      * 33    - 111        3  ->    70   -         10     +      (200-100)        *  20/100 * 50/100 = 70
      * 33    - 333        3  ->    40   -         10     +           0           *  20/100 * 50/100 = 30
      * 44    -   4        3  ->    40   -         10     +           0           *  20/100 * 50/100 = 30
      *
      * 11    - 111        4  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 222        4  ->    87   -         10     +      (200-10)         *  20/100 * 50/100 = 96
      * 22    - 111        4  ->    30   -         10     +           0           *  20/100 * 50/100 = 20
      * 22    - 222        4  ->    30   -         10     +           0           *  20/100 * 50/100 = 20
      * 33    - 111        4  ->    70   -         10     +      (200-20)         *  20/100 * 50/100 = 78
      * 33    - 333        4  ->    30   -         10     +           0           *  20/100 * 50/100 = 20
      * 44    -   4        4  ->    30   -         10     +           0           *  20/100 * 50/100 = 20
      *
      * 11    - 111        5  ->    75   -         10     +      (200-100)        *  20/100 * 50/100 = 75
      * 11    - 222        5  ->    96   -         10     +           0           *  20/100 * 50/100 = 86
      * 22    - 111        5  ->    20   -         10     +           0           *  20/100 * 50/100 = 10
      * 22    - 222        5  ->    20   -         10     +           0           *  20/100 * 50/100 = 10
      * 33    - 111        5  ->    78   -         10     +      (200-100)        *  20/100 * 50/100 = 78
      * 33    - 333        5  ->    20   -         10     +           0           *  20/100 * 50/100 = 10
      * 44    -   4        5  ->    20   -         10     +           0           *  20/100 * 50/100 = 10
      *
      */
    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idStore1, parentSaleArticle1,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 75d, 75d, 75d, 75d, 75d, 75d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1, parentSaleArticle2,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 60d, 69d, 78d, 87d, 96d, 86d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, parentSaleArticle1,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 60d, 50d, 40d, 30d, 20d, 10d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, parentSaleArticle2,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 60d, 50d, 40d, 30d, 20d, 10d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore3, parentSaleArticle1, freshProductsSector, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 78d, 78d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore3, parentSaleArticle3,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 60d, 50d, 40d, 30d, 20d, 10d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore4,         idArticle4,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 60d, 50d, 40d, 30d, 20d, 10d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null)
    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val readEntities =
      ReadAtpEntities(
        dsSaleMovements,
        dsConsolidatedStock,
        dfFixedData,
        dsOrderHeader,
        dsOrderLine,
        dsPartialOrdersDelivered,
        dsStoreCapacity,
        dsTypeStore,
        dsMaraWithArticleTypes,
        dsMaterials,
        dsMaterialsArticle,
        dsCustomerReservations)

    calculateProcessAndCompareResultWithoutTsUpdateColumns(
      readEntities,
      stockConf,
      todayAtMidnightTs,
      yesterdayAtMidnightTs,
      List.empty[ControlEjecucionView],
      appName,
      Seq(StockATP.ccTags.idArticleTag, StockATP.ccTags.idStoreTag),
      dfExpected)

  }


  it must
    " calculate stock ATP with stock to be delivered of box and prepack articles"  taggedAs UnitTestTag in new StockATPFor5ProjectionDays {

    val stockConf = StockConf(
      maxNsp = 80d, minNsp = 10d,
      maxNst = 80d, minNst = 20d,
      rotationArtAA = 50d,
      rotationArtA = 60d,
      rotationArtB = 70d,
      rotationArtC = 80d,
      numberOfDaysToProject,
      tsUpdateDlk = currentTimestamp)

    /** Consolidated Stock **/
    val dsConsolidatedStock: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle2, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle2, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore3, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle3, idStore3, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle4, idStore4, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle6, idStore4, measureUnitEA, 100d, currentTimestamp)
    )).toDS

    /** Fixed Data **/
    val dfFixedData: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row( idArticle1,  idStore1, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle2,  idStore1, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle1,  idStore2, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle2,  idStore2, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle1,  idStore3, todayString, freshProductsSector, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle3,  idStore3, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle4,  idStore4, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle6,  idStore4, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d)
      )), fixedDataStructType)
        .transform(Sql.filterDatosFijos(numberOfDaysToProject, todayAtMidnightTs, yesterdayAtMidnightTs)(spark, appConfig))

    /** Sales movements data : EMPTY **/
    val dsSaleMovements: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS

    /** Orders data : Only for box and prepack articles **/
    val dsMaterials: Dataset[Materials] = sc.parallelize(List[Materials](
      Materials(idListMaterial1,              idArticle1, 10.0d, currentTimestamp),
      Materials(idListMaterial2,              idArticle1,  5.0d, currentTimestamp),
      Materials(idListMaterial2,              idArticle2,  2.0d, currentTimestamp),
      Materials(idListMaterial4, "notInFixedDataArticle",  2.0d, currentTimestamp),
      Materials(idListMaterial5,              idArticle1, 10.0d, currentTimestamp),
      Materials(idListMaterial7,              idArticle6, 10.0d, currentTimestamp)
    )).toDS

    val dsMaterialsArticle: Dataset[MaterialsArticle] = sc.parallelize(List[MaterialsArticle](
      MaterialsArticle(idBoxPrepack1, idListMaterial1, currentTimestamp),
      MaterialsArticle(idBoxPrepack2, idListMaterial2, currentTimestamp),
      MaterialsArticle(idBoxPrepack3, idListMaterial3, currentTimestamp),
      MaterialsArticle(idBoxPrepack4, idListMaterial4, currentTimestamp),
      MaterialsArticle(idBoxPrepack5, idListMaterial5, currentTimestamp),
      MaterialsArticle(idBoxPrepack7, idListMaterial7, currentTimestamp)
    )).toDS
    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List.empty[CustomerReservations]).toDS

    val dsMaraWithArticleTypes: Dataset[MaraWithArticleTypes] = sc.parallelize(List[MaraWithArticleTypes](
      MaraWithArticleTypes(idBoxPrepack1, boxPrepackArticleType1, boxPrepackArticleCategory1, purchaseFlag,                     null, currentTimestamp),
      MaraWithArticleTypes(idBoxPrepack2, boxPrepackArticleType2, boxPrepackArticleCategory2, purchaseFlag, "parentSaleArticleError", currentTimestamp),
      MaraWithArticleTypes(idBoxPrepack3, boxPrepackArticleType2, boxPrepackArticleCategory2, purchaseFlag,                     null, currentTimestamp),
      MaraWithArticleTypes(idBoxPrepack4, boxPrepackArticleType2, boxPrepackArticleCategory2, purchaseFlag,                     null, currentTimestamp),
      MaraWithArticleTypes(idBoxPrepack5, boxPrepackArticleType2, boxPrepackArticleCategory2, purchaseFlag,                     null, currentTimestamp),
      MaraWithArticleTypes(idBoxPrepack6, boxPrepackArticleType2, boxPrepackArticleCategory2, purchaseFlag,                     null, currentTimestamp),
      MaraWithArticleTypes(idBoxPrepack7,            "wrongFlag", boxPrepackArticleCategory2, purchaseFlag,                     null, currentTimestamp),
      MaraWithArticleTypes(idBoxPrepack8, boxPrepackArticleType2, boxPrepackArticleCategory1, purchaseFlag,                     null, currentTimestamp),
      MaraWithArticleTypes(idBoxPrepack9, boxPrepackArticleType1, boxPrepackArticleCategory2, purchaseFlag,                     null, currentTimestamp)
    )).toDS

    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List[OrderHeader](
      OrderHeader(idOrder1, typeOrderHeaderOMS1, currentTimestamp),
      OrderHeader(idOrder2, typeOrderHeaderOMS2, currentTimestamp),
      OrderHeader(idOrder3,      wrongTypeOrder, currentTimestamp),
      OrderHeader(idOrder4, typeOrderHeaderOMS3, currentTimestamp),
      OrderHeader(idOrder5, typeOrderHeaderOMS4, currentTimestamp)
    )).toDS
      .transform(filterOrderHeader(spark, appConfig))

    val dsOrderLine: Dataset[OrderLineView] = sc.parallelize(List[OrderLine](
      OrderLine(idOrder1, idOrderLine1,  idBoxPrepack1, idStore1,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder1, idOrderLine2,  idBoxPrepack2, idStore1,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder1, idOrderLine3,  idBoxPrepack5, idStore1,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder2, idOrderLine1,  idBoxPrepack5, idStore2,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder2, idOrderLine2,  idBoxPrepack5, idStore2, canceledFlagList.head,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder2, idOrderLine3,  idBoxPrepack5, idStore2,       notCanceledFlag,  notYetDeliveredFlag,    returnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder2, idOrderLine4,  idBoxPrepack5, idStore2,       notCanceledFlag, alreadyDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder3, idOrderLine1,  idBoxPrepack5, idStore2,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder4, idOrderLine1,  idBoxPrepack2, idStore3,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder5, idOrderLine1,  idBoxPrepack3, idStore3,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder5, idOrderLine2,  idBoxPrepack4, idStore3,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder5, idOrderLine3,  idBoxPrepack3, idStore4,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder5, idOrderLine4,  idBoxPrepack6, idStore4,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder5, idOrderLine4,  idBoxPrepack7, idStore4,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder5, idOrderLine5,  idBoxPrepack8, idStore4,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder5, idOrderLine6,  idBoxPrepack9, idStore4,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp)
      )).toDS
      .transform(filterOrderLines(spark, appConfig))

    val dsPartialOrdersDelivered: Dataset[PartialOrdersDeliveredView] = sc.parallelize(List[PartialOrdersDeliveredView](
      // 10x article 1 store 1 : partial orders for every day
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered1, 100d,   0d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine3, idNumPartialOrdersDelivered2, 100d,  50d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered3, 200d, 100d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered4, 200d, 100d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered5, 200d, 100d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered6, 200d, 100d,                 dayN3, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered7, 200d, 100d,                 dayN4, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered8, 200d, 100d,   lastProjectionDayTs, currentTimestamp),

      // 5x article 1 store 1 : partial orders for days in between
      // 2x article 2 store 1 : partial orders for days in between
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered1, 200d, 10d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered2, 200d, 10d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered3, 200d, 10d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered4, 200d, 10d,                 dayN3, currentTimestamp),

      // 10x article 1 store 2 : all partial orders are either already delivered, or belong to canceled order lines, or order headers are not of OMS type or are not correct box or prepack
      PartialOrdersDeliveredView(idOrder2, idOrderLine1, idNumPartialOrdersDelivered1, 100d, 100d, yesterdayAtMidnightTs, currentTimestamp), // Already delivered
      PartialOrdersDeliveredView(idOrder2, idOrderLine1, idNumPartialOrdersDelivered2, 200d, 400d,     todayAtMidnightTs, currentTimestamp), // Already delivered (more than expected)
      PartialOrdersDeliveredView(idOrder2, idOrderLine2, idNumPartialOrdersDelivered3, 200d,  10d,                 dayN1, currentTimestamp), // Canceled Order Line
      PartialOrdersDeliveredView(idOrder2, idOrderLine3, idNumPartialOrdersDelivered4, 200d,  10d,                 dayN2, currentTimestamp), // Returned To Provider Order Line
      PartialOrdersDeliveredView(idOrder2, idOrderLine4, idNumPartialOrdersDelivered5, 200d,  10d,                 dayN3, currentTimestamp), // Already Delivered Order Line
      PartialOrdersDeliveredView(idOrder3, idOrderLine1, idNumPartialOrdersDelivered1, 200d,  20d,                 dayN3, currentTimestamp), // Order Header not of OMS type
      PartialOrdersDeliveredView(idOrder5, idOrderLine5, idNumPartialOrdersDelivered1, 200d, 100d,   lastProjectionDayTs, currentTimestamp), // Not correct box or prepack
      PartialOrdersDeliveredView(idOrder5, idOrderLine6, idNumPartialOrdersDelivered1, 200d, 100d,   lastProjectionDayTs, currentTimestamp),  // Not correct box or prepack

      // article 2 store 2 : no partial orders for this article

      // 5x article 1 store 3 : partial orders for every day, but it's from fresh products sector, so stock to be delivered will be taken from the same day
      // 2x article 2 store 3 : not in mara, this will be ignored
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered1, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered2, 100d,  50d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered3, 200d, 100d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered4, 200d, 100d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered5, 200d, 100d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered6, 200d, 100d,                 dayN3, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered7, 200d,  20d,                 dayN4, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered1, 200d, 100d,   lastProjectionDayTs, currentTimestamp),

      // article 1 store 4: not fixed data
      PartialOrdersDeliveredView(idOrder5, idOrderLine2, idNumPartialOrdersDelivered1, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp),

      // idBoxPrepack3 store 4: article with no materials list is ignored
      PartialOrdersDeliveredView(idOrder5, idOrderLine3, idNumPartialOrdersDelivered1, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp),

      // idBoxPrepack6 store 4: article not in materials article
      PartialOrdersDeliveredView(idOrder5, idOrderLine4, idNumPartialOrdersDelivered1, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp),

      // article 6 store 4: article not correct box and prepack flags
      PartialOrdersDeliveredView(idOrder5, idOrderLine5, idNumPartialOrdersDelivered1, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp)

    )).toDS

    /** Capacities and store types: EMPTY --> Defaults are false and true **/
    val dsStoreCapacity: Dataset[StoreCapacityView] = sc.parallelize(List.empty[StoreCapacityView]).toDS
    val dsTypeStore: Dataset[StoreTypeView] = sc.parallelize(List.empty[StoreTypeView]).toDS


    /** ATP N Formula
      *
      * Store - Article  -> consolidated stock * rotation/100 - max(sales forecast - sales N, 0)*isOpen + stock to be delivered          * nsp/100 * nst/100
      * 11    - 1      ->               100  * 70/100       - max(       10      -    0   , 0)*1        + [(100-  0)*10 + (200-100)*10)] *  20/100 * 50/100 = 210
      * 11    - 2      ->               100  * 70/100       - max(       10      -    0   , 0)*1        +           0                    *  20/100 * 50/100 =  60
      * 22    - 1      ->               100  * 70/100       - max(       10      -    0   , 0)*1        +           0                    *  20/100 * 50/100 =  60
      * 22    - 2      ->               100  * 70/100       - max(       10      -    0   , 0)*1        +           0                    *  20/100 * 50/100 =  60
      * 33    - 1      ->               100  * 70/100       - max(       10      -    0   , 0)*1        + (200-100)*5                    *  20/100 * 50/100 = 110
      * 33    - 3      ->               100  * 70/100       - max(       10      -    0   , 0)*1        +           0                    *  20/100 * 50/100 =  60
      * 44    - 4      ->               100  * 70/100       - max(       10      -    0   , 0)*1        +           0                    *  20/100 * 50/100 =  60
      * 44    - 6      ->               100  * 70/100       - max(       10      -    0   , 0)*1        +           0                    *  20/100 * 50/100 =  60
      *
      * ATP NX Formula
      * Store - Article - Day -> ATPNX-1 - sales forecast + stock to be delivered        * nsp/100 * nst/100
      * 11    - 1        1    ->   210   -         10     + [(200-100)*10 + (200-10)*5]  *  20/100 * 50/100 = 395
      * 11    - 2        1    ->    60   -         10     +      (200-10)*2              *  20/100 * 50/100 =  88
      * 22    - 1        1    ->    60   -         10     +           0                  *  20/100 * 50/100 =  50
      * 22    - 2        1    ->    60   -         10     +           0                  *  20/100 * 50/100 =  50
      * 33    - 1        1    ->   110   -         10     +      (200-100)*5             *  20/100 * 50/100 = 150
      * 33    - 3        1    ->    60   -         10     +           0                  *  20/100 * 50/100 =  50
      * 44    - 4        1    ->    60   -         10     +           0                  *  20/100 * 50/100 =  50
      * 44    - 6        1    ->    60   -         10     +           0                  *  20/100 * 50/100 =  50
      *
      * 11    - 1        2    ->   395   -         10     + (200-100)*10 + (200-10)*5    *  20/100 * 50/100 = 580
      * 11    - 2        2    ->    88   -         10     +      (200-10)*2              *  20/100 * 50/100 = 116
      * 22    - 1        2    ->    50   -         10     +           0                  *  20/100 * 50/100 =  40
      * 22    - 2        2    ->    50   -         10     +           0                  *  20/100 * 50/100 =  40
      * 33    - 1        2    ->   150   -         10     +      (200-100)*5             *  20/100 * 50/100 = 190
      * 33    - 3        2    ->    50   -         10     +           0                  *  20/100 * 50/100 =  40
      * 44    - 4        2    ->    50   -         10     +           0                  *  20/100 * 50/100 =  40
      * 44    - 6        2    ->    50   -         10     +           0                  *  20/100 * 50/100 =  40
      *
      * 11    - 1        3    ->   580   -        10     + (200-100)*10 + (200-10)*5    *  20/100 * 50/100 = 765
      * 11    - 2        3    ->   116   -        10     +      (200-10)*2              *  20/100 * 50/100 = 144
      * 22    - 1        3    ->    40   -        10     +           0                  *  20/100 * 50/100 =  30
      * 22    - 2        3    ->    40   -        10     +           0                  *  20/100 * 50/100 =  30
      * 33    - 1        3    ->   190   -        10     +      (200-100)*5             *  20/100 * 50/100 = 230
      * 33    - 3        3    ->    40   -        10     +           0                  *  20/100 * 50/100 =  30
      * 44    - 4        3    ->    40   -        10     +           0                  *  20/100 * 50/100 =  30
      * 44    - 6        3    ->    40   -        10     +           0                  *  20/100 * 50/100 =  30
      *
      * 11    - 1        4    ->   765   -        10     + (200-100)*10 + (200-10)*5    *  20/100 * 50/100 = 950
      * 11    - 2        4    ->   144   -        10     +      (200-10)*2              *  20/100 * 50/100 = 172
      * 22    - 1        4    ->    30   -        10     +           0                  *  20/100 * 50/100 =  20
      * 22    - 2        4    ->    30   -        10     +           0                  *  20/100 * 50/100 =  20
      * 33    - 1        4    ->   230   -        10     +      (200-20)*5              *  20/100 * 50/100 = 310
      * 33    - 3        4    ->    30   -        10     +           0                  *  20/100 * 50/100 =  20
      * 44    - 4        4    ->    30   -        10     +           0                  *  20/100 * 50/100 =  20
      * 44    - 6        4    ->    30   -        10     +           0                  *  20/100 * 50/100 =  20
      *
      * 11    - 1        5    ->   950   -        10     +      (200-100)*10            *  20/100 * 50/100 = 1040
      * 11    - 2        5    ->   172   -        10     +           0                  *  20/100 * 50/100 =  162
      * 22    - 1        5    ->    20   -        10     +           0                  *  20/100 * 50/100 =   10
      * 22    - 2        5    ->    20   -        10     +           0                  *  20/100 * 50/100 =   10
      * 33    - 1        5    ->   310   -        10     +      (200-100)*5             *  20/100 * 50/100 =  350
      * 33    - 3        5    ->    20   -        10     +           0                  *  20/100 * 50/100 =   10
      * 44    - 4        5    ->    20   -        10     +           0                  *  20/100 * 50/100 =   10
      * 44    - 6        5    ->    20   -        10     +           0                  *  20/100 * 50/100 =   10
      *
      */
    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idStore1, idArticle1,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 210d, 395d, 580d, 765d, 950d, 1040d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1, idArticle2,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  60d,  88d, 116d, 144d, 172d,  162d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, idArticle1,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  60d,  50d,  40d,  30d,  20d,   10d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, idArticle2,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  60d,  50d,  40d,  30d,  20d,   10d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore3, idArticle1, freshProductsSector, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 110d, 150d, 190d, 230d, 310d,  350d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore3, idArticle3,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  60d,  50d,  40d,  30d,  20d,   10d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore4, idArticle4,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  60d,  50d,  40d,  30d,  20d,   10d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore4, idArticle6,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  60d,  50d,  40d,  30d,  20d,   10d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null)
    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val readEntities =
      ReadAtpEntities(
        dsSaleMovements,
        dsConsolidatedStock,
        dfFixedData,
        dsOrderHeader,
        dsOrderLine,
        dsPartialOrdersDelivered,
        dsStoreCapacity,
        dsTypeStore,
        dsMaraWithArticleTypes,
        dsMaterials,
        dsMaterialsArticle,
        dsCustomerReservations)

    calculateProcessAndCompareResultWithoutTsUpdateColumns(
      readEntities,
      stockConf,
      todayAtMidnightTs,
      yesterdayAtMidnightTs,
      List.empty[ControlEjecucionView],
      appName,
      Seq(StockATP.ccTags.idArticleTag, StockATP.ccTags.idStoreTag),
      dfExpected)

  }

  it must
    " calculate stock ATP with stock to be delivered of all type of articles"  taggedAs UnitTestTag in new StockATPFor5ProjectionDays {

    val stockConf = StockConf(
      maxNsp = 80d, minNsp = 10d,
      maxNst = 80d, minNst = 20d,
      rotationArtAA = 50d,
      rotationArtA = 60d,
      rotationArtB = 70d,
      rotationArtC = 80d,
      numberOfDaysToProject,
      tsUpdateDlk = currentTimestamp)

    /** Consolidated Stock **/
    val dsConsolidatedStock: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle2, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle2, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore3, measureUnitEA, 100d, currentTimestamp)
    )).toDS

    /** Fixed Data **/
    val dfFixedData: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row( idArticle1,  idStore1, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle2,  idStore1, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle1,  idStore2, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle2,  idStore2, todayString,            sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle1,  idStore3, todayString, freshProductsSector, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d)
      )), fixedDataStructType)
        .transform(Sql.filterDatosFijos(numberOfDaysToProject, todayAtMidnightTs, yesterdayAtMidnightTs)(spark, appConfig))

    /** Sales movements data : EMPTY **/
    val dsSaleMovements: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS

    /** Orders data : Only for box and prepack articles **/
    val dsMaterials: Dataset[Materials] = sc.parallelize(List[Materials](
      Materials(idListMaterial1, idArticle1, 10.0d, currentTimestamp),
      Materials(idListMaterial2, idArticle1,  5.0d, currentTimestamp),
      Materials(idListMaterial2, idArticle2,  2.0d, currentTimestamp),
      Materials(idListMaterial3, idArticle1, 10.0d, currentTimestamp)
    )).toDS

    val dsMaterialsArticle: Dataset[MaterialsArticle] = sc.parallelize(List[MaterialsArticle](
      MaterialsArticle(idBoxPrepack1, idListMaterial1, currentTimestamp),
      MaterialsArticle(idBoxPrepack2, idListMaterial2, currentTimestamp),
      MaterialsArticle(idBoxPrepack3, idListMaterial3, currentTimestamp)
    )).toDS
    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List.empty[CustomerReservations]).toDS

    val dsMaraWithArticleTypes: Dataset[MaraWithArticleTypes] = sc.parallelize(List[MaraWithArticleTypes](
      MaraWithArticleTypes(                 idBoxPrepack1, boxPrepackArticleType1, boxPrepackArticleCategory1, purchaseFlag,                     null, currentTimestamp),
      MaraWithArticleTypes(                 idBoxPrepack2, boxPrepackArticleType2, boxPrepackArticleCategory2, purchaseFlag, "parentSaleArticleError", currentTimestamp),
      MaraWithArticleTypes(                 idBoxPrepack3, boxPrepackArticleType2, boxPrepackArticleCategory2, purchaseFlag,                     null, currentTimestamp),
      MaraWithArticleTypes(purchaseWithUniqueSaleArticle1,   purchaseArticleType1,   purchaseArticleCategory1, purchaseFlag,               idArticle1, currentTimestamp),
      MaraWithArticleTypes(purchaseWithUniqueSaleArticle2,   purchaseArticleType1,   purchaseArticleCategory1, purchaseFlag,               idArticle2, currentTimestamp),
      MaraWithArticleTypes(                    idArticle1, noPurchaseArticleType1, noPurchaseArticleCategory1,     saleFlag,                     null, currentTimestamp),
      MaraWithArticleTypes(                    idArticle2, noPurchaseArticleType1, noPurchaseArticleCategory1,     saleFlag,                     null, currentTimestamp)

    )).toDS

    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List[OrderHeader](
      OrderHeader(idOrder1, typeOrderHeaderOMS1, currentTimestamp),
      OrderHeader(idOrder2, typeOrderHeaderOMS2, currentTimestamp),
      OrderHeader(idOrder3,      wrongTypeOrder, currentTimestamp),
      OrderHeader(idOrder4, typeOrderHeaderOMS3, currentTimestamp),
      OrderHeader(idOrder5, typeOrderHeaderOMS4, currentTimestamp),
      OrderHeader(idOrder6, typeOrderHeaderOMS4, currentTimestamp)
    )).toDS
      .transform(filterOrderHeader(spark, appConfig))

    val dsOrderLine: Dataset[OrderLineView] = sc.parallelize(List[OrderLine](

      // Box And Prepack
      OrderLine(idOrder1, idOrderLine1,                  idBoxPrepack1, idStore1,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder1, idOrderLine2,                  idBoxPrepack2, idStore1,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder2, idOrderLine1,                  idBoxPrepack3, idStore2,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder2, idOrderLine2,                  idBoxPrepack3, idStore2, canceledFlagList.head,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder2, idOrderLine3,                  idBoxPrepack3, idStore2,       notCanceledFlag,  notYetDeliveredFlag,    returnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder2, idOrderLine4,                  idBoxPrepack3, idStore2,       notCanceledFlag, alreadyDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder3, idOrderLine1,                  idBoxPrepack3, idStore2,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder4, idOrderLine1,                  idBoxPrepack2, idStore3,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),

      // Purchase articles with unique parent sale article
      OrderLine(idOrder5, idOrderLine1, purchaseWithUniqueSaleArticle1, idStore1,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder5, idOrderLine2, purchaseWithUniqueSaleArticle2, idStore1,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder5, idOrderLine3, purchaseWithUniqueSaleArticle1, idStore3,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),

      // Sale articles
      OrderLine(idOrder6, idOrderLine1,                     idArticle1, idStore1,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder6, idOrderLine2,                     idArticle2, idStore1,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder6, idOrderLine3,                     idArticle1, idStore3,       notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp)

    )).toDS
      .transform(filterOrderLines(spark, appConfig))

    val dsPartialOrdersDelivered: Dataset[PartialOrdersDeliveredView] = sc.parallelize(List[PartialOrdersDeliveredView](
      // 10x article 1 store 1 : BOX AND PREPACK
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered1, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered2, 100d,  50d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered3, 200d, 100d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered4, 200d, 100d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered5, 200d, 100d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered6, 200d, 100d,                 dayN3, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered7, 200d, 100d,                 dayN4, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered8, 200d, 100d,   lastProjectionDayTs, currentTimestamp),

      // article 1 store 1 : ARTICLES WITH UNIQUE PARENT SALE ARTICLE
      PartialOrdersDeliveredView(idOrder5, idOrderLine1, idNumPartialOrdersDelivered3,  10d,   0d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder5, idOrderLine1, idNumPartialOrdersDelivered1, 200d, 100d,                 dayN3, currentTimestamp),
      PartialOrdersDeliveredView(idOrder5, idOrderLine1, idNumPartialOrdersDelivered2, 200d, 100d,                 dayN4, currentTimestamp),

      // article 1 store 1 : SALE ARTICLES
      PartialOrdersDeliveredView(idOrder6, idOrderLine1, idNumPartialOrdersDelivered3,  10d,   0d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine1, idNumPartialOrdersDelivered1, 200d, 100d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine1, idNumPartialOrdersDelivered2, 200d, 100d,                 dayN4, currentTimestamp),

      // 5x article 1 store 1 : BOX AND PREPACK
      // 2x article 2 store 1 : BOX AND PREPACK
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered1, 200d, 10d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered2, 200d, 10d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered3, 200d, 10d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered4, 200d, 10d,                 dayN3, currentTimestamp),

      // article 2 store 1 : ARTICLES WITH UNIQUE PARENT SALE ARTICLE
      PartialOrdersDeliveredView(idOrder5, idOrderLine2, idNumPartialOrdersDelivered1, 200d, 100d,    todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder5, idOrderLine2, idNumPartialOrdersDelivered2, 200d, 100d,                dayN1, currentTimestamp),

      // article 2 store 1 : SALE ARTICLES
      PartialOrdersDeliveredView(idOrder6, idOrderLine2, idNumPartialOrdersDelivered1, 200d, 100d,                dayN3, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine2, idNumPartialOrdersDelivered2, 200d, 100d,                dayN4, currentTimestamp),

      // 10x article 1 store 2 : all partial orders are either already delivered, or belong to canceled order lines, or order headers are not of OMS type
      PartialOrdersDeliveredView(idOrder2, idOrderLine1, idNumPartialOrdersDelivered1, 100d, 100d, yesterdayAtMidnightTs, currentTimestamp), // Already delivered
      PartialOrdersDeliveredView(idOrder2, idOrderLine1, idNumPartialOrdersDelivered2, 200d, 400d,     todayAtMidnightTs, currentTimestamp), // Already delivered (more than expected)
      PartialOrdersDeliveredView(idOrder2, idOrderLine2, idNumPartialOrdersDelivered3, 200d,  10d,                 dayN1, currentTimestamp), // Canceled Order Line
      PartialOrdersDeliveredView(idOrder2, idOrderLine3, idNumPartialOrdersDelivered4, 200d,  10d,                 dayN2, currentTimestamp), // Returned To Provider Order Line
      PartialOrdersDeliveredView(idOrder2, idOrderLine4, idNumPartialOrdersDelivered5, 200d,  10d,                 dayN3, currentTimestamp), // Already Delivered Order Line
      PartialOrdersDeliveredView(idOrder3, idOrderLine1, idNumPartialOrdersDelivered1, 200d,  20d,                 dayN3, currentTimestamp), // Order Header not of OMS type

      // article 2 store 2 : no partial orders for this article

      // 5x article 1 store 3 : BOX AND PREPACK, but from fresh products sector, so stock to be delivered will be taken from the same day
      // 2x article 2 store 3 : not in mara, this will be ignored
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered1, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered2, 100d,  50d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered3, 200d, 100d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered4, 200d, 100d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered5, 200d, 100d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered6, 200d, 100d,                 dayN3, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered7, 200d,  20d,                 dayN4, currentTimestamp),
      PartialOrdersDeliveredView(idOrder4, idOrderLine1, idNumPartialOrdersDelivered1, 200d, 100d,   lastProjectionDayTs, currentTimestamp),

      // article 1 store 3 : ARTICLES WITH UNIQUE PARENT SALE ARTICLE
      PartialOrdersDeliveredView(idOrder5, idOrderLine3, idNumPartialOrdersDelivered1, 200d, 100d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder5, idOrderLine3, idNumPartialOrdersDelivered2, 200d, 100d,                 dayN4, currentTimestamp),

      // article 1 store 3 : SALE ARTICLES
      PartialOrdersDeliveredView(idOrder6, idOrderLine3, idNumPartialOrdersDelivered1, 200d, 100d,                 dayN3, currentTimestamp),
      PartialOrdersDeliveredView(idOrder6, idOrderLine3, idNumPartialOrdersDelivered2, 200d, 100d,                 dayN4, currentTimestamp)

    )).toDS

    /** Capacities and store types: EMPTY --> Defaults are false and true **/
    val dsStoreCapacity: Dataset[StoreCapacityView] = sc.parallelize(List.empty[StoreCapacityView]).toDS
    val dsTypeStore: Dataset[StoreTypeView] = sc.parallelize(List.empty[StoreTypeView]).toDS


    /** ATP N Formula
      *
      * Store - Article  -> consolidated stock * rotation/100 - max(sales forecast - sales N, 0)*isOpen + stock to be delivered                     * nsp/100 * nst/100
      * 11    - 1      ->               100  * 70/100       - max(       10      -    0   , 0)*1        + [(200-100 + 100-50)*10 + (10-0) + (10-0)] *  20/100 * 50/100 = 212
      * 11    - 2      ->               100  * 70/100       - max(       10      -    0   , 0)*1        +           0                               *  20/100 * 50/100 =  60
      * 22    - 1      ->               100  * 70/100       - max(       10      -    0   , 0)*1        +           0                               *  20/100 * 50/100 =  60
      * 22    - 2      ->               100  * 70/100       - max(       10      -    0   , 0)*1        +           0                               *  20/100 * 50/100 =  60
      * 33    - 1      ->               100  * 70/100       - max(       10      -    0   , 0)*1        + (200-100)*5                               *  20/100 * 50/100 = 110
      *
      * ATP NX Formula
      * Store - Article - Day -> ATPNX-1 - sales forecast + stock to be delivered                   * nsp/100 * nst/100
      * 11    - 1        1    ->   212   -         10     + [(200-100)*10 + (200-10)*5]             *  20/100 * 50/100 = 397
      * 11    - 2        1    ->    60   -         10     + [(200- 10)*2  + (200-100)]              *  20/100 * 50/100 =  98
      * 22    - 1        1    ->    60   -         10     +           0                             *  20/100 * 50/100 =  50
      * 22    - 2        1    ->    60   -         10     +           0                             *  20/100 * 50/100 =  50
      * 33    - 1        1    ->   110   -         10     +      (200-100)*5                        *  20/100 * 50/100 = 150
      *
      * 11    - 1        2    ->   397   -         10     + [(200-100)*10 + (200-10)*5 + (200-100)]  *  20/100 * 50/100 = 592
      * 11    - 2        2    ->    98   -         10     + [(200-10)*2  + (200-100)]                *  20/100 * 50/100 = 136
      * 22    - 1        2    ->    50   -         10     +           0                              *  20/100 * 50/100 =  40
      * 22    - 2        2    ->    50   -         10     +           0                              *  20/100 * 50/100 =  40
      * 33    - 1        2    ->   150   -         10     +      (200-100)*5                         *  20/100 * 50/100 = 190
      *
      * 11    - 1        3    ->   592   -        10     + [(200-100)*10 + (200-10)*5]               *  20/100 * 50/100 = 777
      * 11    - 2        3    ->   136   -        10     +      (200-10)*2                           *  20/100 * 50/100 = 164
      * 22    - 1        3    ->    40   -        10     +           0                               *  20/100 * 50/100 =  30
      * 22    - 2        3    ->    40   -        10     +           0                               *  20/100 * 50/100 =  30
      * 33    - 1        3    ->   190   -        10     + [(200-100)*5 + (200-100)]                 *  20/100 * 50/100 = 240
      *
      * 11    - 1        4    ->   777   -        10     + [(200-100)*10 + (200-10)*5 + (200-100)]   *  20/100 * 50/100 = 972
      * 11    - 2        4    ->   164   -        10     + [(200-10)*2 + (200 -100)]                 *  20/100 * 50/100 = 202
      * 22    - 1        4    ->    30   -        10     +           0                               *  20/100 * 50/100 =  20
      * 22    - 2        4    ->    30   -        10     +           0                               *  20/100 * 50/100 =  20
      * 33    - 1        4    ->   240   -        10     + [(200-20)*5  + (200-100) + (200-100)]     *  20/100 * 50/100 = 340
      *
      * 11    - 1        5    ->   972   -        10     + [(200-100)*10 + (200-100) + (200-100)]    *  20/100 * 50/100 = 1082
      * 11    - 2        5    ->   202   -        10     +      (200-100)                            *  20/100 * 50/100 =  202
      * 22    - 1        5    ->    20   -        10     +           0                               *  20/100 * 50/100 =   10
      * 22    - 2        5    ->    20   -        10     +           0                               *  20/100 * 50/100 =   10
      * 33    - 1        5    ->   340   -        10     +      (200-100)*5                          *  20/100 * 50/100 =  380
      *
      */
    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idStore1, idArticle1,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 212d, 397d, 592d, 777d, 972d, 1082d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1, idArticle2,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  60d,  98d, 136d, 164d, 202d,  202d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, idArticle1,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  60d,  50d,  40d,  30d,  20d,   10d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, idArticle2,            sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs,  60d,  50d,  40d,  30d,  20d,   10d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore3, idArticle1, freshProductsSector, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 110d, 150d, 190d, 240d, 340d,  380d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null)
    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val readEntities =
      ReadAtpEntities(
        dsSaleMovements,
        dsConsolidatedStock,
        dfFixedData,
        dsOrderHeader,
        dsOrderLine,
        dsPartialOrdersDelivered,
        dsStoreCapacity,
        dsTypeStore,
        dsMaraWithArticleTypes,
        dsMaterials,
        dsMaterialsArticle,
        dsCustomerReservations)

    calculateProcessAndCompareResultWithoutTsUpdateColumns(
      readEntities,
      stockConf,
      todayAtMidnightTs,
      yesterdayAtMidnightTs,
      List.empty[ControlEjecucionView],
      appName,
      Seq(StockATP.ccTags.idArticleTag, StockATP.ccTags.idStoreTag),
      dfExpected)

  }

  it must
    " calculate stock ATP with stock to be delivered when nst and nsp are out of range"  taggedAs UnitTestTag in new StockATPFor5ProjectionDays {

    val stockConf = StockConf(
      maxNsp = 80d, minNsp = 10d,
      maxNst = 80d, minNst = 20d,
      rotationArtAA = 50d,
      rotationArtA = 60d,
      rotationArtB = 70d,
      rotationArtC = 80d,
      numberOfDaysToProject,
      tsUpdateDlk = currentTimestamp)

    /** Consolidated Stock **/
    val dsConsolidatedStock: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle2, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle2, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore3, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle3, idStore3, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle3, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle4, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle3, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle4, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle3, idStore3, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle4, idStore3, measureUnitEA, 100d, currentTimestamp)
    )).toDS

    /** Fixed Data **/
    val dfFixedData: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row( idArticle1,  idStore1, todayString,            sector01, measureUnitEA, 90d, 90d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle2,  idStore1, todayString,            sector01, measureUnitEA,  5d, 15d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle3,  idStore1, todayString,            sector01, measureUnitEA, 80d, 90d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d),
        Row( idArticle4,  idStore1, todayString,            sector01, measureUnitEA,  5d, 25d, "B", currentTimestamp, "user",
          10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d, 10d)
      )), fixedDataStructType)
        .transform(Sql.filterDatosFijos(numberOfDaysToProject, todayAtMidnightTs, yesterdayAtMidnightTs)(spark, appConfig))

    /** Sales movements data : EMPTY **/
    val dsSaleMovements: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS

    /** Orders data : Only for sales articles **/
    val dsMaterials: Dataset[Materials] = sc.parallelize(List.empty[Materials]).toDS
    val dsMaterialsArticle: Dataset[MaterialsArticle] = sc.parallelize(List.empty[MaterialsArticle]).toDS
    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List.empty[CustomerReservations]).toDS

    val dsMaraWithArticleTypes: Dataset[MaraWithArticleTypes] = sc.parallelize(List[MaraWithArticleTypes](
      MaraWithArticleTypes(idArticle1, noPurchaseArticleType1, noPurchaseArticleCategory1, saleFlag, null, currentTimestamp),
      MaraWithArticleTypes(idArticle2, noPurchaseArticleType1, noPurchaseArticleCategory1, saleFlag, null, currentTimestamp),
      MaraWithArticleTypes(idArticle3, noPurchaseArticleType1, noPurchaseArticleCategory1, saleFlag, null, currentTimestamp),
      MaraWithArticleTypes(idArticle4, noPurchaseArticleType1, noPurchaseArticleCategory1, saleFlag, null, currentTimestamp)
    )).toDS

    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List[OrderHeader](
      OrderHeader(idOrder1, typeOrderHeaderOMS1, currentTimestamp)
    )).toDS
      .transform(filterOrderHeader(spark, appConfig))

    val dsOrderLine: Dataset[OrderLineView] = sc.parallelize(List[OrderLine](
      OrderLine(idOrder1, idOrderLine1, idArticle1, idStore1, notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder1, idOrderLine2, idArticle2, idStore1, notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder1, idOrderLine3, idArticle3, idStore1, notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp),
      OrderLine(idOrder1, idOrderLine4, idArticle4, idStore1, notCanceledFlag,  notYetDeliveredFlag, notReturnedToProviderFlag, currentTimestamp)
    )).toDS
      .transform(filterOrderLines(spark, appConfig))

    val dsPartialOrdersDelivered: Dataset[PartialOrdersDeliveredView] = sc.parallelize(List[PartialOrdersDeliveredView](
      // article 1 store 1 : partial orders for every day
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered1, 100d, 90d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered2, 100d, 90d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered3, 100d, 90d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered4, 100d, 90d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered5, 100d, 90d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered6, 100d, 90d,                 dayN3, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered7, 100d, 90d,                 dayN4, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine1, idNumPartialOrdersDelivered8, 100d, 90d,   lastProjectionDayTs, currentTimestamp),

      // article 2 store 1 : partial orders for days in between
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered1, 100d, 80d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered2, 100d, 80d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered3, 100d, 80d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine2, idNumPartialOrdersDelivered4, 100d, 80d,                 dayN3, currentTimestamp),

      // article 3 store 1 : partial orders for every day
      PartialOrdersDeliveredView(idOrder1, idOrderLine3, idNumPartialOrdersDelivered1, 100d, 90d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine3, idNumPartialOrdersDelivered2, 100d, 90d, yesterdayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine3, idNumPartialOrdersDelivered3, 100d, 90d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine3, idNumPartialOrdersDelivered4, 100d, 90d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine3, idNumPartialOrdersDelivered5, 100d, 90d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine3, idNumPartialOrdersDelivered6, 100d, 90d,                 dayN3, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine3, idNumPartialOrdersDelivered7, 100d, 90d,                 dayN4, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine3, idNumPartialOrdersDelivered8, 100d, 90d,   lastProjectionDayTs, currentTimestamp),

      // article 4 store 1 : partial orders for days in between
      PartialOrdersDeliveredView(idOrder1, idOrderLine4, idNumPartialOrdersDelivered1, 100d, 80d,     todayAtMidnightTs, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine4, idNumPartialOrdersDelivered2, 100d, 80d,                 dayN1, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine4, idNumPartialOrdersDelivered3, 100d, 80d,                 dayN2, currentTimestamp),
      PartialOrdersDeliveredView(idOrder1, idOrderLine4, idNumPartialOrdersDelivered4, 100d, 80d,                 dayN3, currentTimestamp)

    )).toDS

    /** Capacities and store types: EMPTY --> Defaults are false and true **/
    val dsStoreCapacity: Dataset[StoreCapacityView] = sc.parallelize(List.empty[StoreCapacityView]).toDS
    val dsTypeStore: Dataset[StoreTypeView] = sc.parallelize(List.empty[StoreTypeView]).toDS


    /** ATP N Formula
      *
      * Store - Article  -> consolidated stock * rotation/100 - max(sales forecast - sales N, 0)*isOpen + stock to be delivered * nsp/100 * nst/100
      * 11    - 1        ->               100  * 70/100       - max(       10      -    0   , 0)*1      + (100-90 + 100-90)     *     1   *    1    = 80
      * 11    - 2        ->               100  * 70/100       - max(       10      -    0   , 0)*1      +           0           *     1   *    1    = 60
      * 11    - 3        ->               100  * 70/100       - max(       10      -    0   , 0)*1      + (100-90 + 100-90)     * 80/100  *    1    = 76
      * 11    - 4        ->               100  * 70/100       - max(       10      -    0   , 0)*1      +           0           *     1   * 25/100  = 60
      *
      * ATP NX Formula
      * Store - Article - Day -> ATPNX-1 - sales forecast + stock to be delivered * nsp/100 * nst/100
      * 11    - 1          1  ->    80   -         10     +      (100-90)         *     1   *    1    = 80
      * 11    - 2          1  ->    60   -         10     +      (100-80)         *     1   *    1    = 70
      * 11    - 3          1  ->    76   -         10     +      (100-90)         * 80/100  *    1    = 74
      * 11    - 4          1  ->    60   -         10     +      (100-80)         *     1   * 25/100  = 55
      *
      * 11    - 1          2  ->    80   -         10     +      (100-90)         *     1   *    1    = 80
      * 11    - 2          2  ->    70   -         10     +      (100-80)         *     1   *    1    = 80
      * 11    - 3          2  ->    74   -         10     +      (100-90)         * 80/100  *    1    = 72
      * 11    - 4          2  ->    55   -         10     +      (100-80)         *     1   * 25/100  = 50
      *
      * 11    - 1          3  ->    80   -         10     +      (100-90)         *     1   *    1    = 80
      * 11    - 2          3  ->    80   -         10     +      (100-80)         *     1   *    1    = 90
      * 11    - 3          3  ->    72   -         10     +      (100-90)         * 80/100  *    1    = 70
      * 11    - 4          3  ->    50   -         10     +      (100-80)         *     1   * 25/100  = 45
      *
      * 11    - 1          4  ->    80   -         10     +      (100-90)         *     1   *    1    =  80
      * 11    - 2          4  ->    90   -         10     +      (100-80)         *     1   *    1    = 100
      * 11    - 3          4  ->    70   -         10     +      (100-90)         * 80/100  *    1    =  68
      * 11    - 4          4  ->    45   -         10     +      (100-80)         *     1   * 25/100  =  40
      *
      * 11    - 1          5  ->    80   -         10     +      (100-90)         *     1   *    1    = 80
      * 11    - 2          5  ->   100   -         10     +           0           *     1   *    1    = 90
      * 11    - 3          5  ->    68   -         10     +      (100-90)         * 80/100  *    1    = 66
      * 11    - 4          5  ->    40   -         10     +           0           *     1   * 25/100  = 30
      *
      */
    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idStore1, idArticle1, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 80d, 80d, 80d, 80d,  80d, 80d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1, idArticle2, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 60d, 70d, 80d, 90d, 100d, 90d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1, idArticle3, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 76d, 74d, 72d, 70d,  68d, 66d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore1, idArticle4, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 60d, 55d, 50d, 45d,  40d, 30d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null)
    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val readEntities =
      ReadAtpEntities(
        dsSaleMovements,
        dsConsolidatedStock,
        dfFixedData,
        dsOrderHeader,
        dsOrderLine,
        dsPartialOrdersDelivered,
        dsStoreCapacity,
        dsTypeStore,
        dsMaraWithArticleTypes,
        dsMaterials,
        dsMaterialsArticle,
        dsCustomerReservations)

    calculateProcessAndCompareResultWithoutTsUpdateColumns(
      readEntities,
      stockConf,
      todayAtMidnightTs,
      yesterdayAtMidnightTs,
      List.empty[ControlEjecucionView],
      appName,
      Seq(StockATP.ccTags.idArticleTag, StockATP.ccTags.idStoreTag),
      dfExpected)

  }

  it must
    " calculate stock ATP with customer reservations" taggedAs UnitTestTag in new StockATPFor5ProjectionDays {

    val stockConf = StockConf(
      maxNsp = 80d, minNsp = 10d,
      maxNst = 80d, minNst = 20d,
      rotationArtAA = 50d,
      rotationArtA = 60d,
      rotationArtB = 70d,
      rotationArtC = 80d,
      numberOfDaysToProject,
      tsUpdateDlk = currentTimestamp)

    /** Consolidated Stock **/
    val dsConsolidatedStock: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, 110d, currentTimestamp),
      StockConsolidadoView(idArticle2, idStore1, measureUnitEA, 120d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore2, measureUnitEA, 210d, currentTimestamp),
      StockConsolidadoView(idArticle2, idStore2, measureUnitEA, 220d, currentTimestamp),

      // This will be ignored: not in fixed data
      StockConsolidadoView(idArticle3, idStore2, measureUnitEA, 200d, currentTimestamp)

    )).toDS

    /** Fixed Data **/
    val dfFixedData: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row(idArticle1, idStore1, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row(idArticle2, idStore1, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row(idArticle1, idStore2, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row(idArticle2, idStore2, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d)
      )), fixedDataStructType)
        .transform(Sql.filterDatosFijos(numberOfDaysToProject, todayAtMidnightTs, yesterdayAtMidnightTs)(spark, appConfig))

    /** Sales movements data : EMPTY **/
    val dsSaleMovements: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS

    /** Orders data : Only Customer reservations (the rest is empty) **/
    val dsMaterials: Dataset[Materials] = sc.parallelize(List.empty[Materials]).toDS
    val dsMaterialsArticle: Dataset[MaterialsArticle] = sc.parallelize(List.empty[MaterialsArticle]).toDS

    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List[CustomerReservations](
      CustomerReservations(idArticle1, idStore1, Some(todayAtMidnightTs), Some(5d), currentTimestamp),
      CustomerReservations(idArticle1, idStore1, Some(todayAtMidnightTs), Some(4d), currentTimestamp),
      CustomerReservations(idArticle1, idStore2, Some(todayAtMidnightTs), Some(3d), currentTimestamp),
      CustomerReservations(idArticle1, idStore1, Some(todayAtMidnightTs), Some(3d), currentTimestamp),
      CustomerReservations(idArticle2, idStore1, Some(todayAtMidnightTs), Some(1d), currentTimestamp),
      CustomerReservations(idArticle1, idStore2, Some(todayAtMidnightTs), Some(10d), currentTimestamp),
      CustomerReservations(idArticle2, idStore2, Some(todayAtMidnightTs), Some(2d), currentTimestamp),
      CustomerReservations(idArticle3, idStore2, Some(todayAtMidnightTs), Some(1d), currentTimestamp),
      CustomerReservations(idArticle1, idStore1, Some(dayN1), Some(12d), currentTimestamp),
      CustomerReservations(idArticle2, idStore1, Some(dayN1), Some(4d), currentTimestamp),
      CustomerReservations(idArticle1, idStore2, Some(dayN2), Some(20d), currentTimestamp),
      CustomerReservations(idArticle2, idStore2, Some(dayN2), Some(20d), currentTimestamp),
      CustomerReservations(idArticle1, idStore1, Some(dayN4), Some(1d), currentTimestamp),
      CustomerReservations(idArticle2, idStore1, Some(dayN4), Some(3d), currentTimestamp),
      CustomerReservations(idArticle1, idStore2, Some(dayN4), Some(5d), currentTimestamp),
      CustomerReservations(idArticle2, idStore2, Some(dayN4), Some(7d), currentTimestamp)
    )).toDS

    val dsMaraWithArticleTypes: Dataset[MaraWithArticleTypes] = sc.parallelize(List.empty[MaraWithArticleTypes]).toDS
    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List.empty[OrderHeader]).toDS
    val dsOrderLine: Dataset[OrderLineView] = sc.parallelize(List.empty[OrderLineView]).toDS
    val dsPartialOrdersDelivered: Dataset[PartialOrdersDeliveredView] = sc.parallelize(List.empty[PartialOrdersDeliveredView]).toDS

    /** Capacities and store types: EMPTY --> Defaults are false and true **/
    val dsStoreCapacity: Dataset[StoreCapacityView] = sc.parallelize(List.empty[StoreCapacityView]).toDS
    val dsTypeStore: Dataset[StoreTypeView] = sc.parallelize(List.empty[StoreTypeView]).toDS

    /** ATP N Formula
      *
      * Store - Article  -> consolidated stock * rotation/100 - max(sales forecast - sales N, 0) * isOpen + (stock to be delivered * nsp/100 * nst/100) - customer reservations
      * 11    - 1        ->               110  * 70/100       - max(        0      -    0   , 0) * 1      +           0            *  20/100 * 50/100   -      (5 + 4 + 3)      =  65
      * 11    - 2        ->               120  * 70/100       - max(        0      -    0   , 0) * 1      +           0            *  20/100 * 50/100   -           1           =  83
      * 22    - 1        ->               210  * 70/100       - max(        0      -    0   , 0) * 1      +           0            *  20/100 * 50/100   -       (10 + 3)        = 134
      * 22    - 2        ->               220  * 70/100       - max(        0      -    0   , 0) * 1      +           0            *  20/100 * 50/100   -           2           = 152
      *
      * ATP NX Formula
      * Store - Article - Day -> ATPNX-1 - sales forecast + stock to be delivered - customer reservations
      * 11    - 1          1  ->    65   -         0      +           0           -           12            = 53
      * 11    - 2          1  ->    83   -         0      +           0           -           4             = 79
      * 11    - 3          1  ->   134   -         0      +           0           -           0             = 134
      * 11    - 4          1  ->   152   -         0      +           0           -           0             = 152
      *
      * 11    - 1          2  ->    53   -         0      +           0           -           0             = 53
      * 11    - 2          2  ->    79   -         0      +           0           -           0             = 79
      * 11    - 3          2  ->    134  -         0      +           0           -           20            = 114
      * 11    - 4          2  ->    152  -         0      +           0           -           20            = 132
      *
      * 11    - 1          3  ->    53   -         0      +           0           -           0             = 53
      * 11    - 2          3  ->    79   -         0      +           0           -           0             = 79
      * 11    - 3          3  ->    114  -         0      +           0           -           0             = 114
      * 11    - 4          3  ->    132  -         0      +           0           -           0             = 132
      *
      * 11    - 1          4  ->    53   -         0      +           0           -           1             = 52
      * 11    - 2          4  ->    79   -         0      +           0           -           3             = 76
      * 11    - 3          4  ->    114  -         0      +           0           -           5             = 109
      * 11    - 4          4  ->    132  -         0      +           0           -           7             = 125
      *
      * 11    - 1          5  ->    52   -         0      +           0           -           0             = 52
      * 11    - 2          5  ->    76   -         0      +           0           -           0             = 76
      * 11    - 3          5  ->    109  -         0      +           0           -           0             = 109
      * 11    - 4          5  ->    125  -         0      +           0           -           0             = 125
      *
      *
      */
    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row(idStore1, idArticle1, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 65d, 53d, 53d, 53d, 52d, 52d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row(idStore1, idArticle2, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 83d, 79d, 79d, 79d, 76d, 76d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row(idStore2, idArticle1, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 134d, 134d, 114d, 114d, 109d, 109d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row(idStore2, idArticle2, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 152d, 152d, 132d, 132d, 125d, 125d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null)
    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val readEntities =
      ReadAtpEntities(
        dsSaleMovements,
        dsConsolidatedStock,
        dfFixedData,
        dsOrderHeader,
        dsOrderLine,
        dsPartialOrdersDelivered,
        dsStoreCapacity,
        dsTypeStore,
        dsMaraWithArticleTypes,
        dsMaterials,
        dsMaterialsArticle,
        dsCustomerReservations)

    calculateProcessAndCompareResultWithoutTsUpdateColumns(
      readEntities,
      stockConf,
      todayAtMidnightTs,
      yesterdayAtMidnightTs,
      List.empty[ControlEjecucionView],
      appName,
      Seq(StockATP.ccTags.idArticleTag, StockATP.ccTags.idStoreTag),
      dfExpected)

  }

  it must
    " calculate stock ATP for all articles including if each store is a preparation location"  taggedAs UnitTestTag in new StockATPFor5ProjectionDays {

    val stockConf = StockConf(
      maxNsp = 80d, minNsp = 10d,
      maxNst = 80d, minNst = 20d,
      rotationArtAA = 50d,
      rotationArtA = 60d,
      rotationArtB = 70d,
      rotationArtC = 80d,
      numberOfDaysToProject,
      tsUpdateDlk = currentTimestamp)

    /** Consolidated Stock **/
    val dsConsolidatedStock: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore3, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore4, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore5, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore6, measureUnitEA, 100d, currentTimestamp)
    )).toDS

    /** Fixed Data **/
    val dfFixedData: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row( idArticle1,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle1,  idStore2, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle1,  idStore3, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle1,  idStore4, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle1,  idStore5, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle1,  idStore6, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d)
      )), fixedDataStructType)
        .transform(Sql.filterDatosFijos(numberOfDaysToProject, todayAtMidnightTs, yesterdayAtMidnightTs)(spark, appConfig))

    /** Sales movements data : EMPTY **/
    val dsSaleMovements: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS

    /** Orders data : EMPTY **/
    val dsMaterials: Dataset[Materials] = sc.parallelize(List.empty[Materials]).toDS
    val dsMaterialsArticle: Dataset[MaterialsArticle] = sc.parallelize(List.empty[MaterialsArticle]).toDS
    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List.empty[CustomerReservations]).toDS
    val dsMaraWithArticleTypes: Dataset[MaraWithArticleTypes] = sc.parallelize(List.empty[MaraWithArticleTypes]).toDS
    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List.empty[OrderHeader]).toDS
    val dsOrderLine: Dataset[OrderLineView] = sc.parallelize(List.empty[OrderLineView]).toDS
    val dsPartialOrdersDelivered: Dataset[PartialOrdersDeliveredView] = sc.parallelize(List.empty[PartialOrdersDeliveredView]).toDS

    /** Capacities and store types **/
    val dsStoreCapacity: Dataset[StoreCapacityView] = sc.parallelize(List[StoreCapacityView](
      StoreCapacityView(idStore1,    preparationCapacity5, currentTimestamp),
      StoreCapacityView(idStore2,    preparationCapacity6, currentTimestamp),
      StoreCapacityView(idStore3,    preparationCapacity7, currentTimestamp),
      StoreCapacityView(idStore4,    preparationCapacity8, currentTimestamp),
      StoreCapacityView(idStore5,    preparationCapacity9, currentTimestamp),
      StoreCapacityView(idStore6, notAPreparationCapacity, currentTimestamp)
    )).toDS
    val dsTypeStore: Dataset[StoreTypeView] = sc.parallelize(List.empty[StoreTypeView]).toDS


    /** ATP N Formula
      *
      * Store - Article  -> consolidated stock * rotation/100 - max(sales forecast - sales N, 0)*isOpen + stock to be delivered * nsp/100 * nst/100
      * 11    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 22    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 33    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 44    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 55    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 66    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      *
      * ATP NX Formula
      * Store - Article - Day -> ATPNX-1 - sales forecast + stock to be delivered * nsp/100 * nst/100
      *
      * As there is no other information apart from consolidated stock, ATP will be the same for each store-article every day
      *
      */
    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idStore1, idArticle1, sector01, measureUnitEA, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 70d, 70d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, idArticle1, sector01, measureUnitEA, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 70d, 70d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore3, idArticle1, sector01, measureUnitEA, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 70d, 70d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore4, idArticle1, sector01, measureUnitEA, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 70d, 70d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore5, idArticle1, sector01, measureUnitEA, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 70d, 70d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore6, idArticle1, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 70d, 70d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null)
    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val readEntities =
      ReadAtpEntities(
        dsSaleMovements,
        dsConsolidatedStock,
        dfFixedData,
        dsOrderHeader,
        dsOrderLine,
        dsPartialOrdersDelivered,
        dsStoreCapacity,
        dsTypeStore,
        dsMaraWithArticleTypes,
        dsMaterials,
        dsMaterialsArticle,
        dsCustomerReservations)

    calculateProcessAndCompareResultWithoutTsUpdateColumns(
      readEntities,
      stockConf,
      todayAtMidnightTs,
      yesterdayAtMidnightTs,
      List.empty[ControlEjecucionView],
      appName,
      Seq(StockATP.ccTags.idArticleTag, StockATP.ccTags.idStoreTag),
      dfExpected)

  }

  it must
    " calculate stock ATP and truncate results when they have decimals "  taggedAs UnitTestTag in new StockATPFor5ProjectionDays {

    val stockConf = StockConf(
      maxNsp = 80d, minNsp = 10d,
      maxNst = 80d, minNst = 20d,
      rotationArtAA = 50d,
      rotationArtA = 60d,
      rotationArtB = 70d,
      rotationArtC = 80d,
      numberOfDaysToProject,
      tsUpdateDlk = currentTimestamp)

    /** Consolidated Stock **/
    val dsConsolidatedStock: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, 100d, currentTimestamp)
    )).toDS

    /** Fixed Data **/
    val dfFixedData: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row( idArticle1,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0.1d, 0.1d, 0.1d, 0.1d, 0.1d, 0.1d, 0.1d, 0.1d, 0.1d, 0.1d, 0.1d)
      )), fixedDataStructType)
        .transform(Sql.filterDatosFijos(numberOfDaysToProject, todayAtMidnightTs, yesterdayAtMidnightTs)(spark, appConfig))

    /** Sales movements data : EMPTY **/
    val dsSaleMovements: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS

    /** Orders data : EMPTY **/
    val dsMaterials: Dataset[Materials] = sc.parallelize(List.empty[Materials]).toDS
    val dsMaterialsArticle: Dataset[MaterialsArticle] = sc.parallelize(List.empty[MaterialsArticle]).toDS
    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List.empty[CustomerReservations]).toDS
    val dsMaraWithArticleTypes: Dataset[MaraWithArticleTypes] = sc.parallelize(List.empty[MaraWithArticleTypes]).toDS
    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List.empty[OrderHeader]).toDS
    val dsOrderLine: Dataset[OrderLineView] = sc.parallelize(List.empty[OrderLineView]).toDS
    val dsPartialOrdersDelivered: Dataset[PartialOrdersDeliveredView] = sc.parallelize(List.empty[PartialOrdersDeliveredView]).toDS

    /** Capacities and store types **/
    val dsStoreCapacity: Dataset[StoreCapacityView] = sc.parallelize(List[StoreCapacityView](
      StoreCapacityView(idStore1,    preparationCapacity5, currentTimestamp),
      StoreCapacityView(idStore2,    preparationCapacity6, currentTimestamp),
      StoreCapacityView(idStore3,    preparationCapacity7, currentTimestamp),
      StoreCapacityView(idStore4,    preparationCapacity8, currentTimestamp),
      StoreCapacityView(idStore5,    preparationCapacity9, currentTimestamp),
      StoreCapacityView(idStore6, notAPreparationCapacity, currentTimestamp)
    )).toDS
    val dsTypeStore: Dataset[StoreTypeView] = sc.parallelize(List.empty[StoreTypeView]).toDS


    /** ATP N Formula
      *
      * Store - Article  -> consolidated stock * rotation/100 - max(sales forecast - sales N, 0)*isOpen + stock to be delivered * nsp/100 * nst/100
      * 11    - 1        ->               100  * 70/100       - max(      0.1      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 69,0
      *
      * ATP NX Formula
      * Store - Article - Day -> ATPNX-1 - sales forecast + stock to be delivered * nsp/100 * nst/100
      * 11    - 1          1  ->    69,0   -         0.1     +      0         *     1   *    1    = 68,0
      *
      * 11    - 1          2  ->    68,0   -         0.1     +      0         *     1   *    1    = 67,0
      *
      * 11    - 1          3  ->    67,0   -         0.1     +      0         *     1   *    1    = 66,0
      *
      * 11    - 1          4  ->    66,0   -         0.1     +      0         *     1   *    1    = 65,0
      *
      * 11    - 1          5  ->    65,0   -         0.1     +      0         *     1   *    1    = 64,0
      *
      */
    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idStore1, idArticle1, sector01, measureUnitEA, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 69d, 68d, 67d, 66d, 65d, 64d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null)
    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val readEntities =
      ReadAtpEntities(
        dsSaleMovements,
        dsConsolidatedStock,
        dfFixedData,
        dsOrderHeader,
        dsOrderLine,
        dsPartialOrdersDelivered,
        dsStoreCapacity,
        dsTypeStore,
        dsMaraWithArticleTypes,
        dsMaterials,
        dsMaterialsArticle,
        dsCustomerReservations)

    calculateProcessAndCompareResultWithoutTsUpdateColumns(
      readEntities,
      stockConf,
      todayAtMidnightTs,
      yesterdayAtMidnightTs,
      List.empty[ControlEjecucionView],
      appName,
      Seq(StockATP.ccTags.idArticleTag, StockATP.ccTags.idStoreTag),
      dfExpected)

  }

  it must
    " calculate stock ATP with correct unit measure: first, from consolidated stock; if null,from fixed data; " +
      "or else default unit measure" taggedAs UnitTestTag in new StockATPFor5ProjectionDays {

    val stockConf = StockConf(
      maxNsp = 80d, minNsp = 10d,
      maxNst = 80d, minNst = 20d,
      rotationArtAA = 50d,
      rotationArtA = 60d,
      rotationArtB = 70d,
      rotationArtC = 80d,
      numberOfDaysToProject,
      tsUpdateDlk = currentTimestamp)

    /** Consolidated Stock **/
    val dsConsolidatedStock: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore3,          null, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore4,          null, 100d, currentTimestamp)
    )).toDS

    /** Fixed Data **/
    val dfFixedData: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row( idArticle1,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle1,  idStore2, todayString, sector01, measureUnitKG, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle1,  idStore3, todayString, sector01, measureUnitKG, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle1,  idStore4, todayString, sector01,          null, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d)
      )), fixedDataStructType)
        .transform(Sql.filterDatosFijos(numberOfDaysToProject, todayAtMidnightTs, yesterdayAtMidnightTs)(spark, appConfig))

    /** Sales movements data : EMPTY **/
    val dsSaleMovements: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS

    /** Orders data : EMPTY **/
    val dsMaterials: Dataset[Materials] = sc.parallelize(List.empty[Materials]).toDS
    val dsMaterialsArticle: Dataset[MaterialsArticle] = sc.parallelize(List.empty[MaterialsArticle]).toDS
    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List.empty[CustomerReservations]).toDS
    val dsMaraWithArticleTypes: Dataset[MaraWithArticleTypes] = sc.parallelize(List.empty[MaraWithArticleTypes]).toDS
    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List.empty[OrderHeader]).toDS
    val dsOrderLine: Dataset[OrderLineView] = sc.parallelize(List.empty[OrderLineView]).toDS
    val dsPartialOrdersDelivered: Dataset[PartialOrdersDeliveredView] = sc.parallelize(List.empty[PartialOrdersDeliveredView]).toDS

    /** Capacities and store types **/
    val dsStoreCapacity: Dataset[StoreCapacityView] = sc.parallelize(List[StoreCapacityView](
      StoreCapacityView(idStore1,    preparationCapacity5, currentTimestamp),
      StoreCapacityView(idStore2,    preparationCapacity6, currentTimestamp),
      StoreCapacityView(idStore3,    preparationCapacity7, currentTimestamp),
      StoreCapacityView(idStore4,    preparationCapacity8, currentTimestamp),
      StoreCapacityView(idStore5,    preparationCapacity9, currentTimestamp),
      StoreCapacityView(idStore6, notAPreparationCapacity, currentTimestamp)
    )).toDS
    val dsTypeStore: Dataset[StoreTypeView] = sc.parallelize(List.empty[StoreTypeView]).toDS


    /** ATP N Formula
      *
      * Store - Article  -> consolidated stock * rotation/100 - max(sales forecast - sales N, 0)*isOpen + stock to be delivered * nsp/100 * nst/100
      * 11    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 22    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 33    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 44    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 55    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 66    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      *
      * ATP NX Formula
      * Store - Article - Day -> ATPNX-1 - sales forecast + stock to be delivered * nsp/100 * nst/100
      *
      * As there is no other information apart from consolidated stock, ATP will be the same for each store-article every day
      *
      */
    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idStore1, idArticle1, sector01,       measureUnitEA, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 70d, 70d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, idArticle1, sector01,       measureUnitEA, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 70d, 70d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore3, idArticle1, sector01,       measureUnitKG, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 70d, 70d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore4, idArticle1, sector01, defaultUnityMeasure, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 70d, 70d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null)
    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val readEntities =
      ReadAtpEntities(
        dsSaleMovements,
        dsConsolidatedStock,
        dfFixedData,
        dsOrderHeader,
        dsOrderLine,
        dsPartialOrdersDelivered,
        dsStoreCapacity,
        dsTypeStore,
        dsMaraWithArticleTypes,
        dsMaterials,
        dsMaterialsArticle,
        dsCustomerReservations)

    calculateProcessAndCompareResultWithoutTsUpdateColumns(
      readEntities,
      stockConf,
      todayAtMidnightTs,
      yesterdayAtMidnightTs,
      List.empty[ControlEjecucionView],
      appName,
      Seq(StockATP.ccTags.idArticleTag, StockATP.ccTags.idStoreTag),
      dfExpected)

  }


  it must
    " calculate stock ATP when day_project is 100 "  taggedAs UnitTestTag in new StockATPFor5ProjectionDays {

   override lazy val numberOfDaysToProject = 100

    val stockConf = StockConf(
      maxNsp = 80d, minNsp = 10d,
      maxNst = 80d, minNst = 20d,
      rotationArtAA = 50d,
      rotationArtA = 60d,
      rotationArtB = 70d,
      rotationArtC = 80d,
      numberOfDaysToProject,
      tsUpdateDlk = currentTimestamp)

    /** Consolidated Stock **/
    val dsConsolidatedStock: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore3, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore4, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore5, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore6, measureUnitEA, 100d, currentTimestamp)
    )).toDS

    /** Fixed Data **/
    val dfFixedData: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row( idArticle1,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle1,  idStore2, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle1,  idStore3, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle1,  idStore4, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle1,  idStore5, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
        Row( idArticle1,  idStore6, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
          0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d)
      )), fixedData100DaysStructType)
        .transform(Sql.filterDatosFijos(numberOfDaysToProject, todayAtMidnightTs, yesterdayAtMidnightTs)(spark, appConfig))

    /** Sales movements data : EMPTY **/
    val dsSaleMovements: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS

    /** Orders data : EMPTY **/
    val dsMaterials: Dataset[Materials] = sc.parallelize(List.empty[Materials]).toDS
    val dsMaterialsArticle: Dataset[MaterialsArticle] = sc.parallelize(List.empty[MaterialsArticle]).toDS
    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List.empty[CustomerReservations]).toDS
    val dsMaraWithArticleTypes: Dataset[MaraWithArticleTypes] = sc.parallelize(List.empty[MaraWithArticleTypes]).toDS
    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List.empty[OrderHeader]).toDS
    val dsOrderLine: Dataset[OrderLineView] = sc.parallelize(List.empty[OrderLineView]).toDS
    val dsPartialOrdersDelivered: Dataset[PartialOrdersDeliveredView] = sc.parallelize(List.empty[PartialOrdersDeliveredView]).toDS

    /** Capacities and store types **/
    val dsStoreCapacity: Dataset[StoreCapacityView] = sc.parallelize(List[StoreCapacityView](
      StoreCapacityView(idStore1,    preparationCapacity5, currentTimestamp),
      StoreCapacityView(idStore2,    preparationCapacity6, currentTimestamp),
      StoreCapacityView(idStore3,    preparationCapacity7, currentTimestamp),
      StoreCapacityView(idStore4,    preparationCapacity8, currentTimestamp),
      StoreCapacityView(idStore5,    preparationCapacity9, currentTimestamp),
      StoreCapacityView(idStore6, notAPreparationCapacity, currentTimestamp)
    )).toDS
    val dsTypeStore: Dataset[StoreTypeView] = sc.parallelize(List.empty[StoreTypeView]).toDS


    /** ATP N Formula
      *
      * Store - Article  -> consolidated stock * rotation/100 - max(sales forecast - sales N, 0)*isOpen + stock to be delivered * nsp/100 * nst/100
      * 11    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 22    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 33    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 44    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 55    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 66    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      *
      * ATP NX Formula
      * Store - Article - Day -> ATPNX-1 - sales forecast + stock to be delivered * nsp/100 * nst/100
      *
      * As there is no other information apart from consolidated stock, ATP will be the same for each store-article every day
      *
      */
    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idStore1, idArticle1, sector01, measureUnitEA, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d),
      Row( idStore2, idArticle1, sector01, measureUnitEA, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d),
      Row( idStore3, idArticle1, sector01, measureUnitEA, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d),
      Row( idStore4, idArticle1, sector01, measureUnitEA, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d),
      Row( idStore5, idArticle1, sector01, measureUnitEA, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d),
      Row( idStore6, idArticle1, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d,
        70d, 70d, 70d, 70d, 70d, 70d, 70d, 70d)
    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val readEntities =
      ReadAtpEntities(
        dsSaleMovements,
        dsConsolidatedStock,
        dfFixedData,
        dsOrderHeader,
        dsOrderLine,
        dsPartialOrdersDelivered,
        dsStoreCapacity,
        dsTypeStore,
        dsMaraWithArticleTypes,
        dsMaterials,
        dsMaterialsArticle,
        dsCustomerReservations)

    calculateProcessAndCompareResultWithoutTsUpdateColumns(
      readEntities,
      stockConf,
      todayAtMidnightTs,
      yesterdayAtMidnightTs,
      List.empty[ControlEjecucionView],
      appName,
      Seq(StockATP.ccTags.idArticleTag, StockATP.ccTags.idStoreTag),
      dfExpected)

  }

  it must
    " calculate stock ATP when day_project is 0 "  taggedAs UnitTestTag in new StockATPFor5ProjectionDays {

    override lazy val numberOfDaysToProject = 0

    val stockConf = StockConf(
      maxNsp = 80d, minNsp = 10d,
      maxNst = 80d, minNst = 20d,
      rotationArtAA = 50d,
      rotationArtA = 60d,
      rotationArtB = 70d,
      rotationArtC = 80d,
      numberOfDaysToProject,
      tsUpdateDlk = currentTimestamp)

    /** Consolidated Stock **/
    val dsConsolidatedStock: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore2, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore3, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore4, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore5, measureUnitEA, 100d, currentTimestamp),
      StockConsolidadoView(idArticle1, idStore6, measureUnitEA, 100d, currentTimestamp)
    )).toDS

    /** Fixed Data **/
    val dfFixedData: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row( idArticle1,  idStore1, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d),
        Row( idArticle1,  idStore2, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d),
        Row( idArticle1,  idStore3, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d),
        Row( idArticle1,  idStore4, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d),
        Row( idArticle1,  idStore5, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d),
        Row( idArticle1,  idStore6, todayString, sector01, measureUnitEA, 20d, 50d, "B", currentTimestamp, "user",
          0d, 0d, 0d)
      )), fixedData0DaysStructType)
        .transform(Sql.filterDatosFijos(numberOfDaysToProject, todayAtMidnightTs, yesterdayAtMidnightTs)(spark, appConfig))

    /** Sales movements data : EMPTY **/
    val dsSaleMovements: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS

    /** Orders data : EMPTY **/
    val dsMaterials: Dataset[Materials] = sc.parallelize(List.empty[Materials]).toDS
    val dsMaterialsArticle: Dataset[MaterialsArticle] = sc.parallelize(List.empty[MaterialsArticle]).toDS
    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List.empty[CustomerReservations]).toDS
    val dsMaraWithArticleTypes: Dataset[MaraWithArticleTypes] = sc.parallelize(List.empty[MaraWithArticleTypes]).toDS
    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List.empty[OrderHeader]).toDS
    val dsOrderLine: Dataset[OrderLineView] = sc.parallelize(List.empty[OrderLineView]).toDS
    val dsPartialOrdersDelivered: Dataset[PartialOrdersDeliveredView] = sc.parallelize(List.empty[PartialOrdersDeliveredView]).toDS

    /** Capacities and store types **/
    val dsStoreCapacity: Dataset[StoreCapacityView] = sc.parallelize(List[StoreCapacityView](
      StoreCapacityView(idStore1,    preparationCapacity5, currentTimestamp),
      StoreCapacityView(idStore2,    preparationCapacity6, currentTimestamp),
      StoreCapacityView(idStore3,    preparationCapacity7, currentTimestamp),
      StoreCapacityView(idStore4,    preparationCapacity8, currentTimestamp),
      StoreCapacityView(idStore5,    preparationCapacity9, currentTimestamp),
      StoreCapacityView(idStore6, notAPreparationCapacity, currentTimestamp)
    )).toDS
    val dsTypeStore: Dataset[StoreTypeView] = sc.parallelize(List.empty[StoreTypeView]).toDS


    /** ATP N Formula
      *
      * Store - Article  -> consolidated stock * rotation/100 - max(sales forecast - sales N, 0)*isOpen + stock to be delivered * nsp/100 * nst/100
      * 11    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 22    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 33    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 44    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 55    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      * 66    - 1        ->               100  * 70/100       - max(        0      -    0   , 0)*1      +           0           *  20/100 * 50/100 = 70
      *
      * ATP NX Formula
      * Store - Article - Day -> ATPNX-1 - sales forecast + stock to be delivered * nsp/100 * nst/100
      *
      * As there is no other information apart from consolidated stock, ATP will be the same for each store-article every day
      *
      */
    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idStore1, idArticle1, sector01, measureUnitEA, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 70d, 70d, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore2, idArticle1, sector01, measureUnitEA, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 70d, 70d, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore3, idArticle1, sector01, measureUnitEA, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 70d, 70d, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore4, idArticle1, sector01, measureUnitEA, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 70d, 70d, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore5, idArticle1, sector01, measureUnitEA, currentTimestamp,   isPreparationLocation, user, todayAtMidnightTs, 70d, 70d, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null),
      Row( idStore6, idArticle1, sector01, measureUnitEA, currentTimestamp, notAPreparationLocation, user, todayAtMidnightTs, 70d, 70d, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null)
    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val readEntities =
      ReadAtpEntities(
        dsSaleMovements,
        dsConsolidatedStock,
        dfFixedData,
        dsOrderHeader,
        dsOrderLine,
        dsPartialOrdersDelivered,
        dsStoreCapacity,
        dsTypeStore,
        dsMaraWithArticleTypes,
        dsMaterials,
        dsMaterialsArticle,
        dsCustomerReservations)

    calculateProcessAndCompareResultWithoutTsUpdateColumns(
      readEntities,
      stockConf,
      todayAtMidnightTs,
      yesterdayAtMidnightTs,
      List.empty[ControlEjecucionView],
      appName,
      Seq(StockATP.ccTags.idArticleTag, StockATP.ccTags.idStoreTag),
      dfExpected)

  }

}
