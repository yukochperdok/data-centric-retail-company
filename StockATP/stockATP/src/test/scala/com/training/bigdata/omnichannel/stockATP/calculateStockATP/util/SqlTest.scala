package com.training.bigdata.omnichannel.stockATP.calculateStockATP.util

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs._
import com.training.bigdata.omnichannel.stockATP.common.util.Constants._
import com.training.bigdata.omnichannel.stockATP.common.entities._
import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces._
import com.training.bigdata.omnichannel.stockATP.common.util.tag.TagTest.UnitTestTag
import com.training.bigdata.omnichannel.stockATP.common.util._
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.{FlatSpec, Matchers}
import com.training.bigdata.omnichannel.stockATP.common.util.LensAppConfig._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col


class SqlTest extends FlatSpec with Matchers with DatasetSuiteBase with DefaultConf{
  import spark.implicits._
  override protected implicit def enableHiveSupport: Boolean = false

  sealed trait SqlVariables{
    val currentTimestampLong = System.currentTimeMillis
    val ts = new Timestamp(currentTimestampLong)
    val today = "2018-10-28"
    val yesterday = "2018-08-27"
    val idArticle1 = "1"
    val idArticle2 = "2"
    val idArticle3 = "3"
    val idArticle4 = "4"
    val idArticle5 = "5"
    val idArticle6 = "6"
    val idArticle7 = "7"
    val idStore1 = "11"
    val idStore2 = "22"
    val idStore3 = "33"
    val idStore4 = "44"
    val idStore5 = "55"
    val idStore6 = "66"
    val idStore7 = "77"
    val parentSaleArticle1 = "111"
    val parentSaleArticle2 = "222"
    val parentSaleArticle3 = "333"
    val parentSaleArticleNull = null
    val capacity1 = 1
    val capacity5 = 5
    val defaultUnidadMedida = defaultConfig.getCommonParameters.defaultUnidadMedida
    val measureUnitEA = "EA"
    val measureUnitKG = "KG"
    val sector01 = "01"
    val sector02 = "02"
    val sector03 = "03"
    val stockConsolidado = 200d
    val stockConsolidado2 = 202d
    val stockConsolidado3 = 203d
    val stockConsolidado4 = 403d
    val stockConsolidado5 = 503d
    val stockConsolidado6 = 603d
    val acumVentas = 40d
    val acumVentas2 = 42d
    val acumVentas3 = 43d
    val acumVentas4 = 55d
    val acumVentas5 = 66d


    val user = "user"

    val ts1 = new Timestamp(100l)
    val ts2 = new Timestamp(200l)
    val ts3 = new Timestamp(300l)

    val ts4: Timestamp = new Timestamp(currentTimestampLong - 200)
    val ts5: Timestamp = new Timestamp(currentTimestampLong - 100)

    val afterTs = new Timestamp(currentTimestampLong + 100)
    val beforeTs = new Timestamp(currentTimestampLong - 100)

    implicit val appConfig = defaultConfig
    implicit val debugOptions = appConfig.getDebugStockATPParameters getOrElse DebugParameters()
  }

  sealed trait SqlVariablesPedidos extends SqlVariables {
    val (todayTs, todayLocalDateTime) = Dates.getDateAtMidnight(ts)
    val laterTodayTs1 = new Timestamp(todayTs.getTime + 1000)
    val laterTodayTs2 = new Timestamp(todayTs.getTime + 2000)
    val yesterdayAtMidnightTs: Timestamp = Dates.subtractDaysToLocalDateTime(todayLocalDateTime)
    val numberOfDaysToProject = 4
    val dayInBetweenTs: Timestamp = Dates.sumDaysToTimestamp(todayTs, 1)
    val lastDayToProjectTs: Timestamp = Dates.sumDaysToTimestamp(todayTs, numberOfDaysToProject)
    val lastDayToProjectPlus1Ts: Timestamp = Dates.sumDaysToTimestamp(lastDayToProjectTs, 1)
    val lastDayToProjectPlus2Ts: Timestamp = Dates.sumDaysToTimestamp(lastDayToProjectPlus1Ts, 1)
    val idOrder1 = "11"
    val idOrder2 = "12"
    val idOrder3 = "13"
    val idOrder4 = "14"
    val idOrder5 = "25"
    val idOrder6 = "26"
    val idOrderLine1 = "1"
    val idOrderLine2 = "2"
    val idOrderLine3 = "3"
    val idOrderLine4 = "4"
    val entrega11 = "entrega11"
    val entrega12 = "entrega12"
    val entrega13 = "entrega13"
    val entrega21 = "entrega21"
    val entrega22 = "entrega22"
    val entrega23 = "entrega23"
    val SALE_FLAG = "S"
    val SALE_FLAG_2 = ""
    val PURCHASE_FLAG = "P"
    val PURCHASE_FLAG_2 = "R"
    val BOX_AND_PREPACK_TYPE_AND_CATEGORY_1 = Constants.BOX_AND_PREPACK_TYPE_AND_CATEGORY(0)
    val BOX_AND_PREPACK_TYPE_AND_CATEGORY_2 = Constants.BOX_AND_PREPACK_TYPE_AND_CATEGORY(1)
    val UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1 = Constants.PURCHASE_ARTICLE_TYPE_AND_CATEGORY_WITH_UNIQUE_SALE_ARTICLE(0)
    val SALE_ARTICLE_TYPE_AND_CATEGORY_1 = ArticleTypeAndCategory("ZGEN","00")
    val idMaterials1 = "001"
    val idMaterials2 = "002"
    val idMaterials3 = "003"
    val idMaterials4 = "004"
    val aggPedidoToday1 = 20d
    val aggPedidoToday2 = 30d
    val aggPedidoToday3 = 40d
    val aggPedidoYesterday1 = 60d
    val aggPedidoYesterday2 = 70d
    val aggPedidoYesterday3 = 80d
    val purchaseArticleType1 = UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType
    val saleArticleType1 = SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType
    val boxPrepackArticleType1 = BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.articleType
    val boxPrepackArticleType2 = BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.articleType
    val purchaseArticleCategory1 = UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.category
    val saleArticleCategory1 = SALE_ARTICLE_TYPE_AND_CATEGORY_1.category
    val boxPrepackArticleCategory1 = BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.category
    val boxPrepackArticleCategory2 = BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.category
    val wrongOrderType = "ZZZZZZ"
    val returnedToProviderFlag = appConfig.getStockToBeDeliveredParameters.returnedToProviderFlag
    val alreadyDeliveredFlag = appConfig.getStockToBeDeliveredParameters.alreadyDeliveredFlag
    val canceledFlagList = appConfig.getStockToBeDeliveredParameters.canceledFlagList
    val flagNoAnulado = "S"
    val flagNoEntregado = "S"
    val flagNoDevuelto = "0"
    val yesterdayWithoutSeparator = Dates.timestampToStringWithTZ(yesterdayAtMidnightTs, NO_SEPARATOR, appConfig.getTimezone)
    val todayWithoutSeparator = Dates.timestampToStringWithTZ(todayTs, NO_SEPARATOR, appConfig.getTimezone)
    val dayInBetweenWithoutSeparator = Dates.timestampToStringWithTZ(dayInBetweenTs, NO_SEPARATOR, appConfig.getTimezone)
    val threeDaysAgoWithoutSeparator = Dates.timestampToStringWithTZ(new Timestamp(yesterdayAtMidnightTs.getTime - (3* 24 * 60 * 60)), NO_SEPARATOR, appConfig.getTimezone)
    val lastDayToProjectWithoutSeparator = Dates.timestampToStringWithTZ(lastDayToProjectTs, NO_SEPARATOR, appConfig.getTimezone)
    val lastDayToProjectPlus1WithoutSeparator = Dates.timestampToStringWithTZ(lastDayToProjectPlus1Ts, NO_SEPARATOR, appConfig.getTimezone)
    val lastDayToProjectPlus2WithoutSeparator = Dates.timestampToStringWithTZ(lastDayToProjectPlus2Ts, NO_SEPARATOR, appConfig.getTimezone)
    val currentTimestamp: Timestamp = new Timestamp(System.currentTimeMillis)
    val defaultUnitMeasure = "EA"
    val todayDayStrWithSeparator = Dates.timestampToStringWithTZ(todayTs, HYPHEN_SEPARATOR, appConfig.getTimezone)

    val testDoubleStructType = StructType(List(
      StructField(   DatosFijos.ccTags.idArticleTag,    StringType),
      StructField(     DatosFijos.ccTags.idStoreTag,    StringType),
      StructField(StockConsolidado.ccTags.stockDispTag, DoubleType),
      StructField(   DatosFijos.ccTags.unityMeasureTag, StringType)))

    val stockToBeDeliveredStructType = StructType(List(
      StructField( StockToBeDelivered.ccTags.idArticleTag,    StringType),
      StructField(   StockToBeDelivered.ccTags.idStoreTag,    StringType),
      StructField(               "amountToBeDeliveredN-1",    DoubleType, false),
      StructField(                 "amountToBeDeliveredN",    DoubleType, false),
      StructField(                "amountToBeDeliveredN1",    DoubleType, false),
      StructField(                "amountToBeDeliveredN2",    DoubleType, false),
      StructField(                "amountToBeDeliveredN3",    DoubleType, false),
      StructField(                "amountToBeDeliveredN4",    DoubleType, false),
      StructField(                "amountToBeDeliveredN5",    DoubleType, false),
      StructField(            Audit.ccTags.tsUpdateDlkTag, TimestampType)
    ))

    val customerReservationsStructType = StructType(List(
      StructField( StockToBeDelivered.ccTags.idArticleTag,    StringType),
      StructField(   StockToBeDelivered.ccTags.idStoreTag,    StringType),
      StructField(                  "amountReservationsN",    DoubleType, false),
      StructField(                 "amountReservationsN1",    DoubleType, false),
      StructField(                 "amountReservationsN2",    DoubleType, false),
      StructField(                 "amountReservationsN3",    DoubleType, false),
      StructField(                 "amountReservationsN4",    DoubleType, false),
      StructField(                 "amountReservationsN5",    DoubleType, false),
      StructField(            Audit.ccTags.tsUpdateDlkTag, TimestampType)
    ))

    val fixedDataInitialStructType = StructType(List(
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

    val fixedDataFinalStructType = StructType(List(
      StructField(   DatosFijos.ccTags.idArticleTag,    StringType),
      StructField(     DatosFijos.ccTags.idStoreTag,    StringType),
      StructField(    DatosFijos.ccTags.dateSaleTag,    StringType, false),
      StructField(      DatosFijos.ccTags.sectorTag,    StringType),
      StructField(DatosFijos.ccTags.unityMeasureTag,    StringType),
      StructField(         DatosFijos.ccTags.nspTag,    DoubleType),
      StructField(         DatosFijos.ccTags.nstTag,    DoubleType),
      StructField(    DatosFijos.ccTags.rotationTag,    StringType),
      StructField(      Audit.ccTags.tsUpdateDlkTag, TimestampType, false),
      StructField(                 "salesForecastN",    DoubleType),
      StructField(                "salesForecastN1",    DoubleType),
      StructField(                "salesForecastN2",    DoubleType),
      StructField(                "salesForecastN3",    DoubleType),
      StructField(                "salesForecastN4",    DoubleType),
      StructField(                "salesForecastN5",    DoubleType)
    ))

    val fixedDataWithConsolidatedAndAggSaleMovsStructType = StructType(List(
      StructField(      DatosFijos.ccTags.idArticleTag,    StringType),
      StructField(        DatosFijos.ccTags.idStoreTag,    StringType),
      StructField(         DatosFijos.ccTags.sectorTag,    StringType),
      StructField(   DatosFijos.ccTags.unityMeasureTag,    StringType),
      StructField(            DatosFijos.ccTags.nspTag,    DoubleType),
      StructField(            DatosFijos.ccTags.nstTag,    DoubleType),
      StructField(       DatosFijos.ccTags.rotationTag,    StringType),
      StructField(         Audit.ccTags.tsUpdateDlkTag, TimestampType),
      StructField(                    "salesForecastN",    DoubleType),
      StructField(                   "salesForecastN1",    DoubleType),
      StructField(                   "salesForecastN2",    DoubleType),
      StructField(                   "salesForecastN3",    DoubleType),
      StructField(                   "salesForecastN4",    DoubleType),
      StructField(                   "salesForecastN5",    DoubleType),
      StructField(JoinConsolidatedStockAndAggregatedSaleMovements.ccTags.availableStockTag,    DoubleType),
      StructField(     SaleMovements.ccTags.amountTag,     DoubleType)
    ))

    val fixedDataWithConsolidatedAndAggSaleMovsAndNsStructType = StructType(List(
      StructField(      DatosFijos.ccTags.idArticleTag,    StringType),
      StructField(        DatosFijos.ccTags.idStoreTag,    StringType),
      StructField(         DatosFijos.ccTags.sectorTag,    StringType),
      StructField(   DatosFijos.ccTags.unityMeasureTag,    StringType),
      StructField(       DatosFijos.ccTags.rotationTag,    StringType),
      StructField(         Audit.ccTags.tsUpdateDlkTag, TimestampType),
      StructField(                    "salesForecastN",    DoubleType),
      StructField(                   "salesForecastN1",    DoubleType),
      StructField(                   "salesForecastN2",    DoubleType),
      StructField(                   "salesForecastN3",    DoubleType),
      StructField(                   "salesForecastN4",    DoubleType),
      StructField(                   "salesForecastN5",    DoubleType),
      StructField(JoinConsolidatedStockAndAggregatedSaleMovements.ccTags.availableStockTag,    DoubleType),
      StructField(     SaleMovements.ccTags.amountTag,     DoubleType),
      StructField(                     TempTags.nsTag,     DoubleType)
    ))

    val fixedDataWithConsolidatedAndAggSaleMovsAndOrdersAndCustomerReservationsStructType = StructType(List(
      StructField(                                          DatosFijos.ccTags.idArticleTag,    StringType, false),
      StructField(                                            DatosFijos.ccTags.idStoreTag,    StringType, false),
      StructField(                                             DatosFijos.ccTags.sectorTag,    StringType),
      StructField(                                       DatosFijos.ccTags.unityMeasureTag,    StringType),
      StructField(                                           DatosFijos.ccTags.rotationTag,    StringType),
      StructField(                                             Audit.ccTags.tsUpdateDlkTag, TimestampType),
      StructField(                                                        "salesForecastN",    DoubleType),
      StructField(                                                       "salesForecastN1",    DoubleType),
      StructField(                                                       "salesForecastN2",    DoubleType),
      StructField(                                                       "salesForecastN3",    DoubleType),
      StructField(                                                       "salesForecastN4",    DoubleType),
      StructField(                                                       "salesForecastN5",    DoubleType),
      StructField(JoinConsolidatedStockAndAggregatedSaleMovements.ccTags.availableStockTag,    DoubleType),
      StructField(                                          SaleMovements.ccTags.amountTag,    DoubleType),
      StructField(                                                "amountToBeDeliveredN-1",    DoubleType, false),
      StructField(                                                  "amountToBeDeliveredN",    DoubleType, false),
      StructField(                                                 "amountToBeDeliveredN1",    DoubleType, false),
      StructField(                                                 "amountToBeDeliveredN2",    DoubleType, false),
      StructField(                                                 "amountToBeDeliveredN3",    DoubleType, false),
      StructField(                                                 "amountToBeDeliveredN4",    DoubleType, false),
      StructField(                                                 "amountToBeDeliveredN5",    DoubleType, false),
      StructField(                                                   "amountReservationsN",    DoubleType, false),
      StructField(                                                  "amountReservationsN1",    DoubleType, false),
      StructField(                                                  "amountReservationsN2",    DoubleType, false),
      StructField(                                                  "amountReservationsN3",    DoubleType, false),
      StructField(                                                  "amountReservationsN4",    DoubleType, false),
      StructField(                                                  "amountReservationsN5",    DoubleType, false),
      StructField(                                                          TempTags.nsTag,    DoubleType)
    ))

    val beforeCalculateStockAtpNStructType = StructType(List(
      StructField(                                            DatosFijos.ccTags.idStoreTag,    StringType, false),
      StructField(                                          DatosFijos.ccTags.idArticleTag,    StringType, false),
      StructField(                                             DatosFijos.ccTags.sectorTag,    StringType),
      StructField(                                       DatosFijos.ccTags.unityMeasureTag,    StringType),
      StructField(                                           DatosFijos.ccTags.rotationTag,    StringType),
      StructField(                                             Audit.ccTags.tsUpdateDlkTag,  TimestampType, false),
      StructField(                                                        "salesForecastN",    DoubleType),
      StructField(                                                       "salesForecastN1",    DoubleType),
      StructField(                                                       "salesForecastN2",    DoubleType),
      StructField(                                                       "salesForecastN3",    DoubleType),
      StructField(                                                       "salesForecastN4",    DoubleType),
      StructField(                                                       "salesForecastN5",    DoubleType),
      StructField(JoinConsolidatedStockAndAggregatedSaleMovements.ccTags.availableStockTag,    DoubleType),
      StructField(                                          SaleMovements.ccTags.amountTag,    DoubleType),
      StructField(                                                "amountToBeDeliveredN-1",    DoubleType, false),
      StructField(                                                  "amountToBeDeliveredN",    DoubleType, false),
      StructField(                                                 "amountToBeDeliveredN1",    DoubleType, false),
      StructField(                                                 "amountToBeDeliveredN2",    DoubleType, false),
      StructField(                                                 "amountToBeDeliveredN3",    DoubleType, false),
      StructField(                                                 "amountToBeDeliveredN4",    DoubleType, false),
      StructField(                                                 "amountToBeDeliveredN5",    DoubleType, false),
      StructField(                                                   "amountReservationsN",    DoubleType, false),
      StructField(                                                  "amountReservationsN1",    DoubleType, false),
      StructField(                                                  "amountReservationsN2",    DoubleType, false),
      StructField(                                                  "amountReservationsN3",    DoubleType, false),
      StructField(                                                  "amountReservationsN4",    DoubleType, false),
      StructField(                                                  "amountReservationsN5",    DoubleType, false),
      StructField(                                           StoreType.ccTags.typeStoreTag,    BooleanType),
      StructField(                                                       AggPP.ccTags.isPP,    BooleanType),
      StructField(                                           Audit.ccTags.userUpdateDlkTag,    StringType, false),
      StructField(                                                          TempTags.nsTag,    DoubleType)
    ))

    val afterStockToBeDeliveredCalculusStructType = StructType(List(
      StructField(                                            DatosFijos.ccTags.idStoreTag,    StringType, false),
      StructField(                                          DatosFijos.ccTags.idArticleTag,    StringType, false),
      StructField(                                             DatosFijos.ccTags.sectorTag,    StringType),
      StructField(                                       DatosFijos.ccTags.unityMeasureTag,    StringType),
      StructField(                                           DatosFijos.ccTags.rotationTag,    StringType),
      StructField(                                             Audit.ccTags.tsUpdateDlkTag,  TimestampType, false),
      StructField(                                                        "salesForecastN",    DoubleType),
      StructField(                                                       "salesForecastN1",    DoubleType),
      StructField(                                                       "salesForecastN2",    DoubleType),
      StructField(                                                       "salesForecastN3",    DoubleType),
      StructField(                                                       "salesForecastN4",    DoubleType),
      StructField(                                                       "salesForecastN5",    DoubleType),
      StructField(JoinConsolidatedStockAndAggregatedSaleMovements.ccTags.availableStockTag,    DoubleType),
      StructField(                                          SaleMovements.ccTags.amountTag,    DoubleType),
      StructField(                                                "amountToBeDeliveredN-1",    DoubleType, false),
      StructField(                                                  "amountToBeDeliveredN",    DoubleType, false),
      StructField(                                                 "amountToBeDeliveredN1",    DoubleType, false),
      StructField(                                                 "amountToBeDeliveredN2",    DoubleType, false),
      StructField(                                                 "amountToBeDeliveredN3",    DoubleType, false),
      StructField(                                                 "amountToBeDeliveredN4",    DoubleType, false),
      StructField(                                                 "amountToBeDeliveredN5",    DoubleType, false),
      StructField(                                                   "amountReservationsN",    DoubleType, false),
      StructField(                                                  "amountReservationsN1",    DoubleType, false),
      StructField(                                                  "amountReservationsN2",    DoubleType, false),
      StructField(                                                  "amountReservationsN3",    DoubleType, false),
      StructField(                                                  "amountReservationsN4",    DoubleType, false),
      StructField(                                                  "amountReservationsN5",    DoubleType, false),
      StructField(                                           StoreType.ccTags.typeStoreTag,    BooleanType),
      StructField(                                                       AggPP.ccTags.isPP,    BooleanType),
      StructField(                                           Audit.ccTags.userUpdateDlkTag,    StringType, false),
      StructField(                                                          TempTags.nsTag,    DoubleType),
      StructField(                                         TempTags.tempStockToBeDelivered,    DoubleType, false)
    ))

    val afterCalculateStockAtpNStructType = StructType(List(
      StructField(        DatosFijos.ccTags.idStoreTag,    StringType, false),
      StructField(      DatosFijos.ccTags.idArticleTag,    StringType, false),
      StructField(         DatosFijos.ccTags.sectorTag,    StringType),
      StructField(   DatosFijos.ccTags.unityMeasureTag,    StringType),
      StructField(         Audit.ccTags.tsUpdateDlkTag, TimestampType, false),
      StructField(                   "salesForecastN1",    DoubleType),
      StructField(                   "salesForecastN2",    DoubleType),
      StructField(                   "salesForecastN3",    DoubleType),
      StructField(                   "salesForecastN4",    DoubleType),
      StructField(                   "salesForecastN5",    DoubleType),
      StructField(              "amountToBeDeliveredN",    DoubleType, false),
      StructField(             "amountToBeDeliveredN1",    DoubleType, false),
      StructField(             "amountToBeDeliveredN2",    DoubleType, false),
      StructField(             "amountToBeDeliveredN3",    DoubleType, false),
      StructField(             "amountToBeDeliveredN4",    DoubleType, false),
      StructField(             "amountToBeDeliveredN5",    DoubleType, false),
      StructField(              "amountReservationsN1",    DoubleType, false),
      StructField(              "amountReservationsN2",    DoubleType, false),
      StructField(              "amountReservationsN3",    DoubleType, false),
      StructField(              "amountReservationsN4",    DoubleType, false),
      StructField(              "amountReservationsN5",    DoubleType, false),
      StructField(                   AggPP.ccTags.isPP,   BooleanType),
      StructField(       Audit.ccTags.userUpdateDlkTag,    StringType, false),
      StructField(                      TempTags.nsTag,    DoubleType),
      StructField(            StockATP.ccTags.nDateTag, TimestampType, false),
      StructField(                         "stockAtpN",    DoubleType)
    ))

    val afterCalculateStockAtpNWith1ForecastDayStructType = StructType(List(
      StructField(        DatosFijos.ccTags.idStoreTag,    StringType, false),
      StructField(      DatosFijos.ccTags.idArticleTag,    StringType, false),
      StructField(         DatosFijos.ccTags.sectorTag,    StringType),
      StructField(   DatosFijos.ccTags.unityMeasureTag,    StringType),
      StructField(         Audit.ccTags.tsUpdateDlkTag, TimestampType, false),
      StructField(                   "salesForecastN1",    DoubleType),
      StructField(              "amountToBeDeliveredN",    DoubleType, false),
      StructField(             "amountToBeDeliveredN1",    DoubleType, false),
      StructField(              "amountReservationsN1",    DoubleType, false),
      StructField(                   AggPP.ccTags.isPP,   BooleanType),
      StructField(       Audit.ccTags.userUpdateDlkTag,    StringType, false),
      StructField(                      TempTags.nsTag,    DoubleType),
      StructField(            StockATP.ccTags.nDateTag, TimestampType, false),
      StructField(                         "stockAtpN",    DoubleType)
    ))

    val afterCalculateStockAtpNWith101ForecastDaysStructType = StructType(List(
      StructField(        DatosFijos.ccTags.idStoreTag,    StringType, false),
      StructField(      DatosFijos.ccTags.idArticleTag,    StringType, false),
      StructField(         DatosFijos.ccTags.sectorTag,    StringType),
      StructField(   DatosFijos.ccTags.unityMeasureTag,    StringType),
      StructField(         Audit.ccTags.tsUpdateDlkTag, TimestampType, false),
      StructField(                   "salesForecastN1",    DoubleType),
      StructField(                   "salesForecastN2",    DoubleType),
      StructField(                   "salesForecastN3",    DoubleType),
      StructField(                   "salesForecastN4",    DoubleType),
      StructField(                   "salesForecastN5",    DoubleType),
      StructField(                   "salesForecastN6",    DoubleType),
      StructField(                   "salesForecastN7",    DoubleType),
      StructField(                   "salesForecastN8",    DoubleType),
      StructField(                   "salesForecastN9",    DoubleType),
      StructField(                   "salesForecastN10",    DoubleType),
      StructField(                   "salesForecastN11",    DoubleType),
      StructField(                   "salesForecastN12",    DoubleType),
      StructField(                   "salesForecastN13",    DoubleType),
      StructField(                   "salesForecastN14",    DoubleType),
      StructField(                   "salesForecastN15",    DoubleType),
      StructField(                   "salesForecastN16",    DoubleType),
      StructField(                   "salesForecastN17",    DoubleType),
      StructField(                   "salesForecastN18",    DoubleType),
      StructField(                   "salesForecastN19",    DoubleType),
      StructField(                   "salesForecastN20",    DoubleType),
      StructField(                   "salesForecastN21",    DoubleType),
      StructField(                   "salesForecastN22",    DoubleType),
      StructField(                   "salesForecastN23",    DoubleType),
      StructField(                   "salesForecastN24",    DoubleType),
      StructField(                   "salesForecastN25",    DoubleType),
      StructField(                   "salesForecastN26",    DoubleType),
      StructField(                   "salesForecastN27",    DoubleType),
      StructField(                   "salesForecastN28",    DoubleType),
      StructField(                   "salesForecastN29",    DoubleType),
      StructField(                   "salesForecastN30",    DoubleType),
      StructField(                   "salesForecastN31",    DoubleType),
      StructField(                   "salesForecastN32",    DoubleType),
      StructField(                   "salesForecastN33",    DoubleType),
      StructField(                   "salesForecastN34",    DoubleType),
      StructField(                   "salesForecastN35",    DoubleType),
      StructField(                   "salesForecastN36",    DoubleType),
      StructField(                   "salesForecastN37",    DoubleType),
      StructField(                   "salesForecastN38",    DoubleType),
      StructField(                   "salesForecastN39",    DoubleType),
      StructField(                   "salesForecastN40",    DoubleType),
      StructField(                   "salesForecastN41",    DoubleType),
      StructField(                   "salesForecastN42",    DoubleType),
      StructField(                   "salesForecastN43",    DoubleType),
      StructField(                   "salesForecastN44",    DoubleType),
      StructField(                   "salesForecastN45",    DoubleType),
      StructField(                   "salesForecastN46",    DoubleType),
      StructField(                   "salesForecastN47",    DoubleType),
      StructField(                   "salesForecastN48",    DoubleType),
      StructField(                   "salesForecastN49",    DoubleType),
      StructField(                   "salesForecastN50",    DoubleType),
      StructField(                   "salesForecastN51",    DoubleType),
      StructField(                   "salesForecastN52",    DoubleType),
      StructField(                   "salesForecastN53",    DoubleType),
      StructField(                   "salesForecastN54",    DoubleType),
      StructField(                   "salesForecastN55",    DoubleType),
      StructField(                   "salesForecastN56",    DoubleType),
      StructField(                   "salesForecastN57",    DoubleType),
      StructField(                   "salesForecastN58",    DoubleType),
      StructField(                   "salesForecastN59",    DoubleType),
      StructField(                   "salesForecastN60",    DoubleType),
      StructField(                   "salesForecastN61",    DoubleType),
      StructField(                   "salesForecastN62",    DoubleType),
      StructField(                   "salesForecastN63",    DoubleType),
      StructField(                   "salesForecastN64",    DoubleType),
      StructField(                   "salesForecastN65",    DoubleType),
      StructField(                   "salesForecastN66",    DoubleType),
      StructField(                   "salesForecastN67",    DoubleType),
      StructField(                   "salesForecastN68",    DoubleType),
      StructField(                   "salesForecastN69",    DoubleType),
      StructField(                   "salesForecastN70",    DoubleType),
      StructField(                   "salesForecastN71",    DoubleType),
      StructField(                   "salesForecastN72",    DoubleType),
      StructField(                   "salesForecastN73",    DoubleType),
      StructField(                   "salesForecastN74",    DoubleType),
      StructField(                   "salesForecastN75",    DoubleType),
      StructField(                   "salesForecastN76",    DoubleType),
      StructField(                   "salesForecastN77",    DoubleType),
      StructField(                   "salesForecastN78",    DoubleType),
      StructField(                   "salesForecastN79",    DoubleType),
      StructField(                   "salesForecastN80",    DoubleType),
      StructField(                   "salesForecastN81",    DoubleType),
      StructField(                   "salesForecastN82",    DoubleType),
      StructField(                   "salesForecastN83",    DoubleType),
      StructField(                   "salesForecastN84",    DoubleType),
      StructField(                   "salesForecastN85",    DoubleType),
      StructField(                   "salesForecastN86",    DoubleType),
      StructField(                   "salesForecastN87",    DoubleType),
      StructField(                   "salesForecastN88",    DoubleType),
      StructField(                   "salesForecastN89",    DoubleType),
      StructField(                   "salesForecastN90",    DoubleType),
      StructField(                   "salesForecastN91",    DoubleType),
      StructField(                   "salesForecastN92",    DoubleType),
      StructField(                   "salesForecastN93",    DoubleType),
      StructField(                   "salesForecastN94",    DoubleType),
      StructField(                   "salesForecastN95",    DoubleType),
      StructField(                   "salesForecastN96",    DoubleType),
      StructField(                   "salesForecastN97",    DoubleType),
      StructField(                   "salesForecastN98",    DoubleType),
      StructField(                   "salesForecastN99",    DoubleType),
      StructField(                   "salesForecastN100",    DoubleType),
      StructField(                   "salesForecastN101",    DoubleType),
      StructField(               "amountToBeDeliveredN",    DoubleType, false),
      StructField(              "amountToBeDeliveredN1",    DoubleType, false),
      StructField(              "amountToBeDeliveredN2",    DoubleType, false),
      StructField(              "amountToBeDeliveredN3",    DoubleType, false),
      StructField(              "amountToBeDeliveredN4",    DoubleType, false),
      StructField(              "amountToBeDeliveredN5",    DoubleType, false),
      StructField(              "amountToBeDeliveredN6",    DoubleType, false),
      StructField(              "amountToBeDeliveredN7",    DoubleType, false),
      StructField(              "amountToBeDeliveredN8",    DoubleType, false),
      StructField(              "amountToBeDeliveredN9",    DoubleType, false),
      StructField(             "amountToBeDeliveredN10",    DoubleType, false),
      StructField(             "amountToBeDeliveredN11",    DoubleType, false),
      StructField(             "amountToBeDeliveredN12",    DoubleType, false),
      StructField(             "amountToBeDeliveredN13",    DoubleType, false),
      StructField(             "amountToBeDeliveredN14",    DoubleType, false),
      StructField(             "amountToBeDeliveredN15",    DoubleType, false),
      StructField(             "amountToBeDeliveredN16",    DoubleType, false),
      StructField(             "amountToBeDeliveredN17",    DoubleType, false),
      StructField(             "amountToBeDeliveredN18",    DoubleType, false),
      StructField(             "amountToBeDeliveredN19",    DoubleType, false),
      StructField(             "amountToBeDeliveredN20",    DoubleType, false),
      StructField(             "amountToBeDeliveredN21",    DoubleType, false),
      StructField(             "amountToBeDeliveredN22",    DoubleType, false),
      StructField(             "amountToBeDeliveredN23",    DoubleType, false),
      StructField(             "amountToBeDeliveredN24",    DoubleType, false),
      StructField(             "amountToBeDeliveredN25",    DoubleType, false),
      StructField(             "amountToBeDeliveredN26",    DoubleType, false),
      StructField(             "amountToBeDeliveredN27",    DoubleType, false),
      StructField(             "amountToBeDeliveredN28",    DoubleType, false),
      StructField(             "amountToBeDeliveredN29",    DoubleType, false),
      StructField(             "amountToBeDeliveredN30",    DoubleType, false),
      StructField(             "amountToBeDeliveredN31",    DoubleType, false),
      StructField(             "amountToBeDeliveredN32",    DoubleType, false),
      StructField(             "amountToBeDeliveredN33",    DoubleType, false),
      StructField(             "amountToBeDeliveredN34",    DoubleType, false),
      StructField(             "amountToBeDeliveredN35",    DoubleType, false),
      StructField(             "amountToBeDeliveredN36",    DoubleType, false),
      StructField(             "amountToBeDeliveredN37",    DoubleType, false),
      StructField(             "amountToBeDeliveredN38",    DoubleType, false),
      StructField(             "amountToBeDeliveredN39",    DoubleType, false),
      StructField(             "amountToBeDeliveredN40",    DoubleType, false),
      StructField(             "amountToBeDeliveredN41",    DoubleType, false),
      StructField(             "amountToBeDeliveredN42",    DoubleType, false),
      StructField(             "amountToBeDeliveredN43",    DoubleType, false),
      StructField(             "amountToBeDeliveredN44",    DoubleType, false),
      StructField(             "amountToBeDeliveredN45",    DoubleType, false),
      StructField(             "amountToBeDeliveredN46",    DoubleType, false),
      StructField(             "amountToBeDeliveredN47",    DoubleType, false),
      StructField(             "amountToBeDeliveredN48",    DoubleType, false),
      StructField(             "amountToBeDeliveredN49",    DoubleType, false),
      StructField(             "amountToBeDeliveredN50",    DoubleType, false),
      StructField(             "amountToBeDeliveredN51",    DoubleType, false),
      StructField(             "amountToBeDeliveredN52",    DoubleType, false),
      StructField(             "amountToBeDeliveredN53",    DoubleType, false),
      StructField(             "amountToBeDeliveredN54",    DoubleType, false),
      StructField(             "amountToBeDeliveredN55",    DoubleType, false),
      StructField(             "amountToBeDeliveredN56",    DoubleType, false),
      StructField(             "amountToBeDeliveredN57",    DoubleType, false),
      StructField(             "amountToBeDeliveredN58",    DoubleType, false),
      StructField(             "amountToBeDeliveredN59",    DoubleType, false),
      StructField(             "amountToBeDeliveredN60",    DoubleType, false),
      StructField(             "amountToBeDeliveredN61",    DoubleType, false),
      StructField(             "amountToBeDeliveredN62",    DoubleType, false),
      StructField(             "amountToBeDeliveredN63",    DoubleType, false),
      StructField(             "amountToBeDeliveredN64",    DoubleType, false),
      StructField(             "amountToBeDeliveredN65",    DoubleType, false),
      StructField(             "amountToBeDeliveredN66",    DoubleType, false),
      StructField(             "amountToBeDeliveredN67",    DoubleType, false),
      StructField(             "amountToBeDeliveredN68",    DoubleType, false),
      StructField(             "amountToBeDeliveredN69",    DoubleType, false),
      StructField(             "amountToBeDeliveredN70",    DoubleType, false),
      StructField(             "amountToBeDeliveredN71",    DoubleType, false),
      StructField(             "amountToBeDeliveredN72",    DoubleType, false),
      StructField(             "amountToBeDeliveredN73",    DoubleType, false),
      StructField(             "amountToBeDeliveredN74",    DoubleType, false),
      StructField(             "amountToBeDeliveredN75",    DoubleType, false),
      StructField(             "amountToBeDeliveredN76",    DoubleType, false),
      StructField(             "amountToBeDeliveredN77",    DoubleType, false),
      StructField(             "amountToBeDeliveredN78",    DoubleType, false),
      StructField(             "amountToBeDeliveredN79",    DoubleType, false),
      StructField(             "amountToBeDeliveredN80",    DoubleType, false),
      StructField(             "amountToBeDeliveredN81",    DoubleType, false),
      StructField(             "amountToBeDeliveredN82",    DoubleType, false),
      StructField(             "amountToBeDeliveredN83",    DoubleType, false),
      StructField(             "amountToBeDeliveredN84",    DoubleType, false),
      StructField(             "amountToBeDeliveredN85",    DoubleType, false),
      StructField(             "amountToBeDeliveredN86",    DoubleType, false),
      StructField(             "amountToBeDeliveredN87",    DoubleType, false),
      StructField(             "amountToBeDeliveredN88",    DoubleType, false),
      StructField(             "amountToBeDeliveredN89",    DoubleType, false),
      StructField(             "amountToBeDeliveredN90",    DoubleType, false),
      StructField(             "amountToBeDeliveredN91",    DoubleType, false),
      StructField(             "amountToBeDeliveredN92",    DoubleType, false),
      StructField(             "amountToBeDeliveredN93",    DoubleType, false),
      StructField(             "amountToBeDeliveredN94",    DoubleType, false),
      StructField(             "amountToBeDeliveredN95",    DoubleType, false),
      StructField(             "amountToBeDeliveredN96",    DoubleType, false),
      StructField(             "amountToBeDeliveredN97",    DoubleType, false),
      StructField(             "amountToBeDeliveredN98",    DoubleType, false),
      StructField(             "amountToBeDeliveredN99",    DoubleType, false),
      StructField(             "amountToBeDeliveredN100",    DoubleType, false),
      StructField(             "amountToBeDeliveredN101",    DoubleType, false),
      StructField(                   "amountReservationsN",    DoubleType, false),
      StructField(                   "amountReservationsN1",    DoubleType, false),
      StructField(                   "amountReservationsN2",    DoubleType, false),
      StructField(                   "amountReservationsN3",    DoubleType, false),
      StructField(                   "amountReservationsN4",    DoubleType, false),
      StructField(                   "amountReservationsN5",    DoubleType, false),
      StructField(                   "amountReservationsN6",    DoubleType, false),
      StructField(                   "amountReservationsN7",    DoubleType, false),
      StructField(                   "amountReservationsN8",    DoubleType, false),
      StructField(                   "amountReservationsN9",    DoubleType, false),
      StructField(                   "amountReservationsN10",    DoubleType, false),
      StructField(                   "amountReservationsN11",    DoubleType, false),
      StructField(                   "amountReservationsN12",    DoubleType, false),
      StructField(                   "amountReservationsN13",    DoubleType, false),
      StructField(                   "amountReservationsN14",    DoubleType, false),
      StructField(                   "amountReservationsN15",    DoubleType, false),
      StructField(                   "amountReservationsN16",    DoubleType, false),
      StructField(                   "amountReservationsN17",    DoubleType, false),
      StructField(                   "amountReservationsN18",    DoubleType, false),
      StructField(                   "amountReservationsN19",    DoubleType, false),
      StructField(                   "amountReservationsN20",    DoubleType, false),
      StructField(                   "amountReservationsN21",    DoubleType, false),
      StructField(                   "amountReservationsN22",    DoubleType, false),
      StructField(                   "amountReservationsN23",    DoubleType, false),
      StructField(                   "amountReservationsN24",    DoubleType, false),
      StructField(                   "amountReservationsN25",    DoubleType, false),
      StructField(                   "amountReservationsN26",    DoubleType, false),
      StructField(                   "amountReservationsN27",    DoubleType, false),
      StructField(                   "amountReservationsN28",    DoubleType, false),
      StructField(                   "amountReservationsN29",    DoubleType, false),
      StructField(                   "amountReservationsN30",    DoubleType, false),
      StructField(                   "amountReservationsN31",    DoubleType, false),
      StructField(                   "amountReservationsN32",    DoubleType, false),
      StructField(                   "amountReservationsN33",    DoubleType, false),
      StructField(                   "amountReservationsN34",    DoubleType, false),
      StructField(                   "amountReservationsN35",    DoubleType, false),
      StructField(                   "amountReservationsN36",    DoubleType, false),
      StructField(                   "amountReservationsN37",    DoubleType, false),
      StructField(                   "amountReservationsN38",    DoubleType, false),
      StructField(                   "amountReservationsN39",    DoubleType, false),
      StructField(                   "amountReservationsN40",    DoubleType, false),
      StructField(                   "amountReservationsN41",    DoubleType, false),
      StructField(                   "amountReservationsN42",    DoubleType, false),
      StructField(                   "amountReservationsN43",    DoubleType, false),
      StructField(                   "amountReservationsN44",    DoubleType, false),
      StructField(                   "amountReservationsN45",    DoubleType, false),
      StructField(                   "amountReservationsN46",    DoubleType, false),
      StructField(                   "amountReservationsN47",    DoubleType, false),
      StructField(                   "amountReservationsN48",    DoubleType, false),
      StructField(                   "amountReservationsN49",    DoubleType, false),
      StructField(                   "amountReservationsN50",    DoubleType, false),
      StructField(                   "amountReservationsN51",    DoubleType, false),
      StructField(                   "amountReservationsN52",    DoubleType, false),
      StructField(                   "amountReservationsN53",    DoubleType, false),
      StructField(                   "amountReservationsN54",    DoubleType, false),
      StructField(                   "amountReservationsN55",    DoubleType, false),
      StructField(                   "amountReservationsN56",    DoubleType, false),
      StructField(                   "amountReservationsN57",    DoubleType, false),
      StructField(                   "amountReservationsN58",    DoubleType, false),
      StructField(                   "amountReservationsN59",    DoubleType, false),
      StructField(                   "amountReservationsN60",    DoubleType, false),
      StructField(                   "amountReservationsN61",    DoubleType, false),
      StructField(                   "amountReservationsN62",    DoubleType, false),
      StructField(                   "amountReservationsN63",    DoubleType, false),
      StructField(                   "amountReservationsN64",    DoubleType, false),
      StructField(                   "amountReservationsN65",    DoubleType, false),
      StructField(                   "amountReservationsN66",    DoubleType, false),
      StructField(                   "amountReservationsN67",    DoubleType, false),
      StructField(                   "amountReservationsN68",    DoubleType, false),
      StructField(                   "amountReservationsN69",    DoubleType, false),
      StructField(                   "amountReservationsN70",    DoubleType, false),
      StructField(                   "amountReservationsN71",    DoubleType, false),
      StructField(                   "amountReservationsN72",    DoubleType, false),
      StructField(                   "amountReservationsN73",    DoubleType, false),
      StructField(                   "amountReservationsN74",    DoubleType, false),
      StructField(                   "amountReservationsN75",    DoubleType, false),
      StructField(                   "amountReservationsN76",    DoubleType, false),
      StructField(                   "amountReservationsN77",    DoubleType, false),
      StructField(                   "amountReservationsN78",    DoubleType, false),
      StructField(                   "amountReservationsN79",    DoubleType, false),
      StructField(                   "amountReservationsN80",    DoubleType, false),
      StructField(                   "amountReservationsN81",    DoubleType, false),
      StructField(                   "amountReservationsN82",    DoubleType, false),
      StructField(                   "amountReservationsN83",    DoubleType, false),
      StructField(                   "amountReservationsN84",    DoubleType, false),
      StructField(                   "amountReservationsN85",    DoubleType, false),
      StructField(                   "amountReservationsN86",    DoubleType, false),
      StructField(                   "amountReservationsN87",    DoubleType, false),
      StructField(                   "amountReservationsN88",    DoubleType, false),
      StructField(                   "amountReservationsN89",    DoubleType, false),
      StructField(                   "amountReservationsN90",    DoubleType, false),
      StructField(                   "amountReservationsN91",    DoubleType, false),
      StructField(                   "amountReservationsN92",    DoubleType, false),
      StructField(                   "amountReservationsN93",    DoubleType, false),
      StructField(                   "amountReservationsN94",    DoubleType, false),
      StructField(                   "amountReservationsN95",    DoubleType, false),
      StructField(                   "amountReservationsN96",    DoubleType, false),
      StructField(                   "amountReservationsN97",    DoubleType, false),
      StructField(                   "amountReservationsN98",    DoubleType, false),
      StructField(                   "amountReservationsN99",    DoubleType, false),
      StructField(                   "amountReservationsN100",    DoubleType, false),
      StructField(                   "amountReservationsN101",    DoubleType, false),
      StructField(                   AggPP.ccTags.isPP,   BooleanType),
      StructField(       Audit.ccTags.userUpdateDlkTag,    StringType, false),
      StructField(                      TempTags.nsTag,    DoubleType),
      StructField(            StockATP.ccTags.nDateTag, TimestampType, false),
      StructField(                         "stockAtpN",    DoubleType)
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

    val checkControlExecutionStructType = StructType(List(
      StructField(DatosFijos.ccTags.idArticleTag,       StringType),
      StructField(DatosFijos.ccTags.idStoreTag,         StringType),
      StructField(Audit.ccTags.tsUpdateDlkTag,       TimestampType)))
  }

  sealed trait SqlVariablesStock extends SqlVariables{
    val stockTypeOMS = appConfig.getStockTypeOMS
    val stockTypeAPRO = 2
  }

  sealed trait SqlVariablesSaleMovements extends SqlVariables{
    val salesMovementCode = "601"
    val otherMovementCode = "007"
    val todayTs = Dates.getDateAtMidnight(ts)._1
    val yesterdayTs: Timestamp = Dates.subtractDaysToTimestamp(ts)
    val tomorrowTs: Timestamp = Dates.sumDaysToTimestamp(ts)
  }

  "nullAmountsToZeroAndDateAsStringInCustomerReservations " must " set to 0 if amount of customer reservation is null" taggedAs (UnitTestTag) in new SqlVariablesPedidos  {
    val reservationAmount = 10d

    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List[CustomerReservations](
      CustomerReservations(idArticle1, idStore1, Some(todayTs),            Some(reservationAmount), todayTs),
      CustomerReservations(idArticle1, idStore1, Some(lastDayToProjectTs), Some(reservationAmount), todayTs),
      CustomerReservations(idArticle1, idStore1, Some(todayTs),            None,                    todayTs)
    )).toDS

    val dsExpected: Dataset[CustomerReservationsView] = sc.parallelize(List[CustomerReservationsView](
      CustomerReservationsView(idArticle1, idStore1, todayWithoutSeparator,            reservationAmount, todayTs),
      CustomerReservationsView(idArticle1, idStore1, lastDayToProjectWithoutSeparator, reservationAmount, todayTs),
      CustomerReservationsView(idArticle1, idStore1, todayWithoutSeparator,            0,                 todayTs)
    )).toDS

    val dsResult =
      dsCustomerReservations
        .transform(Sql.nullAmountsToZeroAndDateAsStringInCustomerReservations(spark))
        .sort(CustomerReservations.ccTags.idArticleTag, CustomerReservations.ccTags.idStoreTag)
    assertDatasetEquals(dsResult, dsExpected)
  }

  "filterCustomerReservationsBetweenDays " must " filter the customer reservations between days passed" taggedAs (UnitTestTag) in new SqlVariablesPedidos  {
    val reservationAmount = 10d

    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List[CustomerReservations](
      CustomerReservations(idArticle1, idStore1, Some(todayTs),                 Some(reservationAmount), todayTs),
      CustomerReservations(idArticle1, idStore1, Some(lastDayToProjectTs),      Some(reservationAmount), todayTs),
      CustomerReservations(idArticle1, idStore1, Some(todayTs),                 Some(reservationAmount), todayTs),
      CustomerReservations(idArticle1, idStore1, Some(yesterdayAtMidnightTs),   Some(reservationAmount), todayTs),
      CustomerReservations(idArticle1, idStore1, Some(lastDayToProjectPlus1Ts), Some(reservationAmount), todayTs),
      CustomerReservations(idArticle1, idStore1, Some(lastDayToProjectPlus2Ts), Some(reservationAmount), todayTs)
    )).toDS

    val dsExpected: Dataset[CustomerReservations] = sc.parallelize(List[CustomerReservations](
      CustomerReservations(idArticle1, idStore1, Some(todayTs),            Some(reservationAmount), todayTs),
      CustomerReservations(idArticle1, idStore1, Some(lastDayToProjectTs), Some(reservationAmount), todayTs),
      CustomerReservations(idArticle1, idStore1, Some(todayTs),            Some(reservationAmount), todayTs)
    )).toDS

    val dsResult =
      dsCustomerReservations
        .transform(Sql.filterCustomerReservationsBetweenDays(todayTs, lastDayToProjectPlus1Ts))
        .sort(CustomerReservations.ccTags.idArticleTag, CustomerReservations.ccTags.idStoreTag)
    assertDatasetEquals(dsResult, dsExpected)
  }

  it must " filter the customer reservations with reservation date null" taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val reservationAmount = 10d

    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List[CustomerReservations](
      CustomerReservations(idArticle1, idStore1, Some(todayTs), Some(reservationAmount), todayTs),
      CustomerReservations(idArticle1, idStore1, Some(todayTs), Some(reservationAmount), todayTs),
      CustomerReservations(idArticle1, idStore1, Some(todayTs), Some(reservationAmount), todayTs),
      CustomerReservations(idArticle1, idStore1, None,          Some(reservationAmount), todayTs)
    )).toDS

    val dsExpected: Dataset[CustomerReservations] = sc.parallelize(List[CustomerReservations](
      CustomerReservations(idArticle1, idStore1, Some(todayTs), Some(reservationAmount), todayTs),
      CustomerReservations(idArticle1, idStore1, Some(todayTs), Some(reservationAmount), todayTs),
      CustomerReservations(idArticle1, idStore1, Some(todayTs), Some(reservationAmount), todayTs)
    )).toDS

    val dsResult =
      dsCustomerReservations
        .transform(Sql.filterCustomerReservationsBetweenDays(todayTs, lastDayToProjectPlus1Ts))
        .sort(CustomerReservations.ccTags.idArticleTag, CustomerReservations.ccTags.idStoreTag)
    assertDatasetEquals(dsResult, dsExpected)
  }

  it must " filter the customer reservations when there are no customer reservations" taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val dsCustomerReservations: Dataset[CustomerReservations] = sc.parallelize(List.empty[CustomerReservations]).toDS

    val dsExpected: Dataset[CustomerReservations] = sc.parallelize(List.empty[CustomerReservations]).toDS

    val dsResult =
      dsCustomerReservations
        .transform(Sql.filterCustomerReservationsBetweenDays(todayTs, lastDayToProjectPlus1Ts))
        .sort(CustomerReservations.ccTags.idArticleTag, CustomerReservations.ccTags.idStoreTag)
    assertDatasetEquals(dsResult, dsExpected)
  }

  "filterStockConsolidadoAndManageNulls" must " filter the stock consolidado by type of stock and change nulls to 0" taggedAs (UnitTestTag) in new SqlVariablesStock  {

    val dsStockConsolidado: Dataset[StockConsolidado] = sc.parallelize(List[StockConsolidado](
      StockConsolidado(idArticle1, idStore1, stockTypeOMS,  measureUnitEA, Some(stockConsolidado), ts),
      StockConsolidado(idArticle1, idStore2, stockTypeOMS,  measureUnitEA, Some(stockConsolidado), ts),
      StockConsolidado(idArticle1, idStore3, stockTypeOMS,  measureUnitEA, Some(stockConsolidado), ts),
      StockConsolidado(idArticle1, idStore1, stockTypeAPRO, measureUnitEA, Some(stockConsolidado), ts),
      StockConsolidado(idArticle1, idStore2, stockTypeAPRO, measureUnitEA, Some(stockConsolidado), ts),
      StockConsolidado(idArticle1, idStore3, stockTypeAPRO, measureUnitEA, Some(stockConsolidado), ts),
      StockConsolidado(idArticle1, idStore3, stockTypeOMS,  measureUnitEA, None,                   ts)
    )).toDS

    val dsExpected: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, stockConsolidado, ts),
      StockConsolidadoView(idArticle1, idStore2, measureUnitEA, stockConsolidado, ts),
      StockConsolidadoView(idArticle1, idStore3, measureUnitEA, stockConsolidado, ts),
      StockConsolidadoView(idArticle1, idStore3, measureUnitEA, 0d,               ts)
    )).toDS

    val dsResult =
      dsStockConsolidado
        .transform(Sql.filterStockConsolidadoAndManageNulls(spark,appConfig))
        .sort(StockConsolidado.ccTags.idArticleTag,StockConsolidado.ccTags.idStoreTag)
    assertDatasetEquals(dsResult,dsExpected)
  }

  it must " return empty dataset if there are not OMS's stock consolidado" taggedAs (UnitTestTag) in new SqlVariablesStock  {

    val dsStockConsolidado: Dataset[StockConsolidado] = sc.parallelize(List[StockConsolidado](
      StockConsolidado(idArticle1, idStore1, stockTypeAPRO, measureUnitEA, Some(stockConsolidado), ts),
      StockConsolidado(idArticle1, idStore2, stockTypeAPRO, measureUnitEA, Some(stockConsolidado), ts),
      StockConsolidado(idArticle1, idStore3, stockTypeAPRO, measureUnitEA, Some(stockConsolidado), ts),
      StockConsolidado(idArticle1, idStore1, stockTypeAPRO, measureUnitEA, None,                   ts)
    )).toDS

    val dsExpected: Dataset[StockConsolidadoView] = sc.parallelize(List.empty[StockConsolidadoView]).toDS

    val dsResult = dsStockConsolidado.transform(Sql.filterStockConsolidadoAndManageNulls(spark,appConfig))
    assertDatasetEquals(dsResult,dsExpected)
  }

  "filterSaleMovementsByMovementCode" must " return only the sales movements" taggedAs (UnitTestTag) in new SqlVariablesSaleMovements {

    val dsSaleMovements: Dataset[SaleMovements] = sc.parallelize(List[SaleMovements](
      SaleMovements(idArticle1, idStore1,          ts, salesMovementCode, 10.0, ts),
      SaleMovements(idArticle1, idStore2,         ts4, salesMovementCode, 5.0,  ts),
      SaleMovements(idArticle1, idStore3,     todayTs, salesMovementCode, 1.5,  ts),
      SaleMovements(idArticle1, idStore1,         ts5, salesMovementCode, 25.0, ts),
      SaleMovements(idArticle1, idStore2, yesterdayTs, otherMovementCode, 20.0, yesterdayTs),
      SaleMovements(idArticle1, idStore3, yesterdayTs, otherMovementCode, 40.0, yesterdayTs),
      SaleMovements(idArticle1, idStore3,  tomorrowTs, otherMovementCode, 5.0,  ts)
    )).toDS

    val dsExpected: Dataset[SaleMovements] = sc.parallelize(List[SaleMovements](
      SaleMovements(idArticle1, idStore1,          ts, salesMovementCode, 10.0, ts),
      SaleMovements(idArticle1, idStore2,         ts4, salesMovementCode, 5.0,  ts),
      SaleMovements(idArticle1, idStore3,     todayTs, salesMovementCode, 1.5,  ts),
      SaleMovements(idArticle1, idStore1,         ts5, salesMovementCode, 25.0, ts)
    )).toDS.sort(SaleMovements.ccTags.idArticleTag,SaleMovements.ccTags.idStoreTag)

    val dsResult =
      dsSaleMovements
        .transform(Sql.filterSaleMovementsByMovementCode(spark,appConfig))
        .sort(SaleMovements.ccTags.idArticleTag,SaleMovements.ccTags.idStoreTag)
    assertDatasetEquals(dsResult,dsExpected)
  }

  it must " return empty dataset if there are not sales movements" taggedAs (UnitTestTag) in new SqlVariablesSaleMovements {

    val dsSaleMovements: Dataset[SaleMovements] = sc.parallelize(List[SaleMovements](
      SaleMovements(idArticle1, idStore1, yesterdayTs, otherMovementCode, 10.0, ts),
      SaleMovements(idArticle1, idStore2, yesterdayTs, otherMovementCode, 10.0, ts),
      SaleMovements(idArticle1, idStore3, yesterdayTs, otherMovementCode, 10.0, ts),
      SaleMovements(idArticle1, idStore1,  tomorrowTs, otherMovementCode,  1.0, ts)
    )).toDS

    val dsExpected: Dataset[SaleMovements] = sc.parallelize(List.empty[SaleMovements]).toDS

    val dsResult = dsSaleMovements.transform(Sql.filterSaleMovementsByMovementCode(spark,appConfig))
    assertDatasetEquals(dsResult,dsExpected)
  }

  "getSaleMovementsForDay" must " filter sale movements to get only today movements" taggedAs (UnitTestTag) in new SqlVariablesSaleMovements {

    val dsSaleMovements: Dataset[SaleMovements] = sc.parallelize(List[SaleMovements](
      SaleMovements(idArticle1, idStore1,          ts, salesMovementCode, 10.0, ts),
      SaleMovements(idArticle1, idStore2,         ts4, salesMovementCode, 5.0,  ts),
      SaleMovements(idArticle1, idStore3,     todayTs, salesMovementCode, 1.5,  ts),
      SaleMovements(idArticle1, idStore1,         ts5, salesMovementCode, 25.0, ts),
      SaleMovements(idArticle1, idStore2, yesterdayTs, salesMovementCode, 20.0, yesterdayTs),
      SaleMovements(idArticle1, idStore3, yesterdayTs, salesMovementCode, 40.0, yesterdayTs),
      SaleMovements(idArticle1, idStore3,  tomorrowTs, salesMovementCode, 5.0,  ts)
    )).toDS

    val dsExpected: Dataset[SaleMovementsView] = sc.parallelize(List[SaleMovementsView](
      SaleMovementsView(idArticle1, idStore1, 10.0, ts),
      SaleMovementsView(idArticle1, idStore1, 25.0, ts),
      SaleMovementsView(idArticle1, idStore2, 5.0,  ts),
      SaleMovementsView(idArticle1, idStore3, 1.5,  ts)
    )).toDS

    val dsResult =
      dsSaleMovements
        .transform(Sql.getSaleMovementsForDay(todayTs)(spark,appConfig))
        .sort(SaleMovements.ccTags.idArticleTag,SaleMovements.ccTags.idStoreTag)
    assertDatasetEquals(dsResult,dsExpected)
  }

  it must " return empty dataset if there are not movements for today" taggedAs (UnitTestTag) in new SqlVariablesSaleMovements {

    val dsSaleMovements: Dataset[SaleMovements] = sc.parallelize(List[SaleMovements](
      SaleMovements(idArticle1, idStore1, yesterdayTs, salesMovementCode, 10.0, ts),
      SaleMovements(idArticle1, idStore2, yesterdayTs, salesMovementCode, 10.0, ts),
      SaleMovements(idArticle1, idStore3, yesterdayTs, salesMovementCode, 10.0, ts),
      SaleMovements(idArticle1, idStore1,  tomorrowTs, salesMovementCode,  1.0, ts)
    )).toDS

    val dsExpected: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS

    val dsResult = dsSaleMovements.transform(Sql.getSaleMovementsForDay(todayTs)(spark,appConfig))
    assertDatasetEquals(dsResult,dsExpected)
  }

  "filterDatosFijos" must " take into account the sale date to establish the day N " taggedAs (UnitTestTag) in new SqlVariablesPedidos  {
    val initialDF: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row( "1",  "11", "2018-06-22", "02", "EA", null,  null, "A",  currentTimestamp, "user",
          null, null, null, null, null, null, null, null, null, null, null),
        Row( "2",  "22", "2018-06-22", null, "EA",   2d,  null, "A+",  currentTimestamp, "user",
          10d,  11d,  12d,  13d,  14d,  15d,  16d,  17d,  18d,  19d,  20d),
        Row( "3",  "33", "2018-06-21", null, "EA", null, 15.3d, "B",  currentTimestamp, "user",
          10d,  11d,  12d,  13d,  14d,  15d,  16d,  17d,  18d,  19d,  20d),
        Row( "4",  "44", "2018-06-20", null, "EA", null,  null, "A",  currentTimestamp, "user",
          110d,  111d,  112d,  113d,  114d,  115d,  116d,  117d,  118d,  119d,  120d),
        Row( "5",  "55", "2018-06-23", null, "EA", null,  null, "A",  currentTimestamp, "user",
          110d,  111d,  112d,  113d,  114d,  115d,  116d,  117d,  118d,  119d,  120d)
      )), fixedDataInitialStructType)

    val expectedDF: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row( "1",  "11", "2018-06-22", "02", "EA", null,  null,  "A", currentTimestamp,   0d,   0d,   0d,   0d,   0d,  0d),
        Row( "2",  "22", "2018-06-22", null, "EA",   2d,  null, "A+", currentTimestamp,  10d,  11d,  12d,  13d,  14d, 15d),
        Row( "3",  "33", "2018-06-21", null, "EA", null, 15.3d,  "B", currentTimestamp,  11d,  12d,  13d,  14d,  15d, 16d)
      )), fixedDataFinalStructType)
        .orderBy(DatosFijos.ccTags.idArticleTag, DatosFijos.ccTags.idStoreTag)

    val resultDF: DataFrame =
      initialDF
        .transform(Sql.filterDatosFijos(4, Dates.stringToTimestamp("2018-06-22 00:00:00"), Dates.stringToTimestamp("2018-06-21 00:00:00"))(spark, appConfig))
        .orderBy(DatosFijos.ccTags.idArticleTag, DatosFijos.ccTags.idStoreTag)

    assertDataFrameEquals(expectedDF, resultDF)


  }

  "filterOrderHeader" must " filter the order header by type order" taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List[OrderHeader](
      OrderHeader(idOrder1,Constants.DEFAULT_LIST_TYPE_ORDERS(1),ts),
      OrderHeader(idOrder3,Constants.DEFAULT_LIST_TYPE_ORDERS(3),ts),
      OrderHeader(idOrder4,wrongOrderType,ts),
      OrderHeader(idOrder5,wrongOrderType,ts)
    )).toDS

    val dsExpected: Dataset[OrderHeader] = sc.parallelize(List[OrderHeader](
      OrderHeader(idOrder1,Constants.DEFAULT_LIST_TYPE_ORDERS(1),ts),
      OrderHeader(idOrder3,Constants.DEFAULT_LIST_TYPE_ORDERS(3),ts)
    )).toDS

    val dsResult =
      dsOrderHeader
        .transform(Sql.filterOrderHeader(spark,appConfig))
        .sort(OrderHeader.ccTags.idOrderTag)
    assertDatasetEquals(dsResult,dsExpected)
  }

  it must " return empty dataset if there are not allowed order headers" taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List[OrderHeader](
      OrderHeader(idOrder4,null,ts),
      OrderHeader(idOrder4,wrongOrderType,ts),
      OrderHeader(idOrder5,wrongOrderType,ts)
    )).toDS

    val dsExpected: Dataset[OrderHeader] = sc.parallelize(List.empty[OrderHeader]).toDS

    val dsResult = dsOrderHeader.transform(Sql.filterOrderHeader(spark,appConfig))
    assertDatasetEquals(dsResult,dsExpected)
  }

  "filterOrderLines" must " filter the returned order lines" taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val dsOrderLines: Dataset[OrderLine] = sc.parallelize(List[OrderLine](
      OrderLine(idOrder1,idOrderLine1,idArticle1,idStore1,flagNoAnulado,flagNoEntregado,flagNoDevuelto, ts),
      OrderLine(idOrder2,idOrderLine2,idArticle2,idStore2,flagNoAnulado,flagNoEntregado,          null, ts1),
      OrderLine(idOrder5,idOrderLine3,idArticle3,idStore3,flagNoAnulado,flagNoEntregado,returnedToProviderFlag, ts2),
      OrderLine(idOrder5,idOrderLine4,idArticle3,idStore3,flagNoAnulado,flagNoEntregado,flagNoDevuelto, ts3)
    )).toDS

    val dsExpected: Dataset[OrderLineView] = sc.parallelize(List[OrderLineView](
      OrderLineView(idOrder1,idOrderLine1,idArticle1,idStore1, ts),
      OrderLineView(idOrder2,idOrderLine2,idArticle2,idStore2, ts1),
      OrderLineView(idOrder5,idOrderLine4,idArticle3,idStore3, ts3)
    )).toDS

    val dsResult =
      dsOrderLines
        .transform(Sql.filterOrderLines(spark,appConfig))
        .sort(OrderLine.ccTags.idOrderTag,OrderLine.ccTags.idOrderLineTag)
    assertDatasetEquals(dsResult,dsExpected)
  }

  it must " filter the delivered order lines" taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val dsOrderLines: Dataset[OrderLine] = sc.parallelize(List[OrderLine](
      OrderLine(idOrder1,idOrderLine1,idArticle1,idStore1,flagNoAnulado,           null,flagNoDevuelto,ts),
      OrderLine(idOrder2,idOrderLine2,idArticle2,idStore2,flagNoAnulado,flagNoEntregado,flagNoDevuelto,ts1),
      OrderLine(idOrder5,idOrderLine3,idArticle3,idStore3,flagNoAnulado,  alreadyDeliveredFlag,flagNoDevuelto,ts2),
      OrderLine(idOrder5,idOrderLine4,idArticle3,idStore3,flagNoAnulado,flagNoEntregado,flagNoDevuelto,ts3)
    )).toDS

    val dsExpected: Dataset[OrderLineView] = sc.parallelize(List[OrderLineView](
      OrderLineView(idOrder1,idOrderLine1,idArticle1,idStore1,ts),
      OrderLineView(idOrder2,idOrderLine2,idArticle2,idStore2,ts1),
      OrderLineView(idOrder5,idOrderLine4,idArticle3,idStore3,ts3)
    )).toDS

    val dsResult =
      dsOrderLines
        .transform(Sql.filterOrderLines(spark,appConfig))
        .sort(OrderLine.ccTags.idOrderTag,OrderLine.ccTags.idOrderLineTag)
    assertDatasetEquals(dsResult,dsExpected)
  }


  it must " filter both canceled or removed order lines" taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val dsOrderLines: Dataset[OrderLine] = sc.parallelize(List[OrderLine](
      OrderLine(idOrder1,idOrderLine1,idArticle1,idStore1,canceledFlagList(0),flagNoEntregado,flagNoDevuelto,ts),
      OrderLine(idOrder2,idOrderLine2,idArticle2,idStore2,canceledFlagList(1),flagNoEntregado,flagNoDevuelto,ts1),
      OrderLine(idOrder5,idOrderLine3,idArticle3,idStore3,            flagNoAnulado,flagNoEntregado,flagNoDevuelto,ts2),
      OrderLine(idOrder5,idOrderLine4,idArticle3,idStore3,                     null,flagNoEntregado,flagNoDevuelto,ts3)
    )).toDS

    val dsExpected: Dataset[OrderLineView] = sc.parallelize(List[OrderLineView](
      OrderLineView(idOrder5,idOrderLine3,idArticle3,idStore3,ts2),
      OrderLineView(idOrder5,idOrderLine4,idArticle3,idStore3,ts3)
    )).toDS

    val dsResult =
      dsOrderLines
        .transform(Sql.filterOrderLines(spark,appConfig))
        .sort(OrderLine.ccTags.idOrderTag,OrderLine.ccTags.idOrderLineTag)
    assertDatasetEquals(dsResult,dsExpected)
  }

  it must " return the right order lines" taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val dsOrderLines: Dataset[OrderLine] = sc.parallelize(List[OrderLine](
      OrderLine(idOrder1,idOrderLine1,idArticle1,idStore1,canceledFlagList(0),flagNoEntregado,flagNoDevuelto,ts),
      OrderLine(idOrder2,idOrderLine2,idArticle2,idStore2,canceledFlagList(1),flagNoEntregado,flagNoDevuelto,ts1),
      OrderLine(idOrder5,idOrderLine3,idArticle3,idStore3,            flagNoAnulado,  alreadyDeliveredFlag,flagNoDevuelto,ts2),
      OrderLine(idOrder5,idOrderLine4,idArticle3,idStore3,                     null,flagNoEntregado,returnedToProviderFlag,ts3)
    )).toDS

    val dsExpected: Dataset[OrderLineView] = sc.parallelize(List.empty[OrderLineView]).toDS

    val dsResult = dsOrderLines.transform(Sql.filterOrderLines(spark,appConfig))
    assertDatasetEquals(dsResult,dsExpected)
  }

  "filterPartialOrdersDeliveredBetweenDays " must " filter the partial delivered orders between days passed" taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val wholePartialOrderAmount = 10d
    val amountToBeDelivered = 5d
    val dsPartialOrdersDelivered: Dataset[PartialOrdersDelivered] = sc.parallelize(List[PartialOrdersDelivered](
      PartialOrdersDelivered(idOrder1, idOrderLine1, entrega11, Some(wholePartialOrderAmount), Some(amountToBeDelivered), threeDaysAgoWithoutSeparator,          yesterdayAtMidnightTs),
      PartialOrdersDelivered(idOrder1, idOrderLine1, entrega11, Some(wholePartialOrderAmount), Some(amountToBeDelivered), yesterdayWithoutSeparator,             todayTs),
      PartialOrdersDelivered(idOrder1, idOrderLine1, entrega11, Some(wholePartialOrderAmount), Some(amountToBeDelivered), todayWithoutSeparator,                 todayTs),
      PartialOrdersDelivered(idOrder1, idOrderLine1, entrega11, Some(wholePartialOrderAmount), Some(amountToBeDelivered), lastDayToProjectWithoutSeparator,      todayTs),
      PartialOrdersDelivered(idOrder1, idOrderLine1, entrega11, Some(wholePartialOrderAmount), Some(amountToBeDelivered), lastDayToProjectPlus1WithoutSeparator, todayTs),
      PartialOrdersDelivered(idOrder1, idOrderLine1, entrega11, Some(wholePartialOrderAmount), Some(amountToBeDelivered), lastDayToProjectPlus2WithoutSeparator, todayTs)
    )).toDS

    val dsExpected: Dataset[PartialOrdersDelivered] = sc.parallelize(List[PartialOrdersDelivered](
      PartialOrdersDelivered(idOrder1, idOrderLine1, entrega11, Some(wholePartialOrderAmount), Some(amountToBeDelivered), yesterdayWithoutSeparator,             todayTs),
      PartialOrdersDelivered(idOrder1, idOrderLine1, entrega11, Some(wholePartialOrderAmount), Some(amountToBeDelivered), todayWithoutSeparator,                 todayTs),
      PartialOrdersDelivered(idOrder1, idOrderLine1, entrega11, Some(wholePartialOrderAmount), Some(amountToBeDelivered), lastDayToProjectWithoutSeparator,      todayTs),
      PartialOrdersDelivered(idOrder1, idOrderLine1, entrega11, Some(wholePartialOrderAmount), Some(amountToBeDelivered), lastDayToProjectPlus1WithoutSeparator, todayTs)
    )).toDS

    val dsResult =
      dsPartialOrdersDelivered
        .transform(Sql.filterPendingPartialOrdersDeliveredBetweenDays(yesterdayAtMidnightTs, lastDayToProjectPlus2Ts)(spark, appConfig))
        .sort(PartialOrdersDelivered.ccTags.idOrderTag,PartialOrdersDelivered.ccTags.idOrderLineTag)
    assertDatasetEquals(dsResult,dsExpected)
  }

  it must " filter the partial delivered which whole amount has already been delivered" taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val wholePartialOrderAmount = 10d
    val amountToBeDelivered = 5d
    val dsPartialOrdersDelivered: Dataset[PartialOrdersDelivered] = sc.parallelize(List[PartialOrdersDelivered](
      PartialOrdersDelivered(idOrder1, idOrderLine1, entrega11, Some(wholePartialOrderAmount),     Some(amountToBeDelivered), threeDaysAgoWithoutSeparator,          yesterdayAtMidnightTs),
      PartialOrdersDelivered(idOrder1, idOrderLine1, entrega11, Some(wholePartialOrderAmount),     Some(amountToBeDelivered), yesterdayWithoutSeparator,             todayTs),
      PartialOrdersDelivered(idOrder1, idOrderLine1, entrega11, Some(wholePartialOrderAmount), Some(wholePartialOrderAmount), todayWithoutSeparator,                 todayTs),
      PartialOrdersDelivered(idOrder1, idOrderLine1, entrega11, Some(wholePartialOrderAmount),     Some(amountToBeDelivered), lastDayToProjectWithoutSeparator,      todayTs),
      PartialOrdersDelivered(idOrder1, idOrderLine1, entrega11, Some(wholePartialOrderAmount), Some(wholePartialOrderAmount), lastDayToProjectPlus1WithoutSeparator, todayTs),
      PartialOrdersDelivered(idOrder1, idOrderLine1, entrega11, Some(wholePartialOrderAmount),     Some(amountToBeDelivered), lastDayToProjectPlus2WithoutSeparator, todayTs)
    )).toDS

    val dsExpected: Dataset[PartialOrdersDelivered] = sc.parallelize(List[PartialOrdersDelivered](
      PartialOrdersDelivered(idOrder1, idOrderLine1, entrega11, Some(wholePartialOrderAmount), Some(amountToBeDelivered), yesterdayWithoutSeparator,             todayTs),
      PartialOrdersDelivered(idOrder1, idOrderLine1, entrega11, Some(wholePartialOrderAmount), Some(amountToBeDelivered), lastDayToProjectWithoutSeparator,      todayTs)
    )).toDS

    val dsResult =
      dsPartialOrdersDelivered
        .transform(Sql.filterPendingPartialOrdersDeliveredBetweenDays(yesterdayAtMidnightTs, lastDayToProjectPlus2Ts)(spark, appConfig))
        .sort(PartialOrdersDelivered.ccTags.idOrderTag,PartialOrdersDelivered.ccTags.idOrderLineTag)
    assertDatasetEquals(dsResult,dsExpected)
  }

  it must " filter the partial delivered orders when there are no partial orders" taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val dsPartialOrdersDelivered: Dataset[PartialOrdersDelivered] = sc.parallelize(List.empty[PartialOrdersDelivered]).toDS

    val dsExpected: Dataset[PartialOrdersDelivered] = sc.parallelize(List.empty[PartialOrdersDelivered]).toDS

    val dsResult =
      dsPartialOrdersDelivered
        .transform(Sql.filterPendingPartialOrdersDeliveredBetweenDays(yesterdayAtMidnightTs, lastDayToProjectPlus2Ts)(spark, appConfig))
        .sort(PartialOrdersDelivered.ccTags.idOrderTag,PartialOrdersDelivered.ccTags.idOrderLineTag)
    assertDatasetEquals(dsResult,dsExpected)
  }

  "transformPartialOrdersDelivered " must " transform the partial deliver to timestamp and null-amounts to zero-amounts" taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val cantidadTotalPedido = 10d
    val cantidadEntrega = 10d
    val dsPartialOrdersDelivered: Dataset[PartialOrdersDelivered] = sc.parallelize(List[PartialOrdersDelivered](
      PartialOrdersDelivered(idOrder1,idOrderLine1,entrega11,Some(cantidadTotalPedido),Some(cantidadEntrega),    todayWithoutSeparator,todayTs),
      PartialOrdersDelivered(idOrder1,idOrderLine1,entrega11,                     None,Some(cantidadEntrega),yesterdayWithoutSeparator,todayTs),
      PartialOrdersDelivered(idOrder1,idOrderLine1,entrega11,Some(cantidadTotalPedido),                 None,    todayWithoutSeparator,todayTs)
    )).toDS

    val dsExpected: Dataset[PartialOrdersDeliveredView] = sc.parallelize(List[PartialOrdersDeliveredView](
      PartialOrdersDeliveredView(idOrder1,idOrderLine1,entrega11,cantidadTotalPedido,cantidadEntrega,    todayTs,todayTs),
      PartialOrdersDeliveredView(idOrder1,idOrderLine1,entrega11,                 0d,cantidadEntrega,yesterdayAtMidnightTs,todayTs),
      PartialOrdersDeliveredView(idOrder1,idOrderLine1,entrega11,cantidadTotalPedido,             0d,    todayTs,todayTs)
    )).toDS

    val dsResult =
      dsPartialOrdersDelivered
        .transform(Sql.transformPartialOrdersDelivered(spark,appConfig))
        .sort(PartialOrdersDelivered.ccTags.idOrderTag,PartialOrdersDelivered.ccTags.idOrderLineTag)
    assertDatasetEquals(dsResult,dsExpected)
  }

  "joinOrderLinesById" must
    " return a join with order header and order line" taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val dsOrderLines: Dataset[OrderLineView] = sc.parallelize(List[OrderLineView](
      OrderLineView(idOrder1,idOrderLine1,idArticle1,idStore1,ts),
      OrderLineView(idOrder2,idOrderLine2,idArticle2,idStore2,ts1),
      OrderLineView(idOrder5,idOrderLine3,idArticle3,idStore3,ts2)
    )).toDS

    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List[OrderHeader](
      OrderHeader(idOrder1,Constants.DEFAULT_LIST_TYPE_ORDERS(1),ts),
      OrderHeader(idOrder3,Constants.DEFAULT_LIST_TYPE_ORDERS(3),ts2),
      OrderHeader(idOrder4,Constants.DEFAULT_LIST_TYPE_ORDERS(4),ts3),
      OrderHeader(idOrder5,Constants.DEFAULT_LIST_TYPE_ORDERS(5),ts1)
    )).toDS

    val dsExpected: Dataset[OrderLineView] = sc.parallelize(List[OrderLineView](
      OrderLineView(idOrder1,idOrderLine1,idArticle1,idStore1,ts),
      OrderLineView(idOrder5,idOrderLine3,idArticle3,idStore3,ts2)
    )).toDS.sort(OrderLine.ccTags.idOrderTag)

    val dsResult =
      Sql.joinOrderLinesById(
        dsOrderHeader,
        dsOrderLines)(spark).sort(OrderLine.ccTags.idOrderTag)
    assertDatasetEquals(dsResult,dsExpected)
  }

  it must
    " return an empty dataset if there are no order headers" taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val dsOrderLines: Dataset[OrderLineView] = sc.parallelize(List[OrderLineView](
      OrderLineView(idOrder1,idOrderLine1,idArticle1,idStore1,ts),
      OrderLineView(idOrder2,idOrderLine2,idArticle2,idStore2,ts1),
      OrderLineView(idOrder5,idOrderLine3,idArticle3,idStore3,ts2)
    )).toDS

    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List.empty[OrderHeader]).toDS

    val dsExpected: Dataset[OrderLineView] = sc.parallelize(List.empty[OrderLineView]).toDS

    val dsResult =
      Sql.joinOrderLinesById(
        dsOrderHeader,
        dsOrderLines)(spark)
    assertDatasetEquals(dsResult,dsExpected)
  }

  it must
    " return an empty dataset if there are no order lines" taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val dsOrderLines: Dataset[OrderLineView] = sc.parallelize(List.empty[OrderLineView]).toDS

    val dsOrderHeader: Dataset[OrderHeader] = sc.parallelize(List[OrderHeader](
      OrderHeader(idOrder1,Constants.DEFAULT_LIST_TYPE_ORDERS(1),ts),
      OrderHeader(idOrder3,Constants.DEFAULT_LIST_TYPE_ORDERS(3),ts2),
      OrderHeader(idOrder4,Constants.DEFAULT_LIST_TYPE_ORDERS(4),ts3),
      OrderHeader(idOrder5,Constants.DEFAULT_LIST_TYPE_ORDERS(5),ts1)
    )).toDS

    val dsExpected: Dataset[OrderLineView] = sc.parallelize(List.empty[OrderLineView]).toDS

    val dsResult =
      Sql.joinOrderLinesById(
        dsOrderHeader,
        dsOrderLines)(spark)
    assertDatasetEquals(dsResult,dsExpected)
  }

  "getArticleAndStoreOfOrdersNotCanceled" must
    " return a dataset with all partial orders whose correspondent order is not canceled or returned, i.e., is included in dsOrderLines" taggedAs (UnitTestTag) in new SqlVariablesPedidos {

    val wholePartialOrderAmount = 10d
    val deliveredAmount = 10d

    val orderLinesDs: Dataset[OrderLineView] =
      sc.parallelize(
        List[OrderLineView](
          OrderLineView(idOrder1, idOrderLine1, idArticle1, idStore1, currentTimestamp),
          OrderLineView(idOrder2, idOrderLine1, idArticle2, idStore1, yesterdayAtMidnightTs),
          OrderLineView(idOrder2, idOrderLine2, idArticle2, idStore2, yesterdayAtMidnightTs)
        )).toDS

    val partialOrdersDs: Dataset[PartialOrdersDeliveredView] =
      sc.parallelize(
        List[PartialOrdersDeliveredView](
          PartialOrdersDeliveredView(idOrder1, idOrderLine1, entrega11, wholePartialOrderAmount, deliveredAmount,               todayTs, todayTs),
          PartialOrdersDeliveredView(idOrder1, idOrderLine1, entrega11,                      0d, deliveredAmount, yesterdayAtMidnightTs, todayTs),
          PartialOrdersDeliveredView(idOrder2, idOrderLine3, entrega11, wholePartialOrderAmount,              0d,               todayTs, todayTs),
          PartialOrdersDeliveredView(idOrder2, idOrderLine2, entrega11, wholePartialOrderAmount,              0d,               todayTs, todayTs)
        )
      ).toDS()

    val dsExpected: Dataset[ActivePartialOrdersWithArticleAndStore] =
      sc.parallelize(
        List[ActivePartialOrdersWithArticleAndStore](
          ActivePartialOrdersWithArticleAndStore(idArticle1, idStore1,               todayTs, wholePartialOrderAmount, deliveredAmount, currentTimestamp),
          ActivePartialOrdersWithArticleAndStore(idArticle1, idStore1, yesterdayAtMidnightTs,                      0d, deliveredAmount, currentTimestamp),
          ActivePartialOrdersWithArticleAndStore(idArticle2, idStore2,               todayTs, wholePartialOrderAmount,              0d, todayTs)
        )
      ).toDS()
        .orderBy(Surtido.ccTags.idStoreTag, Surtido.ccTags.idArticleTag, ActivePartialOrdersWithArticleAndStore.ccTags.deliveryDateTsTag)

    val dsResult: Dataset[ActivePartialOrdersWithArticleAndStore] =
      partialOrdersDs
      .transform(Sql.getArticleAndStoreOfOrdersNotCanceled(orderLinesDs)(spark))
      .orderBy(Surtido.ccTags.idStoreTag, Surtido.ccTags.idArticleTag, ActivePartialOrdersWithArticleAndStore.ccTags.deliveryDateTsTag)

    assertDatasetEquals(dsResult,dsExpected)
  }

  it must
    " return an empty dataset of AggregatedStockToBeDeliveredWithLogisticVariable when there are no orders" taggedAs (UnitTestTag) in new SqlVariablesPedidos {

    val orderLinesDs: Dataset[OrderLineView] =
      sc.parallelize(
        List.empty[OrderLineView]).toDS

    val partialOrdersDs: Dataset[PartialOrdersDeliveredView] =
      sc.parallelize(
        List.empty[PartialOrdersDeliveredView]).toDS()

    val dsExpected: Dataset[ActivePartialOrdersWithArticleAndStore] =
      sc.parallelize(
        List.empty[ActivePartialOrdersWithArticleAndStore]
      ).toDS()

    val dsResult: Dataset[ActivePartialOrdersWithArticleAndStore] =
      partialOrdersDs
        .transform(Sql.getArticleAndStoreOfOrdersNotCanceled(orderLinesDs)(spark))
        .orderBy(Surtido.ccTags.idStoreTag, Surtido.ccTags.idArticleTag, ActivePartialOrdersWithArticleAndStore.ccTags.deliveryDateTsTag)
    assertDatasetEquals(dsResult,dsExpected)
  }

  "getStockToBeDeliveredByArticleStoreAndDay" must
    " return a dataset AggregatedStockToBeDeliveredWithLogisticVariable grouped by article, store and day" taggedAs (UnitTestTag) in new SqlVariablesPedidos {

    val ds1: Dataset[ActivePartialOrdersWithArticleAndStore] =
      sc.parallelize(
        List[ActivePartialOrdersWithArticleAndStore](
          ActivePartialOrdersWithArticleAndStore(idArticle1, idStore1,   yesterdayAtMidnightTs, 100,  50, yesterdayAtMidnightTs),
          ActivePartialOrdersWithArticleAndStore(idArticle2, idStore1,                 todayTs, 100,  50, todayTs),
          ActivePartialOrdersWithArticleAndStore(idArticle1, idStore1,           laterTodayTs1,  80,  20, yesterdayAtMidnightTs),
          ActivePartialOrdersWithArticleAndStore(idArticle1, idStore1,           laterTodayTs2,  70,  10, todayTs),
          ActivePartialOrdersWithArticleAndStore(idArticle3, idStore1, lastDayToProjectPlus1Ts, 100,  50, currentTimestamp),
          ActivePartialOrdersWithArticleAndStore(idArticle3, idStore1, lastDayToProjectPlus1Ts, 100, 150, currentTimestamp)
        )).toDS

    val dsExpected: Dataset[StockToBeDelivered] =
      sc.parallelize(
        List[StockToBeDelivered](
          StockToBeDelivered(idArticle1, idStore1,                       yesterdayWithoutSeparator,  50, yesterdayAtMidnightTs),
          StockToBeDelivered(idArticle2, idStore1,                           todayWithoutSeparator,  50, todayTs),
          StockToBeDelivered(idArticle1, idStore1,                           todayWithoutSeparator, 120, todayTs),
          StockToBeDelivered(idArticle3, idStore1, lastDayToProjectPlus1WithoutSeparator,  50, currentTimestamp)
        )
      ).toDS()
      .orderBy(Surtido.ccTags.idStoreTag, Surtido.ccTags.idArticleTag, StockToBeDelivered.ccTags.dayTag)

    val dsResult: Dataset[StockToBeDelivered] = ds1
      .transform(Sql.getStockToBeDeliveredByArticleStoreAndDay(spark))
      .orderBy(Surtido.ccTags.idStoreTag, Surtido.ccTags.idArticleTag, StockToBeDelivered.ccTags.dayTag)

    assertDatasetEquals(dsResult,dsExpected)
  }

  it must
    " return an empty dataset of AggregatedStockToBeDeliveredWithLogisticVariable when there are no orders" taggedAs (UnitTestTag) in new SqlVariablesPedidos {

    val ds1: Dataset[ActivePartialOrdersWithArticleAndStore] =
      sc.parallelize(
        List.empty[ActivePartialOrdersWithArticleAndStore]).toDS

    val dsExpected: Dataset[StockToBeDelivered] =
      sc.parallelize(
        List.empty[StockToBeDelivered]
      ).toDS()

    val dsResult: Dataset[StockToBeDelivered] = ds1.transform(Sql.getStockToBeDeliveredByArticleStoreAndDay(spark))
    assertDatasetEquals(dsResult,dsExpected)
  }

  "aggregateSaleMovementsByArticleAndStore " must
    " return an ATP movements dataset with aggregated rows by article-store of sum amounts and max timestamps" taggedAs (UnitTestTag) in new SqlVariablesSaleMovements {

    val ds1: Dataset[SaleMovementsView] =
      sc.parallelize(
        List[SaleMovementsView](
          SaleMovementsView(idArticle1, idStore1, 10d,ts4),
          SaleMovementsView(idArticle1, idStore2, 20d,ts5),
          SaleMovementsView(idArticle1, idStore1, 5d, todayTs),
          SaleMovementsView(idArticle2, idStore2, 50d,ts4),
          SaleMovementsView(idArticle2, idStore3, 1d, todayTs),
          SaleMovementsView(idArticle2, idStore2, 80d,ts5)
        )
      ).toDS

    val dsExpected: Dataset[SaleMovementsView] =
      sc.parallelize(
        List[SaleMovementsView](
          SaleMovementsView(idArticle1, idStore1, 15d, ts4),
          SaleMovementsView(idArticle1, idStore2, 20d, ts5),
          SaleMovementsView(idArticle2, idStore2, 130d,ts5),
          SaleMovementsView(idArticle2, idStore3, 1d,  todayTs)
        )
      ).toDS()

    val dsResult: Dataset[SaleMovementsView] = ds1
      .transform(Sql.aggregateSaleMovementsByArticleAndStore(spark))
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)
    assertDatasetEquals(dsResult,dsExpected)
  }

  it must
    " return an empty dataset if there are not sale movements" taggedAs (UnitTestTag) in new SqlVariablesSaleMovements {

    val ds1: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS

    val dsExpected: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS()

    val dsResult: Dataset[SaleMovementsView] = ds1.transform(Sql.aggregateSaleMovementsByArticleAndStore(spark))
    assertDatasetEquals(dsResult,dsExpected)
  }

  "aggregationPP " must " return true if any of the capacities is in the list " taggedAs (UnitTestTag) in new SqlVariables  {
    val dsStoreCapacity: Dataset[StoreCapacityView] = sc.parallelize(List[StoreCapacityView](
      StoreCapacityView(idStore1, 4,  ts),
      StoreCapacityView(idStore1, 10, ts),
      StoreCapacityView(idStore2, 0,  ts),
      StoreCapacityView(idStore2, 6,  ts),
      StoreCapacityView(idStore2, 4,  ts),
      StoreCapacityView(idStore3, 6,  ts),
      StoreCapacityView(idStore3, 7,  ts)
    )).toDS

    val listCapacitiesPP  = Seq(5, 6, 7, 8, 9)

    val dsExpected: Dataset[AggPP] = sc.parallelize(List[AggPP](
      AggPP(idStore1, false, ts),
      AggPP(idStore2, true,  ts),
      AggPP(idStore3, true,  ts)
    )).toDS
      .orderBy(Surtido.ccTags.idStoreTag)

    val dsResult =
      Sql.aggregationPP(dsStoreCapacity, listCapacitiesPP)(spark)
      .orderBy(Surtido.ccTags.idStoreTag)
    assertDatasetEquals(dsResult,dsExpected)

  }

  "joinMaterials " must "join with list of materials and its articles" in new SqlVariablesPedidos {

    val dsMaterialsArticle: Dataset[MaterialsArticle] =
      sc.parallelize(
        List[MaterialsArticle](
          MaterialsArticle(idArticle1, idMaterials1, todayTs),
          MaterialsArticle(idArticle1, idMaterials2, todayTs),
          MaterialsArticle(idArticle2, idMaterials3, todayTs),
          MaterialsArticle(idArticle3, idMaterials4, todayTs)
        )
      ).toDS

    val dsMaterials: Dataset[Materials] =
      sc.parallelize(
        List[Materials](
          Materials(idMaterials1, idArticle5,    0, todayTs),
          Materials(idMaterials2, idArticle6,  10d, todayTs),
          Materials(idMaterials3, idArticle7, 3.7d, todayTs)
        )
      ).toDS

    val dsExpected: Dataset[BoxAndPrepackInformation] =
      sc.parallelize(
        List[BoxAndPrepackInformation](
          BoxAndPrepackInformation(idMaterials1, idArticle1, idArticle5,    0, todayTs),
          BoxAndPrepackInformation(idMaterials2, idArticle1, idArticle6,  10d, todayTs),
          BoxAndPrepackInformation(idMaterials3, idArticle2, idArticle7, 3.7d, todayTs)
        )
      ).toDS.sort(Material.ccTags.idListMaterialTag, Surtido.ccTags.idArticleTag)

    val dsResult: Dataset[BoxAndPrepackInformation] =
      Sql.getBoxAndPrepackContentInformation(dsMaterialsArticle, dsMaterials)(spark)
        .sort(Material.ccTags.idListMaterialTag, Surtido.ccTags.idArticleTag)

    assertDatasetEquals(dsResult,dsExpected)
  }

  it must "return empty dataset if there are not materials" in new SqlVariablesPedidos {

    val dsMaterialsArticle: Dataset[MaterialsArticle] =
      sc.parallelize(
        List.empty[MaterialsArticle]
      ).toDS

    val dsMaterials: Dataset[Materials] =
      sc.parallelize(
        List[Materials](
          Materials(idMaterials1, idArticle5,    0, todayTs),
          Materials(idMaterials2, idArticle6,  10d, todayTs),
          Materials(idMaterials3, idArticle7, 3.7d, todayTs)
        )
      ).toDS

    val dsExpected: Dataset[BoxAndPrepackInformation] =
      sc.parallelize(
        List.empty[BoxAndPrepackInformation]
      ).toDS()

    val dsResult: Dataset[BoxAndPrepackInformation] =
      Sql.getBoxAndPrepackContentInformation(dsMaterialsArticle, dsMaterials)(spark)

    assertDatasetEquals(dsResult,dsExpected)
  }

  it must "return empty dataset if there are not sales-articles" in new SqlVariablesPedidos {

    val dsMaterialsArticle: Dataset[MaterialsArticle] =
      sc.parallelize(
        List[MaterialsArticle](
          MaterialsArticle(idArticle1, idMaterials1, todayTs),
          MaterialsArticle(idArticle1, idMaterials2, todayTs),
          MaterialsArticle(idArticle2, idMaterials3, todayTs),
          MaterialsArticle(idArticle3, idMaterials4, todayTs)
        )
      ).toDS

    val dsMaterials: Dataset[Materials] =
      sc.parallelize(
        List.empty[Materials]
      ).toDS

    val dsExpected: Dataset[BoxAndPrepackInformation] =
      sc.parallelize(
        List.empty[BoxAndPrepackInformation]
      ).toDS()

    val dsResult: Dataset[BoxAndPrepackInformation] =
      Sql.getBoxAndPrepackContentInformation(dsMaterialsArticle, dsMaterials)(spark)

    assertDatasetEquals(dsResult,dsExpected)
  }

  it must "return the last update date between both entities" in new SqlVariablesPedidos {

    val dsMaterialsArticle: Dataset[MaterialsArticle] =
      sc.parallelize(
        List[MaterialsArticle](
          MaterialsArticle(idArticle1, idMaterials1, todayTs),
          MaterialsArticle(idArticle1, idMaterials2, yesterdayAtMidnightTs),
          MaterialsArticle(idArticle2, idMaterials3, todayTs),
          MaterialsArticle(idArticle3, idMaterials4, todayTs)
        )
      ).toDS

    val dsMaterials: Dataset[Materials] =
      sc.parallelize(
        List[Materials](
          Materials(idMaterials1, idArticle5,    0, yesterdayAtMidnightTs),
          Materials(idMaterials2, idArticle6,  10d, yesterdayAtMidnightTs),
          Materials(idMaterials3, idArticle7, 3.7d, todayTs),
          Materials(idMaterials4, idArticle7,   20, yesterdayAtMidnightTs)
        )
      ).toDS

    val dsExpected: Dataset[BoxAndPrepackInformation] =
      sc.parallelize(
        List[BoxAndPrepackInformation](
          BoxAndPrepackInformation(idMaterials1, idArticle1, idArticle5,    0, todayTs),
          BoxAndPrepackInformation(idMaterials2, idArticle1, idArticle6,  10d, yesterdayAtMidnightTs),
          BoxAndPrepackInformation(idMaterials3, idArticle2, idArticle7, 3.7d, todayTs),
          BoxAndPrepackInformation(idMaterials4, idArticle3, idArticle7,   20, todayTs)
        )
      ).toDS.sort(Material.ccTags.idListMaterialTag, Surtido.ccTags.idArticleTag)

    val dsResult: Dataset[BoxAndPrepackInformation] =
      Sql.getBoxAndPrepackContentInformation(dsMaterialsArticle, dsMaterials)(spark)
        .sort(Material.ccTags.idListMaterialTag, Surtido.ccTags.idArticleTag)

    assertDatasetEquals(dsResult,dsExpected)
  }

  "joinStockToBeDeliveredWithLogisticVariableWithMara " must "inner join with order aggregation and its master of each article" in new SqlVariablesPedidos {

    val dsAggOrders: Dataset[StockToBeDelivered] =
      sc.parallelize(
        List[StockToBeDelivered](
          StockToBeDelivered(idArticle1, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1,     ts1),
          StockToBeDelivered(idArticle1, idStore1,     todayWithoutSeparator,     aggPedidoToday1,     ts1),
          StockToBeDelivered(idArticle1, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2,     ts2),
          StockToBeDelivered(idArticle1, idStore2,     todayWithoutSeparator,     aggPedidoToday2,     ts2),
          StockToBeDelivered(idArticle2, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday2, todayTs),
          StockToBeDelivered(idArticle2, idStore1,     todayWithoutSeparator,     aggPedidoToday2, todayTs),
          StockToBeDelivered(idArticle3, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1, todayTs),
          StockToBeDelivered(idArticle3, idStore3,     todayWithoutSeparator,     aggPedidoToday1, todayTs),
          StockToBeDelivered(idArticle4, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1, todayTs),
          StockToBeDelivered(idArticle4, idStore3,     todayWithoutSeparator,     aggPedidoToday1, todayTs),
          StockToBeDelivered(idArticle5, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1, todayTs),
          StockToBeDelivered(idArticle5, idStore3,     todayWithoutSeparator,     aggPedidoToday1, todayTs),
          StockToBeDelivered(idArticle6, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1, todayTs),
          StockToBeDelivered(idArticle6, idStore3,     todayWithoutSeparator,     aggPedidoToday1, todayTs),
          StockToBeDelivered(idArticle7, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1, todayTs),
          StockToBeDelivered(idArticle7, idStore3,     todayWithoutSeparator,     aggPedidoToday1, todayTs)
        )
      ).toDS

    val dsMara: Dataset[MaraWithArticleTypes] =
      sc.parallelize(
        List[MaraWithArticleTypes](
          MaraWithArticleTypes(idArticle1,   purchaseArticleType1,   purchaseArticleCategory1, PURCHASE_FLAG,    parentSaleArticle1, ts2),
          MaraWithArticleTypes(idArticle2,   purchaseArticleType1,   purchaseArticleCategory1, PURCHASE_FLAG_2,  parentSaleArticle2, ts1),
          MaraWithArticleTypes(idArticle4,       saleArticleType1,       saleArticleCategory1,     SALE_FLAG,                    "", ts1),
          MaraWithArticleTypes(idArticle5,   purchaseArticleType1,       saleArticleCategory1, PURCHASE_FLAG,    parentSaleArticle3, ts1),
          MaraWithArticleTypes(idArticle6,       saleArticleType1,       saleArticleCategory1,          null, parentSaleArticleNull, ts1),
          MaraWithArticleTypes(idArticle7, boxPrepackArticleType1, boxPrepackArticleCategory1, PURCHASE_FLAG,                  null, ts1)
        )
      ).toDS

    val dsExpected: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      sc.parallelize(
        List[StockToBeDeliveredWithLogisticVariableWithMara](
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1,   purchaseArticleType1,   purchaseArticleCategory1,   PURCHASE_FLAG,    parentSaleArticle1,     ts2),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2,   purchaseArticleType1,   purchaseArticleCategory1,   PURCHASE_FLAG,    parentSaleArticle1,     ts2),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle2, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday2,   purchaseArticleType1,   purchaseArticleCategory1, PURCHASE_FLAG_2,    parentSaleArticle2, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle4, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1,       saleArticleType1,       saleArticleCategory1,       SALE_FLAG,                    "", todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle5, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1,   purchaseArticleType1,       saleArticleCategory1,   PURCHASE_FLAG,    parentSaleArticle3, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle6, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1,       saleArticleType1,       saleArticleCategory1,            null, parentSaleArticleNull, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle7, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1, boxPrepackArticleType1, boxPrepackArticleCategory1,   PURCHASE_FLAG,                  null, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1,     todayWithoutSeparator,     aggPedidoToday1,   purchaseArticleType1,   purchaseArticleCategory1,   PURCHASE_FLAG,    parentSaleArticle1,     ts2),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2,     todayWithoutSeparator,     aggPedidoToday2,   purchaseArticleType1,   purchaseArticleCategory1,   PURCHASE_FLAG,    parentSaleArticle1,     ts2),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle2, idStore1,     todayWithoutSeparator,     aggPedidoToday2,   purchaseArticleType1,   purchaseArticleCategory1, PURCHASE_FLAG_2,    parentSaleArticle2, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle4, idStore3,     todayWithoutSeparator,     aggPedidoToday1,       saleArticleType1,       saleArticleCategory1,       SALE_FLAG,                    "", todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle5, idStore3,     todayWithoutSeparator,     aggPedidoToday1,   purchaseArticleType1,       saleArticleCategory1,   PURCHASE_FLAG,    parentSaleArticle3, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle6, idStore3,     todayWithoutSeparator,     aggPedidoToday1,       saleArticleType1,       saleArticleCategory1,            null, parentSaleArticleNull, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle7, idStore3,     todayWithoutSeparator,     aggPedidoToday1, boxPrepackArticleType1, boxPrepackArticleCategory1,   PURCHASE_FLAG,                  null, todayTs)
        )
      ).toDS.sort(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag, StockToBeDeliveredWithLogisticVariableWithMara.ccTags.dayTag)

    val dsResult: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      dsAggOrders.transform(Sql.addInformationOfArticlesToBeDelivered(dsMara)(spark, appConfig))
        .sort(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag, StockToBeDeliveredWithLogisticVariableWithMara.ccTags.dayTag)

    assertDatasetEquals(dsResult, dsExpected)
  }

  it must " return an empty dataset when input is empty" in new SqlVariablesPedidos {

    val dsAggOrders: Dataset[StockToBeDelivered] =
      sc.parallelize(List.empty[StockToBeDelivered]).toDS

    val dsMara: Dataset[MaraWithArticleTypes] =
      sc.parallelize(List.empty[MaraWithArticleTypes]).toDS

    val dsExpected: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      sc.parallelize(List.empty[StockToBeDeliveredWithLogisticVariableWithMara]).toDS

    val dsResult: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      dsAggOrders.transform(Sql.addInformationOfArticlesToBeDelivered(dsMara)(spark, appConfig))

    assertDatasetEquals(dsResult, dsExpected)
  }

  "filterBoxPrepack " must "return only the articles which are box or prepack" in new SqlVariablesPedidos {


    val dsAggOrdersWithMara: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] = sc.parallelize(
      List[StockToBeDeliveredWithLogisticVariableWithMara](
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore1,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          parentSaleArticle1,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore1,
          todayWithoutSeparator,
          aggPedidoToday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          parentSaleArticle1,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore2,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.category,
          PURCHASE_FLAG_2,
          parentSaleArticle2,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore2,
          todayWithoutSeparator,
          aggPedidoToday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.category,
          PURCHASE_FLAG_2,
          parentSaleArticle2,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore3,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.category,
          null,
          parentSaleArticle3,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore3,
          todayWithoutSeparator,
          aggPedidoToday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.category,
          null,
          parentSaleArticle3,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore1,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          null,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore1,
          todayWithoutSeparator,
          aggPedidoToday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          null,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle2,
          idStore1,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          SALE_FLAG,
          idArticle2,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle2,
          idStore1,
          todayWithoutSeparator,
          aggPedidoToday1,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          SALE_FLAG,
          idArticle2,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle3,
          idStore1,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          SALE_FLAG_2,
          idArticle3,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle3,
          idStore1,
          todayWithoutSeparator,
          aggPedidoToday1,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          SALE_FLAG_2,
          idArticle3,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle3,
          idStore1,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          null,
          idArticle3,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle3,
          idStore1,
          todayWithoutSeparator,
          aggPedidoToday1,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          null,
          idArticle3,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle4,
          idStore2,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          parentSaleArticle3,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle4,
          idStore2,
          todayWithoutSeparator,
          aggPedidoToday1,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          parentSaleArticle3,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle4,
          idStore2,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          null,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle4,
          idStore2,
          todayWithoutSeparator,
          aggPedidoToday1,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          null,
          todayTs)
      )
    ).toDS
    val dsExpected = sc.parallelize(
      List[StockToBeDeliveredWithLogisticVariableWithMara](
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore1,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          parentSaleArticle1,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore1,
          todayWithoutSeparator,
          aggPedidoToday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          parentSaleArticle1,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore2,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.category,
          PURCHASE_FLAG_2,
          parentSaleArticle2,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore2,
          todayWithoutSeparator,
          aggPedidoToday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.category,
          PURCHASE_FLAG_2,
          parentSaleArticle2,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore3,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.category,
          null,
          parentSaleArticle3,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore3,
          todayWithoutSeparator,
          aggPedidoToday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.category,
          null,
          parentSaleArticle3,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore1,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          null,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore1,
          todayWithoutSeparator,
          aggPedidoToday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          null,
          todayTs)
      )
    ).toDS.orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag, StockToBeDelivered.ccTags.dayTag)

    val dsResult = dsAggOrdersWithMara.transform(Sql.filterBoxPrepack(spark, appConfig))
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag, StockToBeDelivered.ccTags.dayTag)

    assertDatasetEquals(dsResult,dsExpected)
  }

  "filterSaleArticleOrPuchaseArticleWithUniqueSaleArticle " must "return both the sale articles and " +
    " the purchase articles with unique sale article" in new SqlVariablesPedidos {

    val dsAggOrdersWithMara: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] = sc.parallelize(
      List[StockToBeDeliveredWithLogisticVariableWithMara](
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore1,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          parentSaleArticle1,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore1,
          todayWithoutSeparator,
          aggPedidoToday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          parentSaleArticle1,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore2,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.category,
          PURCHASE_FLAG_2,
          parentSaleArticle2,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore2,
          todayWithoutSeparator,
          aggPedidoToday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.category,
          PURCHASE_FLAG_2,
          parentSaleArticle2,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore3,
          todayWithoutSeparator,
          aggPedidoToday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.category,
          null,
          parentSaleArticle3,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore6,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          null,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore6,
          todayWithoutSeparator,
          aggPedidoToday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          null,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle2,
          idStore1,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          SALE_FLAG,
          idArticle2,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle2,
          idStore1,
          todayWithoutSeparator,
          aggPedidoToday1,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          SALE_FLAG,
          idArticle2,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle3,
          idStore1,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          SALE_FLAG_2,
          idArticle3,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle3,
          idStore1,
          todayWithoutSeparator,
          aggPedidoToday1,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          SALE_FLAG_2,
          idArticle3,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle3,
          idStore2,
          yesterdayWithoutSeparator,
          aggPedidoToday1,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          null,
          idArticle3,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle3,
          idStore2,
          todayWithoutSeparator,
          aggPedidoToday1,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          null,
          idArticle3,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle4,
          idStore2,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          parentSaleArticle3,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle4,
          idStore2,
          todayWithoutSeparator,
          aggPedidoToday1,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          parentSaleArticle3,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle4,
          idStore4,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG_2,
          null,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle4,
          idStore4,
          todayWithoutSeparator,
          aggPedidoToday1,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG_2,
          null,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle4,
          idStore5,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG_2,
          parentSaleArticle3,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle4,
          idStore5,
          todayWithoutSeparator,
          aggPedidoToday1,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG_2,
          parentSaleArticle3,
          todayTs)
      )
    ).toDS

    val dsExpected = sc.parallelize(
      List[StockToBeDeliveredWithLogisticVariableWithMara](
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle1,
          idStore3,
          todayWithoutSeparator,
          aggPedidoToday1,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.articleType,
          BOX_AND_PREPACK_TYPE_AND_CATEGORY_2.category,
          null,
          parentSaleArticle3,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle2,
          idStore1,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          SALE_FLAG,
          idArticle2,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle2,
          idStore1,
          todayWithoutSeparator,
          aggPedidoToday1,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          SALE_FLAG,
          idArticle2,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle3,
          idStore1,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          SALE_FLAG_2,
          idArticle3,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle3,
          idStore1,
          todayWithoutSeparator,
          aggPedidoToday1,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          SALE_FLAG_2,
          idArticle3,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle3,
          idStore2,
          yesterdayWithoutSeparator,
          aggPedidoToday1,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          null,
          idArticle3,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle3,
          idStore2,
          todayWithoutSeparator,
          aggPedidoToday1,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          null,
          idArticle3,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle4,
          idStore2,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          parentSaleArticle3,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle4,
          idStore2,
          todayWithoutSeparator,
          aggPedidoToday1,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG,
          parentSaleArticle3,
          todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle4,
          idStore5,
          yesterdayWithoutSeparator,
          aggPedidoYesterday1,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG_2,
          parentSaleArticle3,
          yesterdayAtMidnightTs),
        StockToBeDeliveredWithLogisticVariableWithMara(
          idArticle4,
          idStore5,
          todayWithoutSeparator,
          aggPedidoToday1,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.articleType,
          UNIQUE_SALE_ARTICLE_TYPE_AND_CATEGORY_1.category,
          PURCHASE_FLAG_2,
          parentSaleArticle3,
          todayTs)
      )
    ).toDS.orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val dsResult = dsAggOrdersWithMara.transform(Sql.filterSaleArticleOrPuchaseArticleWithUniqueSaleArticle(spark, appConfig))
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    assertDatasetEquals(dsResult,dsExpected)
  }

  "getBoxAndPrepackOrdersContent" must "return info about materials of box and prepack type article" in new SqlVariablesPedidos {

    val dsAggOrdersWithMara: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      sc.parallelize(
        List[StockToBeDeliveredWithLogisticVariableWithMara](
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,                 null, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,                 null, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle2, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday2,   purchaseArticleType1,   purchaseArticleCategory1, PURCHASE_FLAG_2,   parentSaleArticle2, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle4, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1,       saleArticleType1,       saleArticleCategory1,       SALE_FLAG,                   "", todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle5, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1, boxPrepackArticleType1, boxPrepackArticleCategory1,   PURCHASE_FLAG,                 null, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle6, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1,       saleArticleType1,       saleArticleCategory1,            null,parentSaleArticleNull, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle7, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1, boxPrepackArticleType1, boxPrepackArticleCategory1,   PURCHASE_FLAG,                 null, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1,     todayWithoutSeparator,     aggPedidoToday1, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,                 null, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2,     todayWithoutSeparator,     aggPedidoToday2, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,                 null, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle2, idStore1,     todayWithoutSeparator,     aggPedidoToday2,   purchaseArticleType1,   purchaseArticleCategory1, PURCHASE_FLAG_2,   parentSaleArticle2, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle5, idStore3,     todayWithoutSeparator,     aggPedidoToday1, boxPrepackArticleType1, boxPrepackArticleCategory1,   PURCHASE_FLAG,                 null, todayTs)
        )
      ).toDS

    val dsJoinMaterials: Dataset[BoxAndPrepackInformation] =
      sc.parallelize(
        List[BoxAndPrepackInformation](
          BoxAndPrepackInformation(idMaterials1, idArticle1, idArticle5,    0, todayTs),
          BoxAndPrepackInformation(idMaterials2, idArticle1, idArticle6,  10d, todayTs),
          BoxAndPrepackInformation(idMaterials3, idArticle5, idArticle7, 3.7d, todayTs)
        )
      ).toDS

    val dsExpected: Dataset[StockToBeDeliveredOfBoxOrPrepack] =
      sc.parallelize(
        List[StockToBeDeliveredOfBoxOrPrepack](
          StockToBeDeliveredOfBoxOrPrepack(idArticle1, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5,     0, todayTs),
          StockToBeDeliveredOfBoxOrPrepack(idArticle1, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6,   10d, todayTs),
          StockToBeDeliveredOfBoxOrPrepack(idArticle1, idStore1,     todayWithoutSeparator,     aggPedidoToday1, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5,     0, todayTs),
          StockToBeDeliveredOfBoxOrPrepack(idArticle1, idStore1,     todayWithoutSeparator,     aggPedidoToday1, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6,   10d, todayTs),
          StockToBeDeliveredOfBoxOrPrepack(idArticle1, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5,     0, todayTs),
          StockToBeDeliveredOfBoxOrPrepack(idArticle1, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6,   10d, todayTs),
          StockToBeDeliveredOfBoxOrPrepack(idArticle1, idStore2,     todayWithoutSeparator,     aggPedidoToday2, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5,     0, todayTs),
          StockToBeDeliveredOfBoxOrPrepack(idArticle1, idStore2,     todayWithoutSeparator,     aggPedidoToday2, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6,   10d, todayTs),
          StockToBeDeliveredOfBoxOrPrepack(idArticle5, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1, boxPrepackArticleType1, boxPrepackArticleCategory1, PURCHASE_FLAG, idArticle7,  3.7d, todayTs),
          StockToBeDeliveredOfBoxOrPrepack(idArticle5, idStore3,     todayWithoutSeparator,     aggPedidoToday1, boxPrepackArticleType1, boxPrepackArticleCategory1, PURCHASE_FLAG, idArticle7,  3.7d, todayTs)
        )
      ).toDS.sort(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag, StockToBeDeliveredOfBoxOrPrepack.ccTags.dayTag, StockToBeDeliveredOfBoxOrPrepack.ccTags.salesParentArticleIdTag)

    val dsResult: Dataset[StockToBeDeliveredOfBoxOrPrepack] =
      dsAggOrdersWithMara
        .transform(Sql.addBoxAndPrepackOrdersContent(dsJoinMaterials)(spark, appConfig))
        .sort(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag, StockToBeDeliveredOfBoxOrPrepack.ccTags.dayTag, StockToBeDeliveredOfBoxOrPrepack.ccTags.salesParentArticleIdTag)

    assertDatasetEquals(dsResult, dsExpected)
  }


  "getAmountOfSalesArticlesIncludedInBoxOrPrepack " must "return the amount of material times order aggregation " in new SqlVariablesPedidos {

    val dsAggOrdersWithMaraAndMaterials: Dataset[StockToBeDeliveredOfBoxOrPrepack] =
      sc.parallelize(
        List[StockToBeDeliveredOfBoxOrPrepack](
          StockToBeDeliveredOfBoxOrPrepack(idArticle1, idStore1,     todayWithoutSeparator,     aggPedidoToday1, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5,   0, todayTs),
          StockToBeDeliveredOfBoxOrPrepack(idArticle1, idStore1,     todayWithoutSeparator,     aggPedidoToday1, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, 10d, todayTs),
          StockToBeDeliveredOfBoxOrPrepack(idArticle1, idStore2,     todayWithoutSeparator,     aggPedidoToday2, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5,   0, todayTs),
          StockToBeDeliveredOfBoxOrPrepack(idArticle1, idStore2,     todayWithoutSeparator,     aggPedidoToday2, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, 10d, todayTs),
          StockToBeDeliveredOfBoxOrPrepack(idArticle5, idStore3,     todayWithoutSeparator,     aggPedidoToday1, boxPrepackArticleType1, boxPrepackArticleCategory1, PURCHASE_FLAG, idArticle7,3.7d, todayTs),
          StockToBeDeliveredOfBoxOrPrepack(idArticle1, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5,   0, todayTs),
          StockToBeDeliveredOfBoxOrPrepack(idArticle1, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, 10d, todayTs),
          StockToBeDeliveredOfBoxOrPrepack(idArticle1, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5,   0, todayTs),
          StockToBeDeliveredOfBoxOrPrepack(idArticle1, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, 10d, todayTs),
          StockToBeDeliveredOfBoxOrPrepack(idArticle5, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1, boxPrepackArticleType1, boxPrepackArticleCategory1, PURCHASE_FLAG, idArticle7,3.7d, todayTs)

        )
      ).toDS

    val dsExpected: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      sc.parallelize(
        List[StockToBeDeliveredWithLogisticVariableWithMara](
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle5, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1 * 3.7d, boxPrepackArticleType1, boxPrepackArticleCategory1, PURCHASE_FLAG, idArticle7, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1,     todayWithoutSeparator,     aggPedidoToday1 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1,     todayWithoutSeparator,     aggPedidoToday1 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2,     todayWithoutSeparator,     aggPedidoToday2 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2,     todayWithoutSeparator,     aggPedidoToday2 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle5, idStore3,     todayWithoutSeparator,     aggPedidoToday1 * 3.7d, boxPrepackArticleType1, boxPrepackArticleCategory1, PURCHASE_FLAG, idArticle7, todayTs)
        )
      ).toDS.sort(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag, StockToBeDeliveredOfBoxOrPrepack.ccTags.dayTag, StockToBeDeliveredWithLogisticVariableWithMara.ccTags.salesParentArticleIdTag)

    val dsResult: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      dsAggOrdersWithMaraAndMaterials
        .transform(Sql.calculateAmountOfSalesArticlesIncludedInBoxOrPrepack(spark))
        .sort(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag, StockToBeDeliveredOfBoxOrPrepack.ccTags.dayTag, StockToBeDeliveredWithLogisticVariableWithMara.ccTags.salesParentArticleIdTag)

    assertDatasetEquals(dsResult, dsExpected)
  }

  it must "return empty dataset if there are not prepack or box articles" in new SqlVariablesPedidos {

    val dsAggOrdersWithMaraAndMaterials: Dataset[StockToBeDeliveredOfBoxOrPrepack] =
      sc.parallelize(
        List.empty[StockToBeDeliveredOfBoxOrPrepack]
      ).toDS


    val dsExpected: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      sc.parallelize(
        List.empty[StockToBeDeliveredWithLogisticVariableWithMara]
      ).toDS

    val dsResult: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      dsAggOrdersWithMaraAndMaterials
        .transform(Sql.calculateAmountOfSalesArticlesIncludedInBoxOrPrepack(spark))

    assertDatasetEquals(dsResult,dsExpected)
  }

  "unionAllStockToBeDeliveredWithLogisticalVariableAndMara " must "return the union of order aggregations of all article type" in new SqlVariablesPedidos {

    val dsAggOrdersWithMara1: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      sc.parallelize(
        List[StockToBeDeliveredWithLogisticVariableWithMara](
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle2, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday2,  purchaseArticleType1,   purchaseArticleCategory1, PURCHASE_FLAG_2,    parentSaleArticle2, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle4, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1,      saleArticleType1,       saleArticleCategory1,       SALE_FLAG,                    "", yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle6, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1,      saleArticleType1,       saleArticleCategory1,            null, parentSaleArticleNull, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle2, idStore1,     todayWithoutSeparator,     aggPedidoToday2,  purchaseArticleType1,   purchaseArticleCategory1, PURCHASE_FLAG_2,    parentSaleArticle2, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle4, idStore3,     todayWithoutSeparator,     aggPedidoToday1,      saleArticleType1,       saleArticleCategory1,       SALE_FLAG,                    "", todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle6, idStore3,     todayWithoutSeparator,     aggPedidoToday1,      saleArticleType1,       saleArticleCategory1,            null, parentSaleArticleNull, todayTs)
        )
      ).toDS


    val dsAggOrdersWithMara2: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      sc.parallelize(
        List[StockToBeDeliveredWithLogisticVariableWithMara](
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle5, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1 * 3.7d, boxPrepackArticleType1, boxPrepackArticleCategory1, PURCHASE_FLAG, idArticle7, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1,     todayWithoutSeparator,     aggPedidoToday1 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1,     todayWithoutSeparator,     aggPedidoToday1 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2,     todayWithoutSeparator,     aggPedidoToday2 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2,     todayWithoutSeparator,     aggPedidoToday2 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle5, idStore3,     todayWithoutSeparator,     aggPedidoToday1 * 3.7d, boxPrepackArticleType1, boxPrepackArticleCategory1, PURCHASE_FLAG, idArticle7, todayTs)
        )
      ).toDS

    val dsExpected: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      sc.parallelize(
        List[StockToBeDeliveredWithLogisticVariableWithMara](
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle2, idStore1, yesterdayWithoutSeparator,        aggPedidoYesterday2,   purchaseArticleType1,   purchaseArticleCategory1, PURCHASE_FLAG_2,    parentSaleArticle2, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle4, idStore3, yesterdayWithoutSeparator,        aggPedidoYesterday1,       saleArticleType1,       saleArticleCategory1,       SALE_FLAG,                    "", yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle6, idStore3, yesterdayWithoutSeparator,        aggPedidoYesterday1,       saleArticleType1,       saleArticleCategory1,            null, parentSaleArticleNull, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,            idArticle5, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,            idArticle6, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,            idArticle5, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,            idArticle6, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle5, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1 * 3.7d, boxPrepackArticleType1, boxPrepackArticleCategory1,   PURCHASE_FLAG,            idArticle7, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle2, idStore1,     todayWithoutSeparator,            aggPedidoToday2,   purchaseArticleType1,   purchaseArticleCategory1, PURCHASE_FLAG_2,    parentSaleArticle2, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle4, idStore3,     todayWithoutSeparator,            aggPedidoToday1,       saleArticleType1,       saleArticleCategory1,       SALE_FLAG,                    "", todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle6, idStore3,     todayWithoutSeparator,            aggPedidoToday1,       saleArticleType1,       saleArticleCategory1,            null, parentSaleArticleNull, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1,     todayWithoutSeparator,     aggPedidoToday1 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,            idArticle5, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1,     todayWithoutSeparator,     aggPedidoToday1 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,            idArticle6, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2,     todayWithoutSeparator,     aggPedidoToday2 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,            idArticle5, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2,     todayWithoutSeparator,     aggPedidoToday2 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,            idArticle6, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle5, idStore3,     todayWithoutSeparator,     aggPedidoToday1 * 3.7d, boxPrepackArticleType1, boxPrepackArticleCategory1,   PURCHASE_FLAG,            idArticle7, todayTs)

        )
      ).toDS
        .sort(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag, StockToBeDeliveredOfBoxOrPrepack.ccTags.dayTag, StockToBeDeliveredWithLogisticVariableWithMara.ccTags.salesParentArticleIdTag)

    val dsResult: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      dsAggOrdersWithMara1
        .transform(Sql.unionAllStockToBeDeliveredWithLogisticalVariableAndMara(dsAggOrdersWithMara2)(spark))
        .sort(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag, StockToBeDeliveredOfBoxOrPrepack.ccTags.dayTag, StockToBeDeliveredWithLogisticVariableWithMara.ccTags.salesParentArticleIdTag)

    assertDatasetEquals(dsResult,dsExpected)
  }

  it must "return the prepack and box articles if there only are these type of article" in new SqlVariablesPedidos {

    val dsAggOrdersWithMara1: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      sc.parallelize(
        List.empty[StockToBeDeliveredWithLogisticVariableWithMara]
      ).toDS


    val dsAggOrdersWithMara2: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      sc.parallelize(
        List[StockToBeDeliveredWithLogisticVariableWithMara](
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle5, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1 * 3.7d, boxPrepackArticleType1, boxPrepackArticleCategory1, PURCHASE_FLAG, idArticle7, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1,     todayWithoutSeparator,     aggPedidoToday1 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1,     todayWithoutSeparator,     aggPedidoToday1 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2,     todayWithoutSeparator,     aggPedidoToday2 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2,     todayWithoutSeparator,     aggPedidoToday2 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle5, idStore3,     todayWithoutSeparator,     aggPedidoToday1 * 3.7d, boxPrepackArticleType1, boxPrepackArticleCategory1, PURCHASE_FLAG, idArticle7, todayTs)

        )
      ).toDS

    val dsExpected: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      sc.parallelize(
        List[StockToBeDeliveredWithLogisticVariableWithMara](
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle5, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1 * 3.7d, boxPrepackArticleType1, boxPrepackArticleCategory1, PURCHASE_FLAG, idArticle7, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1,     todayWithoutSeparator,     aggPedidoToday1 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1,     todayWithoutSeparator,     aggPedidoToday1 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2,     todayWithoutSeparator,     aggPedidoToday2 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle5, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2,     todayWithoutSeparator,     aggPedidoToday2 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2, PURCHASE_FLAG, idArticle6, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle5, idStore3,     todayWithoutSeparator,     aggPedidoToday1 * 3.7d, boxPrepackArticleType1, boxPrepackArticleCategory1, PURCHASE_FLAG, idArticle7, todayTs)
        )
      ).toDS
        .sort(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag, StockToBeDeliveredWithLogisticVariableWithMara.ccTags.salesParentArticleIdTag)

    val dsResult: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      dsAggOrdersWithMara1
        .transform(Sql.unionAllStockToBeDeliveredWithLogisticalVariableAndMara(dsAggOrdersWithMara2)(spark))
        .sort(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag, StockToBeDeliveredWithLogisticVariableWithMara.ccTags.salesParentArticleIdTag)

    assertDatasetEquals(dsResult,dsExpected)
  }

  it must "return the sale articles or purchase article with unique sale parent article " +
    "if there only are these type of article" in new SqlVariablesPedidos {


    val dsAggOrdersWithMara1: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      sc.parallelize(
        List[StockToBeDeliveredWithLogisticVariableWithMara](
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle2, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday2,  purchaseArticleType1,   purchaseArticleCategory1, PURCHASE_FLAG_2,    parentSaleArticle2, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle4, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1,      saleArticleType1,       saleArticleCategory1,       SALE_FLAG,                    "", yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle6, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1,      saleArticleType1,       saleArticleCategory1,            null, parentSaleArticleNull, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle2, idStore1,     todayWithoutSeparator,     aggPedidoToday2,  purchaseArticleType1,   purchaseArticleCategory1, PURCHASE_FLAG_2,    parentSaleArticle2, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle4, idStore3,     todayWithoutSeparator,     aggPedidoToday1,      saleArticleType1,       saleArticleCategory1,       SALE_FLAG,                    "", todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle6, idStore3,     todayWithoutSeparator,     aggPedidoToday1,      saleArticleType1,       saleArticleCategory1,            null, parentSaleArticleNull, todayTs)
        )
      ).toDS

    val dsAggOrdersWithMara2: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      sc.parallelize(
        List.empty[StockToBeDeliveredWithLogisticVariableWithMara]
      ).toDS

    val dsExpected: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      sc.parallelize(
        List[StockToBeDeliveredWithLogisticVariableWithMara](
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle2, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday2,   purchaseArticleType1,   purchaseArticleCategory1, PURCHASE_FLAG_2,    parentSaleArticle2, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle4, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1,       saleArticleType1,       saleArticleCategory1,       SALE_FLAG,                    "", yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle6, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1,       saleArticleType1,       saleArticleCategory1,            null, parentSaleArticleNull, yesterdayAtMidnightTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle2, idStore1,     todayWithoutSeparator,     aggPedidoToday2,   purchaseArticleType1,   purchaseArticleCategory1, PURCHASE_FLAG_2,    parentSaleArticle2, todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle4, idStore3,     todayWithoutSeparator,     aggPedidoToday1,       saleArticleType1,       saleArticleCategory1,       SALE_FLAG,                    "", todayTs),
          StockToBeDeliveredWithLogisticVariableWithMara(idArticle6, idStore3,     todayWithoutSeparator,     aggPedidoToday1,       saleArticleType1,       saleArticleCategory1,            null, parentSaleArticleNull, todayTs)
        )
      ).toDS
        .sort(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag, StockToBeDeliveredWithLogisticVariableWithMara.ccTags.salesParentArticleIdTag)

    val dsResult: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      dsAggOrdersWithMara1
        .transform(Sql.unionAllStockToBeDeliveredWithLogisticalVariableAndMara(dsAggOrdersWithMara2)(spark))
        .sort(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag, StockToBeDeliveredWithLogisticVariableWithMara.ccTags.salesParentArticleIdTag)

    assertDatasetEquals(dsResult,dsExpected)
  }

  "adaptArticleToSaleArticle " must " change any article to sale article" in new SqlVariablesPedidos {

    val dsAggPedidosWithSaleArticle: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] = sc.parallelize(
      List[StockToBeDeliveredWithLogisticVariableWithMara](
        StockToBeDeliveredWithLogisticVariableWithMara(idArticle2, idStore1, yesterdayWithoutSeparator,        aggPedidoYesterday2,   purchaseArticleType1,   purchaseArticleCategory1, PURCHASE_FLAG_2,    parentSaleArticle2, todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(idArticle4, idStore3, yesterdayWithoutSeparator,        aggPedidoYesterday1,       saleArticleType1,       saleArticleCategory1,       SALE_FLAG,                    "", todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(idArticle6, idStore3, yesterdayWithoutSeparator,        aggPedidoYesterday1,       saleArticleType1,       saleArticleCategory1,            null, parentSaleArticleNull, todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(idArticle7, idStore3, yesterdayWithoutSeparator,        aggPedidoYesterday2,       saleArticleType1,       saleArticleCategory1,     SALE_FLAG_2, parentSaleArticleNull, todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,            idArticle5, todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,            idArticle6, todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,            idArticle5, todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,            idArticle6, todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(idArticle5, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1 * 3.7d, boxPrepackArticleType1, boxPrepackArticleCategory1,   PURCHASE_FLAG,            idArticle7, todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(idArticle2, idStore1,     todayWithoutSeparator,            aggPedidoToday2,   purchaseArticleType1,   purchaseArticleCategory1, PURCHASE_FLAG_2,    parentSaleArticle2, todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(idArticle4, idStore3,     todayWithoutSeparator,            aggPedidoToday1,       saleArticleType1,       saleArticleCategory1,       SALE_FLAG,                    "", todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(idArticle6, idStore3,     todayWithoutSeparator,            aggPedidoToday1,       saleArticleType1,       saleArticleCategory1,            null, parentSaleArticleNull, todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(idArticle7, idStore3,     todayWithoutSeparator,            aggPedidoToday2,       saleArticleType1,       saleArticleCategory1,     SALE_FLAG_2, parentSaleArticleNull, todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1,     todayWithoutSeparator,     aggPedidoToday1 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,            idArticle5, todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore1,     todayWithoutSeparator,     aggPedidoToday1 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,            idArticle6, todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2,     todayWithoutSeparator,     aggPedidoToday2 *    0, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,            idArticle5, todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(idArticle1, idStore2,     todayWithoutSeparator,     aggPedidoToday2 *  10d, boxPrepackArticleType2, boxPrepackArticleCategory2,   PURCHASE_FLAG,            idArticle6, todayTs),
        StockToBeDeliveredWithLogisticVariableWithMara(idArticle5, idStore3,     todayWithoutSeparator,     aggPedidoToday1 * 3.7d, boxPrepackArticleType1, boxPrepackArticleCategory1,   PURCHASE_FLAG,            idArticle7, todayTs)
      )
    ).toDS

    val dsExpected = sc.parallelize(
      List[StockToBeDelivered](
        StockToBeDelivered( parentSaleArticle2, idStore1, yesterdayWithoutSeparator,        aggPedidoYesterday2, todayTs),
        StockToBeDelivered(         idArticle4, idStore3, yesterdayWithoutSeparator,        aggPedidoYesterday1, todayTs),
        StockToBeDelivered(         idArticle6, idStore3, yesterdayWithoutSeparator,        aggPedidoYesterday1, todayTs),
        StockToBeDelivered(         idArticle7, idStore3, yesterdayWithoutSeparator,        aggPedidoYesterday2, todayTs),
        StockToBeDelivered(         idArticle5, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1 *    0, todayTs),
        StockToBeDelivered(         idArticle6, idStore1, yesterdayWithoutSeparator, aggPedidoYesterday1 *  10d, todayTs),
        StockToBeDelivered(         idArticle5, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2 *    0, todayTs),
        StockToBeDelivered(         idArticle6, idStore2, yesterdayWithoutSeparator, aggPedidoYesterday2 *  10d, todayTs),
        StockToBeDelivered(         idArticle7, idStore3, yesterdayWithoutSeparator, aggPedidoYesterday1 * 3.7d, todayTs),
        StockToBeDelivered( parentSaleArticle2, idStore1,     todayWithoutSeparator,            aggPedidoToday2, todayTs),
        StockToBeDelivered(         idArticle4, idStore3,     todayWithoutSeparator,            aggPedidoToday1, todayTs),
        StockToBeDelivered(         idArticle6, idStore3,     todayWithoutSeparator,            aggPedidoToday1, todayTs),
        StockToBeDelivered(         idArticle7, idStore3,     todayWithoutSeparator,            aggPedidoToday2, todayTs),
        StockToBeDelivered(         idArticle5, idStore1,     todayWithoutSeparator,     aggPedidoToday1 *    0, todayTs),
        StockToBeDelivered(         idArticle6, idStore1,     todayWithoutSeparator,     aggPedidoToday1 *  10d, todayTs),
        StockToBeDelivered(         idArticle5, idStore2,     todayWithoutSeparator,     aggPedidoToday2 *    0, todayTs),
        StockToBeDelivered(         idArticle6, idStore2,     todayWithoutSeparator,     aggPedidoToday2 *  10d, todayTs),
        StockToBeDelivered(         idArticle7, idStore3,     todayWithoutSeparator,     aggPedidoToday1 * 3.7d, todayTs)
      )
    ).toDS.orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val dsResult = dsAggPedidosWithSaleArticle.transform(Sql.adaptArticleToSaleArticle(spark,appConfig))
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    assertDatasetEquals(dsResult,dsExpected)
  }

  "pivotAmountForAllProjectionDaysByArticleAndStore" must "calculate all stock to be delivered by day for all projection days (in column format)" in new SqlVariablesPedidos {

    val dsAggPedidosWithSaleArticle: Dataset[StockToBeDelivered] = sc.parallelize(
      List[StockToBeDelivered](
        // Article and store with data only for the day previous to N
        StockToBeDelivered(idArticle1, idStore1,             yesterdayWithoutSeparator,  10d, todayTs),
        // Article and store with data only for the last projection day (plus 1)
        StockToBeDelivered(idArticle2, idStore1, lastDayToProjectPlus1WithoutSeparator, 10d, todayTs),
        // Article and store with data for first (day previous to N) and last day (last projection day plus 1)
        StockToBeDelivered(idArticle1, idStore2,             yesterdayWithoutSeparator, 10d, todayTs),
        StockToBeDelivered(idArticle1, idStore2, lastDayToProjectPlus1WithoutSeparator, 10d, todayTs),
        // Article and store with data only for a day in between
        StockToBeDelivered(idArticle2, idStore2,                 todayWithoutSeparator, 10d, todayTs),
        // Article and store with data for almost all days
        // (we leave one method to test what happens when there are no orders for one day)
        StockToBeDelivered(idArticle3, idStore3,             yesterdayWithoutSeparator, 10d, todayTs),
        StockToBeDelivered(idArticle3, idStore3,                 todayWithoutSeparator, 10d, todayTs),
        StockToBeDelivered(idArticle3, idStore3,      lastDayToProjectWithoutSeparator, 10d, todayTs),
        StockToBeDelivered(idArticle3, idStore3, lastDayToProjectPlus1WithoutSeparator, 10d, todayTs),
        // Article and store with several orders for the same days
        StockToBeDelivered(idArticle4, idStore3,                 todayWithoutSeparator, 10d, todayTs),
        StockToBeDelivered(idArticle4, idStore3,                 todayWithoutSeparator, 10d, todayTs),
        StockToBeDelivered(idArticle4, idStore3,      lastDayToProjectWithoutSeparator, 10d, todayTs),
        StockToBeDelivered(idArticle4, idStore3,      lastDayToProjectWithoutSeparator, 10d, todayTs)
      )
    ).toDS

    val dsExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row(idArticle1, idStore1, 10d,  0d, 0d, 0d, 0d,  0d,  0d, todayTs),
      Row(idArticle2, idStore1,  0d,  0d, 0d, 0d, 0d,  0d, 10d, todayTs),
      Row(idArticle1, idStore2, 10d,  0d, 0d, 0d, 0d,  0d, 10d, todayTs),
      Row(idArticle2, idStore2,  0d, 10d, 0d, 0d, 0d,  0d,  0d, todayTs),
      Row(idArticle3, idStore3, 10d, 10d, 0d, 0d, 0d, 10d, 10d, todayTs),
      Row(idArticle4, idStore3,  0d, 20d, 0d, 0d, 0d, 20d,  0d, todayTs)
    )),
      stockToBeDeliveredStructType)
    .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val dsResult: DataFrame = dsAggPedidosWithSaleArticle
      .transform(Sql.pivotAmountForAllProjectionDaysByArticleAndStore(
        yesterdayAtMidnightTs,
        numberOfDaysToProject + 1,
        StockToBeDelivered.ccTags.dayTag,
        StockToBeDelivered.ccTags.amountToBeDeliveredTag,
        StockToBeDelivered.ccTags.amountToBeDeliveredNTag,
      -1)(spark))
      .select(stockToBeDeliveredStructType.fieldNames.map(col) :_*)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    assertDataFrameEquals(dsResult, dsExpected)
  }

  it must "calculate all reservations by day for all projection days (in column format)" in new SqlVariablesPedidos {

    val dsCustomerReservations: Dataset[CustomerReservationsView] = sc.parallelize(
      List[CustomerReservationsView](
        CustomerReservationsView(idArticle2, idStore1, lastDayToProjectPlus1WithoutSeparator, 10d, todayTs),
        CustomerReservationsView(idArticle1, idStore2, lastDayToProjectPlus1WithoutSeparator, 10d, todayTs),
        CustomerReservationsView(idArticle2, idStore2, todayWithoutSeparator, 10d, todayTs),
        CustomerReservationsView(idArticle3, idStore3, todayWithoutSeparator, 10d, todayTs),
        CustomerReservationsView(idArticle3, idStore3, lastDayToProjectWithoutSeparator, 10d, todayTs),
        CustomerReservationsView(idArticle3, idStore3, lastDayToProjectPlus1WithoutSeparator, 10d, todayTs),
        CustomerReservationsView(idArticle4, idStore3, todayWithoutSeparator, 10d, todayTs),
        CustomerReservationsView(idArticle4, idStore3, todayWithoutSeparator, 10d, todayTs),
        CustomerReservationsView(idArticle4, idStore3, lastDayToProjectWithoutSeparator, 10d, todayTs),
        CustomerReservationsView(idArticle4, idStore3, lastDayToProjectWithoutSeparator, 10d, todayTs)
      )
    ).toDS

    val dsExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row(idArticle2, idStore1,  0d, 0d, 0d, 0d,  0d, 10d, todayTs),
      Row(idArticle1, idStore2,  0d, 0d, 0d, 0d,  0d, 10d, todayTs),
      Row(idArticle2, idStore2, 10d, 0d, 0d, 0d,  0d,  0d, todayTs),
      Row(idArticle3, idStore3, 10d, 0d, 0d, 0d, 10d, 10d, todayTs),
      Row(idArticle4, idStore3, 20d, 0d, 0d, 0d, 20d,  0d, todayTs)
    )),
      customerReservationsStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val dsResult: DataFrame = dsCustomerReservations
      .transform(Sql.pivotAmountForAllProjectionDaysByArticleAndStore(
        todayTs,
        numberOfDaysToProject + 1,
        CustomerReservations.ccTags.reservationDateTag,
        CustomerReservations.ccTags.amountTag,
        CustomerReservations.ccTags.amountNTag,
        0)(spark))
      .select(customerReservationsStructType.fieldNames.map(col) :_*)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    assertDataFrameEquals(dsResult, dsExpected)
  }

  "joinStoreTypeAndAggPP using full join " must
    " return type store true if it is null and isPP false if it is null " taggedAs (UnitTestTag) in new SqlVariables  {

    val dsStoreType: Dataset[StoreTypeView] = sc.parallelize(List[StoreTypeView](
      StoreTypeView(idStore1, true,  ts),
      StoreTypeView(idStore2, true,  ts1),
      StoreTypeView(idStore3, false, ts),
      StoreTypeView(idStore4, false, ts2),
      StoreTypeView(idStore7, false, ts)
    )).toDS

    val dsAggPP: Dataset[AggPP] = sc.parallelize(List[AggPP](
      AggPP(idStore2, false, ts),
      AggPP(idStore3, true,  ts1),
      AggPP(idStore5, true,  ts),
      AggPP(idStore6, false, ts5),
      AggPP(idStore7, false, ts)
    )).toDS

    val dsExpected: Dataset[JoinStoreTypeAndAggPP]= sc.parallelize(List[JoinStoreTypeAndAggPP](
      JoinStoreTypeAndAggPP(idStore1, true, false,  ts),
      JoinStoreTypeAndAggPP(idStore2, true, false,  ts),
      JoinStoreTypeAndAggPP(idStore3, false, true,  ts),
      JoinStoreTypeAndAggPP(idStore4, false, false, ts2),
      JoinStoreTypeAndAggPP(idStore5, true, true,   ts),
      JoinStoreTypeAndAggPP(idStore6, true, false,  ts5),
      JoinStoreTypeAndAggPP(idStore7, false, false, ts)
    )).toDS

    val dsResult = Sql.joinStoreTypeAndAggPP(dsStoreType, dsAggPP)(spark).sort(Surtido.ccTags.idStoreTag)
    assertDatasetEquals(dsResult,dsExpected)
  }

  "addConsolidatedStockToAggregatedSalesMovements" must " return a dataset with the consolidated stock and aggregated sale movements of an article " taggedAs (UnitTestTag) in new SqlVariables  {

    val dsStockConsolidado: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, stockConsolidado, ts)
    )).toDS

    val dsAcumVentas: Dataset[SaleMovementsView] = sc.parallelize(List[SaleMovementsView](
      SaleMovementsView(idArticle1, idStore1, acumVentas, ts)
    )).toDS

    val dsExpected: Dataset[JoinConsolidatedStockAndAggregatedSaleMovements]= sc.parallelize(List[JoinConsolidatedStockAndAggregatedSaleMovements](
      JoinConsolidatedStockAndAggregatedSaleMovements(idArticle1, idStore1, measureUnitEA, stockConsolidado, acumVentas, ts)
    )).toDS

    val dsResult = dsAcumVentas.transform(Sql.addConsolidatedStockToAggregatedSalesMovements(dsStockConsolidado)(spark))
      .sort(Surtido.ccTags.idStoreTag)
    assertDatasetEquals(dsResult,dsExpected)
  }

  it must " return all rows have either consolidated stock or aggregated sale movements" taggedAs (UnitTestTag) in new SqlVariables  {

    val dsStockConsolidado: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, stockConsolidado, ts),
      StockConsolidadoView(idArticle1, idStore2, measureUnitEA, stockConsolidado, ts),
      StockConsolidadoView(idArticle1, idStore3, measureUnitEA, stockConsolidado, ts)
    )).toDS

    val dsAcumVentas: Dataset[SaleMovementsView] = sc.parallelize(List[SaleMovementsView](
      SaleMovementsView(idArticle1, idStore1, acumVentas, ts),
      SaleMovementsView(idArticle1, idStore4, acumVentas, ts)
    )).toDS

    val dsExpected: Dataset[JoinConsolidatedStockAndAggregatedSaleMovements]= sc.parallelize(List[JoinConsolidatedStockAndAggregatedSaleMovements](
      JoinConsolidatedStockAndAggregatedSaleMovements(idArticle1, idStore1, measureUnitEA, stockConsolidado, acumVentas, ts),
      JoinConsolidatedStockAndAggregatedSaleMovements(idArticle1, idStore2, measureUnitEA, stockConsolidado, 0, ts),
      JoinConsolidatedStockAndAggregatedSaleMovements(idArticle1, idStore3, measureUnitEA, stockConsolidado, 0, ts),
      JoinConsolidatedStockAndAggregatedSaleMovements(idArticle1, idStore4,          null, 0, acumVentas, ts)
    )).toDS

    val dsResult = dsAcumVentas.transform(Sql.addConsolidatedStockToAggregatedSalesMovements(dsStockConsolidado)(spark))
      .sort(Surtido.ccTags.idStoreTag)
    assertDatasetEquals(dsResult,dsExpected)
  }

  it must " return aggregated sale movements equal to zero when the article didn't have it" taggedAs (UnitTestTag) in new SqlVariables  {

    val dsStockConsolidado: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, stockConsolidado, ts)
    )).toDS

    val dsAcumVentas: Dataset[SaleMovementsView] = sc.parallelize(List.empty[SaleMovementsView]).toDS

    val dsExpected: Dataset[JoinConsolidatedStockAndAggregatedSaleMovements]= sc.parallelize(List[JoinConsolidatedStockAndAggregatedSaleMovements](
      JoinConsolidatedStockAndAggregatedSaleMovements(idArticle1, idStore1, measureUnitEA, stockConsolidado, 0, ts)
    )).toDS

    val dsResult = dsAcumVentas.transform(Sql.addConsolidatedStockToAggregatedSalesMovements(dsStockConsolidado)(spark))
      .sort(Surtido.ccTags.idStoreTag)
    assertDatasetEquals(dsResult,dsExpected)
  }

  it must
    " return consolidated stock equal to zero and unity measure null when the article didn't have them" taggedAs (UnitTestTag) in new SqlVariables  {

    val dsStockConsolidado: Dataset[StockConsolidadoView] = sc.parallelize(List.empty[StockConsolidadoView]).toDS

    val dsAcumVentas: Dataset[SaleMovementsView] = sc.parallelize(List[SaleMovementsView](
      SaleMovementsView(idArticle1, idStore1, acumVentas, ts)
    )).toDS

    val dsExpected: Dataset[JoinConsolidatedStockAndAggregatedSaleMovements]= sc.parallelize(List[JoinConsolidatedStockAndAggregatedSaleMovements](
      JoinConsolidatedStockAndAggregatedSaleMovements(idArticle1, idStore1, null, 0, acumVentas, ts)
    )).toDS

    val dsResult = dsAcumVentas.transform(Sql.addConsolidatedStockToAggregatedSalesMovements(dsStockConsolidado)(spark))
      .sort(Surtido.ccTags.idStoreTag)
    assertDatasetEquals(dsResult,dsExpected)
  }


  it must " return the latest update date" taggedAs (UnitTestTag) in new SqlVariables  {

    val dsStockConsolidado: Dataset[StockConsolidadoView] = sc.parallelize(List[StockConsolidadoView](
      StockConsolidadoView(idArticle1, idStore1, measureUnitEA, stockConsolidado, beforeTs)
    )).toDS

    val dsAcumVentas: Dataset[SaleMovementsView] = sc.parallelize(List[SaleMovementsView](
      SaleMovementsView(idArticle1, idStore1, acumVentas, afterTs)
    )).toDS

    val dsExpected: Dataset[JoinConsolidatedStockAndAggregatedSaleMovements]= sc.parallelize(List[JoinConsolidatedStockAndAggregatedSaleMovements](
      JoinConsolidatedStockAndAggregatedSaleMovements(idArticle1, idStore1, measureUnitEA, stockConsolidado, acumVentas, afterTs)
    )).toDS

    val dsResult = dsAcumVentas.transform(Sql.addConsolidatedStockToAggregatedSalesMovements(dsStockConsolidado)(spark))
      .sort(Surtido.ccTags.idStoreTag)
    assertDatasetEquals(dsResult,dsExpected)
  }

  "addFixedDataToConsolidatesStockAndMovements" must " return a dataset with the consolidated stock, aggregated sale movements and fixed data of an article " taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val dsJoinConsolidatedStockAndAggregatedSaleMovements: Dataset[JoinConsolidatedStockAndAggregatedSaleMovements] = sc.parallelize(List[JoinConsolidatedStockAndAggregatedSaleMovements](
      JoinConsolidatedStockAndAggregatedSaleMovements(idArticle1, idStore1, measureUnitEA, stockConsolidado, acumVentas, ts)
    )).toDS

    val dfDatosFijos: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
        Row( idArticle1,  idStore1, today, sector01, measureUnitKG, 20d, 50d,  "A", ts,   0d,   70.5d,   0d,   0d, 0d, 0d))),
      fixedDataFinalStructType)
      .orderBy(DatosFijos.ccTags.idArticleTag, DatosFijos.ccTags.idStoreTag)

    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idArticle1,  idStore1, sector01, measureUnitEA, 20d,  50d,  "A",  ts,   0d,   70.5d,   0d,   0d,   0d, 0d, stockConsolidado,  acumVentas))),
       fixedDataWithConsolidatedAndAggSaleMovsStructType)
      .orderBy(DatosFijos.ccTags.idArticleTag, DatosFijos.ccTags.idStoreTag)

    val dfResult = dsJoinConsolidatedStockAndAggregatedSaleMovements
      .transform(Sql.addFixedDataToConsolidatedStockAndMovements(dfDatosFijos)(spark))
      .sort(Surtido.ccTags.idStoreTag)
    assertDatasetEquals(dfExpected, dfResult)
  }

  it must " return the latest update date, stock, aggregated sale movements, unit measure" taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val dsJoinConsolidatedStockAndAggregatedSaleMovements: Dataset[JoinConsolidatedStockAndAggregatedSaleMovements] = sc.parallelize(List[JoinConsolidatedStockAndAggregatedSaleMovements](
      JoinConsolidatedStockAndAggregatedSaleMovements(idArticle1, idStore1, measureUnitEA,  stockConsolidado,  acumVentas,  afterTs),
      JoinConsolidatedStockAndAggregatedSaleMovements(idArticle1, idStore2, measureUnitKG, stockConsolidado2, acumVentas2, beforeTs),
      JoinConsolidatedStockAndAggregatedSaleMovements(idArticle1, idStore3,          null, stockConsolidado3, acumVentas3, beforeTs),
      JoinConsolidatedStockAndAggregatedSaleMovements(idArticle2, idStore1,          null,                0d,  acumVentas, beforeTs),
      JoinConsolidatedStockAndAggregatedSaleMovements(idArticle2, idStore2,          null, stockConsolidado2,          0d, beforeTs),
      JoinConsolidatedStockAndAggregatedSaleMovements(idArticle2, idStore3,          null,                0d,          0d,  afterTs),
      JoinConsolidatedStockAndAggregatedSaleMovements(idArticle3, idStore1,          null,  stockConsolidado,  acumVentas, beforeTs)
    )).toDS

    val dfDatosFijos: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idArticle1,  idStore1, today,              sector01, measureUnitEA, null,  null,  "A", beforeTs,   0d,   0d,   0d,   0d,  0d,  1d),
      Row( idArticle1,  idStore2, today, FRESH_PRODUCTS_SECTOR, measureUnitEA,   2d,  null, "A+",  afterTs,  10d,  11d,  12d,  13d, 14d, 11d),
      Row( idArticle1,  idStore3, today,                  null, measureUnitKG, null, 15.3d,  "B", beforeTs,  11d,  12d,  13d,  14d, 15d, 11d),
      Row( idArticle1,  idStore4, today,                  null,          null, null, 15.3d,  "B", beforeTs,  11d,  12d,  13d,  14d, 15d, 11d),
      Row( idArticle2,  idStore1, today,              sector01, measureUnitEA,  10d,   10d, null, beforeTs,   0d,   0d,   0d,   0d,  0d,  1d),
      Row( idArticle2,  idStore2, today, FRESH_PRODUCTS_SECTOR, measureUnitEA,  10d,   10d,  "C",  afterTs,  10d,  11d,  12d,  13d, 14d, 11d),
      Row( idArticle2,  idStore3, today,                  null, measureUnitKG,  10d,   10d,  "B",  afterTs,  11d,  12d,  13d,  14d, 15d, 11d)
    )), fixedDataFinalStructType)
      .orderBy(DatosFijos.ccTags.idArticleTag, DatosFijos.ccTags.idStoreTag)

    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idArticle1,  idStore1,              sector01, measureUnitEA, null,  null,  "A",  afterTs,   0d,   0d,   0d,   0d,   0d,  1d,  stockConsolidado,  acumVentas),
      Row( idArticle1,  idStore2, FRESH_PRODUCTS_SECTOR, measureUnitKG,   2d,  null, "A+",  afterTs,  10d,  11d,  12d,  13d,  14d, 11d, stockConsolidado2, acumVentas2),
      Row( idArticle1,  idStore3,                  null, measureUnitKG, null, 15.3d,  "B", beforeTs,  11d,  12d,  13d,  14d,  15d, 11d, stockConsolidado3, acumVentas3),
      Row( idArticle1,  idStore4,                  null,          null, null, 15.3d,  "B", beforeTs,  11d,  12d,  13d,  14d,  15d, 11d,                0d,          0d),
      Row( idArticle2,  idStore1,              sector01, measureUnitEA,  10d,   10d, null, beforeTs,   0d,   0d,   0d,   0d,   0d,  1d,                0d,  acumVentas),
      Row( idArticle2,  idStore2, FRESH_PRODUCTS_SECTOR, measureUnitEA,  10d,   10d,  "C",  afterTs,  10d,  11d,  12d,  13d,  14d, 11d, stockConsolidado2,          0d),
      Row( idArticle2,  idStore3,                  null, measureUnitKG,  10d,   10d,  "B",  afterTs,  11d,  12d,  13d,  14d,  15d, 11d,                0d,          0d)
    )), fixedDataWithConsolidatedAndAggSaleMovsStructType)
      .orderBy(DatosFijos.ccTags.idArticleTag, DatosFijos.ccTags.idStoreTag)


    val dfResult: DataFrame = dsJoinConsolidatedStockAndAggregatedSaleMovements
      .transform(
        Sql.addFixedDataToConsolidatedStockAndMovements(dfDatosFijos)(spark))
      .orderBy(DatosFijos.ccTags.idArticleTag, DatosFijos.ccTags.idStoreTag)

    assertDataFrameEquals(dfExpected, dfResult)
  }

  "addStoreTypeAndPreparationInformationToPrecalculatedStockData using left join " must
    " return a properly StockATP's Dataset " taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val stockConf: StockConf = StockConf(80d, 20d, 90d, 10d, 20d, 40d, 60d, 80d, 5, ts1)
    val dateRegistroEjecucion: Option[Timestamp] = Some(ts2)

    val dfJoinDatosFijosAndStockConsAndAcumVentasAndAggPedidos: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idArticle1,  idStore1,              sector01, measureUnitEA,  "a", ts2,  0d,  0d,  0d,  0d,  0d,  0d,  stockConsolidado,  acumVentas, 10d,  0d, 0d, 0d, 0d,  0d,  0d,   10d,  0d,  0d, 0d,  0d,  0d,   0.3d),
      Row( idArticle1,  idStore2, FRESH_PRODUCTS_SECTOR, measureUnitKG, "a+", ts3, 10d, 11d, 12d, 13d, 14d, 14d, stockConsolidado2, acumVentas2, 10d,  0d, 0d, 0d, 0d,  0d, 10d,    1d,  0d,  0d, 0d,  0d,  0d,     1d),
      Row( idArticle1,  idStore3,                  null, measureUnitKG,  "b", ts1, 11d,  0d, 13d, 14d, 15d, 15d, stockConsolidado3, acumVentas3,  0d,  0d, 0d, 0d, 0d,  0d,  0d,    0d,  0d,  0d, 0d,  0d,  0d,     1d),
      Row( idArticle1,  idStore4,                  null,          null,  "c", ts3, 11d, 12d, 13d, 14d,  0d,  0d,                0d,          0d,  0d,  0d, 0d, 0d, 0d,  0d,  0d,    0d,  0d,  0d, 0d,  0d,  0d, 0.505d),
      Row( idArticle1,  idStore5, FRESH_PRODUCTS_SECTOR, measureUnitKG, null, ts4, 10d, 11d, 12d, 13d, 14d, 14d, stockConsolidado2, acumVentas2,  0d,  0d, 0d, 0d, 0d,  0d, 10d,  1.2d,  0d,  0d, 0d,  0d,  0d,   0.3d),
      Row( idArticle1,  idStore6, FRESH_PRODUCTS_SECTOR, measureUnitKG, null, ts1, 10d, 11d, 12d, 13d, 14d, 14d, stockConsolidado2, acumVentas2,  0d,  0d, 0d, 0d, 0d,  0d, 10d, 2.01d,  0d,  0d, 0d,  0d,  0d,     1d),
      Row( idArticle2,  idStore2, FRESH_PRODUCTS_SECTOR, measureUnitKG,  "b", ts1, 10d, 11d, 12d, 13d, 14d, 14d, stockConsolidado2, acumVentas2,  0d,  0d, 0d, 0d, 0d,  0d, 10d,    0d,  0d,  0d, 0d,  0d,  0d,   0.5d)
    )), fixedDataWithConsolidatedAndAggSaleMovsAndOrdersAndCustomerReservationsStructType)

    val dsJoinStoreTypeAndAggPP: Dataset[JoinStoreTypeAndAggPP] = sc.parallelize(List[JoinStoreTypeAndAggPP](
      JoinStoreTypeAndAggPP(idStore1,  true,  true, ts2),
      JoinStoreTypeAndAggPP(idStore2,  true, false, ts1),
      JoinStoreTypeAndAggPP(idStore3, false,  true, ts3),
      JoinStoreTypeAndAggPP(idStore4, false, false, ts3)
    )).toDS

    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row(idStore1, idArticle1,              sector01,      measureUnitEA,  "a", currentTimestamp,  0d,  0d,  0d,  0d,  0d,  0d,  stockConsolidado,  acumVentas, 10d,  0d, 0d, 0d, 0d,  0d,  0d,   10d,  0d,  0d, 0d,  0d,  0d,  true,  true, "user",   0.3d),
      Row(idStore2, idArticle1, FRESH_PRODUCTS_SECTOR,      measureUnitKG, "a+", currentTimestamp, 10d, 11d, 12d, 13d, 14d, 14d, stockConsolidado2, acumVentas2, 10d,  0d, 0d, 0d, 0d,  0d, 10d,    1d,  0d,  0d, 0d,  0d,  0d,  true, false, "user",     1d),
      Row(idStore3, idArticle1,                  null,      measureUnitKG,  "b", currentTimestamp, 11d,  0d, 13d, 14d, 15d, 15d, stockConsolidado3, acumVentas3,  0d,  0d, 0d, 0d, 0d,  0d,  0d,    0d,  0d,  0d, 0d,  0d,  0d, false,  true, "user",     1d),
      Row(idStore4, idArticle1,                  null, defaultUnitMeasure,  "c", currentTimestamp, 11d, 12d, 13d, 14d,  0d,  0d,                0d,          0d,  0d,  0d, 0d, 0d, 0d,  0d,  0d,    0d,  0d,  0d, 0d,  0d,  0d, false, false, "user", 0.505d),
      Row(idStore5, idArticle1, FRESH_PRODUCTS_SECTOR,      measureUnitKG, null, currentTimestamp, 10d, 11d, 12d, 13d, 14d, 14d, stockConsolidado2, acumVentas2,  0d,  0d, 0d, 0d, 0d,  0d, 10d,  1.2d,  0d,  0d, 0d,  0d,  0d,  true, false, "user",   0.3d),
      Row(idStore6, idArticle1, FRESH_PRODUCTS_SECTOR,      measureUnitKG, null, currentTimestamp, 10d, 11d, 12d, 13d, 14d, 14d, stockConsolidado2, acumVentas2,  0d,  0d, 0d, 0d, 0d,  0d, 10d, 2.01d,  0d,  0d, 0d,  0d,  0d,  true, false, "user",     1d),
      Row(idStore2, idArticle2, FRESH_PRODUCTS_SECTOR,      measureUnitKG,  "b", currentTimestamp, 10d, 11d, 12d, 13d, 14d, 14d, stockConsolidado2, acumVentas2,  0d,  0d, 0d, 0d, 0d,  0d, 10d,    0d,  0d,  0d, 0d,  0d,  0d,  true, false, "user",   0.5d)

    )), beforeCalculateStockAtpNStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val dfResult: DataFrame =
      Sql.addStoreTypeAndPreparationInformationToPrecalculatedStockData(
        dfJoinDatosFijosAndStockConsAndAcumVentasAndAggPedidos,
        dsJoinStoreTypeAndAggPP,
        stockConf,
        currentTimestamp,
        dateRegistroEjecucion,
        user)(spark,defaultConfig)
        .select(beforeCalculateStockAtpNStructType.fieldNames.map(col) :_*)
        .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    assertDataFrameEquals(dfResult, dfExpected)
  }

  it must
    " return a properly StockATP's Dataset when the conf is updated" taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val stockConf: StockConf = StockConf(80d, 20d, 90d, 10d, 20d, 40d, 60d, 80d, 5, ts5)
    val dateRegistroEjecucion: Option[Timestamp] = Some(ts2)

    val dfJoinDatosFijosAndStockConsAndAcumVentasAndAggPedidos: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row( idArticle1,  idStore1,              sector01, measureUnitEA,  "a", ts2,  0d,  0d,  0d,  0d,  0d,  0d,  stockConsolidado,  acumVentas, 10d,  0d, 0d, 0d, 0d,  0d,  0d,   10d,  0d,  0d, 0d,  0d,  0d,   0.3d),
      Row( idArticle1,  idStore2, FRESH_PRODUCTS_SECTOR, measureUnitKG, "a+", ts3, 10d, 11d, 12d, 13d, 14d, 14d, stockConsolidado2, acumVentas2, 10d,  0d, 0d, 0d, 0d,  0d, 10d,    1d,  0d,  0d, 0d,  0d,  0d,     1d),
      Row( idArticle1,  idStore3,                  null, measureUnitKG,  "b", ts1, 11d,  0d, 13d, 14d, 15d, 15d, stockConsolidado3, acumVentas3,  0d,  0d, 0d, 0d, 0d,  0d,  0d,    0d,  0d,  0d, 0d,  0d,  0d,     1d),
      Row( idArticle1,  idStore4,                  null,          null,  "c", ts3, 11d, 12d, 13d, 14d,  0d,  0d,                0d,          0d,  0d,  0d, 0d, 0d, 0d,  0d,  0d,    0d,  0d,  0d, 0d,  0d,  0d, 0.505d),
      Row( idArticle1,  idStore5, FRESH_PRODUCTS_SECTOR, measureUnitKG, null, ts4, 10d, 11d, 12d, 13d, 14d, 14d, stockConsolidado2, acumVentas2,  0d,  0d, 0d, 0d, 0d,  0d, 10d,  1.2d,  0d,  0d, 0d,  0d,  0d,   0.3d),
      Row( idArticle1,  idStore6, FRESH_PRODUCTS_SECTOR, measureUnitKG, null, ts1, 10d, 11d, 12d, 13d, 14d, 14d, stockConsolidado2, acumVentas2,  0d,  0d, 0d, 0d, 0d,  0d, 10d, 2.01d,  0d,  0d, 0d,  0d,  0d, 0.505d),
      Row( idArticle2,  idStore2, FRESH_PRODUCTS_SECTOR, measureUnitKG,  "b", ts1, 10d, 11d, 12d, 13d, 14d, 14d, stockConsolidado2, acumVentas2,  0d,  0d, 0d, 0d, 0d,  0d, 10d,    0d,  0d,  0d, 0d,  0d,  0d,   0.3d)
    )), fixedDataWithConsolidatedAndAggSaleMovsAndOrdersAndCustomerReservationsStructType)


    val dsJoinStoreTypeAndAggPP: Dataset[JoinStoreTypeAndAggPP] = sc.parallelize(List[JoinStoreTypeAndAggPP](
      JoinStoreTypeAndAggPP(idStore1,  true,  true, ts2),
      JoinStoreTypeAndAggPP(idStore2,  true, false, ts1),
      JoinStoreTypeAndAggPP(idStore3, false,  true, ts3),
      JoinStoreTypeAndAggPP(idStore4, false, false, ts3)
    )).toDS

    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row(idStore1, idArticle1,              sector01,      measureUnitEA,  "a", currentTimestamp,  0d,  0d,  0d,  0d,  0d,  0d,  stockConsolidado,  acumVentas, 10d,  0d, 0d, 0d, 0d,  0d,  0d,   10d,  0d,  0d, 0d,  0d,  0d,  true,  true, "user",   0.3d),
      Row(idStore2, idArticle1, FRESH_PRODUCTS_SECTOR,      measureUnitKG, "a+", currentTimestamp, 10d, 11d, 12d, 13d, 14d, 14d, stockConsolidado2, acumVentas2, 10d,  0d, 0d, 0d, 0d,  0d, 10d,    1d,  0d,  0d, 0d,  0d,  0d,  true, false, "user",     1d),
      Row(idStore3, idArticle1,                  null,      measureUnitKG,  "b", currentTimestamp, 11d,  0d, 13d, 14d, 15d, 15d, stockConsolidado3, acumVentas3,  0d,  0d, 0d, 0d, 0d,  0d,  0d,    0d,  0d,  0d, 0d,  0d,  0d, false,  true, "user",     1d),
      Row(idStore4, idArticle1,                  null, defaultUnitMeasure,  "c", currentTimestamp, 11d, 12d, 13d, 14d,  0d,  0d,                0d,          0d,  0d,  0d, 0d, 0d, 0d,  0d,  0d,    0d,  0d,  0d, 0d,  0d,  0d, false, false, "user", 0.505d),
      Row(idStore5, idArticle1, FRESH_PRODUCTS_SECTOR,      measureUnitKG, null, currentTimestamp, 10d, 11d, 12d, 13d, 14d, 14d, stockConsolidado2, acumVentas2,  0d,  0d, 0d, 0d, 0d,  0d, 10d,  1.2d,  0d,  0d, 0d,  0d,  0d,  true, false, "user",   0.3d),
      Row(idStore6, idArticle1, FRESH_PRODUCTS_SECTOR,      measureUnitKG, null, currentTimestamp, 10d, 11d, 12d, 13d, 14d, 14d, stockConsolidado2, acumVentas2,  0d,  0d, 0d, 0d, 0d,  0d, 10d, 2.01d,  0d,  0d, 0d,  0d,  0d,  true, false, "user", 0.505d),
      Row(idStore2, idArticle2, FRESH_PRODUCTS_SECTOR,      measureUnitKG,  "b", currentTimestamp, 10d, 11d, 12d, 13d, 14d, 14d, stockConsolidado2, acumVentas2,  0d,  0d, 0d, 0d, 0d,  0d, 10d,    0d,  0d,  0d, 0d,  0d,  0d,  true, false, "user",   0.3d)
    )), beforeCalculateStockAtpNStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val dfResult: DataFrame =
      Sql.addStoreTypeAndPreparationInformationToPrecalculatedStockData(
        dfJoinDatosFijosAndStockConsAndAcumVentasAndAggPedidos,
        dsJoinStoreTypeAndAggPP,
        stockConf,
        currentTimestamp,
        dateRegistroEjecucion,
        user)(spark,defaultConfig)
        .select(beforeCalculateStockAtpNStructType.fieldNames.map(col) :_*)
        .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    assertDataFrameEquals(dfExpected, dfResult)
  }

  "getStockToBeDeliveredBasedOnSector" must " return stock delivered the same day (N) if article is from fresh products sector," +
    "if not, it should return the previous day amount" taggedAs UnitTestTag in new SqlVariablesPedidos {

    val dfFixedDataWithConsolidatedStockMovementsAndOrders: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      //   Store       Article                 Sector   Unity Measure   Rot  tsUpd  SalesN  N1  N2  N3  N4  N5  Consolidated Stock   Sales N  Orders N-1   N   N1   N2  N3   N4   N5  isTypeStore  isPP   user      ns
      Row( idStore1, idArticle1,              sector01, measureUnitEA,   "a",    ts,    0d,  0d, 0d, 0d, 0d, 0d,               200d,   40d,         10d,  20d,  0d,  0d, 0d,  0d,  0d,        true,  true, "user",   0.3d),
      Row( idStore2, idArticle1, FRESH_PRODUCTS_SECTOR, measureUnitKG,  "A+",    ts,    0d,  0d, 0d, 0d, 0d, 0d,               403d,   55d,         20d,  30d,  0d,  0d, 0d,  0d, 10d,        true, false, "user",     1d),
      Row( idStore3, idArticle1,     null, measureUnitKG,   "b",    ts,    0d,  0d, 0d, 0d, 0d, 0d,               503d,   66d,         40d,  50d,  0d,  0d, 0d,  0d,  0d,       false,  true, "user",   0.5d)
    )), beforeCalculateStockAtpNStructType)

    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      //   Store       Article                 Sector   Unity Measure   Rot  tsUpd  SalesN  N1  N2  N3  N4  N5  Consolidated Stock   Sales N  Orders N-1   N   N1   N2  N3   N4   N5  isTypeStore  isPP   user      ns   stockToBedelivered
      Row( idStore1, idArticle1,              sector01, measureUnitEA,   "a",    ts,    0d,  0d, 0d, 0d, 0d, 0d,               200d,   40d,         10d,  20d,  0d,  0d, 0d,  0d,  0d,        true,  true, "user",   0.3d, 10d*0.3),
      Row( idStore2, idArticle1, FRESH_PRODUCTS_SECTOR, measureUnitKG,  "A+",    ts,    0d,  0d, 0d, 0d, 0d, 0d,               403d,   55d,         20d,  30d,  0d,  0d, 0d,  0d, 10d,        true, false, "user",     1d, 30d),
      Row( idStore3, idArticle1,                  null, measureUnitKG,   "b",    ts,    0d,  0d, 0d, 0d, 0d, 0d,               503d,   66d,         40d,  50d,  0d,  0d, 0d,  0d,  0d,       false,  true, "user",   0.5d, 40d*0.5)
     )), beforeCalculateStockAtpNStructType)

    val dfResult: DataFrame =
      dfFixedDataWithConsolidatedStockMovementsAndOrders
        .transform(Sql.getStockToBeDeliveredBasedOnSector(
          StockToBeDelivered.ccTags.amountToBeDeliveredPreviousDayToNTag,
          StockToBeDelivered.ccTags.amountToBeDeliveredNTag)(spark))
        .select(fixedDataWithConsolidatedAndAggSaleMovsAndNsStructType.fieldNames.map(col) :_*)
        .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)
  }

  "calculateNspAndNst" must
    "get Nsp*Nst based on maximum and minimum possible values" taggedAs (UnitTestTag) in new SqlVariablesPedidos {

    val stockConf: StockConf = StockConf(
      maxNsp = 80d, minNsp = 20d,
      maxNst = 90d, minNst = 10d,
      rotationArtAA = 20d,
      rotationArtA = 40d,
      rotationArtB = 60d,
      rotationArtC = 80d,
      numberOfDaysToProject = 5,
      tsUpdateDlk = ts)

    val dfJoinDatosFijosAndStockConsAndAcumVentas: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      //   Article       Store                 Sector   Unity Measure   Slp    Sls   Rot  tsUpd   SalesN  SalesN1  SalesN2 SalesN3  SalesN4 SalesN5  Consolidated Stock   Sales N
      Row( idArticle1, idStore1,              sector01, measureUnitEA,   50d,   60d,  "a", ts,         0d,      0d,    0d,       0d,     0d,      1d,              200d,       40d),
      Row( idArticle1, idStore2, FRESH_PRODUCTS_SECTOR, measureUnitKG,    2d,   98d, "A+", ts,       100d,   1100d, 1200d,    1300d,  1400d,   1401d,              403d,       55d),
      Row( idArticle1, idStore3,                  null, measureUnitKG,  null,  null,  "b", ts,       110d,      0d, 1300d,    1400d,  1500d,   1501d,              503d,       66d),
      Row( idArticle1, idStore4,                  null, measureUnitEA, 50.5d,    2d,  "c", ts,       110d,   1200d, 1300d,    1400d,     0d,      1d,              603d,       40d),
      Row( idArticle1, idStore5, FRESH_PRODUCTS_SECTOR, measureUnitEA,   50d,   60d, "a+", ts,        10d,   1100d, 1200d,    1300d,  1400d,   1401d,              202d,       55d),
      Row( idArticle2, idStore1,              sector01, measureUnitEA,    3d, 50.5d, null, ts,         0d,   1100d, 1200d,    1300d,  1400d,   1401d,              503d,       66d),
      Row( idArticle3, idStore1,              sector01, measureUnitKG,   50d,   60d, "a+", ts,       100d,   1100d, 1200d,    1300d,  1400d,   1401d,                0d,        0d),
      Row( idArticle4, idStore1,              sector01, measureUnitKG,   50d,  null, null, ts,       100d,   1100d, 1200d,    1300d,  1400d,   1401d,              403d,       55d),
      Row( idArticle5, idStore1, FRESH_PRODUCTS_SECTOR, measureUnitKG,    0d,  100d,  "B", ts,       100d,   1100d, 1200d,    1300d,  1400d,   1401d,              403d,      -55d)
    )), fixedDataWithConsolidatedAndAggSaleMovsStructType)

    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      //      Article     Store                 Sector   Unity Measure  Rot  tsUpd   SalesN  SalesN1  SalesN2 SalesN3  SalesN4 SalesN5  Consolidated Stock   Sales N      Ns
      Row( idArticle1, idStore1,              sector01, measureUnitEA,  "a", ts,         0d,      0d,    0d,       0d,     0d,      1d,              200d,       40d,   0.3d),
      Row( idArticle1, idStore2, FRESH_PRODUCTS_SECTOR, measureUnitKG, "A+", ts,       100d,   1100d, 1200d,    1300d,  1400d,   1401d,              403d,       55d,     1d),
      Row( idArticle1, idStore3,                  null, measureUnitKG,  "b", ts,       110d,      0d, 1300d,    1400d,  1500d,   1501d,              503d,       66d,     1d),
      Row( idArticle1, idStore4,                  null, measureUnitEA,  "c", ts,       110d,   1200d, 1300d,    1400d,     0d,      1d,              603d,       40d, 0.505d),
      Row( idArticle1, idStore5, FRESH_PRODUCTS_SECTOR, measureUnitEA, "a+", ts,        10d,   1100d, 1200d,    1300d,  1400d,   1401d,              202d,       55d,   0.3d),
      Row( idArticle2, idStore1,              sector01, measureUnitEA, null, ts,         0d,   1100d, 1200d,    1300d,  1400d,   1401d,              503d,       66d, 0.505d),
      Row( idArticle3, idStore1,              sector01, measureUnitKG, "a+", ts,       100d,   1100d, 1200d,    1300d,  1400d,   1401d,                0d,        0d,   0.3d),
      Row( idArticle4, idStore1,              sector01, measureUnitKG, null, ts,       100d,   1100d, 1200d,    1300d,  1400d,   1401d,              403d,       55d,   0.5d),
      Row( idArticle5, idStore1, FRESH_PRODUCTS_SECTOR, measureUnitKG,  "B", ts,       100d,   1100d, 1200d,    1300d,  1400d,   1401d,              403d,      -55d,     1d)
    )), fixedDataWithConsolidatedAndAggSaleMovsAndNsStructType)

    val dfResult: DataFrame =
      dfJoinDatosFijosAndStockConsAndAcumVentas
        .transform(Sql.calculateNspAndNst(stockConf)(spark, defaultConfig))
        .select(fixedDataWithConsolidatedAndAggSaleMovsAndNsStructType.fieldNames.map(col) :_*)
        .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    assertDataFrameEquals(dfResult, dfExpected)

  }

  "calculateStockATP " must
      " calculate StockATP for day N' " taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val stockConf: StockConf = StockConf(
      maxNsp = 80d, minNsp = 20d,
      maxNst = 90d, minNst = 10d,
      rotationArtAA = 20d,
      rotationArtA = 40d,
      rotationArtB = 60d,
      rotationArtC = 80d,
      numberOfDaysToProject,
      tsUpdateDlk = ts)

    val dateRegistroEjecucion: Option[Timestamp] = Some(ts)

     val dfJoinDatosFijosAndStockConsAndAcumVentasAndAggPedidos: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
       /*           (consolidated stock * rotation/100 )  -  max(sales forecast - sales N, 0)*isOpen      +  (stock to be delivered *     ns)  - reservations   = stockATP(N)
        * 11 - 1 -->(            200    * 40  /100     )  -  max(          0    -  40     , 0)*1          +  (                10    *   0.3 ) -   10             = 73
        * 22 - 1 -->(            403    * 20  /100     )  -  max(        100    -  55     , 0)*1          +  (                 0    *     1 ) -    1             = 34,6
        * 33 - 1 -->(            503    * 60  /100     )  -  max(        110    -  66     , 0)*0          +  (                 0    *     1 ) -    0             = 301,8
        * 44 - 1 -->(            603    * 80  /100     )  -  max(        110    -  40     , 0)*0          +  (                10    * 0.505 ) -    0             = 487,45 --> 487
        * 55 - 1 -->(            202    * 20  /100     )  -  max(         10    -  55     , 0)*1          +  (                40    *   0.3 ) -  1.2             = 51.2   --> 51
        * 11 - 2 -->(            503    * 60  /100     )  -  max(          0    -  66     , 0)*1          +  (                10    * 0.505 ) - 2.01             = 304,84 --> 304
        * 11 - 3 -->(              0    * 20  /100     )  -  max(        100    -   0     , 0)*1          +  (                10    *   0.3 ) -    0             = -97    --> 0
        * 11 - 4 -->(            403    * 60  /100     )  -  max(        100    -  55     , 0)*1          +  (                10    *   0.5 ) -    0             = 201,8  --> 201,8
        * 11 - 5 -->(            403    * 60  /100     )  -  max(        100    - -55     , 0)*1          +  (                 0    *     1 ) -   10             = 76,8   --> 76,8
        */

       //   Store       Article                 Sector   Unity Measure   Rot  tsUpd   SalesN  SalesN1  SalesN2 SalesN3  SalesN4 SalesN5  Consolidated Stock   Sales N  Orders N-1   N   N1   N2  N3   N4   N5   reservationN   N1   N2  N3   N4   N5   isTypeStore  isPP   user      ns
       Row( idStore1, idArticle1,              sector01, measureUnitEA,   "a",    ts,      0d,      0d,     0d,     0d,      0d,      1d,               200d,   40d,         10d,   0d,  0d,  0d, 0d,  0d,  0d,          10d,  0d,  0d, 0d,  0d,  0d,         true,  true, "user",   0.3d),
       Row( idStore2, idArticle1, FRESH_PRODUCTS_SECTOR, measureUnitKG,  "A+",    ts,    100d,   1100d,  1200d,  1300d,   1400d,   1401d,               403d,   55d,         10d,   0d,  0d,  0d, 0d,  0d, 10d,           1d,  0d,  0d, 0d,  0d,  0d,         true, false, "user",     1d),
       Row( idStore3, idArticle1,                  null, measureUnitKG,   "b",    ts,    110d,      0d,  1300d,  1400d,   1500d,   1501d,               503d,   66d,          0d,  10d,  0d,  0d, 0d,  0d,  0d,           0d,  0d,  0d, 0d,  0d,  0d,        false,  true, "user",     1d),
       Row( idStore4, idArticle1,                  null, measureUnitEA,   "c",    ts,    110d,   1200d,  1300d,  1400d,      0d,      1d,               603d,   40d,         10d,   0d,  0d,  0d, 0d,  0d,  0d,           0d,  0d,  0d, 0d,  0d,  0d,        false, false, "user", 0.505d),
       Row( idStore5, idArticle1, FRESH_PRODUCTS_SECTOR, measureUnitEA,  "a+",    ts,     10d,   1100d,  1200d,  1300d,   1400d,   1401d,               202d,   55d,          0d,  40d,  0d,  0d, 0d,  0d, 10d,         1.2d,  0d,  0d, 0d,  0d,  0d,         true,  true, "user",   0.3d),
       Row( idStore1, idArticle2,              sector01, measureUnitEA,  null,    ts,      0d,   1100d,  1200d,  1300d,   1400d,   1401d,               503d,   66d,         10d,   0d,  0d,  0d, 0d,  0d,  0d,        2.01d,  0d,  0d, 0d,  0d,  0d,         true, false, "user", 0.505d),
       Row( idStore1, idArticle3,              sector01, measureUnitKG,  "a+",    ts,    100d,   1100d,  1200d,  1300d,   1400d,   1401d,                 0d,    0d,         10d,   0d,  0d,  0d, 0d,  0d,  0d,           0d,  0d,  0d, 0d,  0d,  0d,         true,  true, "user",   0.3d),
       Row( idStore1, idArticle4,              sector01, measureUnitKG,  null,    ts,    100d,   1100d,  1200d,  1300d,   1400d,   1401d,               403d,   55d,         10d,   0d,  0d, 20d, 0d,  0d,  0d,           0d,  0d,  0d, 0d,  0d,  0d,         true, false, "user",   0.5d),
       Row( idStore1, idArticle5, FRESH_PRODUCTS_SECTOR, measureUnitKG,   "B",    ts,    100d,   1100d,  1200d,  1300d,   1400d,   1401d,               403d,  -55d,         10d,   0d, 20d,  0d, 0d,  0d,  0d,          10d,  0d,  0d, 0d,  0d,  0d,         true, false, "user",     1d)
     )), beforeCalculateStockAtpNStructType)

     val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
       Row( idStore1, idArticle1,              sector01, measureUnitEA, ts,    0d,    0d,    0d,    0d,    1d,  0d,  0d,  0d, 0d,  0d,  0d, 0d,  0d, 0d,  0d,  0d,  true, "user",   0.3d, todayTs,    73d),
       Row( idStore2, idArticle1, FRESH_PRODUCTS_SECTOR, measureUnitKG, ts, 1100d, 1200d, 1300d, 1400d, 1401d,  0d,  0d,  0d, 0d,  0d, 10d, 0d,  0d, 0d,  0d,  0d, false, "user",     1d, todayTs,  34.6d),
       Row( idStore3, idArticle1,                  null, measureUnitKG, ts,    0d, 1300d, 1400d, 1500d, 1501d, 10d,  0d,  0d, 0d,  0d,  0d, 0d,  0d, 0d,  0d,  0d,  true, "user",     1d, todayTs, 301.8d),
       Row( idStore4, idArticle1,                  null, measureUnitEA, ts, 1200d, 1300d, 1400d,    0d,    1d,  0d,  0d,  0d, 0d,  0d,  0d, 0d,  0d, 0d,  0d,  0d, false, "user", 0.505d, todayTs,   487d),
       Row( idStore5, idArticle1, FRESH_PRODUCTS_SECTOR, measureUnitEA, ts, 1100d, 1200d, 1300d, 1400d, 1401d, 40d,  0d,  0d, 0d,  0d, 10d, 0d,  0d, 0d,  0d,  0d,  true, "user",   0.3d, todayTs,    51d),
       Row( idStore1, idArticle2,              sector01, measureUnitEA, ts, 1100d, 1200d, 1300d, 1400d, 1401d,  0d,  0d,  0d, 0d,  0d,  0d, 0d,  0d, 0d,  0d,  0d, false, "user", 0.505d, todayTs,   304d),
       Row( idStore1, idArticle3,              sector01, measureUnitKG, ts, 1100d, 1200d, 1300d, 1400d, 1401d,  0d,  0d,  0d, 0d,  0d,  0d, 0d,  0d, 0d,  0d,  0d,  true, "user",   0.3d, todayTs,     0d),
       Row( idStore1, idArticle4,              sector01, measureUnitKG, ts, 1100d, 1200d, 1300d, 1400d, 1401d,  0d,  0d, 20d, 0d,  0d,  0d, 0d,  0d, 0d,  0d,  0d, false, "user",   0.5d, todayTs, 201.8d),
       Row( idStore1, idArticle5, FRESH_PRODUCTS_SECTOR, measureUnitKG, ts, 1100d, 1200d, 1300d, 1400d, 1401d,  0d, 20d,  0d, 0d,  0d,  0d, 0d,  0d, 0d,  0d,  0d, false, "user",     1d, todayTs,  76.8d)
     )), afterCalculateStockAtpNStructType)

     val dfResult: DataFrame =
       dfJoinDatosFijosAndStockConsAndAcumVentasAndAggPedidos
         .transform(Sql.calculateStockATP(stockConf, todayTs)(defaultConfig, debugOptions, spark))
         .select(afterCalculateStockAtpNStructType.fieldNames.map(col) :_*)
         .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

     assertDataFrameEquals(dfExpected, dfResult)
    }
 
  "calculateStockATPNX " must
    " calculate StockATP for N + X + 1 days ' " taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val stockConf: StockConf = StockConf(
      maxNsp = 80d, minNsp = 20d,
      maxNst = 90d, minNst = 10d,
      rotationArtAA = 20d,
      rotationArtA = 40d,
      rotationArtB = 60d,
      rotationArtC = 80d,
      numberOfDaysToProject,
      tsUpdateDlk = ts)

    val dateRegistroEjecucion: Option[Timestamp] = Some(ts)

    val dfStockAtpN : DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      /*            ATPN - sales + orders * ns - reserv => AtpN1 - sales + orders*ns  - reserv  => AtpN2 - sales + orders*ns - reserv   => AtpN3                  - reserv => AtpN4                   - reserv =>    AtpN5
       * 11 - 1 =>   83d -    0 +  10  *   0.3 - 1      =>    85 -     0 + 20 * 0.3   - 0       =>   91   -  0  +  0 *   0.3 - 1        =>  90  -  0 + 30 * 0.3   - 0      =>  99 -   1 +   0 * 0.3   - 1      =>   97
       * 22 - 1 =>   35d - 1100 +  10  *     1 - 1      =>     0 -     5 + 20 * 1     - 0       =>   15   - 13  +  0 *     1 - 1        =>   1  - 40 + 40 * 1     - 0      =>   1 - 141 + 200 * 1     - 1      =>   59
       * 33 - 1 =>  301d -    0 +  10  *     1 - 2      =>   309 -    10 +  0 * 1     - 0       =>  299   - 14  + 20 *     1 - 2        => 303  - 50 +  0 * 1     - 0      => 253 -  11 +  10 * 1     - 1      =>  251
       * 44 - 1 =>  487d - 1200 +   0  * 0.505 - 2      =>     0 -    30 +  0 * 0.505 - 0       =>    0   - 14  +  0 * 0.505 - 2        =>   0  -  0 +  0 * 0.505 - 0      =>   0 -   1 +   0 * 0.505 - 1      =>    0
       * 55 - 1 =>   52d -   10 +  20  *   0.3 - 0      =>    48 -   120 + 30 * 0.3   - 0       =>    0   - 13  + 10 *   0.3 - 0        =>   0  - 40 + 50 * 0.3   - 0      =>   0 -  10 +  40 * 0.3   - 1      =>    1
       * 11 - 2 =>  306d -   11 +  80  * 0.505 - 0      =>   335 -   100 + 80 * 0.505 - 0       =>  275   - 13  + 80 * 0.505 - 0        => 302  - 14 + 10 * 0.505 - 0      => 293 -   1 +  50 * 0.505 - 1      =>  316
       * 11 - 3 =>    0d -   11 +   0  *   0.3 - 0      =>     0 -   100 +  0 * 0.3   - 0       =>    0   - 13  +  0 *   0.3 - 0        =>   0  - 14 +  0 * 0.3   - 0      =>   0 -   1 +   0 * 0.3   - 1      =>    0
       * 11 - 4 =>  201d -   11 +   1  *   0.5 - 0      => 190,5 -   100 +  2 * 0.5   - 0       => 91,5   - 13  +  3 *   0.5 - 0        =>  80  - 14 +  4 * 0.5   - 0      =>  68 -   1 +   5 * 0.5   - 1      => 68,5
       * 11 - 5 =>   86d -   11 +  20  *     1 - 0      =>    95 -   100 +  0 * 1     - 0       =>    0   - 13  +  0 *     1 - 0        =>   0  - 14 +  0 * 1     - 0      =>   0 -   1 +   0 * 1     - 1      =>    0
       */

      //   Store       Article                 Sector   Unity Measure  tsUpd SalesN1  SalesN2 SalesN3  SalesN4 SalesN5  Orders N   N1   N2   N3  N4   N5   reservationN1   N2  N3   N4   N5  isPP  user      ns      day n        stockATPN
      Row( idStore1, idArticle1,              sector01, measureUnitEA,     ts,    0d,      0d,     0d,     0d,      1d,      10d, 20d,  0d, 30d,  0d,  40d,           1d,  0d, 1d,  0d,  1d,  true, "user",   0.3d, todayTs,  83d),
      Row( idStore2, idArticle1, FRESH_PRODUCTS_SECTOR, measureUnitKG,     ts, 1100d,      5d,    13d,    40d,    141d,       0d, 10d, 20d,  0d, 40d, 200d,           1d,  0d, 1d,  0d,  1d, false, "user",     1d, todayTs,  35d),
      Row( idStore3, idArticle1,                  null, measureUnitKG,     ts,    0d,     10d,    14d,    50d,     11d,      10d,  0d, 20d,  0d, 10d,  10d,           2d,  0d, 2d,  0d,  1d,  true, "user",     1d, todayTs, 301d),
      Row( idStore4, idArticle1,                  null, measureUnitEA,     ts, 1200d,     30d,    14d,     0d,      1d,       0d,  0d,  0d,  0d,  0d,   0d,           2d,  0d, 2d,  0d,  1d, false, "user", 0.505d, todayTs, 487d),
      Row( idStore5, idArticle1, FRESH_PRODUCTS_SECTOR, measureUnitEA,     ts,   10d,    120d,    13d,    40d,     10d,      40d, 20d, 30d, 10d, 50d,  40d,           0d,  0d, 0d,  0d,  1d,  true, "user",   0.3d, todayTs,  52d),
      Row( idStore1, idArticle2,              sector01, measureUnitEA,     ts,   11d,    100d,    13d,    14d,      1d,      80d, 80d, 80d, 10d, 50d,   0d,           0d,  0d, 0d,  0d,  1d, false, "user", 0.505d, todayTs, 306d),
      Row( idStore1, idArticle3,              sector01, measureUnitKG,     ts,   11d,    100d,    13d,    14d,      1d,       0d,  0d,  0d,  0d,  0d,   0d,           0d,  0d, 0d,  0d,  1d,  true, "user",   0.3d, todayTs,   0d),
      Row( idStore1, idArticle4,              sector01, measureUnitKG,     ts,   11d,    100d,    13d,    14d,      1d,       1d,  2d,  3d,  4d,  5d,   6d,           0d,  0d, 0d,  0d,  1d, false, "user",   0.5d, todayTs, 201d),
      Row( idStore1, idArticle5, FRESH_PRODUCTS_SECTOR, measureUnitKG,     ts,   11d,    100d,    13d,    14d,      1d,       0d, 20d,  0d,  0d,  0d,   0d,           0d,  0d, 0d,  0d,  1d, false, "user",     1d, todayTs,  86d)
    )), afterCalculateStockAtpNStructType)

    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      //     store     article                 sector   unity measure tsUpd   isPP    user   day n   stockATPN   N1    N2    N3    N4    N5
      Row( idStore1, idArticle1,              sector01, measureUnitEA, ts,    true, "user", todayTs,      83d,  85d,  91d,  90d, 99d, 97d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null
      ),
      Row( idStore2, idArticle1, FRESH_PRODUCTS_SECTOR, measureUnitKG, ts,   false, "user", todayTs,      35d,   0d,  15d,   1d,   1d,  59d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null
      ),
      Row( idStore3, idArticle1,                  null, measureUnitKG, ts,    true, "user", todayTs,     301d, 309d, 299d, 303d, 253d, 251d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null
      ),
      Row( idStore4, idArticle1,                  null, measureUnitEA, ts,   false, "user", todayTs,     487d,   0d,   0d,   0d,   0d,   0d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null
      ),
      Row( idStore5, idArticle1, FRESH_PRODUCTS_SECTOR, measureUnitEA, ts,    true, "user", todayTs,      52d,  48d,   0d,   0d,   0d,   1d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null
      ),
      Row( idStore1, idArticle2,              sector01, measureUnitEA, ts,   false, "user", todayTs,     306d, 335d, 275d, 302d, 293d, 316d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null
      ),
      Row( idStore1, idArticle3,              sector01, measureUnitKG, ts,    true, "user", todayTs,       0d,   0d,   0d,   0d,   0d,   0d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null
      ),
      Row( idStore1, idArticle4,              sector01, measureUnitKG, ts,   false, "user", todayTs,     201d, 190.5d, 91.5d, 80d, 68d, 68.5d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null
      ),
      Row( idStore1, idArticle5, FRESH_PRODUCTS_SECTOR, measureUnitKG, ts,   false, "user", todayTs,      86d,  95d,   0d,   0d,   0d,   0d,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null
      )
    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val dfResult: DataFrame =
      dfStockAtpN
        .transform(Sql.calculateStockATPNX(stockConf)(spark, defaultConfig))
        .select(stockAtpNXStructType.fieldNames.map(col) :_*)
        .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    assertDataFrameEquals(dfResult, dfExpected)
  }


  it must
    " calculate StockATP for N + X when project day is 100' " taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val stockConf: StockConf = StockConf(
      maxNsp = 80d, minNsp = 20d,
      maxNst = 90d, minNst = 10d,
      rotationArtAA = 20d,
      rotationArtA = 40d,
      rotationArtB = 60d,
      rotationArtC = 80d,
      numberOfDaysToProject = 100,
      tsUpdateDlk = ts)

    val dateRegistroEjecucion: Option[Timestamp] = Some(ts)

    val dfStockAtpN : DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      /*            ATPN - sales + orders * ns - reserv => AtpN1 - sales + orders*ns  - reserv  => AtpN2 - sales + orders*ns - reserv   => AtpN3                  - reserv => AtpN4                   - reserv =>   AtpN5
       * 11 - 1 =>   83d -    0 +  10  *   0.3 - 1      =>    85 -     0 + 20 * 0.3   - 0       =>   91   -  0  +  0 *   0.3 - 1        =>  90  -  0 + 30 * 0.3   - 0      =>  99 -   1 +   0 * 0.3   - 1      =>   97
       * 22 - 1 =>   35d - 1100 +  10  *     1 - 1      =>     0 -     5 + 20 * 1     - 0       =>   15   - 13  +  0 *     1 - 1        =>   1  - 40 + 40 * 1     - 0      =>   1 - 141 + 200 * 1     - 1      =>   59
       * 33 - 1 =>  301d -    0 +  10  *     1 - 2      =>   309 -    10 +  0 * 1     - 0       =>  299   - 14  + 20 *     1 - 2        => 303  - 50 +  0 * 1     - 0      => 253 -  11 +  10 * 1     - 1      =>  251
       * 44 - 1 =>  487d - 1200 +   0  * 0.505 - 2      =>     0 -    30 +  0 * 0.505 - 0       =>    0   - 14  +  0 * 0.505 - 2        =>   0  -  0 +  0 * 0.505 - 0      =>   0 -   1 +   0 * 0.505 - 1      =>    0
       * 55 - 1 =>   52d -   10 +  20  *   0.3 - 0      =>    48 -   120 + 30 * 0.3   - 0       =>    0   - 13  + 10 *   0.3 - 0        =>   0  - 40 + 50 * 0.3   - 0      =>   0 -  10 +  40 * 0.3   - 1      =>    1
       */  

      //   Store       Article                 Sector   Unity Measure  tsUpd SalesN1  SalesN2 SalesN3  SalesN4 SalesN5   N6  N7  N8  N9 N10 N11 N12 N13 N14 N15 N16             N20                                     N30                                     N40                                     N50                                      N60                                     N70                                    N80                                     N90                                    N100      Orders N   N1   N2   N3  N4   N5    N6  N7  N8  N9 N10 N11 N12 N13 N14 N15 N16             N20                                     N30                                     N40                                     N50                                      N60                                     N70                                    N80                                     N90                                    N100      Reserv N   N1   N2   N3  N4   N5  N6  N7  N8  N9 N10 N11 N12 N13 N14 N15 N16             N20                                     N30                                     N40                                     N50                                      N60                                     N70                                    N80                                     N90                                     N100     isPP    user      ns   day n   stockATPN
      Row( idStore1, idArticle1,              sector01, measureUnitEA,     ts,    0d,      0d,     0d,     0d,      1d,  0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,      10d, 20d,  0d, 30d,  0d,  40d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,       0d,  1d,  0d, 1d,  0d,  1d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,  true, "user",   0.3d, todayTs,  83d),
      Row( idStore2, idArticle1, FRESH_PRODUCTS_SECTOR, measureUnitKG,     ts, 1100d,      5d,    13d,    40d,    141d,  0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,       0d, 10d, 20d,  0d, 40d, 200d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,       0d,  1d,  0d, 1d,  0d,  1d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, false, "user",     1d, todayTs,  35d),
      Row( idStore3, idArticle1,                  null, measureUnitKG,     ts,    0d,     10d,    14d,    50d,     11d,  0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,      10d,  0d, 20d,  0d, 10d,  10d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,       0d,  2d,  0d, 2d,  0d,  1d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,  true, "user",     1d, todayTs, 301d),
      Row( idStore4, idArticle1,                  null, measureUnitEA,     ts, 1200d,     30d,    14d,     0d,      1d,  0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,       0d,  0d,  0d,  0d,  0d,   0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,       0d,  2d,  0d, 2d,  0d,  1d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, false, "user", 0.505d, todayTs, 487d),
      Row( idStore5, idArticle1, FRESH_PRODUCTS_SECTOR, measureUnitEA,     ts,   10d,    120d,    13d,    40d,     10d,  0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,      40d, 20d, 30d, 10d, 50d,  40d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,       0d,  0d,  0d, 0d,  0d,  1d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,  true, "user",   0.3d, todayTs,  52d)
    )), afterCalculateStockAtpNWith101ForecastDaysStructType)

    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      //     store     article                 sector   unity measure tsUpd   isPP    user   day n   stockATPN   N1    N2    N3    N4   N5
      Row( idStore1, idArticle1,              sector01, measureUnitEA, ts,    true, "user", todayTs,      83d,  85d,  91d,  90d, 99d,  97d,
        109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d,
        109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d,
        109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d,
        109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d,
        109d, 109d, 109d, 109d, 109d, 109d, 109d, 109d
      ),
      Row( idStore2, idArticle1, FRESH_PRODUCTS_SECTOR, measureUnitKG, ts,   false, "user", todayTs,      35d,   0d,  15d,   1d,   1d,  59d,
        59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d,
        59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d,
        59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d,
        59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d,
        59d, 59d, 59d, 59d, 59d, 59d, 59d, 59d
      ),
      Row( idStore3, idArticle1,                  null, measureUnitKG, ts,    true, "user", todayTs,     301d, 309d, 299d, 303d, 253d, 251d,
        261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d,
        261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d,
        261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d,
        261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d,
        261d, 261d, 261d, 261d, 261d, 261d, 261d, 261d
      ),
      Row( idStore4, idArticle1,                  null, measureUnitEA, ts,   false, "user", todayTs,     487d,   0d,   0d,   0d,   0d,   0d,
        0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
        0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
        0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
        0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d,
        0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d
      ),
      Row( idStore5, idArticle1, FRESH_PRODUCTS_SECTOR, measureUnitEA, ts,    true, "user", todayTs,      52d,  48d,   0d,   0d,   0d,   1d,
        1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d,
        1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d,
        1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d,
        1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d,
        1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d
      )
    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val dfResult: DataFrame =
      dfStockAtpN
        .transform(Sql.calculateStockATPNX(stockConf)(spark, defaultConfig))
        .select(stockAtpNXStructType.fieldNames.map(col) :_*)
        .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    assertDataFrameEquals(dfResult, dfExpected)
  }

  "calculateStockATPNX " must
    " calculate StockATP for N + X for 0 days ' " taggedAs (UnitTestTag) in new SqlVariablesPedidos  {

    val stockConf: StockConf = StockConf(
      maxNsp = 80d, minNsp = 20d,
      maxNst = 90d, minNst = 10d,
      rotationArtAA = 20d,
      rotationArtA = 40d,
      rotationArtB = 60d,
      rotationArtC = 80d,
      numberOfDaysToProject =  0,
      tsUpdateDlk = ts)

    val dateRegistroEjecucion: Option[Timestamp] = Some(ts)

    val dfStockAtpN : DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      /*            ATPN - sales + orders * ns - reserv => AtpN1
       * 11 - 1 =>   83d -    0 +  10  *   0.3 - 1      =>    85
       * 22 - 1 =>   35d - 1100 +  10  *     1 - 1      =>     0
       * 33 - 1 =>  301d -    0 +  10  *     1 - 2      =>   309
       * 44 - 1 =>  487d - 1200 +   0  * 0.505 - 2      =>     0
       * 55 - 1 =>   52d -   10 +  20  *   0.3 - 0      =>    48
       */

      //   Store       Article                 Sector   Unity Measure  tsUpd SalesN1  Orders N   N1 reservationN1  isPP  user      ns      day n        stockATPN
      Row( idStore1, idArticle1,              sector01, measureUnitEA,     ts,    0d,      10d, 20d,          1d,  true, "user",   0.3d, todayTs,  83d),
      Row( idStore2, idArticle1, FRESH_PRODUCTS_SECTOR, measureUnitKG,     ts, 1100d,       0d, 10d,          1d, false, "user",     1d, todayTs,  35d),
      Row( idStore3, idArticle1,                  null, measureUnitKG,     ts,    0d,      10d,  0d,          2d,  true, "user",     1d, todayTs, 301d),
      Row( idStore4, idArticle1,                  null, measureUnitEA,     ts, 1200d,       0d,  0d,          2d, false, "user", 0.505d, todayTs, 487d),
      Row( idStore5, idArticle1, FRESH_PRODUCTS_SECTOR, measureUnitEA,     ts,   10d,      40d, 20d,          0d,  true, "user",   0.3d, todayTs,  52d)
    )), afterCalculateStockAtpNWith1ForecastDayStructType)

    val dfExpected: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      //     store     article                 sector   unity measure tsUpd   isPP    user   day n   stockATPN   N1    N2    N3    N4    N5
      Row( idStore1, idArticle1,              sector01, measureUnitEA, ts,    true, "user", todayTs,      83d,  85d,  null,  null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null
      ),
      Row( idStore2, idArticle1, FRESH_PRODUCTS_SECTOR, measureUnitKG, ts,   false, "user", todayTs,      35d,   0d,  null,   null,   null,  null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null
      ),
      Row( idStore3, idArticle1,                  null, measureUnitKG, ts,    true, "user", todayTs,     301d, 309d, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null
      ),
      Row( idStore4, idArticle1,                  null, measureUnitEA, ts,   false, "user", todayTs,     487d,   0d, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null
      ),
      Row( idStore5, idArticle1, FRESH_PRODUCTS_SECTOR, measureUnitEA, ts,    true, "user", todayTs,      52d,  48d, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null
      )
    )), stockAtpNXStructType)
      .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    val dfResult: DataFrame =
      dfStockAtpN
        .transform(Sql.calculateStockATPNX(stockConf)(spark, defaultConfig))
        .select(stockAtpNXStructType.fieldNames.map(col) :_*)
        .orderBy(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag)

    assertDataFrameEquals(dfResult, dfExpected)
  }

  "filterRegistriesToProcess" must " keep only with the registries with an update after the last execution" taggedAs(UnitTestTag) in new SqlVariablesPedidos {

    val df: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row(idArticle1, idStore1, ts1),
        Row(idArticle2, idStore2, ts2),
        Row(idArticle3, idStore3, ts3),
        Row(idArticle4, idStore4, ts4)
      )), checkControlExecutionStructType)

    val dfExpected = spark.createDataFrame(sc.parallelize(List[Row](
      Row(idArticle2, idStore2, ts2),
      Row(idArticle3, idStore3, ts3),
      Row(idArticle4, idStore4, ts4)
    )), checkControlExecutionStructType)

     val dfResult: DataFrame = df.transform(Sql.filterRegistriesToProcess(Some(ts2)))
     assertDataFrameEquals(dfExpected, dfResult)

  }

  it must " return all registries when there is no last execution date" taggedAs(UnitTestTag) in new SqlVariablesPedidos {

    val df: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row(idArticle1, idStore1, ts1),
        Row(idArticle2, idStore2, ts2),
        Row(idArticle3, idStore3, ts3),
        Row(idArticle4, idStore4, ts4)
      )), checkControlExecutionStructType)

    val dfExpected = spark.createDataFrame(sc.parallelize(List[Row](
      Row(idArticle1, idStore1, ts1),
      Row(idArticle2, idStore2, ts2),
      Row(idArticle3, idStore3, ts3),
      Row(idArticle4, idStore4, ts4)
    )), checkControlExecutionStructType)

    val dfResult: DataFrame = df.transform(Sql.filterRegistriesToProcess(None))
    assertDataFrameEquals(dfExpected, dfResult)

  }

  "roundDownAndMaxWithZero" must " round down a value and if it is negative set it to 0" taggedAs(UnitTestTag) in {
    Sql.roundDownAndMaxWithZero(20d) shouldBe 20d
    Sql.roundDownAndMaxWithZero(15.5d) shouldBe 15d
    Sql.roundDownAndMaxWithZero(0d) shouldBe 0d
    Sql.roundDownAndMaxWithZero(-5d) shouldBe 0d
    Sql.roundDownAndMaxWithZero(-15.6d) shouldBe 0d
  }

  "round3DecimalsAndMaxWithZero" must " round to 3 decimals a value and if it is negative set it to 0" taggedAs(UnitTestTag) in {
    Sql.round3DecimalsAndMaxWithZero(20d) shouldBe        20d
    Sql.round3DecimalsAndMaxWithZero(15.5d) shouldBe      15.5d
    Sql.round3DecimalsAndMaxWithZero(0d) shouldBe         0d
    Sql.round3DecimalsAndMaxWithZero(5.10d) shouldBe      5.1d
    Sql.round3DecimalsAndMaxWithZero(-15.6d) shouldBe     0d
    Sql.round3DecimalsAndMaxWithZero(1.606d) shouldBe     1.606d
    Sql.round3DecimalsAndMaxWithZero(10.120005d) shouldBe 10.12d
    Sql.round3DecimalsAndMaxWithZero(10.120905d) shouldBe 10.121d
  }

  "udfRoundBasedOnUnityMeasureAndMaxWithZero" must " round down a value when unity measure is not KG and if it is negative set it to 0" taggedAs(UnitTestTag) in new SqlVariablesPedidos {

    val df1 = spark.createDataFrame(sc.parallelize(List[Row](
      Row(idArticle2, idStore2,     20.7d,        "EA"),
      Row(idArticle3, idStore3,  15.5001d,        "KG"),
      Row(idArticle4, idStore4,       10d,        "EA"),
      Row(idArticle4, idStore4,       -5d,        "KG"),
      Row(idArticle4, idStore4,    -15.6d,        "EA"),
      Row(idArticle4, idStore4,  15.6911d,          ""),
      Row(idArticle4, idStore4, 5.425300d, "something")
    )), testDoubleStructType)

    val dfExpected = spark.createDataFrame(sc.parallelize(List[Row](
      Row(idArticle2, idStore2,     20d,        "EA"),
      Row(idArticle3, idStore3,   15.5d,        "KG"),
      Row(idArticle4, idStore4,     10d,        "EA"),
      Row(idArticle4, idStore4,      0d,        "KG"),
      Row(idArticle4, idStore4,      0d,        "EA"),
      Row(idArticle4, idStore4, 15.691d,          ""),
      Row(idArticle4, idStore4,  5.425d, "something")
    )), testDoubleStructType)

    val dfResult: DataFrame = df1.withColumn(
      StockConsolidado.ccTags.stockDispTag,
      Sql.udfRoundBasedOnUnityMeasureAndMaxWithZero(col(StockConsolidado.ccTags.stockDispTag), col(DatosFijos.ccTags.unityMeasureTag))
    )

    assertDataFrameEquals(dfExpected, dfResult)

  }

  "roundBasedOnUnityMeasureAndMaxWithZero transform" must " round down a value when unity measure is not KG and if it is negative set it to 0" taggedAs(UnitTestTag) in new SqlVariablesPedidos {

    val df1 = spark.createDataFrame(sc.parallelize(List[Row](
      Row(idArticle2, idStore2,     20.7d, "EA"),
      Row(idArticle3, idStore3, 15.50251d, "KG"),
      Row(idArticle4, idStore4,       10d, "EA"),
      Row(idArticle4, idStore4,       -5d, "KG"),
      Row(idArticle4, idStore4,    -15.6d, "EA"),
      Row(idArticle3, idStore3,       15d, "KG")
    )), testDoubleStructType)

    val dfExpected = spark.createDataFrame(sc.parallelize(List[Row](
      Row(idArticle2, idStore2,     20d, "EA"),
      Row(idArticle3, idStore3, 15.503d, "KG"),
      Row(idArticle4, idStore4,     10d, "EA"),
      Row(idArticle4, idStore4,      0d, "KG"),
      Row(idArticle4, idStore4,      0d, "EA"),
      Row(idArticle3, idStore3,     15d, "KG")
    )), testDoubleStructType)

    val dfResult: DataFrame = df1.transform(
      Sql.roundBasedOnUnityMeasureAndMaxWithZero(StockConsolidado.ccTags.stockDispTag, DatosFijos.ccTags.unityMeasureTag)
    )

    assertDataFrameEquals(dfExpected, dfResult)

  }
}
