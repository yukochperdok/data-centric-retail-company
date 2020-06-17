package com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.util

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.inputs._
import com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.joins._
import com.training.bigdata.omnichannel.stockATP.common.entities.{DatosFijos, SurtidoMara}
import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.Surtido
import com.training.bigdata.omnichannel.stockATP.common.util.tag.TagTest.UnitTestTag
import com.training.bigdata.omnichannel.stockATP.common.util.{Dates, FieldsUtils}
import com.training.bigdata.omnichannel.stockATP.common.util.Constants._
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.{FlatSpec, Matchers}

class SqlOperationsTest extends FlatSpec with Matchers with DatasetSuiteBase {

  import spark.implicits._
  override protected implicit def enableHiveSupport: Boolean = false
  val currentTimestamp: Timestamp = new Timestamp(System.currentTimeMillis)
  val beforeToday: String = "2018-09-05"
  val beforeTodayTs: Timestamp = Timestamp.valueOf(beforeToday + " 23:59:59")
  val today: String = "2018-09-06"
  val todayTs: Timestamp = Timestamp.valueOf(today + " 00:00:00")
  val tomorrow: String = "2018-09-07"
  val tomorrowTs: Timestamp = Timestamp.valueOf(tomorrow + " 00:00:00")
  val afterTomorrow: String = "2018-09-08"
  val afterTomorrowTs: Timestamp = Timestamp.valueOf(afterTomorrow + " 00:00:00")
  val oneWeekLater: String = "2018-09-13"
  val oneWeekLaterTs: Timestamp = Timestamp.valueOf(oneWeekLater + " 00:00:00")
  val oneMonthLater: String = "2018-10-04"
  val oneMonthLaterTs: Timestamp = Timestamp.valueOf(oneMonthLater + " 00:00:00")
  val futurePlanningDay: String = "2018-12-17"
  val futurePlanningDayTs: Timestamp = Timestamp.valueOf(futurePlanningDay + " 00:00:00")
  val timezone: String = "Europe/Paris"
  val saleArticleList = List("S", "")
  val forecastDateMap: Map[String, String] = Map(
    today         -> "",
    tomorrow      -> "1",
    afterTomorrow -> "2",
    "2018-09-09"  -> "3",
    "2018-09-10"  -> "4",
    "2018-09-11"  -> "5",
    "2018-09-12"  -> "6",
    oneWeekLater  -> "7")


  "getForecastSalesForTodayAndFuturePlanningDay" must " return a dataset of SalesForecastView " +
    "with the correct regular sales amount" taggedAs UnitTestTag in {

    val salesForecastDS: Dataset[SalesForecast] = sc.parallelize(List[SalesForecast](
      SalesForecast("store1", todayTs, "article1", Some(1.0), Some(0.0)),
      SalesForecast("store1", todayTs, "article2", Some(0.0), Some(2.0)),
      SalesForecast("store2", todayTs, "article3", Some(3.0),      None),
      SalesForecast("store2", todayTs, "article4",      None, Some(4.0)),
      SalesForecast("store3", todayTs, "article5",      None,      None))).toDS

    val expectedDS: Dataset[SalesForecastView] =
      sc.parallelize(List[SalesForecastView](
        SalesForecastView(today, "article1", 1.0, "store1"),
        SalesForecastView(today, "article2", 2.0, "store1"),
        SalesForecastView(today, "article3", 3.0, "store2"),
        SalesForecastView(today, "article4", 4.0, "store2"),
        SalesForecastView(today, "article5", 0.0, "store3"))).toDS

    val resultDS = salesForecastDS.transform(SqlOperations.getForecastSalesForTodayAndFuturePlanningDay(todayTs, afterTomorrowTs, timezone)(spark))
    assertDatasetEquals(resultDS, expectedDS)

  }

  it must " return a dataset of SalesForecastView " +
    "for rows between today and given date" taggedAs UnitTestTag in {

    val salesForecastDS: Dataset[SalesForecast] = sc.parallelize(List[SalesForecast](
      SalesForecast("store1",   beforeTodayTs, "article1", Some(1.0), Some(0.0)),
      SalesForecast("store1",         todayTs, "article2", Some(0.0), Some(2.0)),
      SalesForecast("store2",      tomorrowTs, "article3", Some(3.0),      None),
      SalesForecast("store2", afterTomorrowTs, "article4",      None, Some(4.0)),
      SalesForecast("store3", oneMonthLaterTs, "article5",      None, Some(3.0)),
      SalesForecast("store3", futurePlanningDayTs, "article6",  None, Some(1.0))
    )).toDS

    val expectedDS: Dataset[SalesForecastView] =
      sc.parallelize(List[SalesForecastView](
        SalesForecastView(   today, "article2", 2.0, "store1"),
        SalesForecastView(tomorrow, "article3", 3.0, "store2"))).toDS

    val resultDS = salesForecastDS.transform(SqlOperations.getForecastSalesForTodayAndFuturePlanningDay(todayTs, afterTomorrowTs, timezone)(spark))
    assertDatasetEquals(resultDS, expectedDS)

    val expectedDS2: Dataset[SalesForecastView] =
      sc.parallelize(List[SalesForecastView](
        SalesForecastView(   today, "article2", 2.0, "store1"),
        SalesForecastView(tomorrow, "article3", 3.0, "store2"),
        SalesForecastView(afterTomorrow, "article4", 4.0, "store2"),
        SalesForecastView(oneMonthLater, "article5", 3.0, "store3")
      )).toDS

    val resultDS2 = salesForecastDS.transform(SqlOperations.getForecastSalesForTodayAndFuturePlanningDay(todayTs, futurePlanningDayTs, timezone)(spark))
    assertDatasetEquals(resultDS2, expectedDS2)

  }

  it must " return an empty dataset of SalesForecastView " +
    "when an empty dataframe is given" taggedAs UnitTestTag in {

    val salesForecastDS: Dataset[SalesForecast] = sc.emptyRDD[SalesForecast].toDS()

    val expectedDS: Dataset[SalesForecastView] = sc.emptyRDD[SalesForecastView].toDS()

    val resultDS = salesForecastDS.transform(SqlOperations.getForecastSalesForTodayAndFuturePlanningDay(todayTs, afterTomorrowTs, timezone)(spark))
    assertDatasetEquals(resultDS, expectedDS)

  }

  "getSalesPlatformForTodayAndFuturePlanningDay" must " return a dataset of SalesForecastView " +
    "with the correct regular sales amount and when the dates are between today and given date" taggedAs UnitTestTag in {

    val salesPlatformDS: Dataset[SalesPlatform] = sc.parallelize(List[SalesPlatform](
      SalesPlatform(             todayTs, "article1", "store1", Some(1.0)),
      SalesPlatform(             todayTs, "article2", "store1", Some(0.0)),
      SalesPlatform(             todayTs, "article3", "store2", Some(3.0)),
      SalesPlatform(             todayTs, "article4", "store2",      None),
      SalesPlatform(       beforeTodayTs, "article5", "store3", Some(5.0)),
      SalesPlatform(          tomorrowTs, "article6", "store3", Some(6.0)),
      SalesPlatform(     afterTomorrowTs, "article7", "store3", Some(7.0)),
      SalesPlatform(     oneMonthLaterTs, "article8", "store4", Some(4.0)),
      SalesPlatform( futurePlanningDayTs, "article9", "store4", Some(2.0))
    )).toDS

    val expectedDS: Dataset[SalesForecastView] =
      sc.parallelize(List[SalesForecastView](
        SalesForecastView(         today, "article1", 1.0, "store1"),
        SalesForecastView(         today, "article2", 0.0, "store1"),
        SalesForecastView(         today, "article3", 3.0, "store2"),
        SalesForecastView(         today, "article4", 0.0, "store2"),
        SalesForecastView(      tomorrow, "article6", 6.0, "store3"),
        SalesForecastView( afterTomorrow, "article7", 7.0, "store3"),
        SalesForecastView( oneMonthLater, "article8", 4.0, "store4")
      )).toDS

    val resultDS = salesPlatformDS.transform(SqlOperations.getSalesPlatformForTodayAndFuturePlanningDay(todayTs, futurePlanningDayTs, timezone)(spark))
    assertDatasetEquals(resultDS, expectedDS)

  }

  it must " return an empty dataset of SalesForecastView " +
    "when an empty dataframe is given" taggedAs UnitTestTag in {

    val salesPlatformDS: Dataset[SalesPlatform] = sc.emptyRDD[SalesPlatform].toDS()

    val expectedDS: Dataset[SalesForecastView] = sc.emptyRDD[SalesForecastView].toDS()

    val resultDS = salesPlatformDS.transform(SqlOperations.getSalesPlatformForTodayAndFuturePlanningDay(todayTs, afterTomorrowTs, timezone)(spark))
    assertDatasetEquals(resultDS, expectedDS)

  }


  "filterJoinMarcMara" must " return the a dataset of marc-mara only with sale articles" taggedAs UnitTestTag in {

    val dsMarcMara: Dataset[(SurtidoMarc, SurtidoMara)] = sc.parallelize(List[(SurtidoMarc, SurtidoMara)](
      (SurtidoMarc("1", "11", "ACTIVO"), SurtidoMara("1", "EA", "2753", "S")),
      (SurtidoMarc("2", "22", "ACTIVO"), SurtidoMara("2", "EB", "2754", "")),
      (SurtidoMarc("3", "33", "ACTIVO"), SurtidoMara("3", "EA", "2755", null))
    )).toDS

    val dsResult: Dataset[(SurtidoMarc, SurtidoMara)] = sc.parallelize(List[(SurtidoMarc, SurtidoMara)](
      (SurtidoMarc("1", "11", "ACTIVO"), SurtidoMara("1", "EA", "2753", "S")),
      (SurtidoMarc("2", "22", "ACTIVO"), SurtidoMara("2", "EB", "2754", "")),
      (SurtidoMarc("3", "33", "ACTIVO"), SurtidoMara("3", "EA", "2755", null))
    )).toDS

    val filterMarcMara = SqlOperations.filterJoinMarcMara[SurtidoMarc](saleArticleList)(spark)
    val joinResult = filterMarcMara(dsMarcMara)
    assertDatasetEquals(joinResult, dsResult)

  }

  it must " return the a dataset of marc-mara with sale and buy articles" taggedAs UnitTestTag in {

    val dsMarcMara: Dataset[(SurtidoMarc, SurtidoMara)] = sc.parallelize(List[(SurtidoMarc, SurtidoMara)](
      (SurtidoMarc("1", "11", "ACTIVO"), SurtidoMara("1", "EA", "2753", "S")),
      (SurtidoMarc("2", "22", "ACTIVO"), SurtidoMara("2", "EB", "2754", "")),
      (SurtidoMarc("3", "33", "ACTIVO"), SurtidoMara("3", "EA", "2755", null)),
      (SurtidoMarc("3", "33", "ACTIVO"), SurtidoMara("3", "EA", "2755", "P"))
    )).toDS

    val dsResult: Dataset[(SurtidoMarc, SurtidoMara)] = sc.parallelize(List[(SurtidoMarc, SurtidoMara)](
      (SurtidoMarc("1", "11", "ACTIVO"), SurtidoMara("1", "EA", "2753", "S")),
      (SurtidoMarc("2", "22", "ACTIVO"), SurtidoMara("2", "EB", "2754", "")),
      (SurtidoMarc("3", "33", "ACTIVO"), SurtidoMara("3", "EA", "2755", null))
    )).toDS

    val filterMarcMara = SqlOperations.filterJoinMarcMara[SurtidoMarc](saleArticleList)(spark)
    val joinResult = filterMarcMara(dsMarcMara)
    assertDatasetEquals(joinResult, dsResult)

  }

  "joinFilteredMarcMara " must " return the a dataset of JoinSurtidoMarcMara only with sale articles" taggedAs UnitTestTag in {

    val dsMarc: Dataset[SurtidoMarc] = sc.parallelize(List[SurtidoMarc](
      SurtidoMarc("1", "11", "ACTIVO"),
      SurtidoMarc("2", "22", "ACTIVO"),
      SurtidoMarc("3", "33", "ACTIVO"))).toDS

    val dsMara: Dataset[SurtidoMara] = sc.parallelize(List[SurtidoMara](
      SurtidoMara("1", "EA", "2753", "S"),
      SurtidoMara("2", "EB", "2754", ""),
      SurtidoMara("3", "EA", "2755", null))).toDS

    val dsResult: Dataset[JoinSurtidoMarcMara] =
      sc.parallelize(List[JoinSurtidoMarcMara](
        JoinSurtidoMarcMara("1", "11", "EA", "2753"),
        JoinSurtidoMarcMara("2", "22", "EB", "2754"),
        JoinSurtidoMarcMara("3", "33", "EA", "2755"))).toDS

    val joinResult = SqlOperations.joinFilteredMarcMara(dsMarc, dsMara, saleArticleList)(spark).orderBy("idArticle")
    assertDatasetEquals(joinResult, dsResult)

  }

  it must " return the a dataset of JoinSurtidoMarcMara with sale and buy articles" taggedAs UnitTestTag in {

    val dsMarc: Dataset[SurtidoMarc] = sc.parallelize(List[SurtidoMarc](
      SurtidoMarc("1", "11", "ACTIVO"),
      SurtidoMarc("2", "22", "ACTIVO"),
      SurtidoMarc("3", "33", "ACTIVO"),
      SurtidoMarc("4", "44", "ACTIVO"))).toDS

    val dsMara: Dataset[SurtidoMara] = sc.parallelize(List[SurtidoMara](
      SurtidoMara("1", "EA", "2753", "S"),
      SurtidoMara("2", "EB", "2754", ""),
      SurtidoMara("3", "EA", "2755", null),
      SurtidoMara("4", "EB", "2756", "P"))).toDS

    val dsResult: Dataset[JoinSurtidoMarcMara] =
      sc.parallelize(List[JoinSurtidoMarcMara](
        JoinSurtidoMarcMara("1", "11", "EA", "2753"),
        JoinSurtidoMarcMara("2", "22", "EB", "2754"),
        JoinSurtidoMarcMara("3", "33", "EA", "2755"))).toDS

    val joinResult = SqlOperations.joinFilteredMarcMara(dsMarc, dsMara, saleArticleList)(spark).orderBy("idArticle")
    assertDatasetEquals(joinResult, dsResult)

  }

  it must
    " return the a dataset of JoinSurtidoMarcMara with default values for left side elements when there are mara rows missing" taggedAs UnitTestTag in {

    val dsMarc: Dataset[SurtidoMarc] = sc.parallelize(List[SurtidoMarc](
      SurtidoMarc("1", "11", "ACTIVO"),
      SurtidoMarc("2", "22", "ACTIVO"),
      SurtidoMarc("3", "22", "ACTIVO"))).toDS

    val dsMara: Dataset[SurtidoMara] = sc.parallelize(List[SurtidoMara](
      SurtidoMara("1", "EA", "2753", "S"),
      SurtidoMara("3", "EB", "2755", "P"),
      SurtidoMara("4", "EB", "2754", "S"),
      SurtidoMara("5", "EB", "2754", "S")
    )).toDS

    val dsResult: Dataset[JoinSurtidoMarcMara] =
      sc.parallelize(List[JoinSurtidoMarcMara](
        JoinSurtidoMarcMara("1", "11", "EA", "2753"),
        JoinSurtidoMarcMara("2", "22", null, null))).toDS

    val joinResult = SqlOperations.joinFilteredMarcMara(dsMarc, dsMara, saleArticleList)(spark)
    assertDatasetEquals(joinResult, dsResult)

  }

  "default joinMarcMaraSector " must " return the a dataset of JoinSurtidoMarcMaraSector " taggedAs (UnitTestTag) in {

    val dsMarcMara: Dataset[JoinSurtidoMarcMara] = sc.parallelize(List[JoinSurtidoMarcMara](
      JoinSurtidoMarcMara("1", "11", "EA", "2753"),
      JoinSurtidoMarcMara("2", "22", "EB", "2754"))).toDS

    val dsSector: Dataset[SurtidoEstructuraComercialView] = sc.parallelize(List[SurtidoEstructuraComercialView](
      SurtidoEstructuraComercialView("2753", "1"),
      SurtidoEstructuraComercialView("2754", "3"))).toDS

    val dsResult: Dataset[JoinSurtidoMarcMaraSector] =
      sc.parallelize(List[JoinSurtidoMarcMaraSector](
        JoinSurtidoMarcMaraSector("1", "11", "1", "EA"),
        JoinSurtidoMarcMaraSector("2", "22", "3", "EB"))).toDS

    val joinResult = SqlOperations.joinMarcMaraSector(dsMarcMara, dsSector)(spark)
    assertDatasetEquals(joinResult, dsResult)
  }

  "joinMarcMaraSector without some rows " must
    " return the a dataset of JoinSurtidoMarcMaraSector " taggedAs (UnitTestTag) in {

    val dsMarcMara: Dataset[JoinSurtidoMarcMara] = sc.parallelize(List[JoinSurtidoMarcMara](
      JoinSurtidoMarcMara("1", "11", "EA", "2753"),
      JoinSurtidoMarcMara("2", "22", "EB", "2754"))).toDS

    val dsSector: Dataset[SurtidoEstructuraComercialView] = sc.parallelize(List[SurtidoEstructuraComercialView](
      SurtidoEstructuraComercialView("2753", "1"),
      SurtidoEstructuraComercialView("2755", "3"))).toDS

    val dsResult: Dataset[JoinSurtidoMarcMaraSector] =
      sc.parallelize(List[JoinSurtidoMarcMaraSector](
        JoinSurtidoMarcMaraSector("1", "11", "1", "EA" ),
        JoinSurtidoMarcMaraSector("2", "22", null, "EB" ))).toDS

    val joinResult = SqlOperations.joinMarcMaraSector(dsMarcMara, dsSector)(spark)
    assertDatasetEquals(joinResult, dsResult)
  }

  "joinMarcMaraSector with with null management " must
    " return the a dataset of JoinSurtidoMarcMaraSector " taggedAs (UnitTestTag) in {

    val dsMarcMara: Dataset[JoinSurtidoMarcMara] = sc.parallelize(List[JoinSurtidoMarcMara](
      JoinSurtidoMarcMara("1", "11", "EA", "2753"),
      JoinSurtidoMarcMara("2", "22", "EB", null))).toDS

    val dsSector: Dataset[SurtidoEstructuraComercialView] = sc.parallelize(List[SurtidoEstructuraComercialView](
      SurtidoEstructuraComercialView("2753", "1"),
      SurtidoEstructuraComercialView("2755", "3"))).toDS

    val dsResult: Dataset[JoinSurtidoMarcMaraSector] =
      sc.parallelize(List[JoinSurtidoMarcMaraSector](
        JoinSurtidoMarcMaraSector("1", "11", "1", "EA" ),
        JoinSurtidoMarcMaraSector("2", "22", null, "EB"))).toDS

    val joinResult = SqlOperations.joinMarcMaraSector(dsMarcMara, dsSector)(spark).orderBy(Surtido.ccTags.idArticleTag)
    assertDatasetEquals(joinResult, dsResult)
  }

  "default joinNstNsp " must " return the a dataset of JoinNstNsp " taggedAs (UnitTestTag) in {
    val dsNst: Dataset[NstView] = sc.parallelize(List[NstView](
      NstView("1", "11", 20d, currentTimestamp),
      NstView("2", "22", 30d, currentTimestamp))).toDS

    val dsNsp: Dataset[NspView] = sc.parallelize(List[NspView](
      NspView("1", "11", 5d, currentTimestamp),
      NspView("2", "22", 65d, currentTimestamp))).toDS

    val dsResult: Dataset[JoinNstNsp] =
      sc.parallelize(List[JoinNstNsp](
        JoinNstNsp("1", "11", Some(20d), Some(5d)),
        JoinNstNsp("2", "22", Some(30d), Some(65d)))).toDS.orderBy(Nst.ccTags.idArticleTag)

    val joinResult = SqlOperations.joinNstNsp(dsNst, dsNsp)(spark).orderBy(Nst.ccTags.idArticleTag)
    assertDatasetEquals(joinResult, dsResult)
  }

  "joinNstNsp without rows " must " return the a dataset of JoinNstNsp with None values" taggedAs (UnitTestTag) in {
    val dsNst: Dataset[NstView] = sc.parallelize(List[NstView](
      NstView("1", "11", 20d, currentTimestamp),
      NstView("2", "22", 30d, currentTimestamp))).toDS

    val dsNsp: Dataset[NspView] = sc.parallelize(List[NspView](
      NspView("1", "11", 5d, currentTimestamp),
      NspView("3", "33", 65d, currentTimestamp))).toDS

    val dsResult: Dataset[JoinNstNsp] =
      sc.parallelize(List[JoinNstNsp](
        JoinNstNsp("1", "11", Some(20d), Some(5d)),
        JoinNstNsp("2", "22", Some(30d), None),
        JoinNstNsp("3", "33", None, Some(65d)))).toDS.orderBy(Nst.ccTags.idArticleTag)

    val joinResult = SqlOperations.joinNstNsp(dsNst, dsNsp)(spark).orderBy(Nst.ccTags.idArticleTag)
    assertDatasetEquals(joinResult, dsResult)
  }

  "default joinSurtidoNs " must " return the a dataset of JoinSurtidoAndNs " taggedAs (UnitTestTag) in {
     val dsSurtido: Dataset[JoinSurtidoMarcMaraSector] = sc.parallelize(List[JoinSurtidoMarcMaraSector](
       JoinSurtidoMarcMaraSector("1", "11", "4", "U"),
       JoinSurtidoMarcMaraSector("2", "22", "5", "M"))).toDS

    val dsNs: Dataset[JoinNstNsp] = sc.parallelize(List[JoinNstNsp](
      JoinNstNsp("1", "11", Some(10d), Some(30d)),
      JoinNstNsp("2", "22", Some(65d), Some(5d)))).toDS

    val dsResult: Dataset[JoinSurtidoAndNs] =
      sc.parallelize(List[JoinSurtidoAndNs](
        JoinSurtidoAndNs("1", "11", "4", "U", Some(10d), Some(30d)),
        JoinSurtidoAndNs("2", "22", "5", "M", Some(65d), Some(5d)))).toDS.orderBy(Nst.ccTags.idArticleTag)

    val joinResult = SqlOperations.joinSurtidoNs(dsSurtido, dsNs)(spark).orderBy(SurtidoMarc.ccTags.idArticleTag)
    assertDatasetEquals(joinResult, dsResult)
  }

  "joinSurtidoNs without rows" must " return the a dataset of JoinSurtidoAndNs with None values " taggedAs (UnitTestTag) in {
    val dsSurtido: Dataset[JoinSurtidoMarcMaraSector] = sc.parallelize(List[JoinSurtidoMarcMaraSector](
      JoinSurtidoMarcMaraSector("1", "11", "4", "U"),
      JoinSurtidoMarcMaraSector("3", "33", "5", "M"),
      JoinSurtidoMarcMaraSector("4", "44", "6", "M"),
      JoinSurtidoMarcMaraSector("5", "55", "7", "M")
    )).toDS

    val dsNs: Dataset[JoinNstNsp] = sc.parallelize(List[JoinNstNsp](
      JoinNstNsp("1", "11", Some(10d), Some(30d)),
      JoinNstNsp("2", "22", Some(65d), Some(5d)),
      JoinNstNsp("4", "44", None, Some(5d)),
      JoinNstNsp("5", "55", Some(65d), None)
    )).toDS

    val dsResult: Dataset[JoinSurtidoAndNs] =
      sc.parallelize(List[JoinSurtidoAndNs](
        JoinSurtidoAndNs("1", "11", "4", "U", Some(10d), Some(30d)),
        JoinSurtidoAndNs("3", "33", "5", "M", None, None),
        JoinSurtidoAndNs("4", "44", "6", "M", None, Some(5d)),
        JoinSurtidoAndNs("5", "55", "7", "M", Some(65d), None))
      ).toDS.orderBy(Nst.ccTags.idArticleTag)

    val joinResult = SqlOperations.joinSurtidoNs(dsSurtido, dsNs)(spark).orderBy(SurtidoMarc.ccTags.idArticleTag)
    assertDatasetEquals(joinResult, dsResult)
  }

  "default joinSurtidoNsAndRotation " must " return the a dataset of JoinSurtidoNsRotation " taggedAs (UnitTestTag) in {
    val dsSurtidoNs: Dataset[JoinSurtidoAndNs] = sc.parallelize(List[JoinSurtidoAndNs](
      JoinSurtidoAndNs("1", "11", "4", "U", Some(10d), Some(30d)),
      JoinSurtidoAndNs("2", "22", "5", "M", Some(65d), Some(5d)))).toDS

    val dsRotation: Dataset[SupplySkuRotation] = sc.parallelize(List[SupplySkuRotation](
      SupplySkuRotation("11", "1", "a", currentTimestamp),
      SupplySkuRotation("22", "2", "c", currentTimestamp))).toDS

    val dsResult: Dataset[JoinSurtidoNsRotation] =
      sc.parallelize(List[JoinSurtidoNsRotation](
        JoinSurtidoNsRotation("1", "11", "4", "U", Some(10d), Some(30d), "a"),
        JoinSurtidoNsRotation("2", "22", "5", "M", Some(65d), Some(5d), "c"))).toDS.orderBy(Nst.ccTags.idArticleTag)

    val joinResult = SqlOperations.joinSurtidoNsAndRotation(dsSurtidoNs, dsRotation)(spark).orderBy(SurtidoMarc.ccTags.idArticleTag)
    assertDatasetEquals(joinResult, dsResult)
  }

  "joinSurtidoNsAndRotation without rows " must " return the a dataset of JoinSurtidoNsRotation with default values " taggedAs (UnitTestTag) in {
    val dsSurtidoNs: Dataset[JoinSurtidoAndNs] = sc.parallelize(List[JoinSurtidoAndNs](
      JoinSurtidoAndNs("1", "11", "4", "U", Some(10d), Some(30d)),
      JoinSurtidoAndNs("3", "33", "5", "M", Some(65d), Some(5d)),
      JoinSurtidoAndNs("5", "55", "6", "Kg", None, None))).toDS

    val dsRotation: Dataset[SupplySkuRotation] = sc.parallelize(List[SupplySkuRotation](
      SupplySkuRotation("11", "1", "a", currentTimestamp),
      SupplySkuRotation("22", "2", "c", currentTimestamp),
      SupplySkuRotation("55", "5", null, currentTimestamp)
    )).toDS

    val dsResult: Dataset[JoinSurtidoNsRotation] =
      sc.parallelize(List[JoinSurtidoNsRotation](
        JoinSurtidoNsRotation("1", "11", "4", "U", Some(10d), Some(30d), "a"),
        JoinSurtidoNsRotation("3", "33", "5", "M", Some(65d), Some(5d), null),
        JoinSurtidoNsRotation("5", "55", "6", "Kg", None, None, null)
      )).toDS.orderBy(Nst.ccTags.idArticleTag)

    val joinResult = SqlOperations.joinSurtidoNsAndRotation(dsSurtidoNs, dsRotation)(spark).orderBy(SurtidoMarc.ccTags.idArticleTag)
    assertDatasetEquals(joinResult, dsResult)
  }

  "default joinDatosFijos " must " return the dataset of DatosFijos " taggedAs (UnitTestTag) in {
    val dsSurtidoNs: Dataset[JoinSurtidoNsRotation] = sc.parallelize(List[JoinSurtidoNsRotation](
      JoinSurtidoNsRotation("1", "11", "4", "U", Some(10d), Some(30d), "a"),
      JoinSurtidoNsRotation("2", "22", "5", "M", Some(65d), Some(5d), "c"))).toDS

    val salesDF: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("1", "11", "2018-06-22", 50d, 55d),
      Row("2", "22", "2018-06-23", 34d, 40d))),
      StructType(List(
      StructField("idArticle",       StringType),
      StructField("idStore",         StringType),
      StructField("dateSale",        StringType, false),
      StructField("salesForecastN",  DoubleType),
      StructField("salesForecastN1", DoubleType)
    )))

    val resultDF: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("1", "11", "4", "U", 10d, 30d, "a", "2018-06-22", 50d, 55d, currentTimestamp, "user"),
      Row("2", "22", "5", "M", 65d, 5d, "c", "2018-06-23", 34d, 40d, currentTimestamp, "user"))),
      StructType(List(
        StructField("idArticle",       StringType),
        StructField("idStore",         StringType),
        StructField("sector",        StringType),
        StructField("unityMeasure",        StringType),
        StructField("nst",        DoubleType),
        StructField("nsp",        DoubleType),
        StructField("rotation",        StringType),
        StructField("dateSale",        StringType, false),
        StructField("salesForecastN",  DoubleType),
        StructField("salesForecastN1", DoubleType)
      ))).orderBy(DatosFijos.ccTags.idArticleTag)

    val joinResultDF = salesDF.transform(SqlOperations.joinDatosFijos(dsSurtidoNs, today)(spark)).orderBy(DatosFijos.ccTags.idArticleTag)
    assertDataFrameEquals(joinResultDF, resultDF)
  }

  "joinDatosFijos without some rows" must " return the dataset of DatosFijos with default values " taggedAs (UnitTestTag) in {

    val today: String = Dates.timestampToStringWithTZ(new Timestamp(System.currentTimeMillis()), HYPHEN_SEPARATOR)

    val dsSurtidoNs: Dataset[JoinSurtidoNsRotation] = sc.parallelize(List[JoinSurtidoNsRotation](
      JoinSurtidoNsRotation("1", "11", "4", "U", Some(10d), Some(30d), "a"),
      JoinSurtidoNsRotation("2", "22", "5", "M", Some(65d), Some(5d), "c"))).toDS

    val salesDF: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("1", "11", "2018-06-22", 50.5d, 55.6d),
      Row("3", "22", "2018-06-23", 34d, 40d))),
      StructType(List(
        StructField("idArticle",       StringType),
        StructField("idStore",         StringType),
        StructField("dateSale",        StringType, false),
        StructField("salesForecastN",  DoubleType),
        StructField("salesForecastN1", DoubleType)
      )))

    val resultDF: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("1", "11", "4", "U", 10d, 30d, "a", "2018-06-22", 50.5d, 55.6d),
      Row("2", "22", "5", "M", 65d, 5d, "c", today, null, null))),
      StructType(List(
        StructField("idArticle",       StringType),
        StructField("idStore",         StringType),
        StructField("sector",        StringType),
        StructField("unityMeasure",        StringType),
        StructField("nst",        DoubleType),
        StructField("nsp",        DoubleType),
        StructField("rotation",        StringType),
        StructField("dateSale",        StringType, false),
        StructField("salesForecastN",  DoubleType),
        StructField("salesForecastN1", DoubleType)
      ))).orderBy(DatosFijos.ccTags.idArticleTag)

    val joinResultDF = salesDF.transform(SqlOperations.joinDatosFijos(dsSurtidoNs, today)(spark)).orderBy(DatosFijos.ccTags.idArticleTag)
    assertDataFrameEquals(joinResultDF, resultDF)
  }

  "addAuditFields" must " return the dataframe given with audit columns for insert and update values " taggedAs (UnitTestTag) in {
    val tsInsert: Timestamp = new Timestamp(System.currentTimeMillis())
    val tsInsertStr: String = Dates.timestampToStringWithTZ(tsInsert, HYPHEN_SEPARATOR)
    val userInsert: String = "userInsert"
    val tsUpdate: Timestamp = new Timestamp(System.currentTimeMillis())
    val tsUpdateStr: String = Dates.timestampToStringWithTZ(tsUpdate, HYPHEN_SEPARATOR)
    val userUpdate: String = "userUpdate"

    val inputDF: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("1", "11"),
      Row("2", "22"))),
      StructType(List(
        StructField("col1",  StringType),
        StructField("col2",  StringType)

      )))

    val expectedDF: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("1", "11", tsInsert, userInsert, tsUpdate, userUpdate),
      Row("2", "22", tsInsert, userInsert, tsUpdate, userUpdate))),
      StructType(List(
        StructField("col1",              StringType),
        StructField("col2",              StringType),
        StructField("tsInsertDlk",    TimestampType, false),
        StructField("userInsertDlk",     StringType, false),
        StructField("tsUpdateDlk",    TimestampType, false),
        StructField("userUpdateDlk",     StringType, false)
      )))

    val resultDF = inputDF.transform(SqlOperations.addAuditFields(tsInsert, userInsert, tsUpdate, userUpdate))
    assertDataFrameEquals(expectedDF, resultDF)
  }

  "pivotSalesForecast " must " return the aggregate " taggedAs (UnitTestTag) in {
    val dsSalesForecastView = sc.parallelize(List[SalesForecastView](
      SalesForecastView(today,         "1", 50.5,  "11"),
      SalesForecastView(tomorrow,      "1", 60.6,  "11"),
      SalesForecastView(today,         "2", 50.5,  "22"),
      SalesForecastView(tomorrow,      "3", 50.5,  "33"),
      SalesForecastView(afterTomorrow, "1", 0.5,   "11"),
      SalesForecastView(oneWeekLater,  "2", 10.25, "22")
    )).toDS

    val expectedDF: DataFrame = spark.createDataFrame(sc.parallelize(List[Row](
      Row("1", "11", today, 50.5, 60.6, 0.5,  null, null, null, null, null),
      Row("2", "22", today, 50.5, null, null, null, null, null, null, 10.25),
      Row("3", "33", today, null, 50.5, null, null, null, null, null, null)
    )), StructType(List(
      StructField("idArticle",       StringType),
      StructField("idStore",         StringType),
      StructField("dateSale",        StringType, false),
      StructField("salesForecastN",  DoubleType),
      StructField("salesForecastN1", DoubleType),
      StructField("salesForecastN2", DoubleType),
      StructField("salesForecastN3", DoubleType),
      StructField("salesForecastN4", DoubleType),
      StructField("salesForecastN5", DoubleType),
      StructField("salesForecastN6", DoubleType),
      StructField("salesForecastN7", DoubleType)
    )))

    val resultDF: DataFrame = dsSalesForecastView.transform(SqlOperations.pivotSalesForecast(forecastDateMap, today)(spark))
      .orderBy(DatosFijos.ccTags.idArticleTag).select(expectedDF.columns.head, expectedDF.columns.tail: _*)
    assertDataFrameEquals(expectedDF, resultDF)

  }

}
