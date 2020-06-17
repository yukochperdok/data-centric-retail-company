package com.training.bigdata.omnichannel.stockATP.calculateDatosFijos

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.inputs._
import com.training.bigdata.omnichannel.stockATP.common.entities.{DatosFijos, SurtidoMara}
import com.training.bigdata.omnichannel.stockATP.common.util.tag.TagTest.UnitTestTag
import com.training.bigdata.omnichannel.stockATP.common.util.{CommonParameters, Dates, DefaultConf}
import com.training.bigdata.omnichannel.stockATP.common.util.LensAppConfig._
import com.training.bigdata.omnichannel.stockATP.common.util.Constants._
import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.Audit
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.{FlatSpec, Matchers}

class DatosFijosMainTest extends FlatSpec with Matchers with DatasetSuiteBase{
  import spark.implicits._
  override protected implicit def enableHiveSupport: Boolean = false
  val currentTimestamp: Timestamp = new Timestamp(System.currentTimeMillis)
  val today: String = Dates.timestampToStringWithTZ(currentTimestamp, HYPHEN_SEPARATOR)
  val ts2: Timestamp = new Timestamp(1529670737000L) // 22 de junio de 2018 14:32:17
  val tomorrowTs = Dates.sumDaysToTimestamp(currentTimestamp, plusDays = 1)
  val tomorrow: String = Dates.timestampToStringWithTZ(tomorrowTs, HYPHEN_SEPARATOR)
  val inTwoWeeksTs = Dates.sumDaysToTimestamp(currentTimestamp, plusDays = 14)
  val inTwoWeeks: String = Dates.timestampToStringWithTZ(inTwoWeeksTs, HYPHEN_SEPARATOR)
  val inOneMonthTs = Dates.sumDaysToTimestamp(currentTimestamp, plusDays = 30)
  val inOneMonth: String = Dates.timestampToStringWithTZ(inOneMonthTs, HYPHEN_SEPARATOR)
  val inFuturePlanningTs = Dates.sumDaysToTimestamp(currentTimestamp, plusDays = SALES_FORECAST_DAYS)
  val inFuturePlanning: String = Dates.timestampToStringWithTZ(inFuturePlanningTs, HYPHEN_SEPARATOR)

  trait DatosFijosConf extends DefaultConf{
    implicit val appConfig =
      defaultConfig
        .setCommonParameters(
          CommonParameters(
            kuduHost = "172.17.0.2:7051", // "192.168.173.2:7051", "172.17.0.2:7051"
            timezone = "Europe/Paris",
            defaultUnidadMedida = "EA",
            defaultNst = 100,
            defaultNsp = 100,
            defaultRotation = "B",
            defaultRegularSales = 0
          )
        )

    val ACTIVO = "Z2"

    val resultDFStructType = StructType(List(
      StructField(DatosFijos.ccTags.idArticleTag,    StringType),
      StructField(DatosFijos.ccTags.idStoreTag,      StringType),
      StructField(DatosFijos.ccTags.sectorTag,       StringType),
      StructField(DatosFijos.ccTags.unityMeasureTag, StringType),
      StructField(DatosFijos.ccTags.nstTag,          DoubleType),
      StructField(DatosFijos.ccTags.nspTag,          DoubleType),
      StructField(DatosFijos.ccTags.rotationTag,     StringType),
      StructField("salesForecastN",                  DoubleType),
      StructField("salesForecastN1",                 DoubleType),
      StructField("salesForecastN2",                 DoubleType),
      StructField("salesForecastN3",                 DoubleType),
      StructField("salesForecastN4",                 DoubleType),
      StructField("salesForecastN5",                 DoubleType),
      StructField("salesForecastN6",                 DoubleType),
      StructField("salesForecastN7",                 DoubleType),
      StructField("salesForecastN8",                 DoubleType),
      StructField("salesForecastN9",                 DoubleType),
      StructField("salesForecastN10",                DoubleType),
      StructField("salesForecastN11",                DoubleType),
      StructField("salesForecastN12",                DoubleType),
      StructField("salesForecastN13",                DoubleType),
      StructField("salesForecastN14",                DoubleType),
      StructField("salesForecastN15",                DoubleType),
      StructField("salesForecastN16",                DoubleType),
      StructField("salesForecastN17",                DoubleType),
      StructField("salesForecastN18",                DoubleType),
      StructField("salesForecastN19",                DoubleType),
      StructField("salesForecastN20",                DoubleType),
      StructField("salesForecastN21",                DoubleType),
      StructField("salesForecastN22",                DoubleType),
      StructField("salesForecastN23",                DoubleType),
      StructField("salesForecastN24",                DoubleType),
      StructField("salesForecastN25",                DoubleType),
      StructField("salesForecastN26",                DoubleType),
      StructField("salesForecastN27",                DoubleType),
      StructField("salesForecastN28",                DoubleType),
      StructField("salesForecastN29",                DoubleType),
      StructField("salesForecastN30",                DoubleType),
      StructField("salesForecastN31",                DoubleType),
      StructField("salesForecastN32",                DoubleType),
      StructField("salesForecastN33",                DoubleType),
      StructField("salesForecastN34",                DoubleType),
      StructField("salesForecastN35",                DoubleType),
      StructField("salesForecastN36",                DoubleType),
      StructField("salesForecastN37",                DoubleType),
      StructField("salesForecastN38",                DoubleType),
      StructField("salesForecastN39",                DoubleType),
      StructField("salesForecastN40",                DoubleType),
      StructField("salesForecastN41",                DoubleType),
      StructField("salesForecastN42",                DoubleType),
      StructField("salesForecastN43",                DoubleType),
      StructField("salesForecastN44",                DoubleType),
      StructField("salesForecastN45",                DoubleType),
      StructField("salesForecastN46",                DoubleType),
      StructField("salesForecastN47",                DoubleType),
      StructField("salesForecastN48",                DoubleType),
      StructField("salesForecastN49",                DoubleType),
      StructField("salesForecastN50",                DoubleType),
      StructField("salesForecastN51",                DoubleType),
      StructField("salesForecastN52",                DoubleType),
      StructField("salesForecastN53",                DoubleType),
      StructField("salesForecastN54",                DoubleType),
      StructField("salesForecastN55",                DoubleType),
      StructField("salesForecastN56",                DoubleType),
      StructField("salesForecastN57",                DoubleType),
      StructField("salesForecastN58",                DoubleType),
      StructField("salesForecastN59",                DoubleType),
      StructField("salesForecastN60",                DoubleType),
      StructField("salesForecastN61",                DoubleType),
      StructField("salesForecastN62",                DoubleType),
      StructField("salesForecastN63",                DoubleType),
      StructField("salesForecastN64",                DoubleType),
      StructField("salesForecastN65",                DoubleType),
      StructField("salesForecastN66",                DoubleType),
      StructField("salesForecastN67",                DoubleType),
      StructField("salesForecastN68",                DoubleType),
      StructField("salesForecastN69",                DoubleType),
      StructField("salesForecastN70",                DoubleType),
      StructField("salesForecastN71",                DoubleType),
      StructField("salesForecastN72",                DoubleType),
      StructField("salesForecastN73",                DoubleType),
      StructField("salesForecastN74",                DoubleType),
      StructField("salesForecastN75",                DoubleType),
      StructField("salesForecastN76",                DoubleType),
      StructField("salesForecastN77",                DoubleType),
      StructField("salesForecastN78",                DoubleType),
      StructField("salesForecastN79",                DoubleType),
      StructField("salesForecastN80",                DoubleType),
      StructField("salesForecastN81",                DoubleType),
      StructField("salesForecastN82",                DoubleType),
      StructField("salesForecastN83",                DoubleType),
      StructField("salesForecastN84",                DoubleType),
      StructField("salesForecastN85",                DoubleType),
      StructField("salesForecastN86",                DoubleType),
      StructField("salesForecastN87",                DoubleType),
      StructField("salesForecastN88",                DoubleType),
      StructField("salesForecastN89",                DoubleType),
      StructField("salesForecastN90",                DoubleType),
      StructField("salesForecastN91",                DoubleType),
      StructField("salesForecastN92",                DoubleType),
      StructField("salesForecastN93",                DoubleType),
      StructField("salesForecastN94",                DoubleType),
      StructField("salesForecastN95",                DoubleType),
      StructField("salesForecastN96",                DoubleType),
      StructField("salesForecastN97",                DoubleType),
      StructField("salesForecastN98",                DoubleType),
      StructField("salesForecastN99",                DoubleType),
      StructField("salesForecastN100",               DoubleType),
      StructField("salesForecastN101",               DoubleType),
      StructField("salesForecastN102",               DoubleType),
      StructField(DatosFijos.ccTags.dateSaleTag,     StringType,    false),
      StructField("tsUpdateDlk",                     TimestampType, false),
      StructField("userUpdateDlk",                   StringType,    false)
    ))
  }

  "processCalculate " must " return datos fijos"  taggedAs (UnitTestTag) in new DatosFijosConf {

    val dsMarc: Dataset[SurtidoMarc] = sc.parallelize(List[SurtidoMarc](
      SurtidoMarc("1", "11", ACTIVO),
      SurtidoMarc("2", "22", ACTIVO),
      SurtidoMarc("3", "33", ACTIVO),
      SurtidoMarc("4", "44", ACTIVO),
      SurtidoMarc("5", "55", ACTIVO),
      SurtidoMarc("6", "66", ACTIVO),
      SurtidoMarc("10", "100", ACTIVO),
      SurtidoMarc("11", "111", ACTIVO),
      SurtidoMarc("1", "P11", ACTIVO),
      SurtidoMarc("2", "P11", ACTIVO)
    )).toDS

    val dsMara: Dataset[SurtidoMara] = sc.parallelize(List[SurtidoMara](
      SurtidoMara("1", "EA", "2753", ""),
      SurtidoMara("2", "EB", "2754", null),
      SurtidoMara("3", "EA", "2755", "S"),
      SurtidoMara("4", "EC", "2756", ""),
      SurtidoMara("5", null, "2757", "S"),
      SurtidoMara("7", "ED", "2758", null),
      SurtidoMara("8", "EA", "2759", "S"),
      SurtidoMara("10","EF",   null, "S"),
      SurtidoMara("11","EF",   null, "P")
    )).toDS

    val dsCommercialStructure: Dataset[SurtidoEstructuraComercialView] = sc.parallelize(List[SurtidoEstructuraComercialView](
      SurtidoEstructuraComercialView("2750", "01"),
      SurtidoEstructuraComercialView("2754", "02"),
      SurtidoEstructuraComercialView("2755", "03"),
      SurtidoEstructuraComercialView("2756", "04"),
      SurtidoEstructuraComercialView("2757", "04"),
      SurtidoEstructuraComercialView("2758", "05"),
      SurtidoEstructuraComercialView("2759", "06"),
      SurtidoEstructuraComercialView("2760", "06")
    )).toDS

    val dsNst: Dataset[NstView] = sc.parallelize(List[NstView](
      NstView("1", "22", 20d, currentTimestamp),
      NstView("3", "33", 30d, currentTimestamp),
      NstView("4", "44", 40d, currentTimestamp),
      NstView("6", "66", 50d, currentTimestamp),
      NstView("7", "77", 60d, currentTimestamp),
      NstView("8", "88", 70d, currentTimestamp)
    )).toDS

    val dsNsp: Dataset[NspView] = sc.parallelize(List[NspView](
      NspView("1", "22", 5d, currentTimestamp),
      NspView("2", "22", 15d, currentTimestamp),
      NspView("3", "33", 25d, currentTimestamp),
      NspView("5", "55", 35d, currentTimestamp),
      NspView("8", "88", 45d, currentTimestamp),
      NspView("9", "99", 55d, currentTimestamp)
    )).toDS

    val dsRotation: Dataset[SupplySkuRotation] = sc.parallelize(List[SupplySkuRotation](
      SupplySkuRotation( "11", "1",   "A", currentTimestamp),
      SupplySkuRotation( "22", "2",  "A+", currentTimestamp),
      SupplySkuRotation("P11", "2",  "A+", currentTimestamp),
      SupplySkuRotation( "33", "3",   "C", currentTimestamp),
      SupplySkuRotation( "55", "5",   "B", currentTimestamp),
      SupplySkuRotation( "66", "6",   "C", currentTimestamp),
      SupplySkuRotation( "88", "8",   "A", currentTimestamp),
      SupplySkuRotation("100", "10", null, currentTimestamp)
    )).toDS

    val dsSales: Dataset[SalesForecastView] = sc.parallelize(List[SalesForecastView](
      SalesForecastView("2018-06-22", "2", 50d, "22"),
      SalesForecastView("2018-06-23", "3", 34d, "44"),
      SalesForecastView("2018-06-23", "4", 64d, "44"),
      SalesForecastView("2018-06-23", "6", 22d, "66"),
      SalesForecastView("2018-06-23", "9", 11d, "99"),
      SalesForecastView("2018-06-24", "2",  1d, "22"),
      SalesForecastView("2018-10-01", "2", 56d, "22"),
      SalesForecastView("2018-10-02", "2", 58d, "22")
    )).toDS

    val dsSalesPlatform: Dataset[SalesForecastView] = sc.parallelize(List[SalesForecastView](
      SalesForecastView("2018-06-22", "1", 20d, "P11"),
      SalesForecastView("2018-06-23", "1", 25d, "P11"),
      SalesForecastView("2018-06-22", "2", 30d, "P11"),
      SalesForecastView("2018-06-22", "1", 20d, "P12"),
      SalesForecastView("2018-06-25", "1", 20d, "P11"),
      SalesForecastView("2018-10-01", "1", 22d, "P11"),
      SalesForecastView("2018-10-02", "1", 45d, "P11")
    )).toDS

    val resultDF: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row( "1",  "11", null, "EA", null, null, "A",  null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, "2018-06-22", currentTimestamp, "user"),
        Row( "2",  "22", "02", "EB", null, 15d,  "A+", 50d,  null,   1d, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
           56d,  58d, "2018-06-22", currentTimestamp, "user"),
        Row( "3",  "33", "03", "EA", 30d,  25d,  "C",  null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, "2018-06-22", currentTimestamp, "user"),
        Row( "4",  "44", "04", "EC", 40d,  null, null, null, 64d,  null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, "2018-06-22", currentTimestamp, "user"),
        Row( "5",  "55", "04", null, null, 35d,  "B",  null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, "2018-06-22", currentTimestamp, "user"),
        Row( "6",  "66", null, null, 50d,  null, "C",  null, 22d,  null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, "2018-06-22", currentTimestamp, "user"),
        Row("10", "100", null, "EF", null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, "2018-06-22", currentTimestamp, "user"),
        Row( "1", "P11", null, "EA", null, null, null, 20d,  25d,  null,  20d, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          22d, 45d, "2018-06-22", currentTimestamp, "user"),
        Row( "2", "P11", "02", "EB", null, null, "A+", 30d,  null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, "2018-06-22", currentTimestamp, "user"))),
        resultDFStructType
      ).orderBy(DatosFijos.ccTags.idArticleTag, DatosFijos.ccTags.idStoreTag)

    val readEntities =
      ReadEntities(
        dsRotation,
        dsSales,
        dsSalesPlatform,
        dsNst,
        dsNsp,
        dsMara,
        dsMarc,
        dsCommercialStructure)

    val finalDF: DataFrame =
      DatosFijosMain
        .processCalculate(readEntities, ts2, "user")(spark, appConfig)
        .orderBy(DatosFijos.ccTags.idArticleTag, DatosFijos.ccTags.idStoreTag)
        .select(resultDF.columns.head, resultDF.columns.tail: _*)
    assertDataFrameEquals(resultDF.drop(Audit.ccTags.tsUpdateDlkTag), finalDF.drop(Audit.ccTags.tsUpdateDlkTag))
  }

  "processCalculate with currentDate" must " return datos fijos"  taggedAs (UnitTestTag) in new DatosFijosConf {

    val dsMarc: Dataset[SurtidoMarc] = sc.parallelize(List[SurtidoMarc](
      SurtidoMarc( "1",  "11", ACTIVO),
      SurtidoMarc( "2",  "22", ACTIVO),
      SurtidoMarc( "3",  "33", ACTIVO),
      SurtidoMarc( "4",  "44", ACTIVO),
      SurtidoMarc( "5",  "55", ACTIVO),
      SurtidoMarc( "6",  "66", ACTIVO),
      SurtidoMarc("10", "100", ACTIVO),
      SurtidoMarc("11", "111", ACTIVO),
      SurtidoMarc( "1", "P11", ACTIVO),
      SurtidoMarc( "2", "P11", ACTIVO)
    )).toDS

    val dsMara: Dataset[SurtidoMara] = sc.parallelize(List[SurtidoMara](
      SurtidoMara("1", "EA", "2753", ""),
      SurtidoMara("2", "EB", "2754", null),
      SurtidoMara("3", "EA", "2755", "S"),
      SurtidoMara("4", "EC", "2756", ""),
      SurtidoMara("5", null, "2757", "S"),
      SurtidoMara("7", "ED", "2758", null),
      SurtidoMara("8", "EA", "2759", "S"),
      SurtidoMara("10","EF", null, "S"),
      SurtidoMara("11","EF",   null, "P")
    )).toDS

    val dsCommercialStructure: Dataset[SurtidoEstructuraComercialView] = sc.parallelize(List[SurtidoEstructuraComercialView](
      SurtidoEstructuraComercialView("2750", "01"),
      SurtidoEstructuraComercialView("2754", "02"),
      SurtidoEstructuraComercialView("2755", "03"),
      SurtidoEstructuraComercialView("2756", "04"),
      SurtidoEstructuraComercialView("2757", "04"),
      SurtidoEstructuraComercialView("2758", "05"),
      SurtidoEstructuraComercialView("2759", "06"),
      SurtidoEstructuraComercialView("2760", "06")
    )).toDS

    val dsNst: Dataset[NstView] = sc.parallelize(List[NstView](
      NstView("1", "22", 20d, currentTimestamp),
      NstView("3", "33", 30d, currentTimestamp),
      NstView("4", "44", 40d, currentTimestamp),
      NstView("6", "66", 50d, currentTimestamp),
      NstView("7", "77", 60d, currentTimestamp),
      NstView("8", "88", 70d, currentTimestamp)
    )).toDS

    val dsNsp: Dataset[NspView] = sc.parallelize(List[NspView](
      NspView("1", "22", 5d, currentTimestamp),
      NspView("2", "22", 15d, currentTimestamp),
      NspView("3", "33", 25d, currentTimestamp),
      NspView("5", "55", 35d, currentTimestamp),
      NspView("8", "88", 45d, currentTimestamp),
      NspView("9", "99", 55d, currentTimestamp)
    )).toDS

    val dsRotation: Dataset[SupplySkuRotation] = sc.parallelize(List[SupplySkuRotation](
      SupplySkuRotation( "11",  "1",  "A", currentTimestamp),
      SupplySkuRotation( "22",  "2", "A+", currentTimestamp),
      SupplySkuRotation("P11",  "2", "A+", currentTimestamp),
      SupplySkuRotation( "33",  "3",  "C", currentTimestamp),
      SupplySkuRotation( "55",  "5",  "B", currentTimestamp),
      SupplySkuRotation( "66",  "6",  "C", currentTimestamp),
      SupplySkuRotation( "88",  "8",  "A", currentTimestamp),
      SupplySkuRotation("100", "10", null, currentTimestamp)
    )).toDS

    val dsSales: Dataset[SalesForecastView] = sc.parallelize(List[SalesForecastView](
      SalesForecastView(today,            "2", 50.5d, "22"),
      SalesForecastView(tomorrow,         "2", 60.6d, "22"),
      SalesForecastView(tomorrow,         "3",   34d, "44"),
      SalesForecastView(tomorrow,         "4",   64d, "44"),
      SalesForecastView(tomorrow,         "6",   22d, "66"),
      SalesForecastView(tomorrow,         "9",   11d, "99"),
      SalesForecastView(inTwoWeeks,       "6",   10d, "66"),
      SalesForecastView(inFuturePlanning, "6",  100d, "66")
    )).toDS

    val dsSalesPlatform: Dataset[SalesForecastView] = sc.parallelize(List[SalesForecastView](
      SalesForecastView(today,      "1", 20d, "P11"),
      SalesForecastView(tomorrow,   "1", 70d, "P11"),
      SalesForecastView(today,      "2", 30d, "P11"),
      SalesForecastView(today,      "1", 20d, "P12"),
      SalesForecastView(inOneMonth, "2", 30d, "P11")
    )).toDS

    val resultDF: DataFrame =
      spark.createDataFrame(sc.parallelize(List[Row](
        Row( "1",  "11", null, "EA", null, null, "A",  null,  null,  null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, today, currentTimestamp, "user"),
        Row( "2",  "22", "02", "EB", null, 15d,  "A+", 50.5d, 60.6d, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, today, currentTimestamp, "user"),
        Row( "3",  "33", "03", "EA", 30d,  25d,  "C",  null,  null,  null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, today, currentTimestamp, "user"),
        Row( "4",  "44", "04", "EC", 40d,  null, null, null,  64d,   null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, today, currentTimestamp, "user"),
        Row( "5",  "55", "04", null, null, 35d,  "B",  null,  null,  null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, today, currentTimestamp, "user"),
        Row( "6",  "66", null, null, 50d,  null, "C",  null,  22d,   null, null, null, null, null, null, null, null, null,
          null, null, null,  10d, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, 100d, today, currentTimestamp, "user"),
        Row("10", "100", null, "EF", null, null, null, null,  null,  null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, today, currentTimestamp, "user"),
        Row( "1", "P11", null, "EA", null, null, null, 20d,   70d,   null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, today, currentTimestamp, "user"),
        Row( "2", "P11", "02", "EB", null, null, "A+", 30d,   null,  null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null,  30d, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, today, currentTimestamp, "user"))),
        resultDFStructType
      ).orderBy(DatosFijos.ccTags.idArticleTag, DatosFijos.ccTags.idStoreTag)

    val readEntities =
      ReadEntities(
        dsRotation,
        dsSales,
        dsSalesPlatform,
        dsNst,
        dsNsp,
        dsMara,
        dsMarc,
        dsCommercialStructure)

    val finalDF: DataFrame =
      DatosFijosMain
        .processCalculate(readEntities, currentTimestamp, "user")(spark, appConfig)
        .orderBy(DatosFijos.ccTags.idArticleTag, DatosFijos.ccTags.idStoreTag)
        .select(resultDF.columns.head, resultDF.columns.tail: _*)

    assertDataFrameEquals(resultDF.drop(Audit.ccTags.tsUpdateDlkTag), finalDF.drop(Audit.ccTags.tsUpdateDlkTag))
  }

  "getDayNAtMidnightTs" must " return currentDay in timestamp by default"  taggedAs (UnitTestTag) in new DatosFijosConf{
    val currentDateExpected = Dates.timestampToStringWithTZ(currentTimestamp, HYPHEN_SEPARATOR, appConfig.getTimezone)
    val currentDate =
      Dates.timestampToStringWithTZ(DatosFijosMain.getDayNAtMidnightTs(None)(appConfig), HYPHEN_SEPARATOR, appConfig.getTimezone)
    currentDate should be (currentDateExpected)
  }

  it must " return defaultTimestamp at midnight when indicated" taggedAs (UnitTestTag)  in new DatosFijosConf {
    val dateDataDefaultTs = new Timestamp(1535148000000L)
    DatosFijosMain.getDayNAtMidnightTs(None,dateDataDefaultTs)(appConfig) should be (dateDataDefaultTs)
  }

  it must " return the day passed as parameter to the method at midnight" taggedAs (UnitTestTag)  in new DatosFijosConf {
    val dateDataTsExpected = new Timestamp(1535148000000L)
    val dateDataExpected = Dates.timestampToStringWithTZ(dateDataTsExpected, HYPHEN_SEPARATOR, appConfig.getTimezone)
    DatosFijosMain.getDayNAtMidnightTs(Option("2018-08-25"))(appConfig) should be (dateDataTsExpected)
  }

}
