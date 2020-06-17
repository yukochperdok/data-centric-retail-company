package com.training.bigdata.omnichannel.stockATP.calculateDatosFijos


import java.sql.Timestamp
import java.time._

import com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.inputs._
import com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.joins._
import com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.util.SqlOperations._
import com.training.bigdata.omnichannel.stockATP.common.entities.{ControlEjecucion, DatosFijos, SurtidoMara}
import com.training.bigdata.omnichannel.stockATP.common.io.tables.{Hive, Kudu}
import com.training.bigdata.omnichannel.stockATP.common.util.Constants._
import com.training.bigdata.omnichannel.stockATP.common.util.LensAppConfig._
import com.training.bigdata.omnichannel.stockATP.common.util.debug.DebugUtils.ops
import com.training.bigdata.omnichannel.stockATP.common.util.log.FunctionalLogger
import com.training.bigdata.omnichannel.stockATP.common.util.{AppConfig, Dates, DebugParameters}
import com.github.dwickern.macros.NameOf._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

case class ReadEntities(
  dsRotation: Dataset[SupplySkuRotation],
  dsSalesForecast: Dataset[SalesForecastView],
  dsSalesPlatform: Dataset[SalesForecastView],
  dsNst: Dataset[NstView],
  dsNsp: Dataset[NspView],
  dsSurtidoMara: Dataset[SurtidoMara],
  dsSurtidoMarc: Dataset[SurtidoMarc],
  dsCommercialStructure: Dataset[SurtidoEstructuraComercialView])

object DatosFijosMain extends FunctionalLogger {


  def main(args: Array[String]): Unit = {

    if (args.isEmpty) {
      throw new Error(s"The main program needs to be called with the enough number of parameters." +
        s"The given are : [${args.toList.mkString(";")}]")
    }
    // Get path config
    val pathToConf: String = args.take(1).headOption.getOrElse(throw new Error("The CONFIG_PATH parameter is mandatory as argument"))
    logConfig(s"The given conf path is $pathToConf")

    // Get user
    val updateUser: String = args.drop(1).headOption.getOrElse(throw new Error("The USER parameter is mandatory as argument"))
    logConfig(s"The given user is $updateUser")

    // Get optional date of data
    val optDataDate: Option[String] = args.drop(2).headOption
    logConfig(s"The given dataDate is ${optDataDate.getOrElse("NOT FOUND PARAMETER")}")

    implicit val spark: SparkSession = SparkSession
      .builder()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val appName: String = spark.conf.get("spark.app.name")
    logInfo(s"Application name is: [$appName]")

    logProcess(appName) {
      implicit val conf: AppConfig = AppConfig.apply(pathToConf.concat("/application.conf"))
      logInfo(s"Loaded configuration")
      logConfig(s"[getProject: ${conf getProjectCMDB}]")
      logConfig(s"[getListProcess: ${conf getListProcess}]")
      logConfig(s"[getListTopic: ${conf getListTopic}]")
      logConfig(s"[getCommonParameters: ${conf getCommonParameters}]")
      logConfig(s"[getDatosFijosParameters: ${conf getDatosFijosParameters}]")

      val startProcessTs = new Timestamp(System.currentTimeMillis)

      val dayNAtMidnightTs = getDayNAtMidnightTs(optDataDate)

      val readEntities = logSubProcess("readingInfo") {
        readingInfo(dayNAtMidnightTs)
      }

      val datosFijos: DataFrame = logSubProcess("processCalculate") {
        processCalculate(readEntities, dayNAtMidnightTs, updateUser)
      }

      logSubProcess("writeDatosFijos"){
        val initWriteTs = new Timestamp(System.currentTimeMillis)
        writeDatosFijos(datosFijos, startProcessTs, initWriteTs, appName, updateUser)
      }
    }
  }

  /*
   * We get dataDate to read, in timestamp format
   * @param optDataDate Parametrized optional dataDate
   *
   * return Timestamp date of data to read
   */
  def getDayNAtMidnightTs(
    optDataDate: Option[String],
    defaultDataDateTs: Timestamp = new Timestamp(System.currentTimeMillis))
    (implicit conf: AppConfig): Timestamp = {

    val dayNAtMidnightTs = Dates.getDateAtMidnight(optDataDate
      .map(dateStr => Dates.stringToTimestamp(dateStr.concat(" 00:00:00"), conf.getTimezone))
      .getOrElse(defaultDataDateTs))._1
    logDebug(s"[dayNAtMidnightTs: $dayNAtMidnightTs]")

    dayNAtMidnightTs
  }

  /*
   * We read all tables
   * @param currentTimestamp Timestamp of now
   *
   * return ReadEntities that are all entities we read
   */
  def readingInfo(dayNAtMidnightTs: Timestamp)(implicit spark: SparkSession, conf: AppConfig): ReadEntities = {
    import com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.inputs.implicits._
    import com.training.bigdata.omnichannel.stockATP.common.entities.common_entities.common_implicits._
    import com.training.bigdata.omnichannel.stockATP.common.io.tables.FromDbTable.ops._
    import org.apache.spark.sql.functions._
    import spark.implicits._

    implicit val kuduHost = conf.getKuduHost
    logDebug(s"[kuduHost: ${kuduHost}]")
    implicit val debugOptions = conf.getDebugDatosFijosParameters getOrElse DebugParameters()
    logDebug(s"[debugOptions: ${debugOptions}]")

    val futurePlanningDayAtMidnightTs = Dates.sumDaysToTimestamp(dayNAtMidnightTs, SALES_FORECAST_DAYS + 1)
    logDebug(s"[futurePlanningDayAtMidnightTs: ${futurePlanningDayAtMidnightTs}]")

    // Start to read
    val dsRotation: Dataset[SupplySkuRotation] = read[SupplySkuRotation, Kudu]
    dsRotation.logDSDebug(nameOf(dsRotation))

    val dsSalesForecast: Dataset[SalesForecastView] = read[SalesForecast, Kudu]
      .transform(getForecastSalesForTodayAndFuturePlanningDay(dayNAtMidnightTs, futurePlanningDayAtMidnightTs, conf.getTimezone))
    dsSalesForecast.logDSDebug(nameOf(dsSalesForecast))

    val dsSalesPlatform: Dataset[SalesForecastView] = read[SalesPlatform, Hive]
      .transform(getSalesPlatformForTodayAndFuturePlanningDay(dayNAtMidnightTs, futurePlanningDayAtMidnightTs, conf.getTimezone))
    dsSalesPlatform.logDSDebug(nameOf(dsSalesPlatform))

    val dsNst: Dataset[NstView] = read[Nst, Kudu]
      .filter(
        col(Nst.ccTags.tsNstTag) < lit(dayNAtMidnightTs))
      .drop(col(Nst.ccTags.tsNstTag))
      .as[NstView]
    dsNst.logDSDebug(nameOf(dsNst))

    val dsNsp: Dataset[NspView] = read[Nsp, Kudu]
      .filter(
        col(Nsp.ccTags.tsNspTag) < lit(dayNAtMidnightTs))
      .drop(col(Nsp.ccTags.tsNspTag))
      .as[NspView]
    dsNsp.logDSDebug(nameOf(dsNsp))

    // Reading surtido tables
    val dsSurtidoMara: Dataset[SurtidoMara] = read[SurtidoMara, Kudu]
    dsSurtidoMara.logDSDebug(nameOf(dsSurtidoMara))

    val dsSurtidoMarc: Dataset[SurtidoMarc] = read[SurtidoMarc, Kudu]
      .filter(col(SurtidoMarc.ccTags.idStatusTag) === SurtidoMarc.ACTIVO)
    dsSurtidoMarc.logDSDebug(nameOf(dsSurtidoMarc))

    val dsSurtidoEstructuraComercial: Dataset[SurtidoEstructuraComercialView] = read[SurtidoEstructuraComercial, Kudu]
        .filter(trim(col(SurtidoEstructuraComercial.ccTags.levelTag)) === SurtidoEstructuraComercial.SUBFAMILY_LEVEL)
        .drop(col(SurtidoEstructuraComercial.ccTags.levelTag))
        .as[SurtidoEstructuraComercialView]
    dsSurtidoEstructuraComercial.logDSDebug(nameOf(dsSurtidoEstructuraComercial))

    ReadEntities(
      dsRotation,
      dsSalesForecast,
      dsSalesPlatform,
      dsNst,
      dsNsp,
      dsSurtidoMara,
      dsSurtidoMarc,
      dsSurtidoEstructuraComercial)
  }

  /*
   * Calculate the datos fijos dataset
   * @param readEntities all datasets we need to read to calculate datos fijos
   * @param currentTimestamp Timestamp of now
   *
   * @return dsFinal Dataset[DatosFijos] the result of all joins
   */
  def processCalculate(
    readEntities: ReadEntities,
    currentTimestamp: Timestamp,
    updateUser: String)(implicit spark: SparkSession, conf: AppConfig): DataFrame = {

    implicit val kuduHost = conf.getKuduHost
    logDebug(s"[kuduHost: ${kuduHost}]")
    implicit val debugOptions = conf.getDebugDatosFijosParameters getOrElse DebugParameters()
    logDebug(s"[debugOptions: ${debugOptions}]")

    // Default values
    val defaultSalesDate: String = Dates.timestampToStringWithTZ(currentTimestamp, HYPHEN_SEPARATOR, conf.getTimezone)
    val defaultRegularSales: Double = conf.getCommonParameters.defaultRegularSales.toDouble
    logDebug(s"[defaultSalesDate: ${defaultSalesDate}]")
    logDebug(s"[defaultRegularSales: ${defaultRegularSales}]")

    val forecastDateMap: Map[String, String] =
      Dates.getFuturePlanningDaysMap(currentTimestamp, HYPHEN_SEPARATOR, plusDays = SALES_FORECAST_DAYS + 1)

    // We are not running any distinct method because platform skus won't match with store skus
    val dsForecastUnion: Dataset[SalesForecastView] = readEntities.dsSalesForecast unionByName readEntities.dsSalesPlatform

    val salesForecastDF: DataFrame = dsForecastUnion.transform(pivotSalesForecast(forecastDateMap, defaultSalesDate))
    salesForecastDF.logDSDebug(nameOf(salesForecastDF))

    val dsSurtidoMarcMara: Dataset[JoinSurtidoMarcMara] = joinFilteredMarcMara(
      readEntities.dsSurtidoMarc,
      readEntities.dsSurtidoMara,
      conf.getSaleArticlesList)
    dsSurtidoMarcMara.logDSDebug(nameOf(dsSurtidoMarcMara))

    val dsSurtido: Dataset[JoinSurtidoMarcMaraSector] = joinMarcMaraSector(
      dsSurtidoMarcMara,
      readEntities.dsCommercialStructure)
    dsSurtido.logDSDebug(nameOf(dsSurtido))

    val dsNstNsp = joinNstNsp(readEntities.dsNst, readEntities.dsNsp)
    dsNstNsp.logDSDebug(nameOf(dsNstNsp))

    val dsSurtidoAndNS = joinSurtidoNs(dsSurtido, dsNstNsp)
    dsSurtidoAndNS.logDSDebug(nameOf(dsSurtidoAndNS))

    val dsSurtidoNsRotation = joinSurtidoNsAndRotation(dsSurtidoAndNS, readEntities.dsRotation)
    dsSurtidoNsRotation.logDSDebug(nameOf(dsSurtidoNsRotation))

    lazy val updatedTimestamp =
      new Timestamp(ZonedDateTime.now(ZoneId.of(conf.getTimezone)).toInstant.toEpochMilli)

    val dsFinal: DataFrame = salesForecastDF.transform(
      joinDatosFijos(
        dsSurtidoNsRotation,
        defaultSalesDate) andThen
      addAuditFields(updatedTimestamp, updateUser, updatedTimestamp, updateUser)
    )

    dsFinal.logDSDebug(nameOf(dsFinal))
    dsFinal
  }

  /*
   * Writing datos fijos in KUDU
   * @param dsFinal dataset with the result of joining all tables
   */
  def writeDatosFijos(
    dfFinal: DataFrame,
    startProcessTs: Timestamp,
    initWriteTs: Timestamp,
    appName: String,
    updateUser: String)(implicit spark: SparkSession, conf: AppConfig): Unit = {
    import com.training.bigdata.omnichannel.stockATP.common.entities.common_entities.common_implicits._
    import com.training.bigdata.omnichannel.stockATP.common.io.tables.ToDbTable.ops._
    import spark.implicits._

    implicit val kuduHost: String = conf.getKuduHost
    logDebug(s"[kuduHost: ${kuduHost}]")
    //Write to KUDU
    dfFinal.writeTo[DatosFijos, Hive](_.mode(SaveMode.Overwrite))

    logInfo(s"DatosFijos have been correctly write in HIVE")

    lazy val finishWriteTs = new Timestamp(System.currentTimeMillis())
    lazy val updateTs = new Timestamp(System.currentTimeMillis())

    val thisExecution: Dataset[ControlEjecucion] = Seq(ControlEjecucion(
      appName,
      startProcessTs,
      initWriteTs,
      finishWriteTs,
      updateTs,
      updateUser
    )).toDS()

    thisExecution.writeTo[Kudu](_.mode(SaveMode.Overwrite))

    logInfo(s"Last execution of DatosFijos updated in KUDU")

  }

}
