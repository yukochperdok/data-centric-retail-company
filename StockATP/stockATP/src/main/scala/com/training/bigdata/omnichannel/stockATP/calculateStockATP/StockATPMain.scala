package com.training.bigdata.omnichannel.stockATP.calculateStockATP

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs._
import com.training.bigdata.omnichannel.stockATP.calculateStockATP.util.Sql.{getArticleAndStoreOfOrdersNotCanceled, _}
import com.training.bigdata.omnichannel.stockATP.common.entities.{MaraWithArticleTypes, _}
import com.training.bigdata.omnichannel.stockATP.common.io.tables.{Hive, Kudu}
import com.training.bigdata.omnichannel.stockATP.common.util.CommonSqlLayer._
import com.training.bigdata.omnichannel.stockATP.common.util.Constants._
import com.training.bigdata.omnichannel.stockATP.common.util.LensAppConfig._
import com.training.bigdata.omnichannel.stockATP.common.util.debug.DebugUtils
import com.training.bigdata.omnichannel.stockATP.common.util.debug.DebugUtils.ops
import com.training.bigdata.omnichannel.stockATP.common.util.log.FunctionalLogger
import com.training.bigdata.omnichannel.stockATP.common.util.{AppConfig, CommonSqlLayer, Dates, DebugParameters}
import com.github.dwickern.macros.NameOf._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

case class ReadAtpEntities(
                            dsSaleMovements: Dataset[SaleMovementsView],
                            dsStockConsolidado: Dataset[StockConsolidadoView],
                            dsDatosFijos: DataFrame,
                            dsOrderHeader: Dataset[OrderHeader],
                            dsOrderLine: Dataset[OrderLineView],
                            dsPartialOrdersDelivered: Dataset[PartialOrdersDeliveredView],
                            dsStoreCapacity: Dataset[StoreCapacityView],
                            dsStoreType: Dataset[StoreTypeView],
                            dsMaraWithArticleTypes: Dataset[MaraWithArticleTypes],
                            dsMaterials: Dataset[Materials],
                            dsMaterialsArticle: Dataset[MaterialsArticle],
                            dsCustomerReservations: Dataset[CustomerReservations])

object StockATPMain extends FunctionalLogger {

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

    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .config("spark.debug.maxToStringFields", 300)
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
      logConfig(s"[getStockATPParameters: ${conf getStockATPParameters}]")


      val startProcessTs = new Timestamp(System.currentTimeMillis)
      val (dayNAtMidnightTs, previousDayToNAtMidnightTs) = logSubProcess("getDayNPreviousDayAtMidnight") {
        getDayNAndPreviousDayAtMidnight(optDataDate, startProcessTs)
      }

      val (readEntities, stockConf, listControlEjecucion): (ReadAtpEntities, StockConf, List[ControlEjecucionView]) =
        logSubProcess("readingInfo") {
          readingInfo(dayNAtMidnightTs, previousDayToNAtMidnightTs)
        }

      val dfResult: DataFrame =
        logSubProcess("processCalculate") {
          processCalculate(readEntities, stockConf, dayNAtMidnightTs, previousDayToNAtMidnightTs, listControlEjecucion, appName, updateUser)
        }

      logSubProcess("writeDsResult") {
        val initWriteTs = new Timestamp(System.currentTimeMillis)
        writeDfResult(dfResult, startProcessTs, initWriteTs, appName, updateUser)
      }
    }
  }

  /*
   * Gets parameter day if present or else current execution day and its previous day.
   * Both dates are in timestamp format and calculated at midnight.
   *
   * @param optDataDate Parametrized optional dataDate
   *
   * return (dayNAtMidnightTs: Timestamp, previousToNDayAtMidnightTs: Timestamp)
   * date of data to process, its previous day and last projection day plus one
   */
  def getDayNAndPreviousDayAtMidnight(
                                       optDataDate: Option[String],
                                       defaultDataDateTs: Timestamp = new Timestamp(System.currentTimeMillis))
                                     (implicit conf: AppConfig): (Timestamp, Timestamp) = {

    val (dayNAtMidnightTs, dayNAtMidNightLocalDateTime) = Dates.getDateAtMidnight(optDataDate
      .map(dateStr => Dates.stringToTimestamp(dateStr.concat(" 00:00:00"), conf.getTimezone))
      .getOrElse(defaultDataDateTs))
    logDebug(s"[dayNAtMidnightTs: $dayNAtMidnightTs]")

    val previousDayToNAtMidnightTs = Dates.subtractDaysToLocalDateTime(dayNAtMidNightLocalDateTime, 1)
    logDebug(s"[previousDayToNAtMidnightTs: $previousDayToNAtMidnightTs]")

    (dayNAtMidnightTs, previousDayToNAtMidnightTs)
  }

  /*
   * We read all tables
   * @param currentDate String today date
   * @param previousDate String yesterday date
   *
   * return ReadAtpEntities that are all entities we read
   */
  def readingInfo(
                   dayNAtMidnightTs: Timestamp,
                   previousDayToNAtMidnightTs: Timestamp)
                 (implicit spark: SparkSession, conf: AppConfig): (ReadAtpEntities, StockConf, List[ControlEjecucionView]) = {

    import com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs._
    import com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs.implicits._
    import com.training.bigdata.omnichannel.stockATP.common.entities.common_entities.common_implicits._
    import com.training.bigdata.omnichannel.stockATP.common.io.tables.FromDbTable.ops._

    implicit val kuduHost: String = conf.getKuduHost
    implicit val debugOptions: DebugParameters = conf.getDebugStockATPParameters getOrElse DebugParameters()
    val stockTypeOMS = conf.getStockTypeOMS
    val returnedToProviderFlag = conf.getStockToBeDeliveredParameters.returnedToProviderFlag
    val alreadyDeliveredFlag = conf.getStockToBeDeliveredParameters.alreadyDeliveredFlag
    val canceledFlagList = conf.getStockToBeDeliveredParameters.canceledFlagList
    val typeOrdersList = conf.getTypeOrdersList
    val controlEjecucionProcesses = conf.getControlEjecucionProcesses

    logDebug(s"[kuduHost: $kuduHost]")
    logDebug(s"[debugOptions: $debugOptions]")
    logDebug(s"[stockTypeOMS: $stockTypeOMS]")
    logDebug(s"[returnedToProviderFlag: $returnedToProviderFlag]")
    logDebug(s"[alreadyDeliveredFlag: $alreadyDeliveredFlag]")
    logDebug(s"[canceledFlagList: $canceledFlagList]")
    logDebug(s"[typeOrdersList: $typeOrdersList]")
    logDebug(s"[controlEjecucionProcesses: $controlEjecucionProcesses]")

    val dsStockConf: Dataset[StockConf] = read[StockConf, Kudu]
    val stockConf: StockConf =
      dsStockConf.collect.headOption
        .getOrElse {
          logAlert(s"There isn't configuration in order to calculate stock ATP")
          throw new Error(s"There isn't configuration in order to calculate stock ATP")
        }

    val finalStockConf =
      stockConf.copy(numberOfDaysToProject = if (stockConf.numberOfDaysToProject > MAX_PROJECT_DAYS) MAX_PROJECT_DAYS else stockConf.numberOfDaysToProject)
    logDebug(s"[number of days to project: ${finalStockConf.numberOfDaysToProject}]")

    val lastProjectionDayPlusTwoAtMidnightTs = Dates.sumDaysToTimestamp(dayNAtMidnightTs, finalStockConf.numberOfDaysToProject + 2)

    val dsControlEjecucion: Dataset[ControlEjecucionView] =
      read[ControlEjecucionView, Kudu]
        .transform(filterControlEjecucion(controlEjecucionProcesses))
    dsControlEjecucion.logDSDebug(nameOf(dsControlEjecucion))
    val listControlEjecucion: List[ControlEjecucionView] = dsControlEjecucion.collect().toList

    val dsStoreCapacity: Dataset[StoreCapacityView] = read[StoreCapacityView, Kudu]
    dsStoreCapacity.logDSDebug(nameOf(dsStoreCapacity))

    val dsStoreType: Dataset[StoreTypeView] = read[StoreTypeView, Kudu]
    dsStoreType.logDSDebug(nameOf(dsStoreType))

    logInfo(s"OMSConf has been read correctly from KUDU")

    val dsSaleMovements: Dataset[SaleMovementsView] = read[SaleMovements, Kudu]
      .transform(
        filterSaleMovementsByMovementCode andThen
        getSaleMovementsForDay(dayNAtMidnightTs)
      )

    val dsStockConsolidado: Dataset[StockConsolidadoView] =
      read[StockConsolidado, Kudu]
        .transform(filterStockConsolidadoAndManageNulls)

    dsSaleMovements.logDSDebug(nameOf(dsSaleMovements))
    dsStockConsolidado.logDSDebug(nameOf(dsStockConsolidado))

    val dfDatosFijos: DataFrame = readDF[DatosFijos, Hive]
      .transform(filterDatosFijos(finalStockConf.numberOfDaysToProject, dayNAtMidnightTs, previousDayToNAtMidnightTs))
    dfDatosFijos.logDSDebug(nameOf(dfDatosFijos))

    val dsMaraWithArticleTypes: Dataset[MaraWithArticleTypes] =
      read[MaraWithArticleTypes, Kudu]
    dsMaraWithArticleTypes.logDSDebug(nameOf(dsMaraWithArticleTypes))

    val dsOrderHeader: Dataset[OrderHeader] =
      read[OrderHeader, Kudu]
        .transform(filterOrderHeader)
    dsOrderHeader.logDSDebug(nameOf(dsOrderHeader))

    val dsOrderLine: Dataset[OrderLineView] =
      read[OrderLine, Kudu]
        .transform(filterOrderLines)
    dsOrderLine.logDSDebug(nameOf(dsOrderLine))

    val dsPartialOrdersDelivered: Dataset[PartialOrdersDeliveredView] =
      read[PartialOrdersDelivered, Kudu]
        .transform(
          filterPendingPartialOrdersDeliveredBetweenDays(previousDayToNAtMidnightTs, lastProjectionDayPlusTwoAtMidnightTs) andThen
            DebugUtils.logDebugTransform("dsFilterPartialOrdersDelivered") andThen
            transformPartialOrdersDelivered
        )
    dsPartialOrdersDelivered.logDSDebug(nameOf(dsPartialOrdersDelivered))

    val dsMaterials: Dataset[Materials] = read[Materials, Kudu]
    dsMaterials.logDSDebug(nameOf(dsMaterials))

    val dsMaterialsArticle: Dataset[MaterialsArticle] = read[MaterialsArticle, Kudu]
    dsMaterialsArticle.logDSDebug(nameOf(dsMaterialsArticle))

    val dsCustomerReservations: Dataset[CustomerReservations] = read[CustomerReservations, Kudu]
    dsCustomerReservations.logDSDebug(nameOf(dsCustomerReservations))

    logInfo(s"Inputs have read correctly from KUDU for the date [$dayNAtMidnightTs]")

    (ReadAtpEntities(
      dsSaleMovements,
      dsStockConsolidado,
      dfDatosFijos,
      dsOrderHeader,
      dsOrderLine,
      dsPartialOrdersDelivered,
      dsStoreCapacity,
      dsStoreType,
      dsMaraWithArticleTypes,
      dsMaterials,
      dsMaterialsArticle,
      dsCustomerReservations),
      finalStockConf,
      listControlEjecucion)
  }

  /*
   * Calculate the datos fijos dataset
   * @param readEntities all datasets we need to read to calculate datos fijos
   * @param stockConf Oms configuration
   * @param dayNAtMidnight Timestamp today date
   * @param previousDayToNAtMidnight Timestamp yesterday date
   *
   * @return dsFinal DataFrame with result of all joins (stock ATP)
   */
  def processCalculate(
                        readEntities: ReadAtpEntities,
                        stockConf: StockConf,
                        dayNAtMidnight: Timestamp,
                        previousDayToNAtMidnight: Timestamp,
                        listControlEjecucion: List[ControlEjecucionView],
                        appName: String,
                        updateUser: String)(implicit spark: SparkSession, conf: AppConfig): DataFrame = {

    implicit val debugOptions = conf.getDebugStockATPParameters getOrElse DebugParameters()
    val listCapacitiesPP = conf.getListCapacitiesPP
    val saleArticlesList = conf.getSaleArticlesList
    val boxOrPrepackArticleList = conf.getBoxOrPrepackArticleList
    val puchaseArticleWithUniqueSaleArticleList = conf.getPuchaseArticleWithUniqueSaleArticleList

    logDebug(s"[debugOptions: $debugOptions]")
    logDebug(s"[listCapacitiesPP: $listCapacitiesPP]")
    logDebug(s"[saleArticlesList: $saleArticlesList]")
    logDebug(s"[boxOrPrepackArticleList: $boxOrPrepackArticleList]")
    logDebug(s"[puchaseArticleWithUniqueSaleArticleList: $puchaseArticleWithUniqueSaleArticleList]")

    val lastUpdateExecutionControlTs: Option[Timestamp] = Dates.getDateRegistroEjecucion(listControlEjecucion, appName)
    logWarn(s"[lastUpdateExecutionControlTs: ${lastUpdateExecutionControlTs.getOrElse("lastUpdateExecutionControlTs not found")}]")

    val dsFilteredOrderLines: Dataset[OrderLineView] =
      joinOrderLinesById(readEntities.dsOrderHeader, readEntities.dsOrderLine)
    dsFilteredOrderLines.logDSDebug(nameOf(dsFilteredOrderLines))

    val dsStockToBeDeliveredWithLogisticVariableWithMara: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      readEntities.dsPartialOrdersDelivered.transform(
        getArticleAndStoreOfOrdersNotCanceled(dsFilteredOrderLines) andThen
          DebugUtils.logDebugTransform("dsActivePartialOrdersWithArticleAndStore") andThen
          getStockToBeDeliveredByArticleStoreAndDay andThen
          DebugUtils.logDebugTransform("dsStockToBeDeliveredGroupedByArticleStoreAndDay") andThen
          addInformationOfArticlesToBeDelivered(readEntities.dsMaraWithArticleTypes) andThen
          DebugUtils.logDebugTransform("dsStockToBeDeliveredWithLogisticVariableWithItsInformation")
      ).cache

    val dsStockToBeDeliveredExceptBoxAndPrepackWithMara: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      dsStockToBeDeliveredWithLogisticVariableWithMara
        .transform(
          filterSaleArticleOrPuchaseArticleWithUniqueSaleArticle
        )
    dsStockToBeDeliveredExceptBoxAndPrepackWithMara.logDSDebug(nameOf(dsStockToBeDeliveredExceptBoxAndPrepackWithMara))

    val dsBoxAndPrepackContent: Dataset[BoxAndPrepackInformation] =
      getBoxAndPrepackContentInformation(readEntities.dsMaterialsArticle, readEntities.dsMaterials)
    dsBoxAndPrepackContent.logDSDebug(nameOf(dsBoxAndPrepackContent))

    val dsStockToBeDeliveredOfBoxAndPrepack: Dataset[StockToBeDeliveredWithLogisticVariableWithMara] =
      dsStockToBeDeliveredWithLogisticVariableWithMara
        .transform(
          filterBoxPrepack andThen
            DebugUtils.logDebugTransform("dsStockToBeDeliveredOfBoxAndPrepack") andThen
            addBoxAndPrepackOrdersContent(dsBoxAndPrepackContent) andThen
            DebugUtils.logDebugTransform("dsStockToBeDeliveredOfBoxAndPrepackWithItsContent") andThen
            calculateAmountOfSalesArticlesIncludedInBoxOrPrepack
        )
    dsStockToBeDeliveredOfBoxAndPrepack.logDSDebug(nameOf(dsStockToBeDeliveredOfBoxAndPrepack))

    val dsWholeStockToBeDeliveredWithLogisticVariable = dsStockToBeDeliveredExceptBoxAndPrepackWithMara
      .transform(
        unionAllStockToBeDeliveredWithLogisticalVariableAndMara(dsStockToBeDeliveredOfBoxAndPrepack)
      )
    dsWholeStockToBeDeliveredWithLogisticVariable.logDSDebug(nameOf(dsWholeStockToBeDeliveredWithLogisticVariable))

    val dfStockToBeDelivered: DataFrame = dsWholeStockToBeDeliveredWithLogisticVariable
      .transform(
        adaptArticleToSaleArticle andThen
          DebugUtils.logDebugTransform("dsStockToBeDeliveredAdaptedToSaleArticles") andThen
          pivotAmountForAllProjectionDaysByArticleAndStore(
            previousDayToNAtMidnight,
            stockConf.numberOfDaysToProject + 1,
            StockToBeDelivered.ccTags.dayTag,
            StockToBeDelivered.ccTags.amountToBeDeliveredTag,
            StockToBeDelivered.ccTags.amountToBeDeliveredNTag,
            firstDayIndex = -1)
      )
    dsStockToBeDeliveredWithLogisticVariableWithMara.unpersist()
    dfStockToBeDelivered.logDSDebug(nameOf(dfStockToBeDelivered))

    val dfCustomerReservations: DataFrame = readEntities.dsCustomerReservations
      .transform(
        filterCustomerReservationsBetweenDays(dayNAtMidnight, Dates.sumDaysToTimestamp(dayNAtMidnight, stockConf.numberOfDaysToProject + 2)) andThen
          DebugUtils.logDebugTransform("dsFilterCustomerReservations") andThen
          nullAmountsToZeroAndDateAsStringInCustomerReservations andThen
          DebugUtils.logDebugTransform("dsCustomerReservationsWithNullAmountsToZeroAndDateAsString") andThen
          pivotAmountForAllProjectionDaysByArticleAndStore(
            dayNAtMidnight,
            stockConf.numberOfDaysToProject + 1,
            CustomerReservations.ccTags.reservationDateTag,
            CustomerReservations.ccTags.amountTag,
            CustomerReservations.ccTags.amountNTag,
            firstDayIndex = 0)
      )
    dfCustomerReservations.logDSDebug(nameOf(dfCustomerReservations))

    val dsPreCalculatedStockData: DataFrame = readEntities.dsSaleMovements
      .transform(
        aggregateSaleMovementsByArticleAndStore andThen
          DebugUtils.logDebugTransform("dsAggSaleMovements") andThen
          addConsolidatedStockToAggregatedSalesMovements(readEntities.dsStockConsolidado) andThen
          DebugUtils.logDebugTransform("dsAggSaleMovementsWithConsolidatedStock") andThen
          addFixedDataToConsolidatedStockAndMovements(readEntities.dsDatosFijos) andThen
          DebugUtils.logDebugTransform("dfAggSaleMovementsConsolidatedStockAndFixedData") andThen
          calculateNspAndNst(stockConf) andThen
          DebugUtils.logDebugTransform("dfAggSaleMovementsConsolidatedStockAndFixedDataWithCorrectedNspAndNstValues") andThen
          CommonSqlLayer.addDFAndFillNullWithZeros(dfStockToBeDelivered) andThen
          DebugUtils.logDebugTransform("dfAggSaleMovementsConsolidatedStockFixedDataAndStockToBeDelivered") andThen
          CommonSqlLayer.addDFAndFillNullWithZeros(dfCustomerReservations) andThen
          DebugUtils.logDebugTransform("dfAggSaleMovementsConsolidatedStockFixedDataAndStockToBeDeliveredAndCustomerReservations")
      )

    /*
     * Not all store have both dsStoreCapacity and dsStoreType.
     * That's why, it's mandatory to do full outer join between both entities. And later left join with final join.
     */
    val dsAggPP: Dataset[AggPP] =
      aggregationPP(readEntities.dsStoreCapacity, listCapacitiesPP)
    dsAggPP.logDSDebug(nameOf(dsAggPP))

    val dsJoinStoreTypeAndAggPP: Dataset[JoinStoreTypeAndAggPP] =
      joinStoreTypeAndAggPP(readEntities.dsStoreType, dsAggPP)
    dsJoinStoreTypeAndAggPP.logDSDebug(nameOf(dsJoinStoreTypeAndAggPP))

    lazy val updatedTimestamp: Timestamp = new Timestamp(System.currentTimeMillis())

    val dfStockATPN: DataFrame =
      addStoreTypeAndPreparationInformationToPrecalculatedStockData(
        dsPreCalculatedStockData,
        dsJoinStoreTypeAndAggPP,
        stockConf,
        updatedTimestamp,
        lastUpdateExecutionControlTs,
        updateUser)
        .transform(
          DebugUtils.logDebugTransform("dfBeforeCalculateStockATP") andThen
            calculateStockATP(stockConf, dayNAtMidnight))

    val dfStockATPNX: DataFrame =
      dfStockATPN
        .transform(
          DebugUtils.logDebugTransform("dfAfterCalculateStockATPN") andThen
            calculateStockATPNX(stockConf)
        )

    dfStockATPNX
      .transform(DebugUtils.logDebugAndBreakPlanningTransform(nameOf(dfStockATPNX)))

  }

  /*
   * Writing datos fijos in KUDU
   * @param dsResult dataset with the result of joining all tables
   */
  def writeDfResult(
                     dfStockAtpResult: DataFrame,
                     startProcessTs: Timestamp,
                     initWriteTs: Timestamp,
                     appName: String,
                     updateUser: String)(implicit spark: SparkSession, conf: AppConfig): Unit = {
    //Write to KUDU
    import com.training.bigdata.omnichannel.stockATP.common.entities.common_entities.common_implicits._
    import com.training.bigdata.omnichannel.stockATP.common.io.tables.ToDbTable.ops._
    import spark.implicits._

    implicit val kuduHost = conf.getKuduHost
    logDebug(s"[kuduHost: ${kuduHost}]")
    implicit val debugOptions = conf.getDebugStockATPParameters getOrElse DebugParameters()

    dfStockAtpResult.writeTo[StockATP, Kudu](_.mode(SaveMode.Overwrite))
    dfStockAtpResult.logDSDebugExplain(nameOf(dfStockAtpResult))
    logInfo(s"StockATP have been correctly write in KUDU")

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

    logInfo(s"Last execution of StockATP updated in KUDU")
  }


}
