package com.training.bigdata.omnichannel.stockATP.calculateStockATP.util

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.calculateStockATP.StockATPMain
import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.Debugger
import com.training.bigdata.omnichannel.stockATP.common.util.LensAppConfig._
import com.training.bigdata.omnichannel.stockATP.common.util._
import com.training.bigdata.omnichannel.stockATP.common.util.log.FunctionalLogger


/**
  * PRE-REQUIREMENT:
  *   1. A kudu master must be risen up in ipAddress' value-config
  *   2. debug-mode: true activate debug mode, otherwise false
  *   3. data date (optional)
  */
object RunnerStockATP extends App with SparkLocalTest with DefaultConf with FunctionalLogger{

  val (kuduHost: String, activateFiltersDebug: Boolean, optDataDate: Option[String]) = args.toList match {
    case Nil => ("172.17.0.2:7051", false, None)
    case x :: Nil => (x, false, None)
    case x :: y :: ys => (x, y.toBoolean, ys.headOption)
  }
  logInfo(s"The given kudu host is $kuduHost")
  logInfo(s"The given activateFiltersDebug is ${activateFiltersDebug}")
  logInfo(s"The given dataDate is ${optDataDate.getOrElse("NOT FOUND PARAMETER")}")

  implicit val appConfig =
    defaultConfig
      .setCommonParameters(
        CommonParameters(
          kuduHost = kuduHost,
          timezone = "Europe/Paris",
          defaultUnidadMedida = "EA",
          defaultNst = 100,
          defaultNsp = 100,
          defaultRotation = "B",
          defaultRegularSales = 0
        )
      )
      .setDebugStockATPParameters(
        DebugParameters(false, false, true,
          if(activateFiltersDebug)
            Map(
              Debugger.ccTags.idArticle -> "1",
              Debugger.ccTags.idStore -> "11"
            )
          else Map.empty
        )
      )

  val user = "user"
  val startProcessTs = new Timestamp(System.currentTimeMillis)
  logInfo("Call getDataDates")
  val (currentDateTs, previousDateTs) = StockATPMain.getDayNAndPreviousDayAtMidnight(optDataDate, startProcessTs)
  logInfo("Call getDataDates finished correctly")

  val appName = "stockATP"
  val (readEntities, stockConf, listControlExecution) =
    StockATPMain.readingInfo(currentDateTs, previousDateTs)(spark, appConfig)

  val dsResult =
    StockATPMain.processCalculate(readEntities, stockConf, currentDateTs, previousDateTs, listControlExecution, appName, user)(spark, appConfig)

  val initWriteTs = new Timestamp(System.currentTimeMillis())
  StockATPMain.writeDfResult(dsResult, startProcessTs, initWriteTs, appName, user)(spark, appConfig)

}
