package com.training.bigdata.omnichannel.stockATP.calculateDatosFijos

import java.sql.Timestamp

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
object RunnerCalculateDatosFijos extends App with SparkLocalTest with DefaultConf with FunctionalLogger {

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
    .setDebugDatosFijosParameters(
      DebugParameters(false, false, true,
        if(activateFiltersDebug)
          Map(
            Debugger.ccTags.idArticle -> "1",
            Debugger.ccTags.idStore -> "11"
          )
        else Map.empty
      )
    )

  val startProcessTs: Timestamp = new Timestamp(System.currentTimeMillis)

  val user = "user"

  logInfo("Call getDataDateTs")
  val dataDateTs: Timestamp = DatosFijosMain.getDayNAtMidnightTs(optDataDate)
  logInfo("Call getDataDateTs finished correctly")

  val appName = "datosFijos"
  logInfo("Call readingInfo")
  val readEntities: ReadEntities = DatosFijosMain.readingInfo(dataDateTs)
  logInfo("Call readingInfo finished correctly")

  logInfo("Call processCalculate")
  val datosFijos = DatosFijosMain.processCalculate(readEntities, dataDateTs, user)(spark, appConfig)
  logInfo("Call processCalculate finished correctly")

  logInfo("Call writeDatosFijos")
  val initWriteTs: Timestamp = new Timestamp(System.currentTimeMillis)
  DatosFijosMain.writeDatosFijos(datosFijos,startProcessTs,initWriteTs,appName, user)(spark, appConfig)
  logInfo("Call writeDatosFijos finished correctly")

}
