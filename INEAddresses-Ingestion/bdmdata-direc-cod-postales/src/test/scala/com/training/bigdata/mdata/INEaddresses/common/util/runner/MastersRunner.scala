package com.training.bigdata.mdata.INEaddresses.common.util.runner

import java.io.FileNotFoundException

import com.training.bigdata.mdata.INEaddresses.MastersMain._
import com.training.bigdata.mdata.INEaddresses.{ReadEntities, ReadMasterEntities, MastersEntitiesToPersist}
import com.training.bigdata.mdata.INEaddresses.common.exception.RowFormatException
import com.training.bigdata.mdata.INEaddresses.common.util.log.FunctionalLogger
import com.training.bigdata.mdata.INEaddresses.common.util.{CommonParameters, DebugParameters, DefaultConf, SparkLocalTest}
import com.training.bigdata.mdata.INEaddresses.common.util.LensAppConfig._

/**
 * PRE-REQUIREMENT:
 *   1. A kudu master must be risen up in ipAddress' value-config
 */
object MastersRunner extends SparkLocalTest with DefaultConf with FunctionalLogger {

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      throw new Error(s"Runner needs kudu ip_address and debug-mode flag to work.")
    }

    val (ipAddress, activateFiltersDebug, hdfsPath) = (args.take(3).head, args.drop(1).head.toBoolean, args.take(3).last)
    logInfo(s"The given path is ${hdfsPath}")
    logInfo(s"The given ipAddress is ${ipAddress}")
    logInfo(s"The given activateFiltersDebug is ${activateFiltersDebug}")

    implicit val appConfig =
      defaultConfig
        .setCommonParameters(
          CommonParameters(
            "172.17.0.2:7051",
            "Europe/Paris"
          ))
        .setDebugINEFilesIngestionParameters(
          DebugParameters(false, false, true, Map.empty)
        )
        .setZipCodesTable("impala::mdata_user.zip_codes")
        .setTownsTable("impala::mdata_user.towns")
        .setCCAAAndRegionTable("impala::mdata_user.ccaa_regions")
        .setZipCodesStreetMapTable("impala::mdata_user.zip_codes_street_map")

    val appName = "MastersIngestion"
    val updateUser = "user_test"

    try {
      logProcess(appName) {

        val readMastersEntities: ReadMasterEntities = logSubProcess("readingInfo") {
          readEntities
        }

        val entitiesToPersist: MastersEntitiesToPersist = logSubProcess("processCalculate") {
          buildEntities(readMastersEntities, updateUser)
        }

        logSubProcess("writeResult") {
          writeEntities(entitiesToPersist, readMastersEntities)
        }
      }
    } catch {
      case ex: RowFormatException =>
        logAlert("File could not be parsed correctly, so no street stretches have been inserted")
        logNonFunctionalMessage(s"${ex.getMessage}")
        logNonFunctionalMessage(ex.getStackTrace.mkString("\n"))

      case ex: FileNotFoundException =>
        logAlert(s"No file with correct name has been found, so no street stretches have been inserted")
        logNonFunctionalMessage(s"${ex.getMessage}")
        logNonFunctionalMessage(ex.getStackTrace.mkString("\n"))

      case ex: Exception =>
        logAlert(s"An error has occured trying to process street stretches from INE, therefore, they haven't been correctly updated")
        logNonFunctionalMessage(s"${ex.getMessage}")
        logNonFunctionalMessage(ex.getStackTrace.mkString("\n"))
    }
  }

}
