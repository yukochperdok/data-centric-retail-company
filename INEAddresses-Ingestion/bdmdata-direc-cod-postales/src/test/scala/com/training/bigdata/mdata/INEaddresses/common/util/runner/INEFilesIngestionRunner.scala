package com.training.bigdata.mdata.INEaddresses.common.util.runner

import java.io.FileNotFoundException

import com.training.bigdata.mdata.INEaddresses.INEFilesIngestion.util.ArgumentsParser
import com.training.bigdata.mdata.INEaddresses.INEFilesIngestionMain._
import com.training.bigdata.mdata.INEaddresses.{EntitiesToPersist, ReadEntities}
import com.training.bigdata.mdata.INEaddresses.common.exception._
import com.training.bigdata.mdata.INEaddresses.common.util.log.FunctionalLogger
import com.training.bigdata.mdata.INEaddresses.common.util.{CommonParameters, DebugParameters, DefaultConf, SparkLocalTest}
import com.training.bigdata.mdata.INEaddresses.common.util.LensAppConfig._

/**
  * PRE-REQUIREMENT:
  *   1. A kudu master must be risen up in ipAddress' value-config
  */
object INEFilesIngestionRunner extends SparkLocalTest with DefaultConf with FunctionalLogger {

  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      throw new Error(s"Runner needs kudu ip_address and debug-mode flag to work.")
    }

    val (ipAddress, activateFiltersDebug, hdfsPath, workflow) =
      (args.take(4).head, args.drop(1).head.toBoolean, args.drop(2).head, args.drop(3).headOption.getOrElse(ArgumentsParser.NORMAL_EXECUTION))
    logInfo(s"The given HDFS path is ${hdfsPath}")
    logInfo(s"The given ipAddress is ${ipAddress}")
    logInfo(s"The given activateFiltersDebug is ${activateFiltersDebug}")
    logInfo(s"The given workflow is ${workflow}")

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
        .setStreetStretchesFileNamePattern("TRAM-NAL.F[0-9]{6}")
        .setHDFSPath("")
        .setStreetStretchesLastTable("mdata_last.national_street_stretches")
        .setStreetStretchesRawTable("mdata_raw.national_street_stretches")
        .setZipCodesStreetMapTable("impala::mdata_user.zip_codes_street_map")
        .setZipCodesTable("impala::mdata_user.zip_codes")
        .setTownsTable("impala::mdata_user.towns")
        .setCCAAAndRegionTable("impala::mdata_user.ccaa_regions")

    val appName = "INEFilesIngestion"
    val updateUser = "user_test"

    try {
      logProcess(appName) {

        val readFilesAndTables: ReadEntities = logSubProcess("readEntities") {
          readEntities(hdfsPath, workflow)
        }

        val entitiesToPersist: EntitiesToPersist = logSubProcess("buildEntities") {
          buildEntities(readFilesAndTables, updateUser, workflow)
        }

        logSubProcess("writeEntities") {
          writeEntities(entitiesToPersist, readFilesAndTables.zipCodesStreetMapDF, workflow)
        }
      }
    } catch {
      case ex: WrongFileException =>
        logAlert(s"${ex.getMessage}")
        logNonFunctionalMessage(s"${ex.getMessage}")
        logNonFunctionalMessage(ex.getStackTrace.mkString("\n"))

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
