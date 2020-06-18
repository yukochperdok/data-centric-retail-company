package com.training.bigdata.mdata.INEaddresses

import java.sql.Timestamp

import com.training.bigdata.mdata.INEaddresses.common.entities.interfaces.Audit
import com.training.bigdata.mdata.INEaddresses.common.util.AppConfig
import com.training.bigdata.mdata.INEaddresses.common.util.LensAppConfig._
import com.training.bigdata.mdata.INEaddresses.common.util.OperationsUtils.addAuditFields
import com.training.bigdata.mdata.INEaddresses.common.util.TablesUtils.overwriteInKuduWithCorrectAuditFields
import com.training.bigdata.mdata.INEaddresses.common.util.log.FunctionalLogger
import com.training.bigdata.mdata.INEaddresses.masters.entities.{Region, Town, ZipCode}
import com.training.bigdata.mdata.INEaddresses.masters.util.{Arguments, ArgumentsParser}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

case class ReadMasterEntities(
  zipCodesStreetMapDF: DataFrame,
  ccaaRegionsDF: DataFrame,
  oldZipCodesDF: DataFrame,
  oldTownsDF: DataFrame
)

case class MastersEntitiesToPersist(
  zipCodesDF: DataFrame,
  townsDF: DataFrame
)

object MastersMain extends FunctionalLogger {

  val Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val arguments: Arguments = ArgumentsParser.parse(args, Arguments()).getOrElse(
      throw new IllegalArgumentException("Invalid Configuration")
    )
    logConfig(s"Arguments passed to process is $arguments")

    implicit val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition.mode", "nonstrict" )
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.hive.convertMetastoreParquet", "false" )
      .config("spark.sql.parquet.compression.codec", "snappy" )
      .getOrCreate()

    val appName: String = spark.conf.get("spark.app.name")
    logInfo(s"Application name is: [$appName]")

    try {

      logProcess(appName) {
        implicit val conf: AppConfig = AppConfig.apply(s"${arguments.configFilePath}/application.conf")
        logInfo(s"Loaded configuration")
        logConfig(s"[getProject: ${conf getProjectCMDB}]")
        logConfig(s"[getListProcess: ${conf getListProcess}]")
        logConfig(s"[getCommonParameters: ${conf getCommonParameters}]")
        logConfig(s"[getMastersParameters: ${conf getMastersParameters}]")

        val readingEntities: ReadMasterEntities = logSubProcess("readEntities") {
          readEntities
        }
        logInfo(s"Entities have been read correctly from KUDU")

        val entitiesToPersist: MastersEntitiesToPersist = logSubProcess("buildEntities"){
          buildEntities(readingEntities, arguments.user)
        }
        logInfo(s"Entities have been built correctly")

        logSubProcess("writeResult") {
          writeEntities(entitiesToPersist, readingEntities)
        }
        logInfo(s"Entities have been written correctly")

      }

    } catch {
       case ex: Exception =>
         logAlert(s"There was an error in the process, therefore, they haven't been correctly updated")
         logNonFunctionalMessage(s"${ex.getMessage}")
         logNonFunctionalMessage(ex.getStackTrace.mkString("\n"))
         System.exit(1)
    }

  }

  def readEntities(implicit spark: SparkSession, conf: AppConfig): ReadMasterEntities = {

    import com.training.bigdata.mdata.INEaddresses.common.util.io.Tables.Implementations.KuduDs
    import com.training.bigdata.mdata.INEaddresses.common.util.io.Tables.{DataFramesReadOperations, Kudu}

    val zipCodesStreetMapDF: DataFrame = conf.getZipCodesStreetMapTable.readFrom[Kudu](conf.getKuduHost).get
    val ccaaRegionsDF: DataFrame = conf.getCCAAAndRegionTable.readFrom[Kudu](conf.getKuduHost).get
    val oldZipCodesDF: DataFrame = conf.getZipCodesTable.readFrom[Kudu](conf.getKuduHost).get
    val oldTownsDF: DataFrame = conf.getTownsTable.readFrom[Kudu](conf.getKuduHost).get

    ReadMasterEntities(
      zipCodesStreetMapDF,
      ccaaRegionsDF,
      oldZipCodesDF,
      oldTownsDF
    )

  }

  def buildEntities(
    readEntities: ReadMasterEntities,
    user: String)(implicit spark: SparkSession, conf: AppConfig): MastersEntitiesToPersist = {

    import com.training.bigdata.mdata.INEaddresses.common.entities.ZipCodesStreetMap
    import com.training.bigdata.mdata.INEaddresses.common.util.OperationsUtils.{groupByAndGetFirstValue, innerJoinAndTraceLogsForRest}
    import com.training.bigdata.mdata.INEaddresses.common.util.Constants.Messages._

    val joinPostalCodesStreetMapAndCCAARegionsDF: DataFrame = readEntities.zipCodesStreetMapDF
      .transform(
        innerJoinAndTraceLogsForRest(
          readEntities.ccaaRegionsDF,
          ZipCodesStreetMap.dbTags.province,
          Region.dbTags.region,
          PROVINCE_NOT_FOUND_IN_CCAA,
          Seq(
            Audit.tsUpdateDlk,
            Audit.userUpdateDlk,
            Audit.tsInsertDlk,
            Audit.userInsertDlk,
            Region.dbTags.countryCode,
            Region.dbTags.language
          )
        )
      )
    joinPostalCodesStreetMapAndCCAARegionsDF.cache

    val zipCodesDF: DataFrame = joinPostalCodesStreetMapAndCCAARegionsDF
      .transform(
        groupByAndGetFirstValue(
          Seq(
            ZipCode.dbTags.zipCode,
            ZipCode.dbTags.townCode),
          Seq(
            ZipCode.dbTags.countryCode,
            ZipCode.dbTags.region,
            ZipCode.dbTags.ccaaCode,
            ZipCode.dbTags.modificationDateINE
          )
        )
      )
      .transform(addAuditFields(user, new Timestamp(System.currentTimeMillis())))

    val townsDF: DataFrame = joinPostalCodesStreetMapAndCCAARegionsDF
      .transform(
        groupByAndGetFirstValue(
          Seq(Town.dbTags.townCode),
          Seq(
            Town.dbTags.countryCode,
            Town.dbTags.language,
            Town.dbTags.townName,
            Town.dbTags.region,
            Town.dbTags.ccaaCode,
            Town.dbTags.modificationDateINE
          )
        )
      )
      .transform(addAuditFields(user, new Timestamp(System.currentTimeMillis())))

    MastersEntitiesToPersist(zipCodesDF,townsDF)

  }

  def writeEntities(entitiesToPersist: MastersEntitiesToPersist,
                    readEntities: ReadMasterEntities)(implicit spark: SparkSession, conf: AppConfig): Unit = {

    implicit val kuduHost: String = conf.getKuduHost
    logDebug(s"[kuduHost: $kuduHost]")

    overwriteInKuduWithCorrectAuditFields(
      entitiesToPersist.zipCodesDF,
      readEntities.oldZipCodesDF,
      conf.getZipCodesTable,
      ZipCode.pkFields
    )

    overwriteInKuduWithCorrectAuditFields(
      entitiesToPersist.townsDF,
      readEntities.oldTownsDF,
      conf.getTownsTable,
      Town.pkFields
    )

  }

}
