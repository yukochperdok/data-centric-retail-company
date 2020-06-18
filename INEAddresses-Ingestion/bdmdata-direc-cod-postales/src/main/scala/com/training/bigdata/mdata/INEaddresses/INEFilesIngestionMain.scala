package com.training.bigdata.mdata.INEaddresses

import java.io.FileNotFoundException
import java.sql.Timestamp

import com.training.bigdata.mdata.INEaddresses.common.entities.ZipCodesStreetMap
import com.training.bigdata.mdata.INEaddresses.INEFilesIngestion.entities.{StreetStretches, StreetTypes}
import com.training.bigdata.mdata.INEaddresses.common.exception._
import com.training.bigdata.mdata.INEaddresses.common.util.log.FunctionalLogger
import com.training.bigdata.mdata.INEaddresses.common.util.io.Files
import com.training.bigdata.mdata.INEaddresses.common.util.{AppConfig, OperationsUtils}
import com.training.bigdata.mdata.INEaddresses.INEFilesIngestion.util.{Arguments, ArgumentsParser}
import com.training.bigdata.mdata.INEaddresses.INEFilesIngestion.util.ArgumentsParser._
import com.training.bigdata.mdata.INEaddresses.common.util.TablesUtils._
import com.training.bigdata.mdata.INEaddresses.common.util.OperationsUtils._
import com.training.bigdata.mdata.INEaddresses.common.util.LensAppConfig._
import com.training.bigdata.mdata.INEaddresses.common.util.io.Tables._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}


case class ReadEntities(
  streetStretchesRDD: RDD[String],
  streetTypesRDD: RDD[String],
  zipCodesStreetMapDF: DataFrame
)

case class EntitiesToPersist(
  streetStretchesRawDF: DataFrame,
  streetStretchesLastDF: DataFrame,
  streetTypesRawDF: DataFrame,
  streetTypesLastDF: DataFrame,
  cityStreetGuideDF: DataFrame
)

object INEFilesIngestionMain extends FunctionalLogger {

  val Logger: Logger = LoggerFactory.getLogger(getClass)

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
        logConfig(s"[getDebugZipCodesParameters: ${conf getDebugINEFilesIngestionParameters}]")

        val readFilesAndTables: ReadEntities = logSubProcess("readEntities") {
          readEntities(arguments.date, arguments.workflow)
        }

        val entitiesToPersist: EntitiesToPersist = logSubProcess("buildEntities") {
          buildEntities(readFilesAndTables, arguments.user, arguments.workflow)
        }

        logSubProcess("writeEntities") {
          writeEntities(entitiesToPersist, readFilesAndTables.zipCodesStreetMapDF, arguments.workflow)
        }

      }

    } catch {
       case ex: WrongFileException =>
        logAlert(s"${ex.getMessage}")
        logNonFunctionalMessage(ex.getStackTrace.mkString("\n"))
        System.exit(1)

       case ex: RowFormatException =>
         logAlert("Files could not be parsed correctly, so neither street stretches nor street types have been inserted")
         logNonFunctionalMessage(s"${ex.getMessage}")
         logNonFunctionalMessage(ex.getStackTrace.mkString("\n"))
         System.exit(1)

       case ex: FileNotFoundException =>
         logAlert(s"No files with correct name has been found, so no street stretches nor street types have been inserted")
         logNonFunctionalMessage(s"${ex.getMessage}")
         logNonFunctionalMessage(ex.getStackTrace.mkString("\n"))
         System.exit(1)

       case ex: Exception =>
         logAlert(s"An error has occured while processing street stretches and types from INE, therefore, they haven't been correctly updated")
         logNonFunctionalMessage(s"${ex.getMessage}")
         logNonFunctionalMessage(ex.getStackTrace.mkString("\n"))
         System.exit(1)
    }

  }

  def readEntities(filesDatePath: String, workflow: String)(implicit spark: SparkSession, conf: AppConfig): ReadEntities = {
    import com.training.bigdata.mdata.INEaddresses.common.util.io.Tables.Implementations.KuduDs

    val kuduHost = conf.getKuduHost
    logDebug(s"[kuduHost: $kuduHost]")
    val hdfsPath = conf.getHDFSPath
    logDebug(s"[hdfsPath: $hdfsPath]")
    val streetStretchesFileNamePattern = conf.getStreetStretchesFileNamePattern
    logDebug(s"[streetStretchesFileNamePattern: $streetStretchesFileNamePattern]")
    val streetTypesFileNamePattern = conf.getStreetTypesFileNamePattern
    logDebug(s"[streetTypesFileNamePattern: $streetTypesFileNamePattern]")
    val zipCodesStreetMapTable = conf.getZipCodesStreetMapTable
    logDebug(s"[zipCodesStreetMapTable: $zipCodesStreetMapTable]")

    val wholePath = s"$hdfsPath$filesDatePath"
    lazy val streetStretchesFileName = Files.getMostRecentFileOfPattern(wholePath, streetStretchesFileNamePattern)
    lazy val streetTypesFileName = Files.getMostRecentFileOfPattern(wholePath, streetTypesFileNamePattern)

    workflow match {

      case HISTORIFY_STREET_STRETCHES =>
        val streetStretchesDF = Files.readTextFileFromHDFS(wholePath, streetStretchesFileName)

        ReadEntities(
          streetStretchesDF,
          spark.sparkContext.emptyRDD[String],
          spark.emptyDataFrame
        )

      case HISTORIFY_STREET_TYPES =>
        val streetTypesDF = Files.readTextFileFromHDFS(wholePath, streetTypesFileName)

        ReadEntities(
          spark.sparkContext.emptyRDD[String],
          streetTypesDF,
          spark.emptyDataFrame
        )

      case NORMAL_EXECUTION =>

        if(Files.areFilesFromTheSameDate(streetStretchesFileName, streetStretchesFileNamePattern, streetTypesFileName, streetTypesFileNamePattern)) {

          val streetStretchesRDD: RDD[String] =
            Files.readTextFileFromHDFS(wholePath, streetStretchesFileName)

          val streetTypesRDD: RDD[String] =
            Files.readTextFileFromHDFS(wholePath, streetTypesFileName)

          val zipCodesStreetMapDF: DataFrame =
            zipCodesStreetMapTable
              .readFrom[Kudu](kuduHost)
              .get
              .cache

          ReadEntities(
            streetStretchesRDD,
            streetTypesRDD,
            zipCodesStreetMapDF
          )
        } else {
          throw new WrongFileException("Street-stretches file (TRAM-NAL) and Street-types file (VIAS-NAL) belong to different dates, so nothing has been updated")
        }

      case unknownWorkflow => throw new Exception(s"Entity to historify, or workflow '$unknownWorkflow' passed as an argument unknown")
    }

  }

  def buildEntities(
    readEntities: ReadEntities,
    user: String,
    workflow: String)(implicit spark: SparkSession, conf: AppConfig): EntitiesToPersist = {

    val streetStretchesLastDF: DataFrame = StreetStretches
      .parse(readEntities.streetStretchesRDD)
      .transform(addAuditFields(user, new Timestamp(System.currentTimeMillis())))
      .cache

    val streetStretchesRawDF = streetStretchesLastDF
      .transform(addPartitionFields(StreetStretches.INE_MODIFICATION_DATE))

    val streetTypesLastDF = StreetTypes
      .parse(readEntities.streetTypesRDD)
      .transform(addAuditFields(user, new Timestamp(System.currentTimeMillis())))
      .cache

    val streetTypesRawDF = streetTypesLastDF
      .transform(addPartitionFields(StreetTypes.INE_MODIFICATION_DATE))

    if(workflow == NORMAL_EXECUTION &&
      OperationsUtils.isStreetStretchesOlderThanMaxStreetMapInfo(
        streetStretchesLastDF,
        readEntities.zipCodesStreetMapDF,
        StreetStretches.INE_MODIFICATION_DATE)) {
      throw new WrongFileException("INE files dates are older than persisted data in zip_codes_street_map")
    }

    // Generating Zip Codes Street Map
    val cityStreetGuideDF: DataFrame = streetStretchesLastDF
      .transform(
        ZipCodesStreetMap.addStreetTypes(streetTypesLastDF) andThen
        ZipCodesStreetMap.toZipCodesStreetMap andThen
        ZipCodesStreetMap.filterStreetName)

    EntitiesToPersist(streetStretchesRawDF, streetStretchesLastDF, streetTypesRawDF, streetTypesLastDF, cityStreetGuideDF)
  }

  def writeEntities(entitiesToPersist: EntitiesToPersist,
                    persistedCityStreetMap: DataFrame,
                    workflow: String)(implicit spark: SparkSession, conf: AppConfig): Unit = {

    import com.training.bigdata.mdata.INEaddresses.common.util.io.Tables.Implementations.HiveDs

    implicit val kuduHost: String = conf.getKuduHost
    logDebug(s"[kuduHost: $kuduHost]")

    workflow match {

      case HISTORIFY_STREET_STRETCHES =>
        entitiesToPersist.streetStretchesRawDF.writeTo[Hive](conf.getStreetStretchesRawTable, kuduHost, numPartitions = conf.getStreetStretchesNumFilesHive)

      case HISTORIFY_STREET_TYPES =>
        entitiesToPersist.streetTypesRawDF.writeTo[Hive](conf.getStreetTypesRawTable, kuduHost, numPartitions = conf.getStreetTypesNumFilesHive)

      case NORMAL_EXECUTION =>
        entitiesToPersist.streetStretchesRawDF.writeTo[Hive](conf.getStreetStretchesRawTable, kuduHost, numPartitions = conf.getStreetStretchesNumFilesHive)
        entitiesToPersist.streetStretchesLastDF.writeTo[Hive](conf.getStreetStretchesLastTable, kuduHost, numPartitions = conf.getStreetStretchesNumFilesHive)

        entitiesToPersist.streetTypesRawDF.writeTo[Hive](conf.getStreetTypesRawTable, kuduHost, numPartitions = conf.getStreetTypesNumFilesHive)
        entitiesToPersist.streetTypesLastDF.writeTo[Hive](conf.getStreetTypesLastTable, kuduHost, numPartitions = conf.getStreetTypesNumFilesHive)

        overwriteInKuduWithCorrectAuditFields(
          entitiesToPersist.cityStreetGuideDF,
          persistedCityStreetMap,
          conf.getZipCodesStreetMapTable,
          ZipCodesStreetMap.pkFields)
    }

  }

}
