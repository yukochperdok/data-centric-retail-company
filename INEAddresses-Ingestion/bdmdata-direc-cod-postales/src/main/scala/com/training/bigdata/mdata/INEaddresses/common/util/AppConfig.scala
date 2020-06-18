package com.training.bigdata.mdata.INEaddresses.common.util

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import pureconfig.{CamelCase, ConfigFieldMapping, ProductHint, loadConfigOrThrow}


object AppConfig {
  implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  def fromConfig(conf: Config = ConfigFactory.load()): AppConfig = loadConfigOrThrow[AppConfig](conf)

  def apply(path: String): AppConfig = fromConfig(ConfigFactory.parseFile(new File(path)))
}

case class AppConfig(application: Application)

case class Application(cmdb: ProjectCMDB,
                       process: List[Process],
                       parameters: Parameters)

case class ProjectCMDB(CMDBapp: String,
                       CMDBMod: String,
                       Confluence: String,
                       BitBucket: String)

case class Process(name: String,
                   description: String)

case class Parameters(common: CommonParameters,
                      INEFilesIngestion: INEFilesIngestionParameters,
                      masters: MastersParameters)

case class CommonParameters(kuduHost: String,
                            timezone: String = "Europe/Paris")

case class DebugParameters(activateExplain: Boolean = false,
                           activateDebugDataset: Boolean = false,
                           activateShowDataset: Boolean = false,
                           filters: Map[String, String] = Map.empty[String, String])

case class INEFilesIngestionParameters(streetStretchesFileNamePattern: String,
                                       streetTypesFileNamePattern: String,
                                       hdfsPath: String,
                                       streetStretchesRawTable: String,
                                       streetStretchesLastTable: String,
                                       streetStretchesNumFilesHive: Int,
                                       streetTypesRawTable: String,
                                       streetTypesLastTable: String,
                                       streetTypesNumFilesHive: Int,
                                       debug: Option[DebugParameters] = Option(DebugParameters()))

case class MastersParameters(zipCodesStreetMapTable: String,
                             ccaaAndRegionTable: String,
                             zipCodesTable: String,
                             townsTable: String)

import monocle.Lens
import monocle.macros.GenLens

object LensAppConfig {
  val applicationLens: Lens[AppConfig, Application] = GenLens[AppConfig](_.application)
  val projectLens: Lens[AppConfig, ProjectCMDB] = GenLens[AppConfig](_.application.cmdb)
  val processLens: Lens[AppConfig, List[Process]] = GenLens[AppConfig](_.application.process)
  val parametersLens: Lens[AppConfig, Parameters] = GenLens[AppConfig](_.application.parameters)
  val commonParametersLens: Lens[AppConfig, CommonParameters] = GenLens[AppConfig](_.application.parameters.common)
  val INEFilesIngestionParametersLens: Lens[AppConfig, INEFilesIngestionParameters] = GenLens[AppConfig](_.application.parameters.INEFilesIngestion)
  val mastersParametersLens: Lens[AppConfig, MastersParameters] = GenLens[AppConfig](_.application.parameters.masters)
  val debugINESFilesIngestionParametersLens: Lens[AppConfig, Option[DebugParameters]] =
    GenLens[AppConfig](_.application.parameters.INEFilesIngestion.debug)

  val kuduHostLens: Lens[CommonParameters, String] = GenLens[CommonParameters](_.kuduHost)
  val kuduHostComposeLens: Lens[AppConfig, String] = commonParametersLens composeLens kuduHostLens

  val timeZoneLens: Lens[CommonParameters, String] = GenLens[CommonParameters](_.timezone)
  val timeZoneComposeLens: Lens[AppConfig, String] = commonParametersLens composeLens timeZoneLens

  val streetStretchesFileNamePatternLens: Lens[INEFilesIngestionParameters, String] = GenLens[INEFilesIngestionParameters](_.streetStretchesFileNamePattern)
  val streetStretchesFileNamePatternComposeLens: Lens[AppConfig, String] = INEFilesIngestionParametersLens composeLens streetStretchesFileNamePatternLens

  val streetTypesFileNamePatternLens: Lens[INEFilesIngestionParameters, String] = GenLens[INEFilesIngestionParameters](_.streetTypesFileNamePattern)
  val streetTypesFileNamePatternComposeLens: Lens[AppConfig, String] = INEFilesIngestionParametersLens composeLens streetTypesFileNamePatternLens

  val hdfsPathLens: Lens[INEFilesIngestionParameters, String] = GenLens[INEFilesIngestionParameters](_.hdfsPath)
  val hdfsPathComposeLens: Lens[AppConfig, String] = INEFilesIngestionParametersLens composeLens hdfsPathLens

  val streetStretchesRawTableLens: Lens[INEFilesIngestionParameters, String] = GenLens[INEFilesIngestionParameters](_.streetStretchesRawTable)
  val streetStretchesRawTableComposeLens: Lens[AppConfig, String] = INEFilesIngestionParametersLens composeLens streetStretchesRawTableLens

  val streetStretchesLastTableLens: Lens[INEFilesIngestionParameters, String] = GenLens[INEFilesIngestionParameters](_.streetStretchesLastTable)
  val streetStretchesLastTableComposeLens: Lens[AppConfig, String] = INEFilesIngestionParametersLens composeLens streetStretchesLastTableLens

  val streetStretchesNumFilesHiveLens: Lens[INEFilesIngestionParameters, Int] = GenLens[INEFilesIngestionParameters](_.streetStretchesNumFilesHive)
  val streetStretchesNumFilesHiveComposeLens: Lens[AppConfig, Int] = INEFilesIngestionParametersLens composeLens streetStretchesNumFilesHiveLens

  val streetTypesRawTableLens: Lens[INEFilesIngestionParameters, String] = GenLens[INEFilesIngestionParameters](_.streetTypesRawTable)
  val streetTypesRawTableComposeLens: Lens[AppConfig, String] = INEFilesIngestionParametersLens composeLens streetTypesRawTableLens

  val streetTypesLastTableLens: Lens[INEFilesIngestionParameters, String] = GenLens[INEFilesIngestionParameters](_.streetTypesLastTable)
  val streetTypesLastTableComposeLens: Lens[AppConfig, String] = INEFilesIngestionParametersLens composeLens streetTypesLastTableLens

  val streetTypesNumFilesHiveLens: Lens[INEFilesIngestionParameters, Int] = GenLens[INEFilesIngestionParameters](_.streetTypesNumFilesHive)
  val streetTypesNumFilesHiveComposeLens: Lens[AppConfig, Int] = INEFilesIngestionParametersLens composeLens streetTypesNumFilesHiveLens

  val zipCodesStreetMapTableLens: Lens[MastersParameters, String] = GenLens[MastersParameters](_.zipCodesStreetMapTable)
  val zipCodesStreetMapTableComposeLens: Lens[AppConfig, String] = mastersParametersLens composeLens zipCodesStreetMapTableLens

  val ccaaAndRegionTableLens: Lens[MastersParameters, String] = GenLens[MastersParameters](_.ccaaAndRegionTable)
  val ccaaAndRegionTableComposeLens: Lens[AppConfig, String] = mastersParametersLens composeLens ccaaAndRegionTableLens

  val zipCodesTableLens: Lens[MastersParameters, String] = GenLens[MastersParameters](_.zipCodesTable)
  val zipCodesTableComposeLens: Lens[AppConfig, String] = mastersParametersLens composeLens zipCodesTableLens

  val townsTableLens: Lens[MastersParameters, String] = GenLens[MastersParameters](_.townsTable)
  val townsTableComposeLens: Lens[AppConfig, String] = mastersParametersLens composeLens townsTableLens

  implicit class AppConfigToValues(conf: AppConfig) {
    def getApplication: Application = {
      applicationLens.get(conf)
    }

    def setApplication(application: Application): AppConfig = {
      applicationLens.set(application)(conf)
    }

    def getProjectCMDB: ProjectCMDB = {
      projectLens.get(conf)
    }

    def setProjectCMDB(project: ProjectCMDB): AppConfig = {
      projectLens.set(project)(conf)
    }

    def getListProcess: List[Process] = {
      processLens.get(conf)
    }

    def setListProcess(listProcces: List[Process]): AppConfig = {
      processLens.set(listProcces)(conf)
    }

    def getParameters: Parameters = {
      parametersLens.get(conf)
    }

    def setParameters(parameters: Parameters): AppConfig = {
      parametersLens.set(parameters)(conf)
    }

    def getCommonParameters: CommonParameters = {
      commonParametersLens.get(conf)
    }

    def setCommonParameters(commonParameters: CommonParameters): AppConfig = {
      commonParametersLens.set(commonParameters)(conf)
    }

    def getINEFilesIngestionParameters: INEFilesIngestionParameters = {
      INEFilesIngestionParametersLens.get(conf)
    }

    def setINEFilesIngestionParameters(INEFilesIngestionParameters: INEFilesIngestionParameters): AppConfig = {
      INEFilesIngestionParametersLens.set(INEFilesIngestionParameters)(conf)
    }

    def getDebugINEFilesIngestionParameters: Option[DebugParameters] = {
      debugINESFilesIngestionParametersLens.get(conf)
    }

    def setDebugINEFilesIngestionParameters(debugINEFilesIngestionParameters: DebugParameters): AppConfig = {
      debugINESFilesIngestionParametersLens.set(Option(debugINEFilesIngestionParameters))(conf)
    }

    def getMastersParameters: MastersParameters = {
      mastersParametersLens.get(conf)
    }

    def setMastersParameters(mastersParameters: MastersParameters): AppConfig = {
      mastersParametersLens.set(mastersParameters)(conf)
    }

    def getStreetStretchesFileNamePattern: String = {
      streetStretchesFileNamePatternComposeLens.get(conf)
    }

    def setStreetStretchesFileNamePattern(streetStretchesFileNamePattern: String): AppConfig = {
      streetStretchesFileNamePatternComposeLens.set(streetStretchesFileNamePattern)(conf)
    }

    def getStreetTypesFileNamePattern: String = {
      streetTypesFileNamePatternComposeLens.get(conf)
    }

    def setStreetTypesFileNamePattern(streetTypesFileNamePattern: String): AppConfig = {
      streetTypesFileNamePatternComposeLens.set(streetTypesFileNamePattern)(conf)
    }

    def getHDFSPath: String = {
      hdfsPathComposeLens.get(conf)
    }

    def setHDFSPath(fileNamePattern: String): AppConfig = {
      hdfsPathComposeLens.set(fileNamePattern)(conf)
    }

    def getStreetStretchesRawTable: String = {
      streetStretchesRawTableComposeLens.get(conf)
    }

    def setStreetStretchesRawTable(streetStretchesRawTable: String): AppConfig = {
      streetStretchesRawTableComposeLens.set(streetStretchesRawTable)(conf)
    }

    def getStreetStretchesLastTable: String = {
      streetStretchesLastTableComposeLens.get(conf)
    }

    def setStreetStretchesLastTable(streetStretchesLastTable: String): AppConfig = {
      streetStretchesLastTableComposeLens.set(streetStretchesLastTable)(conf)
    }

    def getStreetStretchesNumFilesHive: Int = {
      streetStretchesNumFilesHiveComposeLens.get(conf)
    }

    def setStreetStretchesNumFilesHive(streetStretchesNumFilesHive: Int): AppConfig = {
      streetStretchesNumFilesHiveComposeLens.set(streetStretchesNumFilesHive)(conf)
    }

    def getStreetTypesRawTable: String = {
      streetTypesRawTableComposeLens.get(conf)
    }

    def setStreetTypesRawTable(streetTypesRawTable: String): AppConfig = {
      streetTypesRawTableComposeLens.set(streetTypesRawTable)(conf)
    }

    def getStreetTypesLastTable: String = {
      streetTypesLastTableComposeLens.get(conf)
    }

    def setStreetTypesLastTable(streetTypesLastTable: String): AppConfig = {
      streetTypesLastTableComposeLens.set(streetTypesLastTable)(conf)
    }

    def getStreetTypesNumFilesHive: Int = {
      streetTypesNumFilesHiveComposeLens.get(conf)
    }

    def setStreetTypesNumFilesHive(streetTypesNumFilesHive: Int): AppConfig = {
      streetTypesNumFilesHiveComposeLens.set(streetTypesNumFilesHive)(conf)
    }

    def getZipCodesStreetMapTable: String = {
      zipCodesStreetMapTableComposeLens.get(conf)
    }

    def setZipCodesStreetMapTable(zipCodesStreetMapTable: String): AppConfig = {
      zipCodesStreetMapTableComposeLens.set(zipCodesStreetMapTable)(conf)
    }

    def getCCAAAndRegionTable: String = {
      ccaaAndRegionTableComposeLens.get(conf)
    }

    def setCCAAAndRegionTable(ccaaAndRegionTable: String): AppConfig = {
      ccaaAndRegionTableComposeLens.set(ccaaAndRegionTable)(conf)
    }

    def getZipCodesTable: String = {
      zipCodesTableComposeLens.get(conf)
    }

    def setZipCodesTable(zipCodesTable: String): AppConfig = {
      zipCodesTableComposeLens.set(zipCodesTable)(conf)
    }

    def getTownsTable: String = {
      townsTableComposeLens.get(conf)
    }

    def setTownsTable(townsTable: String): AppConfig = {
      townsTableComposeLens.set(townsTable)(conf)
    }

    def getKuduHost: String = {
      kuduHostComposeLens.get(conf)
    }

    def setKuduHost(kuduHost: String): AppConfig = {
      kuduHostComposeLens.set(kuduHost)(conf)
    }

    def getTimezone: String = {
      timeZoneComposeLens.get(conf)
    }

    def setTimezone(timezone: String): AppConfig = {
      timeZoneComposeLens.set(timezone)(conf)
    }

  }

}