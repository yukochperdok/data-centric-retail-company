package com.training.bigdata.mdata.INEaddresses.common.util

import com.training.bigdata.mdata.INEaddresses.common.util.LensAppConfig._
import org.scalatest.{FlatSpecLike, Matchers}
import pureconfig.error.ConfigReaderException

trait DefaultConf {
  final val defaultProjectCMDB =
    ProjectCMDB("app", "mod", "url", "bitbucket")
  final val defaultProcess: List[Process] =
    Process("process1", "des_process1") :: Nil
  final val defaultCommonParam =
    CommonParameters(
      kuduHost = "kuduHost",
      timezone = "Europe/Paris"
    )

  final val defaultDebug = Option(DebugParameters())
  final val defaultStreetStretchesFilePattern = "pattern"
  final val defaultStreetTypesFilePattern = "pattern2"
  final val defaultHdfsPath = "/hdfs/path"
  final val defaultStreetStretchesRawTable = "streetStretchesRawTable.table"
  final val defaultStreetStretchesLastTable = "streetStretchesLastTable.table"
  final val defaultStreetStretchesNumFilesHive = 1
  final val defaultStreetTypesRawTable = "streetTypesRawTable.table"
  final val defaultStreetTypesLastTable = "streetTypesLastTable.table"
  final val defaultStreetTypesNumFilesHive = 2
  final val defaultZipCodesStreetMapTable = "zipCodesStreetMapTable.table"
  final val defaultCCAAAndRegionTable = "ccaaAndRegionTable.table"
  final val defaultZipCodesTable = "zipCodesTable.table"
  final val defaultTownsTable = "townsTable.table"
  final val defaultZipCodesParam =
    INEFilesIngestionParameters(
      defaultStreetStretchesFilePattern,
      defaultStreetTypesFilePattern,
      defaultHdfsPath,
      defaultStreetStretchesRawTable,
      defaultStreetStretchesLastTable,
      defaultStreetStretchesNumFilesHive,
      defaultStreetTypesRawTable,
      defaultStreetTypesLastTable,
      defaultStreetTypesNumFilesHive,
      defaultDebug)

  final val mastersParam =
    MastersParameters(zipCodesStreetMapTable = defaultZipCodesStreetMapTable,
                      ccaaAndRegionTable = defaultCCAAAndRegionTable,
                      zipCodesTable = defaultZipCodesTable,
                      townsTable = defaultTownsTable)

  final val defaultParameters =
    Parameters(
      defaultCommonParam, defaultZipCodesParam, mastersParam
    )

  final val defaultApplication =
    Application(
      defaultProjectCMDB,
      defaultProcess,
      defaultParameters
    )

  final val defaultConfig =
    AppConfig(
      defaultApplication
    )
}

class AppConfigTest extends FlatSpecLike with Matchers with DefaultConf {

  final val APPCONFIG = "application.conf"
  final val APPCONFIG_LIST = "application_list.conf"
  final val APPCONFIG_ERRORS = "application_errors.conf"
  final val APPCONFIG_DEBUG = "application_with_debug.conf"

  "The AppConfig" must "retrieve correctly a default config" in {
    val appConfig = AppConfig(getClass.getClassLoader.getResource(APPCONFIG).getPath)

    appConfig shouldBe defaultConfig
    appConfig.getApplication shouldBe defaultApplication
    appConfig.getProjectCMDB shouldBe defaultProjectCMDB
    appConfig.getListProcess shouldBe defaultProcess
    appConfig.getParameters shouldBe defaultParameters
    appConfig.getCommonParameters shouldBe defaultCommonParam
    appConfig.getStreetStretchesFileNamePattern shouldBe defaultStreetStretchesFilePattern
    appConfig.getStreetTypesFileNamePattern shouldBe defaultStreetTypesFilePattern
    appConfig.getHDFSPath shouldBe defaultHdfsPath
    appConfig.getStreetStretchesRawTable shouldBe defaultStreetStretchesRawTable
    appConfig.getStreetStretchesLastTable shouldBe defaultStreetStretchesLastTable
    appConfig.getStreetStretchesNumFilesHive shouldBe defaultStreetStretchesNumFilesHive
    appConfig.getStreetTypesRawTable shouldBe defaultStreetTypesRawTable
    appConfig.getStreetTypesLastTable shouldBe defaultStreetTypesLastTable
    appConfig.getStreetTypesNumFilesHive shouldBe defaultStreetTypesNumFilesHive
    appConfig.getZipCodesStreetMapTable shouldBe defaultZipCodesStreetMapTable
    appConfig.getCCAAAndRegionTable shouldBe defaultCCAAAndRegionTable
    appConfig.getZipCodesTable shouldBe defaultZipCodesTable
    appConfig.getTownsTable shouldBe defaultTownsTable
  }

  it must "retrieve correctly a list of process" in {
    val appConfig = AppConfig(getClass.getClassLoader.getResource(APPCONFIG_LIST).getPath)

    appConfig.getListProcess should contain theSameElementsAs (
        Process("process1", "des_process1") ::
        Process("process2", "des_process2") ::
        Process("process3", "des_process3") ::
        Process("process4", "des_process4") ::
        Nil)
  }

  it must "retrieve correctly a debug options"  in {
    val appConfig = AppConfig(getClass.getClassLoader.getResource(APPCONFIG_DEBUG).getPath)
    val filters1 = Map("idArticle"->"1","idStore"->"2")
    appConfig.getDebugINEFilesIngestionParameters should be (Some(DebugParameters(true,true,true,filters1)))
  }

  it must "fail if not all keys are found" in {
    val caught = intercept[ConfigReaderException[AppConfig]] {
      AppConfig(getClass.getClassLoader.getResource(APPCONFIG_ERRORS).getPath)
    }
    info(caught.getMessage)
    println(caught.getMessage)
    assert(caught.getMessage.indexOf("application.parameters.common") != -1)
  }

  it should "have mechanisms to set values" in {

    val appConfig = AppConfig(getClass.getClassLoader.getResource(APPCONFIG).getPath)

    val otherTimezone = "Europe/London"
    val otherKuduHost = "otherKuduHost"
    val otherListProcess: List[Process] = Process("process1", "des_process2") :: Nil
    val otherProjectCMDB = ProjectCMDB("app2", "mod2", "url2", "bitbucket2")

    val otherAppConfig =
      defaultConfig
        .setTimezone(otherTimezone)
        .setKuduHost(otherKuduHost)
        .setListProcess(otherListProcess)
        .setProjectCMDB(otherProjectCMDB)

    appConfig
      .setTimezone(otherTimezone)
      .setKuduHost(otherKuduHost)
      .setListProcess(otherListProcess)
      .setProjectCMDB(otherProjectCMDB) should be(otherAppConfig)
  }
}
