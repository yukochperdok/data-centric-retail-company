package com.training.bigdata.omnichannel.customerOrderReservation.utils

import com.training.bigdata.omnichannel.customerOrderReservation.utils.tag.TagTest.UnitTestTag
import org.scalatest.{FlatSpecLike, Matchers}
import pureconfig.error.ConfigReaderException
import com.training.bigdata.omnichannel.customerOrderReservation.utils.LensAppConfig._

trait DefaultConf {
  final val defaultProjectCMDB =
    ProjectCMDB("app","mod","url","bitbucket")
  final val defaultProcess: List[Process] =
    Process("process1","des_process1")::Nil
  final val defaultTopic: List[Topic] =
    Topic("topicToConsume","des_topic1",1000000,"parquet")::Nil
  final val defaultParameters =
    Parameters(
      kuduHost = "kuduHost",
      kafkaServers = "localhost:9092",
      schemaRegistryURL = "localhost:8081",
      timezone = "timezone")

  final val defaultApplication =
    Application(
      defaultProjectCMDB,
      defaultProcess,
      defaultTopic,
      defaultParameters
    )

  final val defaultConfig =
    AppConfig(
      defaultApplication
    )
}

class AppConfigTest extends FlatSpecLike with Matchers with DefaultConf {

  final val APPCONFIG = PropertiesUtil.DEFAULT_APPLICATION_FILE_NAME
  final val APPCONFIG_LIST = "application_list.conf"
  final val APPCONFIG_ERRORS = "application_errors.conf"

  "The AppConfig" must "be retrieved correctly a default config" taggedAs (UnitTestTag) in {
    val appConfig = AppConfig(getClass.getClassLoader.getResource(APPCONFIG).getPath)

    appConfig shouldBe defaultConfig
    appConfig.getApplication shouldBe defaultApplication
    appConfig.getProjectCMDB shouldBe defaultProjectCMDB
    appConfig.getListProcess shouldBe defaultProcess
    appConfig.getListTopic shouldBe defaultTopic
    appConfig.getParameters shouldBe defaultParameters

  }

  it must "be retrieved correctly a list of process" taggedAs (UnitTestTag) in {
    val appConfig = AppConfig(getClass.getClassLoader.getResource(APPCONFIG_LIST).getPath)

    appConfig.getListProcess should contain theSameElementsAs(
      Process("process1","des_process1")::
      Process("process2","des_process2")::
      Process("process3","des_process3")::
      Process("process4","des_process4")::
      Nil)
  }

  it must "be retrieved correctly a list of topics" taggedAs (UnitTestTag) in {
    val appConfig = AppConfig(getClass.getClassLoader.getResource(APPCONFIG_LIST).getPath)

    appConfig.getListTopic should contain theSameElementsAs(
      Topic("topic1","des_topic1",1000000,"parquet")::
      Topic("topic2","des_topic2",1000000,"parquet")::
      Topic("topic3","des_topic3",1000000,"parquet")::
      Topic("topic4","des_topic4",1000000,"parquet")::
      Nil)
  }

  it must "fail if not all keys are found" taggedAs (UnitTestTag) in {
    val caught = intercept[ConfigReaderException[AppConfig]] {
      AppConfig(getClass.getClassLoader.getResource(APPCONFIG_ERRORS).getPath)
    }
    info(caught.getMessage)
    assert(caught.getMessage.indexOf("application.parameters") != -1)
  }

  it should "have mechanisms to set values" taggedAs (UnitTestTag) in {
    val appConfig = AppConfig(getClass.getClassLoader.getResource(APPCONFIG).getPath)

    val otherTimezone = "otherTimezone"
    val otherKuduHost = "otherKuduHost"
    val otherListTopic: List[Topic] = List.empty[Topic]
    val otherListProcess: List[Process] = Process("process1","des_process2") :: Nil
    val otherProjectCMDB = ProjectCMDB("app2","mod2","url2","bitbucket2")

    val otherAppConfig =
      defaultConfig
      .setTimezone(otherTimezone)
      .setKuduHost(otherKuduHost)
      .setListTopic(otherListTopic)
      .setListProcess(otherListProcess)
      .setProjectCMDB(otherProjectCMDB)


    appConfig
      .setTimezone(otherTimezone)
      .setKuduHost(otherKuduHost)
      .setListTopic(otherListTopic)
      .setListProcess(otherListProcess)
      .setProjectCMDB(otherProjectCMDB) should be (otherAppConfig)

  }
}
