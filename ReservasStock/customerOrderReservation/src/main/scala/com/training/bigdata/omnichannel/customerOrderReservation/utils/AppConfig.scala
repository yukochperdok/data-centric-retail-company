package com.training.bigdata.omnichannel.customerOrderReservation.utils

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import pureconfig.{CamelCase, ConfigFieldMapping, ProductHint, loadConfigOrThrow}


object AppConfig {
  implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
  def fromConfig(conf:Config = ConfigFactory.load()):AppConfig = loadConfigOrThrow[AppConfig](conf)
  def apply(path:String): AppConfig = fromConfig(ConfigFactory.parseFile(new File(path)))
}

case class AppConfig(application: Application)

case class Application(
  cmdb: ProjectCMDB,
  process: List[Process],
  topic: List[Topic],
  parameters: Parameters
)
case class ProjectCMDB(
  CMDBapp: String,
  CMDBMod: String,
  Confluence: String,
  BitBucket: String
)
case class Process(
  name: String,
  description: String
)
case class Topic(
  name: String,
  description: String,
  max_message_bytes: Int,
  format: String
)
case class Parameters(
  kuduHost: String,
  kafkaServers: String,
  schemaRegistryURL: String,
  timezone: String = "Europe/Madrid",
  isKafkaSecured: Boolean = true
)


import monocle.Lens
import monocle.macros.GenLens

object LensAppConfig{
  val applicationLens: Lens[AppConfig, Application] = GenLens[AppConfig](_.application)
  val projectLens: Lens[AppConfig, ProjectCMDB] = GenLens[AppConfig](_.application.cmdb)
  val processLens: Lens[AppConfig, List[Process]] = GenLens[AppConfig](_.application.process)
  val topicLens: Lens[AppConfig, List[Topic]] = GenLens[AppConfig](_.application.topic)
  val parametersLens: Lens[AppConfig, Parameters] = GenLens[AppConfig](_.application.parameters)

  val kuduHostLens: Lens[Parameters, String] = GenLens[Parameters](_.kuduHost)
  val kuduHostComposeLens: Lens[AppConfig, String] = parametersLens composeLens kuduHostLens

  val kafkaServersLens: Lens[Parameters, String] = GenLens[Parameters](_.kafkaServers)
  val kafkaServersComposeLens: Lens[AppConfig, String] = parametersLens composeLens kafkaServersLens

  val schemaRegistryURLLens: Lens[Parameters, String] = GenLens[Parameters](_.schemaRegistryURL)
  val schemaRegistryURLComposeLens: Lens[AppConfig, String] = parametersLens composeLens schemaRegistryURLLens

  val timeZoneLens: Lens[Parameters, String] = GenLens[Parameters](_.timezone)
  val timeZoneComposeLens: Lens[AppConfig, String] = parametersLens composeLens timeZoneLens

  val securedKafkaLens: Lens[Parameters, Boolean] = GenLens[Parameters](_.isKafkaSecured)
  val securedKafkaComposeLens: Lens[AppConfig, Boolean] = parametersLens composeLens securedKafkaLens

  implicit class AppConfigToValues (conf:AppConfig) {
    def getApplication: Application = {
      applicationLens.get(conf)
    }

    def setApplication (application: Application): AppConfig = {
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

    def setListProcess(listProcess: List[Process]): AppConfig = {
      processLens.set(listProcess)(conf)
    }

    def getListTopic: List[Topic] = {
      topicLens.get(conf)
    }

    def setListTopic(listTopic: List[Topic]): AppConfig = {
      topicLens.set(listTopic)(conf)
    }

    def getParameters: Parameters = {
      parametersLens.get(conf)
    }

    def setParameters(parameters: Parameters): AppConfig = {
      parametersLens.set(parameters)(conf)
    }

    def getKuduHost: String = {
      kuduHostComposeLens.get(conf)
    }

    def setKuduHost(kuduHost:String): AppConfig = {
      kuduHostComposeLens.set(kuduHost)(conf)
    }

    def getKafkaServers: String = {
      kafkaServersComposeLens.get(conf)
    }

    def setKafkaServers(kafkaServers: String): AppConfig = {
      kafkaServersComposeLens.set(kafkaServers)(conf)
    }

    def getSchemaRegistryURL: String = {
      schemaRegistryURLComposeLens.get(conf)
    }

    def setSchemaRegistryURL(schemaRegistryURL: String): AppConfig = {
      schemaRegistryURLComposeLens.set(schemaRegistryURL)(conf)
    }

    def getTimezone: String = {
      timeZoneComposeLens.get(conf)
    }

    def setTimezone(timezone: String): AppConfig = {
      timeZoneComposeLens.set(timezone)(conf)
    }

    def isKafkaSecured: Boolean = {
      securedKafkaComposeLens.get(conf)
    }

    def setKafkaSecured(isKafkaSecured: Boolean): AppConfig = {
      securedKafkaComposeLens.set(isKafkaSecured)(conf)
    }
  }
}
