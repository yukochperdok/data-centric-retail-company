package com.training.bigdata.supply.customerOrderReservation.producer

import com.training.bigdata.omnichannel.customerOrderReservation.events.CustomerOrderReservationEvent
import com.training.bigdata.omnichannel.customerOrderReservation.producer.KafkaAvroProducer
import com.training.bigdata.omnichannel.customerOrderReservation.utils._
import com.training.bigdata.omnichannel.customerOrderReservation.utils.LensAppConfig._
import com.training.bigdata.omnichannel.customerOrderReservation.utils.log.FunctionalLogger

object RunnerKafkaAvroProducer extends App with DefaultConf with FunctionalLogger{

  private val CONFIGURATION_FILE = "customerOrderReservationStreaming_runner.conf"

  val user = "user"
  System.setProperty("config.file", getClass.getClassLoader.getResource(CONFIGURATION_FILE).getPath)

  if (args.isEmpty) {
    throw new Error(s"Runner needs to be called with the kudu ip_address.")
  }

  val ipAddress = args.head
  logConfig(s"The given ipAddress is $ipAddress")

  val appConfig =
    defaultConfig
      .setListTopic(
        Topic(
          "ecommerce_raw_apps_orders",
          "desc",
          1000000,
          "parquet") :: Nil
      )
      .setParameters(
        Parameters(
          ipAddress,
          "localhost:9092",
          "https://ld6mk03.es.wcorp.training.com:8085",
          isKafkaSecured = false
        )
      )

  implicit val properties: PropertiesUtil = PropertiesUtil(user, appConfig)
  logConfig(s"$properties")

  val optInputFileJson =
    args
      .tail
      .headOption
      .map(getClass.getClassLoader.getResource(_).getPath)

  KafkaAvroProducer.processPipeline(new KafkaAvroProducer[CustomerOrderReservationEvent.Order], optInputFileJson)

}
