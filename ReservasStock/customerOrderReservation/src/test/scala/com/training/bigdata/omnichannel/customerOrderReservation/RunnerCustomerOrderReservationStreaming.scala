package com.training.bigdata.omnichannel.customerOrderReservation

import com.training.bigdata.omnichannel.customerOrderReservation.process.CustomerOrderReservationStreamingProcess
import com.training.bigdata.omnichannel.customerOrderReservation.utils.LensAppConfig._
import com.training.bigdata.omnichannel.customerOrderReservation.utils._
import com.training.bigdata.omnichannel.customerOrderReservation.utils.log.FunctionalLogger
import org.apache.kudu.spark.kudu.KuduContext

object RunnerCustomerOrderReservationStreaming extends App with SparkLocalTest with DefaultConf with FunctionalLogger{

  private val CONFIGURATION_FILE = "customerOrderReservationStreaming_runner.conf"

  val user = "user"
  System.setProperty("config.file", getClass.getClassLoader.getResource(CONFIGURATION_FILE).getPath)

  if (args.isEmpty) {
    throw new Error(s"Runner needs to be called with the kudu ip_address.")
  }

  val ipAddress = args.drop(0).head
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

  CustomerOrderReservationStreamingProcess.processPipeline(sparkSession, streamingContext, new KuduContext(properties.getKuduHosts, sparkSession.sparkContext), properties)
  streamingContext.awaitTermination()

}
