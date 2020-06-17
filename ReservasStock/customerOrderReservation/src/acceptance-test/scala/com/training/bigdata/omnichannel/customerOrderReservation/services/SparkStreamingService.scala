package com.training.bigdata.omnichannel.customerOrderReservation.services

import java.io.File

import com.training.bigdata.omnichannel.customerOrderReservation.process.CustomerOrderReservationStreamingProcess
import com.training.bigdata.omnichannel.customerOrderReservation.utils.{AppConfig, PropertiesUtil}
import com.training.bigdata.omnichannel.customerOrderReservation.utils.LensAppConfig._
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.FixedClock
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Clock, Duration, Seconds, StreamingContext}

/**
  * This service is a Docker Kafka Service with a producer in order to generate kafka Events
  */
object SparkStreamingService {

  val user = "AT_User"
  val confAppFile = "application_acc.conf"
  val confProcessFile = "customerOrderReservationStreaming_acc.conf"

  var batchDuration: Duration = _
  implicit var streamingContext: StreamingContext = _
  var fixedClock: FixedClock = _

  implicit var propertiesUtil: PropertiesUtil = _
  implicit var kuduContext: KuduContext = _

  def startStreaming(implicit sparkSession: SparkSession) : Unit = {

    val confAppFilePath = new File(s"src/acceptance-test/resources/${confAppFile}").getAbsolutePath
    val confProcessFilePath = new File(s"src/acceptance-test/resources/${confProcessFile}").getAbsolutePath
    System.setProperty("config.file", confProcessFilePath)
    propertiesUtil = PropertiesUtil(user, AppConfig(confAppFilePath).setSchemaRegistryURL(SchemaRegistryService.schemaRegistryUrl))
    kuduContext = new KuduContext(propertiesUtil.getKuduHosts, sparkSession.sparkContext)
    batchDuration = Seconds(propertiesUtil.getBatchDuration)
    streamingContext = new StreamingContext(sparkSession.sparkContext, batchDuration)
    fixedClock = Clock.getFixedClock(streamingContext)

    CustomerOrderReservationStreamingProcess.processPipeline
  }

  def stopStreaming() : Unit = {
    streamingContext.stop(stopSparkContext = false, stopGracefully = false)
  }

}
