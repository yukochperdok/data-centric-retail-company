package com.training.bigdata.omnichannel.customerOrderReservation.utils

import com.training.bigdata.omnichannel.customerOrderReservation.utils.log.FunctionalLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

trait SparkCommons extends FunctionalLogger {

  var isOK: Boolean = true
  val fileSystem: FileSystem = FileSystem.get(new Configuration())

  def createSparkSession(sparkMaster: String): SparkSession = {
    val sparkConf: SparkConf = new SparkConf()
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .setMaster(sparkMaster)
      .setAppName("customerOrderReservationStreaming")
    SparkSession.builder().config(sparkConf).getOrCreate()
  }

  def setupGracefulStop(gracefulFile: String)(implicit ssc: StreamingContext): Unit = {
    val sparkGracefulStopFile = new Path(gracefulFile)
    val checkIntervalMillis = 10000

    fileSystem.create(sparkGracefulStopFile, true)

    var isStopped = false

    while (!isStopped) {
      try {
        isStopped = ssc.awaitTerminationOrTimeout(checkIntervalMillis)
        if (!isStopped && isShutdownRequested(sparkGracefulStopFile)) {
          //This means file does not exist in path, so stop gracefully
          logInfo("Gracefully stopping Spark Streaming ...")
          isOK = false
          ssc.stop(stopSparkContext = true, stopGracefully = true)
          isStopped = true
          logInfo("Application stopped!")
        }
      } catch {
        case t: Throwable =>
          logNonFunctionalMessage(s"Error processing streaming: ${t.getMessage}")
          ssc.stop(stopSparkContext = true, stopGracefully = true)
          throw t // to exit with error condition
      }
    }
  }

  def isShutdownRequested(sparkGracefulStopFile: Path): Boolean = {
    try {
      !fileSystem.exists(sparkGracefulStopFile)
    } catch {
      case _: Throwable => false
    }
  }

}
