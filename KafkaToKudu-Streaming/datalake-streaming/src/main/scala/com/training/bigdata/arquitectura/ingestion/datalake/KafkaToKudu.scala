package com.training.bigdata.arquitectura.ingestion.datalake

import com.training.bigdata.arquitectura.ingestion.datalake.config.AppConfig
import com.training.bigdata.arquitectura.ingestion.datalake.streams.{DataObject, RecordStreamMapper}
import com.training.bigdata.arquitectura.ingestion.datalake.streams.reader.KafkaStreamReader
import com.training.bigdata.arquitectura.ingestion.datalake.streams.writer._
import com.training.bigdata.arquitectura.ingestion.datalake.utils.CommonUtils.getTopicToSchemaMappings
import com.training.bigdata.ingestion.utils.{FunctionalLogger, LogType, MonitorType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}
import org.apache.spark.streaming.{Seconds, StreamingContext}


case class Config(configPath:String = "", appName:String = "")

case class Record(id:String, timestamp:Long, table:String, operation:Operation, data:DataObject)


object KafkaToKudu extends FunctionalLogger{

  val fileSystem = FileSystem.get(new Configuration())

  val parser = new scopt.OptionParser[Config]("KafkaToKudu") {
    head("datalake-streaming", "1.x")

    opt[String]('f', "configPath").required().valueName("").action( (x, c) =>
      c.copy(configPath = x) ).text("config path")

    opt[String]('a', "appName").required().valueName("").action( (x, c) =>
      c.copy(appName = x) ).text("application name")

    help("help").text("prints this usage text")
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, Config()) match {
      case Some(config) =>

        val appConfig = AppConfig(s"${config.configPath}/application.conf")
        info(s"Loaded configuration",LogType.Configuration,MonitorType.Functional)
        info(s"[config.name: ${appConfig.application.process}]",LogType.Configuration,MonitorType.Functional)
        info(s"[config.parameters: ${appConfig.application.parameters}]",LogType.Configuration,MonitorType.Functional)

        val sparkMaster = appConfig.application.parameters("sparkMaster")
        val interval = appConfig.application.parameters("sparkBatchDuration").toLong

        val sparkConf = new SparkConf().setMaster(sparkMaster).setAppName(config.appName)

        info(s"Generated spark session for sparkMaster [${sparkMaster}] and appName [${config.appName}]",LogType.Configuration,MonitorType.Functional)

        val ssc = new StreamingContext(sparkConf, Seconds(interval))


        processPipeline(appConfig, ssc)


        val sparkGracefulStopFile = new Path(appConfig.application.parameters("gracefulShutdownFile"))
        val checkIntervalMillis = 10000

        fileSystem.create(sparkGracefulStopFile, true)

        var isStopped = false

        while (!isStopped) {
          try {

            isStopped = ssc.awaitTerminationOrTimeout(checkIntervalMillis)

            if (!isStopped && isShutdownRequested(sparkGracefulStopFile)) {
              //This means file does not exist in path, so stop gracefully
              info("Gracefully stopping Spark Streaming ...",LogType.Message,MonitorType.Functional)
              ssc.stop(stopSparkContext = true, stopGracefully = true)
              info("Application stopped!",LogType.Message,MonitorType.Functional)
              isStopped = true
            }

          } catch {
            case t: Throwable =>
              error(s"Error processing streaming: ${t.getMessage}")
              ssc.stop(stopSparkContext = true, stopGracefully = true)
              throw t // to exit with error condition
          }
        }

      case _ =>
    }
  }


  def processPipeline(appConfig: AppConfig, ssc: StreamingContext): Unit = {

    info("Processing pipeline ...",LogType.Message,MonitorType.Functional)

    val datalakeUser = appConfig.application.parameters("datalakeUser")
    val mappings = getTopicToSchemaMappings(appConfig.application.topic)
    val histTable:List[String] = appConfig.application.parameters("historyTable").split(",").toList
    val kuduMaxNumberOfColumns = appConfig.application.parameters("kuduMaxNumberOfColumns").toInt

    val kafkaStreamReader = new KafkaStreamReader(appConfig, ssc)
    val kuduStreamWriter = new KuduStreamWriter(appConfig, ssc.sparkContext)

    val recordStreamMapper = new RecordStreamMapper(datalakeUser, mappings, histTable, kuduMaxNumberOfColumns)


    info("Creating Kafka stream ...",LogType.Message,MonitorType.Functional)
    val stream = kafkaStreamReader.readStream()

    stream.foreachRDD{rdd => {

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      kuduStreamWriter.write(recordStreamMapper.map(rdd), offsetRanges)

      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    }}

    sys.ShutdownHookThread {
      info("Shutdown hook called ...",LogType.Message,MonitorType.Functional)
      ssc.stop(stopSparkContext = true, stopGracefully = true)
      info("Application stopped!",LogType.Message,MonitorType.Functional)
    }

    ssc.start

    info("StreamingContext started ...",LogType.Message,MonitorType.Functional)

  }

  def isShutdownRequested(sparkGracefulStopFile: Path): Boolean = {
    try {
      !fileSystem.exists(sparkGracefulStopFile)
    } catch {
      case _: Throwable => false
    }
  }

}
