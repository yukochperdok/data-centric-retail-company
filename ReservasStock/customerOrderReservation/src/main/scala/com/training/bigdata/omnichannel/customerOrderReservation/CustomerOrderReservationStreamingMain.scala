package com.training.bigdata.omnichannel.customerOrderReservation

import com.training.bigdata.omnichannel.customerOrderReservation.arguments.ArgsParser
import com.training.bigdata.omnichannel.customerOrderReservation.process.CustomerOrderReservationStreamingProcess
import com.training.bigdata.omnichannel.customerOrderReservation.utils.PropertiesUtil
import com.training.bigdata.omnichannel.customerOrderReservation.utils.log.FunctionalLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import scala.util.{Failure, Success, Try}

object CustomerOrderReservationStreamingMain extends FunctionalLogger{
  val fs: FileSystem = FileSystem.get(new Configuration())

  def main(args: Array[String]): Unit = {

    val propertiesUtil: PropertiesUtil = Try(PropertiesUtil(ArgsParser.getArguments(args).user)) match {
      case Failure(f: IllegalArgumentException) =>
        logAlert(f.getMessage)
        sys.exit(1)
      case Failure(f) =>
        logNonFunctionalMessage(s"Configuration could not be loaded")
        logNonFunctionalMessage(f.getStackTrace.mkString("\n"))
        sys.exit(1)
      case Success(propsUtil) => propsUtil
    }

    logInfo(s"Generated spark session")
    logConfig(s"$propertiesUtil")

    CustomerOrderReservationStreamingProcess(propertiesUtil)
  }

}
