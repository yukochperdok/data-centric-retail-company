package com.training.bigdata.mdata.INEaddresses.INEFilesIngestion.util

import scopt.OptionParser

case class Arguments(
  configFilePath: String = "",
  user: String = "",
  date: String = "",
  workflow: String = ArgumentsParser.NORMAL_EXECUTION
)

object ArgumentsParser extends ArgumentsParser {
  val HISTORIFY_STREET_STRETCHES = "stretches"
  val HISTORIFY_STREET_TYPES = "types"
  val NORMAL_EXECUTION = "normal"
}

class ArgumentsParser extends OptionParser[Arguments]("INE Files Ingestion") {

  head("INE Files Ingestion")

  opt[String]('c', "config-file-path") required() valueName "<config file path>" action { (value, config) =>
    config.copy(configFilePath = value)
  } text "Configuration file with the ingestion parameters (HOCON format)"

  opt[String]('d', "date") required() valueName "<load date>" action { (value, config) =>
    config.copy(date = value)
  } text "HDFS folder date that contains files to ingest"

  opt[String]('u', "user") required() valueName "<load user>" action { (value, config) =>
    config.copy(user = value)
  } text "User that executes the process for audit reasons"

  opt[String]('w', "workflow") optional() valueName "<load workflow>" action { (value, config) =>
    config.copy(workflow = value)
  } text "Use 'stretches' to historify TRAM-NAL file or 'types' to only historify VIAS-NAL file"

  help("help") text "Prints this help"
}
