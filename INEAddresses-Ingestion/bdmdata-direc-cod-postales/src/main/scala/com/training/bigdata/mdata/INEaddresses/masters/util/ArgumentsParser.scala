package com.training.bigdata.mdata.INEaddresses.masters.util

import scopt.OptionParser

case class Arguments(
  configFilePath: String = "",
  user: String = ""
)

object ArgumentsParser extends ArgumentsParser

class ArgumentsParser extends OptionParser[Arguments]("Build postal codes and towns masters") {

  head("Build postal codes and towns masters", "1.0.0")

  opt[String]('c', "config-file-path") required() valueName "<config file path>" action { (value, config) =>
    config.copy(configFilePath = value)
  } text "Configuration file with the ingestion parameters (HOCON format)"

  opt[String]('u', "user") required() valueName "<load user>" action { (value, config) =>
    config.copy(user = value)
  } text "User that executes the process for audit reasons"

  help("help") text "Prints this help"
}
