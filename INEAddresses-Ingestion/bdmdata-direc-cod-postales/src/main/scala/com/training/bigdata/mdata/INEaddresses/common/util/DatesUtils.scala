package com.training.bigdata.mdata.INEaddresses.common.util

import java.sql.Timestamp

import com.training.bigdata.mdata.INEaddresses.common.util.log.FunctionalLogger
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object DatesUtils extends FunctionalLogger {

  final val yyyyMMdd_FORMATTER: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd")
  final val yyyyMMdd_hhmmss_FORMATTER: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  def stringToTimestamp(dateString: String, dateFormatter: DateTimeFormatter): Timestamp = {
      val dateTime = dateFormatter.parseDateTime(dateString)
      new Timestamp(dateTime.getMillis)
  }

}
