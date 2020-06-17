package com.training.bigdata.omnichannel.customerOrderReservation.utils

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

import com.training.bigdata.omnichannel.customerOrderReservation.utils.Constants.Date._
import com.training.bigdata.omnichannel.customerOrderReservation.utils.log.FunctionalLogger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import scala.util.Try
import org.apache.spark.sql.functions._

object DateUtils extends FunctionalLogger {

  def stringToTimestamp(executionDateTime: String,
                        timezone: String = DEFAULT_TIMEZONE,
                        formatter: DateTimeFormatter = DEFAULT_DATETIME_FORMATTER): Timestamp = {
    val zonedDT: ZonedDateTime = LocalDateTime.parse(executionDateTime.replace("T", " "), formatter).atZone(ZoneId.of(timezone))
    new Timestamp(zonedDT.toEpochSecond*1000)
  }

  def castTimestampColumn(columnName: String,
                          timezone: String = DEFAULT_TIMEZONE,
                          format: String = DEFAULT_DATETIME_FORMAT): DataFrame => DataFrame = df => {

    val udfStringToTimestamp: UserDefinedFunction =
      udf[Timestamp, String, String, String, Timestamp]{
        (date:String, tz: String, format: String, defaultValue: Timestamp) => {
          val formatter = DateTimeFormatter.ofPattern(format)
          Try(stringToTimestamp(date, tz, formatter)).getOrElse(defaultValue)
        }
      }

    df
      .withColumn(
        columnName,
        udfStringToTimestamp(col(columnName), lit(timezone), lit(format), lit(null))
      )
  }
}
