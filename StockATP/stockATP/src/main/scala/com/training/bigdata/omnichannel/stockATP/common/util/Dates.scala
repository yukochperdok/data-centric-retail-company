package com.training.bigdata.omnichannel.stockATP.common.util

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId, ZonedDateTime}
import com.training.bigdata.omnichannel.stockATP.common.entities.ControlEjecucionView
import Constants._

object Dates {

  def stringToTimestamp(
    executionDateTime: String,
    timezone: String = DEFAULT_TIMEZONE,
    formatter: DateTimeFormatter = DEFAULT_DATETIME_FORMATTER): Timestamp = {
    localDateTimeToTimestamp(LocalDateTime.parse(executionDateTime, formatter), timezone)
  }

  def localDateTimeToTimestamp(localDT: LocalDateTime, timezone: String = DEFAULT_TIMEZONE): Timestamp = {
    val zonedDT: ZonedDateTime = localDT.atZone(ZoneId.of(timezone))
    new Timestamp(zonedDT.toEpochSecond*1000)
  }

  def parseDateNumber(n: Int): String =
    if(n.toString.length == 1) "0" + n.toString else n.toString


  def timestampToStringWithTZ(ts: Timestamp, separator: String, timezone: String = DEFAULT_TIMEZONE): String = {
    val currentLocalTime = ts.toInstant
      .atZone(ZoneId.of(timezone))
      .toLocalDate
    Seq(
      currentLocalTime.getYear,
      parseDateNumber(currentLocalTime.getMonthValue),
      parseDateNumber(currentLocalTime.getDayOfMonth)).mkString(separator)
  }

  def getDateAtMidnight(ts: Timestamp, timezone: String = DEFAULT_TIMEZONE) : (Timestamp, LocalDateTime) = {
    val currentLocalTime: LocalDate = ts.toInstant
      .atZone(ZoneId.of(timezone))
      .toLocalDate
    val beginningOfDayDate: LocalDateTime = LocalDateTime.of(currentLocalTime, LocalTime.MIDNIGHT)
    val beginningOfDayTs = Dates.localDateTimeToTimestamp(beginningOfDayDate, timezone)

    (beginningOfDayTs, beginningOfDayDate)
  }

  def sumDaysToTimestamp(ts:Timestamp, plusDays: Int = 1, timezone: String = DEFAULT_TIMEZONE) : Timestamp = {
    val currentLocalTime: LocalDate = ts.toInstant
      .atZone(ZoneId.of(timezone))
      .toLocalDate

    val beginningOfDayDate: LocalDateTime = LocalDateTime.of(currentLocalTime, LocalTime.MIDNIGHT)
    val endOfDayDate: LocalDateTime = beginningOfDayDate.plusDays(plusDays)
    val endOfDayTs = Dates.localDateTimeToTimestamp(endOfDayDate, timezone)

    endOfDayTs
  }

  def sumDaysToLocalDateTime(localDateTime: LocalDateTime, plusDays: Int = 1, timezone: String = DEFAULT_TIMEZONE) : Timestamp = {
    val endOfDayDate: LocalDateTime = LocalDateTime.of(localDateTime.toLocalDate, LocalTime.MIDNIGHT).plusDays(plusDays)
    val endOfDayTs = Dates.localDateTimeToTimestamp(endOfDayDate, timezone)

    endOfDayTs
  }

  def subtractDaysToTimestamp(ts:Timestamp, minusDays: Int = 1, timezone: String = DEFAULT_TIMEZONE) : Timestamp = {
    val currentLocalTime: LocalDate = ts.toInstant
      .atZone(ZoneId.of(timezone))
      .toLocalDate

    val beginningOfDayDate: LocalDateTime = LocalDateTime.of(currentLocalTime, LocalTime.MIDNIGHT)
    val endOfDayDate: LocalDateTime = beginningOfDayDate.minusDays(minusDays)
    val endOfDayTs = Dates.localDateTimeToTimestamp(endOfDayDate, timezone)

    endOfDayTs
  }

  def subtractDaysToLocalDateTime(localDateTime: LocalDateTime, minusDays: Int = 1, timezone: String = DEFAULT_TIMEZONE) : Timestamp = {
    val endOfDayDate: LocalDateTime = LocalDateTime.of(localDateTime.toLocalDate, LocalTime.MIDNIGHT).minusDays(minusDays)
    val endOfDayTs = Dates.localDateTimeToTimestamp(endOfDayDate, timezone)

    endOfDayTs
  }

  def getFuturePlanningDaysMap(
    firstDayTs: Timestamp,
    separator: String,
    timezone: String = DEFAULT_TIMEZONE,
    plusDays: Int = 1,
    firstDayIndex: Int = 0): Map[String, String] = {

    val dayList = List.range(firstDayIndex, plusDays)
    val currentLocalDate: LocalDate = firstDayTs.toInstant.atZone(ZoneId.of(timezone)).toLocalDate
    val firstDate: LocalDateTime = LocalDateTime.of(currentLocalDate, LocalTime.MIDNIGHT)

    dayList
      .foldLeft((Map.empty, firstDate): (Map[String, String], LocalDateTime)) { (acc, e) =>
        val nextDate = acc._2.plusDays(1)
        val newTs: Timestamp = Dates.localDateTimeToTimestamp(acc._2, timezone)
        val idx = if(e.toString=="0") "" else e.toString
        (acc._1 ++ Map(timestampToStringWithTZ(newTs, separator, timezone) -> idx ), nextDate)
      }._1
  }

  def minTimestamp(ts1: Timestamp, ts2: Timestamp): Timestamp =
    (Option(ts1), Option(ts2)) match {
      case (None,None) => ts1
      case (Some(ts1),None) => ts1
      case (None,Some(ts2)) => ts2
      case _ => new Timestamp (Math.min (ts1.getTime, ts2.getTime))
    }

  val DELAY_MILLISECONDS_WINDOW_STREAMINGS = 10 * 1000

  def getDateRegistroEjecucion(listControlEjecucion: List[ControlEjecucionView], appName: String): Option[Timestamp] = {
    val (appStock, others) = listControlEjecucion.partition(_.idProcess==appName)
    appStock.headOption.map(
      rowAppStock => {
        val minLastExecutionTs: Timestamp =
        others.flatMap(rowOther => {
          Option(rowAppStock.lastExecutionDate).map{
            lastExecutionDate => {
              (Option(rowOther.initWriteDate), Option(rowOther.finishWriteDate)) match {
                case (Some(initWriteDate), Some(finishWriteDate)) =>
                  if (!finishWriteDate.after(lastExecutionDate)) lastExecutionDate
                  else if (initWriteDate.after(lastExecutionDate)) lastExecutionDate
                  else initWriteDate
                case _ => lastExecutionDate
              }
            }
          }
        }).foldLeft(rowAppStock.lastExecutionDate)((tsAcc, tsElem) => minTimestamp(tsAcc,tsElem))
        new Timestamp(minLastExecutionTs.getTime - DELAY_MILLISECONDS_WINDOW_STREAMINGS)
      }
    )
  }
}
