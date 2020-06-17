package com.training.bigdata.omnichannel.stockATP.common.util

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId}
import java.time.format.{DateTimeFormatter, DateTimeParseException}

import Constants._
import com.training.bigdata.omnichannel.stockATP.common.entities.ControlEjecucionView
import com.training.bigdata.omnichannel.stockATP.common.util.tag.TagTest._
import org.scalatest.{FlatSpec, Matchers}

trait TestDatesVariables {
  val formatterWithHyphen = DateTimeFormatter.ofPattern(DEFAULT_DATETIME_FORMAT)
  val formatterWithoutHyphen = DateTimeFormatter.ofPattern(IN_PROCESS_ORDERS_DATETIME_FORMAT)
  val defaultTimestamp = new Timestamp(0L)
}

class DatesTest extends FlatSpec with Matchers {

  "stringToTimestamp " must "return the timestamp corresponding to the string given" taggedAs (UnitTestTag) in new TestDatesVariables {
    Dates.stringToTimestamp("2018-08-25 05:06:07") shouldBe new Timestamp(1535166367000L)
    Dates.stringToTimestamp("2018-08-25 05:06:07", formatter = formatterWithHyphen) shouldBe new Timestamp(1535166367000L)
    Dates.stringToTimestamp("20180825 05:06:07", formatter = formatterWithoutHyphen) shouldBe new Timestamp(1535166367000L)
    Dates.stringToTimestamp("20180825 17:06:07", formatter = formatterWithoutHyphen) shouldBe new Timestamp(1535209567000L)

  }

  it must "return the timestamp corresponding to the string given but in another zone (London)" taggedAs (UnitTestTag) in new TestDatesVariables {
    Dates.stringToTimestamp("2018-08-25 04:06:07", "Europe/London") shouldBe new Timestamp(1535166367000L)
    Dates.stringToTimestamp("2018-08-25 04:06:07", "Europe/London", formatterWithHyphen) shouldBe new Timestamp(1535166367000L)
    Dates.stringToTimestamp("20180825 04:06:07", "Europe/London", formatterWithoutHyphen) shouldBe new Timestamp(1535166367000L)
    Dates.stringToTimestamp("20180825 16:06:07", "Europe/London", formatterWithoutHyphen) shouldBe new Timestamp(1535209567000L)
  }

  it must "throw a DateTimeParseException when the format does not match " taggedAs (UnitTestTag) in new TestDatesVariables {
    val thrown1 = intercept[DateTimeParseException] {
      Dates.stringToTimestamp("20180825 05:06:07")
    }

    val thrown2 = intercept[DateTimeParseException] {
      Dates.stringToTimestamp("2018-08-25")
    }

    val thrown3 = intercept[DateTimeParseException] {
      Dates.stringToTimestamp("2018-08-25 05:06:07", formatter = formatterWithoutHyphen)
    }
  }


  "localDateTimeToTimestamp" must "return the timestamp corresponding to that LocalDateTime " taggedAs (UnitTestTag) in {
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val date: LocalDateTime = LocalDateTime.parse("2018-08-25 05:06:07", formatter)
    Dates.localDateTimeToTimestamp(date) shouldBe new Timestamp(1535166367000L)

    val formatter2: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss")
    val date2: LocalDateTime = LocalDateTime.parse("20180825 05:06:07", formatter2)
    Dates.localDateTimeToTimestamp(date2) shouldBe new Timestamp(1535166367000L)
  }

  it must "return the timestamp corresponding to that LocalDateTime but in another zone (London)" taggedAs (UnitTestTag) in {
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val date: LocalDateTime = LocalDateTime.parse("2018-08-25 04:06:07", formatter)
    Dates.localDateTimeToTimestamp(date, "Europe/London") shouldBe new Timestamp(1535166367000L)

    val formatter2: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss")
    val date2: LocalDateTime = LocalDateTime.parse("20180825 04:06:07", formatter2)
    Dates.localDateTimeToTimestamp(date2, "Europe/London") shouldBe new Timestamp(1535166367000L)
  }

  "getDateAtMidnight" must "return the timestamp at 00:00:00 of that day" taggedAs (UnitTestTag) in new TestDatesVariables {
    val notAtMidnightTs = Dates.stringToTimestamp("2018-08-25 05:06:07", formatter = formatterWithHyphen)
    val sameDayAtMidnightTs = Dates.stringToTimestamp("2018-08-25 00:00:00", formatter = formatterWithHyphen)
    val sameDayAtMidnightLocalDateTime = sameDayAtMidnightTs.toLocalDateTime

    Dates.getDateAtMidnight(notAtMidnightTs) shouldBe (sameDayAtMidnightTs, sameDayAtMidnightLocalDateTime)
    Dates.getDateAtMidnight(sameDayAtMidnightTs) shouldBe (sameDayAtMidnightTs, sameDayAtMidnightLocalDateTime)
  }

  "parseDateNumber " must "return a String with 2 digits when it's a number of 1 digit " taggedAs (UnitTestTag) in {
    Dates.parseDateNumber(3) shouldBe "03"
  }

  it must "return a String with 2 digits when it's a number of 2 digit " taggedAs (UnitTestTag) in {
    Dates.parseDateNumber(10) shouldBe "10"
  }

  "timestampToStringWithTZ " must "return a String formatted as the separator establish to a timezone" taggedAs (UnitTestTag) in {
    Dates.timestampToStringWithTZ(new Timestamp(1535166367000L), "-","Europe/Paris") shouldBe "2018-08-25"
    Dates.timestampToStringWithTZ(new Timestamp(1535166367000L), "","Europe/Paris") shouldBe "20180825"
  }

  it must "return a String formatted as the separator establish to a default timezone" taggedAs (UnitTestTag) in {
    Dates.timestampToStringWithTZ(new Timestamp(1535166367000L), "-") shouldBe "2018-08-25"
    Dates.timestampToStringWithTZ(new Timestamp(1535166367000L), "") shouldBe "20180825"
  }

  it must "return a String formatted as the separator establish to another timezone (London)" taggedAs (UnitTestTag) in {
    Dates.timestampToStringWithTZ(new Timestamp(1535166367000L),"-", "Europe/London") shouldBe "2018-08-25"
    Dates.timestampToStringWithTZ(new Timestamp(1535166367000L),"", "Europe/London") shouldBe "20180825"
  }

  it must "pick out between timezones at roughly midnight" taggedAs (UnitTestTag) in {
    val tsParis = Dates.stringToTimestamp("2018-08-25 00:06:07", "Europe/Paris")
    Dates.timestampToStringWithTZ(tsParis, "-", "Europe/Paris") shouldBe "2018-08-25"
    Dates.timestampToStringWithTZ(tsParis, "", "Europe/London") shouldBe "20180824"
  }

  "sumDaysToTimestamp" must "get its following planning day appropriate to a timezone in timestamp format" taggedAs (UnitTestTag) in {
    val tsParis = Dates.stringToTimestamp("2018-08-25 00:06:07", "Europe/Paris")
    val tsExpectedFollowingPlanningDay = Dates.stringToTimestamp("2018-08-26 00:00:00", "Europe/Paris")
    Dates.sumDaysToTimestamp(tsParis, timezone = "Europe/Paris") shouldBe tsExpectedFollowingPlanningDay
  }

  it must "get its following planning day appropriate to a default timezone in timestamp format" taggedAs (UnitTestTag) in {
    val tsParis = Dates.stringToTimestamp("2018-08-25 00:06:07")
    val tsExpectedFollowingPlanningDay = Dates.stringToTimestamp("2018-08-26 00:00:00")
    Dates.sumDaysToTimestamp(tsParis) shouldBe tsExpectedFollowingPlanningDay
  }

  it must "add the specified number of days to the timestamp in another timezone" taggedAs (UnitTestTag) in {
    val tsLondon = Dates.stringToTimestamp("2018-08-25 00:06:07", "Europe/London")
    val tsExpectedFollowingPlanningDay = Dates.stringToTimestamp("2018-08-28 00:00:00", "Europe/London")
    Dates.sumDaysToTimestamp(tsLondon, 3, timezone = "Europe/London") shouldBe tsExpectedFollowingPlanningDay
  }

  it must "add the specified number of days to the timestamp for the default timezone" taggedAs (UnitTestTag) in {
    val tsDefault = Dates.stringToTimestamp("2019-08-19 00:00:00", Constants.DEFAULT_TIMEZONE)
    val tsExpectedFollowingPlanningDay = Dates.stringToTimestamp("2019-11-27 00:00:00", Constants.DEFAULT_TIMEZONE)
    Dates.sumDaysToTimestamp(tsDefault, 100) shouldBe tsExpectedFollowingPlanningDay
  }

  "sumDaysToLocalDateTime" must "get its following planning day appropriate to a timezone in timestamp format" taggedAs (UnitTestTag) in {
    val tsParis = Dates.stringToTimestamp("2018-08-25 00:06:07", "Europe/Paris").toLocalDateTime
    val tsExpectedFollowingPlanningDay = Dates.stringToTimestamp("2018-08-26 00:00:00", "Europe/Paris")
    Dates.sumDaysToLocalDateTime(tsParis, timezone = "Europe/Paris") shouldBe tsExpectedFollowingPlanningDay
  }

  it must "get its following planning day appropriate to a default timezone in timestamp format" taggedAs (UnitTestTag) in {
    val tsParis = Dates.stringToTimestamp("2018-08-25 00:06:07").toLocalDateTime
    val tsExpectedFollowingPlanningDay = Dates.stringToTimestamp("2018-08-26 00:00:00")
    Dates.sumDaysToLocalDateTime(tsParis) shouldBe tsExpectedFollowingPlanningDay
  }

  it must "add the specified number of days to the timestamp in another timezone" taggedAs (UnitTestTag) in {
    val tsLondon = Dates.stringToTimestamp("2018-08-25 00:06:07", "Europe/London").toLocalDateTime
    val tsExpectedFollowingPlanningDay = Dates.stringToTimestamp("2018-08-28 00:00:00", "Europe/London")
    Dates.sumDaysToLocalDateTime(tsLondon, 3, timezone = "Europe/London") shouldBe tsExpectedFollowingPlanningDay
  }

  it must "add the specified number of days to the timestamp for the default timezone" taggedAs (UnitTestTag) in {
    val tsDefault = Dates.stringToTimestamp("2019-08-19 00:00:00", Constants.DEFAULT_TIMEZONE).toLocalDateTime
    val tsExpectedFollowingPlanningDay = Dates.stringToTimestamp("2019-11-27 00:00:00", Constants.DEFAULT_TIMEZONE)
    Dates.sumDaysToLocalDateTime(tsDefault, 100) shouldBe tsExpectedFollowingPlanningDay
  }

  "subtractDaysToTimestamp" must "get its previous day when no parameter is passed" taggedAs (UnitTestTag) in {
    val tsParis = Dates.stringToTimestamp("2018-08-25 00:06:07")
    val tsExpectedPreviousDay = Dates.stringToTimestamp("2018-08-24 00:00:00")
    Dates.subtractDaysToTimestamp(tsParis) shouldBe tsExpectedPreviousDay
  }

  it must "subtract the specified number of days to the timestamp" taggedAs (UnitTestTag) in {
    val tsParis = Dates.stringToTimestamp("2018-08-25 00:06:07")
    val tsExpectedPreviousDay = Dates.stringToTimestamp("2018-07-31 00:00:00")
    Dates.subtractDaysToTimestamp(tsParis, 25) shouldBe tsExpectedPreviousDay
  }

  "subtractDaysToLocalDateTime" must "get its previous day when no parameter is passed" taggedAs (UnitTestTag) in {
    val tsParis = Dates.stringToTimestamp("2018-08-25 00:06:07").toLocalDateTime
    val tsExpectedPreviousDay = Dates.stringToTimestamp("2018-08-24 00:00:00")
    Dates.subtractDaysToLocalDateTime(tsParis) shouldBe tsExpectedPreviousDay
  }

  it must "subtract the specified number of days to the timestamp" taggedAs (UnitTestTag) in {
    val tsParis = Dates.stringToTimestamp("2018-08-25 00:06:07").toLocalDateTime
    val tsExpectedPreviousDay = Dates.stringToTimestamp("2018-07-31 00:00:00")
    Dates.subtractDaysToLocalDateTime(tsParis, 25) shouldBe tsExpectedPreviousDay
  }

  "minTimestamp" must " return the minimun of two timestamp" taggedAs (UnitTestTag) in {
    val ts1 = new Timestamp(100)
    val ts2 = new Timestamp(101)
    Dates.minTimestamp(ts1,ts2) shouldBe (ts1)
    Dates.minTimestamp(ts1,null) shouldBe (ts1)
    Dates.minTimestamp(null,ts2) shouldBe (ts2)
    Dates.minTimestamp(ts1,ts1) shouldBe (ts1)
    Dates.minTimestamp(null,null) shouldBe (null)
  }

  "getDateRegistroEjecucion" must " return the minimun timestamp which works as update's edge of executions" taggedAs (UnitTestTag) in {
    val procesoRef = "proceso_ref"
    val dateExpected = Dates.stringToTimestamp("2018-10-20 09:58:00")
    val dateExpectedWithDelay = new Timestamp(dateExpected.getTime - Dates.DELAY_MILLISECONDS_WINDOW_STREAMINGS)

    val listControlEjecucion: List[ControlEjecucionView] =
      ControlEjecucionView(
        procesoRef,
        dateExpected,
        Dates.stringToTimestamp("2018-10-20 09:59:00"),
        Dates.stringToTimestamp("2018-10-20 10:00:00")) ::
      ControlEjecucionView(
        "proceso_1",
        Dates.stringToTimestamp("2018-10-20 09:55:00"),
        Dates.stringToTimestamp("2018-10-20 09:56:00"),
        Dates.stringToTimestamp("2018-10-20 09:57:00")) ::
      ControlEjecucionView(
        "proceso_2",
        Dates.stringToTimestamp("2018-10-20 09:00:00"),
        Dates.stringToTimestamp("2018-10-20 09:01:00"),
        Dates.stringToTimestamp("2018-10-20 09:02:00")) ::
      ControlEjecucionView(
        "proceso_3",
        Dates.stringToTimestamp("2018-10-20 09:00:00"),
        Dates.stringToTimestamp("2018-10-20 09:10:00"),
        Dates.stringToTimestamp("2018-10-20 09:20:00")) :: Nil

    Dates.getDateRegistroEjecucion(listControlEjecucion, procesoRef) shouldBe (Some(dateExpectedWithDelay))
  }

  it must
    " return the lastExecutionDate of reference process when that date is greater than all finishWriteDates" taggedAs (UnitTestTag) in {
    val procesoRef = "proceso_ref"
    val dateExpected = Dates.stringToTimestamp("2018-10-20 09:58:00")
    val dateExpectedWithDelay = new Timestamp(dateExpected.getTime - Dates.DELAY_MILLISECONDS_WINDOW_STREAMINGS)

    val listControlEjecucion: List[ControlEjecucionView] =
      ControlEjecucionView(
        procesoRef,
        dateExpected,
        Dates.stringToTimestamp("2018-10-20 09:59:00"),
        Dates.stringToTimestamp("2018-10-20 10:00:00")) ::
        ControlEjecucionView(
          "proceso_1",
          Dates.stringToTimestamp("2018-10-20 09:55:00"),
          Dates.stringToTimestamp("2018-10-20 09:56:00"),
          Dates.stringToTimestamp("2018-10-20 09:57:00")) ::
        ControlEjecucionView(
          "proceso_2",
          Dates.stringToTimestamp("2018-10-20 09:00:00"),
          Dates.stringToTimestamp("2018-10-20 09:01:00"),
          Dates.stringToTimestamp("2018-10-20 09:02:00")) ::
        ControlEjecucionView(
          "proceso_3",
          Dates.stringToTimestamp("2018-10-20 09:00:00"),
          Dates.stringToTimestamp("2018-10-20 09:10:00"),
          Dates.stringToTimestamp("2018-10-20 09:20:00")) :: Nil

    Dates.getDateRegistroEjecucion(listControlEjecucion, procesoRef) shouldBe (Some(dateExpectedWithDelay))
  }

  it must
  " return the lastExecutionDate of reference process when that date is earlier than all initWriteDates" taggedAs (UnitTestTag) in {
    val procesoRef = "proceso_ref"
    val dateExpected = Dates.stringToTimestamp("2018-10-20 09:52:00")
    val dateExpectedWithDelay = new Timestamp(dateExpected.getTime - Dates.DELAY_MILLISECONDS_WINDOW_STREAMINGS)

    val listControlEjecucion: List[ControlEjecucionView] =
    ControlEjecucionView(
      procesoRef,
      dateExpected,
      Dates.stringToTimestamp("2018-10-20 09:53:00"),
      Dates.stringToTimestamp("2018-10-20 09:54:00")) ::
    ControlEjecucionView(
      "proceso_1",
      Dates.stringToTimestamp("2018-10-20 09:55:00"),
      Dates.stringToTimestamp("2018-10-20 09:56:00"),
      Dates.stringToTimestamp("2018-10-20 09:57:00")) ::
    ControlEjecucionView(
      "proceso_2",
      Dates.stringToTimestamp("2018-10-20 09:00:00"),
      Dates.stringToTimestamp("2018-10-20 09:01:00"),
      Dates.stringToTimestamp("2018-10-20 09:02:00")) ::
    ControlEjecucionView(
      "proceso_3",
      Dates.stringToTimestamp("2018-10-20 09:00:00"),
      Dates.stringToTimestamp("2018-10-20 09:10:00"),
      Dates.stringToTimestamp("2018-10-20 09:20:00")) :: Nil

    Dates.getDateRegistroEjecucion(listControlEjecucion, procesoRef) shouldBe (Some(dateExpectedWithDelay))
  }

  it must
    " return the initWriteDates of one id process when lastExecutionDate of reference process is greater than that initWriteDates and earlier than its finishWriteDates" taggedAs (UnitTestTag) in {
    val procesoRef = "proceso_ref"
    val dateExpected = Dates.stringToTimestamp("2018-10-20 09:56:00")
    val dateExpectedWithDelay = new Timestamp(dateExpected.getTime - Dates.DELAY_MILLISECONDS_WINDOW_STREAMINGS)

    val listControlEjecucion: List[ControlEjecucionView] =
      ControlEjecucionView(
        procesoRef,
        Dates.stringToTimestamp("2018-10-20 09:56:30"),
        Dates.stringToTimestamp("2018-10-20 09:57:00"),
        Dates.stringToTimestamp("2018-10-20 09:58:00")) ::
        ControlEjecucionView(
          "proceso_1",
          Dates.stringToTimestamp("2018-10-20 09:55:00"),
          dateExpected,
          Dates.stringToTimestamp("2018-10-20 09:57:00")) ::
        ControlEjecucionView(
          "proceso_2",
          Dates.stringToTimestamp("2018-10-20 09:00:00"),
          Dates.stringToTimestamp("2018-10-20 09:01:00"),
          Dates.stringToTimestamp("2018-10-20 09:02:00")) ::
        ControlEjecucionView(
          "proceso_3",
          Dates.stringToTimestamp("2018-10-20 09:00:00"),
          Dates.stringToTimestamp("2018-10-20 09:10:00"),
          Dates.stringToTimestamp("2018-10-20 09:20:00")) :: Nil

    Dates.getDateRegistroEjecucion(listControlEjecucion, procesoRef) shouldBe (Some(dateExpectedWithDelay))
  }

  it must
    " return the initWriteDates of one id process when lastExecutionDate of reference process is earlier than some finnishWriteDates and greater than their initWriteDates" taggedAs (UnitTestTag) in {
    val procesoRef = "proceso_ref"
    val dateExpected = Dates.stringToTimestamp("2018-10-20 09:51:00")
    val dateExpectedWithDelay = new Timestamp(dateExpected.getTime - Dates.DELAY_MILLISECONDS_WINDOW_STREAMINGS)

    val listControlEjecucion: List[ControlEjecucionView] =
      ControlEjecucionView(
        procesoRef,
        Dates.stringToTimestamp("2018-10-20 09:56:30"),
        Dates.stringToTimestamp("2018-10-20 09:57:00"),
        Dates.stringToTimestamp("2018-10-20 09:58:00")) ::
        ControlEjecucionView(
          "proceso_1",
          Dates.stringToTimestamp("2018-10-20 09:55:00"),
          Dates.stringToTimestamp("2018-10-20 09:56:00"),
          Dates.stringToTimestamp("2018-10-20 09:57:00")) ::
        ControlEjecucionView(
          "proceso_2",
          Dates.stringToTimestamp("2018-10-20 09:00:00"),
          dateExpected,
          Dates.stringToTimestamp("2018-10-20 09:56:40")) ::
        ControlEjecucionView(
          "proceso_3",
          Dates.stringToTimestamp("2018-10-20 09:00:00"),
          Dates.stringToTimestamp("2018-10-20 09:10:00"),
          Dates.stringToTimestamp("2018-10-20 09:20:00")) :: Nil

    Dates.getDateRegistroEjecucion(listControlEjecucion, procesoRef) shouldBe (Some(dateExpectedWithDelay))
  }

  it must
    " return the initWriteDates of one id process when lastExecutionDate of reference process is earlier than some finnishWriteDates and greater than their initWriteDates" +
      " and this lastExecutionDate also is both earlier than another finnishWriteDates and initWriteDates process " taggedAs (UnitTestTag) in {
    val procesoRef = "proceso_ref"
    val dateExpected = Dates.stringToTimestamp("2018-10-20 09:40:00")
    val dateExpectedWithDelay = new Timestamp(dateExpected.getTime - Dates.DELAY_MILLISECONDS_WINDOW_STREAMINGS)

    val listControlEjecucion: List[ControlEjecucionView] =
      ControlEjecucionView(
        procesoRef,
        Dates.stringToTimestamp("2018-10-20 09:56:30"),
        Dates.stringToTimestamp("2018-10-20 09:57:00"),
        Dates.stringToTimestamp("2018-10-20 09:58:00")) ::
        ControlEjecucionView(
          "proceso_1",
          Dates.stringToTimestamp("2018-10-20 09:55:00"),
          Dates.stringToTimestamp("2018-10-20 09:57:00"),
          Dates.stringToTimestamp("2018-10-20 09:58:00")) ::
        ControlEjecucionView(
          "proceso_2",
          Dates.stringToTimestamp("2018-10-20 09:00:00"),
          Dates.stringToTimestamp("2018-10-20 09:51:00"),
          Dates.stringToTimestamp("2018-10-20 09:56:50")) ::
        ControlEjecucionView(
          "proceso_3",
          Dates.stringToTimestamp("2018-10-20 09:00:00"),
          dateExpected,
          Dates.stringToTimestamp("2018-10-20 09:57:40")) :: Nil

    Dates.getDateRegistroEjecucion(listControlEjecucion, procesoRef) shouldBe (Some(dateExpectedWithDelay))
  }

  it must " return lastExecution when there only is a reference process " taggedAs (UnitTestTag) in {
    val procesoRef = "proceso_ref"
    val dateExpected = Dates.stringToTimestamp("2018-10-20 09:40:00")
    val dateExpectedWithDelay = new Timestamp(dateExpected.getTime - Dates.DELAY_MILLISECONDS_WINDOW_STREAMINGS)

    val listControlEjecucion: List[ControlEjecucionView] =
      ControlEjecucionView(
        procesoRef,
        dateExpected,
        Dates.stringToTimestamp("2018-10-20 09:57:00"),
        Dates.stringToTimestamp("2018-10-20 09:58:00")) :: Nil

    Dates.getDateRegistroEjecucion(listControlEjecucion, procesoRef) shouldBe (Some(dateExpectedWithDelay))
  }

  it must " return None when list of control ejecucion is empty or there isn't reference process" taggedAs (UnitTestTag) in {
    val procesoRef = "proceso_ref"

    Dates.getDateRegistroEjecucion(List.empty[ControlEjecucionView], procesoRef) shouldBe (None)

    val listControlEjecucion: List[ControlEjecucionView] =
      ControlEjecucionView(
        "proceso_prueba",
        Dates.stringToTimestamp("2018-10-20 09:00:00"),
        Dates.stringToTimestamp("2018-10-20 09:10:00"),
        Dates.stringToTimestamp("2018-10-20 09:20:00")) :: Nil

    Dates.getDateRegistroEjecucion(listControlEjecucion, procesoRef) shouldBe (None)
  }

  "getFuturePlanningDaysMap " must " return a map with one result " taggedAs (UnitTestTag) in {
    val ts = Dates.stringToTimestamp("2018-08-25 00:06:07")
    val date0 = "2018-08-25"

    val expectedMap: Map[String, String] = Map(
      date0 -> "")
    Dates.getFuturePlanningDaysMap(ts, "-", "Europe/Paris", 1) shouldBe expectedMap
  }

  it must " return a map with two results " taggedAs (UnitTestTag) in {
    val ts = Dates.stringToTimestamp("2018-08-25 00:06:07")
    val date0 = "2018-08-25"
    val date1 = "2018-08-26"

    val expectedMap: Map[String, String] = Map(
      date0 -> "",
      date1 -> "1")
    Dates.getFuturePlanningDaysMap(ts,"-", "Europe/Paris", 2) shouldBe expectedMap
  }

  it must " return a map with two results with other format " taggedAs (UnitTestTag) in {
    val ts = Dates.stringToTimestamp("2018-08-25 00:06:07")
    val date0 = "20180825"
    val date1 = "20180826"

    val expectedMap: Map[String, String] = Map(
      date0 -> "",
      date1 -> "1")
    Dates.getFuturePlanningDaysMap(ts,"", "Europe/Paris", 2) shouldBe expectedMap
  }


  it must " return a map with no results " taggedAs (UnitTestTag) in {
    val ts = Dates.stringToTimestamp("2018-08-25 00:06:07")

    val expectedMap: Map[String, String] = Map.empty
    Dates.getFuturePlanningDaysMap(ts,"-", "Europe/Paris", 0) shouldBe expectedMap
  }
}
