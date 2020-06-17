package com.training.bigdata.omnichannel.stockATP.common.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DoubleType

object FieldsUtils {

  def generateNFieldsSequence(baseColName: String, lastIndex: Int, firstIndex: Int = 0): Seq[String] = {
    require(lastIndex >= 0, "last index must be non negative")

    firstIndex to lastIndex map (
      day =>
        if (day == 0)
          baseColName
        else baseColName + day)
  }

  def generateNFieldsSequenceGroupedByPreviousAndActual(baseColName: String, lastIndex: Int, firstIndex: Int = 0): Seq[(String, String)] = {
    require(lastIndex >= 0, "last index must be non negative")
    val fieldsFromPreviousDayToN = FieldsUtils.generateNFieldsSequence(baseColName, lastIndex, firstIndex)
    val fieldsFromDayN = FieldsUtils.generateNFieldsSequence(baseColName, lastIndex, firstIndex + 1)

    fieldsFromPreviousDayToN zip fieldsFromDayN
  }

  def zip3(seq1: Seq[String], seq2: Seq[String], seq3: Seq[(String, String)])
    : Seq[(String, String, String, String)] = {

    seq1 zip seq2 zip seq3 map {
      case ((a, b), (c, d)) => (a, b, c, d)
    }

  }

  def renamePivotedColumns(forecastDays: Map[String, String], baseColName: String): DataFrame => DataFrame = {

    df =>
      forecastDays.keys.foldLeft(df)((accDF, day) =>
          accDF.withColumnRenamed(day, baseColName + forecastDays.getOrElse(day, null))
      )
  }

}
