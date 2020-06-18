package com.training.bigdata.mdata.INEaddresses.common.util

import java.sql.Timestamp

import com.training.bigdata.mdata.INEaddresses.INEFilesIngestion.entities.StreetStretches
import com.training.bigdata.mdata.INEaddresses.common.entities.interfaces.{Audit, PartitioningFields}
import com.training.bigdata.mdata.INEaddresses.common.util.log.FunctionalLogger
import org.apache.spark.sql.{DataFrame, Row}
import Constants._


object OperationsUtils extends FunctionalLogger{

  def addAuditFields(user: String, ts: Timestamp): DataFrame => DataFrame = {
    import org.apache.spark.sql.functions._

    inputDF: DataFrame =>
      inputDF
        .withColumn(  Audit.tsInsertDlk,   lit(ts))
        .withColumn(Audit.userInsertDlk, lit(user))
        .withColumn(  Audit.tsUpdateDlk,   lit(ts))
        .withColumn(Audit.userUpdateDlk, lit(user))

  }

  def addPartitionFields(partitionFieldName: String): DataFrame => DataFrame = {
    import org.apache.spark.sql.functions._

    inputDF: DataFrame =>
      inputDF
        .withColumn(PartitioningFields.YEAR, col(partitionFieldName).substr(0, 4))
        .withColumn(PartitioningFields.MONTH, col(partitionFieldName).substr(6, 2))
  }


  def isStreetStretchesOlderThanMaxStreetMapInfo(
    streetStretchesDF: DataFrame,
    zipCodesStreetMapDF: DataFrame,
    dateFieldName: String = StreetStretches.INE_MODIFICATION_DATE): Boolean = {

    import org.apache.spark.sql.functions._

    val optRowMaxModificationDateOfZipCodesStreetMap: Option[Row] =
      zipCodesStreetMapDF
        .agg(max(dateFieldName))
        .collect()
        .headOption

    optRowMaxModificationDateOfZipCodesStreetMap match {

      case None =>
        logInfo("There are no rows in zipCodesStreetMap table")
        false

      case Some(rowMaxModificationDate) =>
        val tsMaxModificationDateOfZipCodesStreetMap = rowMaxModificationDate.getAs[Timestamp](0)
        logInfo(s"The maximum modification date in zipCodesStreetMap table is [$tsMaxModificationDateOfZipCodesStreetMap]")
        streetStretchesDF
          .filter(to_date(col(dateFieldName)) < to_date(lit(tsMaxModificationDateOfZipCodesStreetMap)))
          .count() > 0
    }
  }

  def groupByAndGetFirstValue(fieldsToGroupBy: Seq[String], selectedFields: Seq[String]): DataFrame => DataFrame = {
    import org.apache.spark.sql.functions._

    inputDF: DataFrame =>
      val firstFieldValues = selectedFields.map(x => first(x).as(x))
      inputDF
        .groupBy(fieldsToGroupBy.head, fieldsToGroupBy.tail:_*)
        .agg(firstFieldValues.head, firstFieldValues.tail:_*)
  }


  def innerJoinAndTraceLogsForRest(df: DataFrame, firstDfJoinField: String, secondDfJoinField: String, logMessage: String, duplicatedColumns: Seq[String] = Seq()): DataFrame => DataFrame = {

    inputDF: DataFrame =>

      inputDF
        .cache
        .join(df, inputDF(firstDfJoinField)===df(secondDfJoinField), LEFTANTI)
        .foreach(row =>  logInfo(s"${logMessage}: ${row.mkString(",")}"))

      val joinWithDuplicatedColumns = inputDF
        .join(df, inputDF(firstDfJoinField)===df(secondDfJoinField), INNER)
        .drop(df(secondDfJoinField))

      // drop duplicated columns
      duplicatedColumns.foldLeft(joinWithDuplicatedColumns)((accDf, duplicatedField) => accDf.drop(df(duplicatedField)))
  }

}
