package com.training.bigdata.mdata.INEaddresses.common.util

import com.training.bigdata.mdata.INEaddresses.common.entities.interfaces.Audit
import com.training.bigdata.mdata.INEaddresses.common.util.log.FunctionalLogger
import com.training.bigdata.mdata.INEaddresses.common.util.io.Tables._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object TablesUtils extends FunctionalLogger {

  def insertInKuduWithCorrectAuditFields(
    df: DataFrame,
    table: String)(implicit spark: SparkSession, kuduHost: String): Try[Boolean] = {
    import com.training.bigdata.mdata.INEaddresses.common.util.io.Tables.Implementations.KuduDs

    for {
      _ <- df.cache.writeTo[Kudu](table, kuduHost, OperationType.InsertIgnoreRows)
      isOkUpdate <- df.drop(Audit.tsInsertDlk, Audit.userInsertDlk).writeTo[Kudu](table, kuduHost, OperationType.Update)
    } yield isOkUpdate
  }

  def deleteOldData(
    persistedDF: DataFrame,
    newDF: DataFrame,
    pkFields: Seq[String],
    table: String)(implicit spark: SparkSession, kuduHost: String): Try[Boolean] = {

    import com.training.bigdata.mdata.INEaddresses.common.util.io.Tables.Implementations.KuduDs

    val rowsToDelete: DataFrame = persistedDF
      .select(pkFields.head, pkFields.tail: _*)
      .join(newDF, pkFields, "leftanti")

    rowsToDelete.deleteFrom[Kudu](table, kuduHost)
  }
  
  def overwriteInKuduWithCorrectAuditFields(
    newDF: DataFrame,
    persistedDF: DataFrame,
    table: String,
    pkFields: Seq[String])(implicit spark: SparkSession, kuduHost: String): Try[Boolean] = {

    insertInKuduWithCorrectAuditFields(newDF, table)
    deleteOldData(persistedDF, newDF, pkFields, table)
  }
}
