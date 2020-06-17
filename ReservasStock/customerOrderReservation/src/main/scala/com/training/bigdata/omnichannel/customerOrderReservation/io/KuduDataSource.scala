package com.training.bigdata.omnichannel.customerOrderReservation.io

import com.training.bigdata.omnichannel.customerOrderReservation.entities.ArticleTranscod._
import com.training.bigdata.omnichannel.customerOrderReservation.entities.ReservationActions._
import com.training.bigdata.omnichannel.customerOrderReservation.entities.SiteTranscod._
import com.training.bigdata.omnichannel.customerOrderReservation.entities.{ArticleTranscod, ReservationActions, SiteTranscod}
import com.training.bigdata.omnichannel.customerOrderReservation.utils.Constants.Sql
import com.training.bigdata.omnichannel.customerOrderReservation.utils.{Constants, PropertiesUtil}
import com.training.bigdata.omnichannel.customerOrderReservation.utils.log.FunctionalLogger
import org.apache.kudu.spark.kudu.{KuduContext, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

object KuduDataSource {

  def apply(implicit sparkSession: SparkSession, kuduContext: KuduContext, propertiesUtil: PropertiesUtil): KuduDataSource = {
    initCreateViews
    new KuduDataSource
  }

  def initCreateViews(implicit sparkSession: SparkSession, kuduContext: KuduContext, propertiesUtil: PropertiesUtil): Unit = {
    val dfArticleTranscod: DataFrame = sparkSession.read.options(
      Map(
        "kudu.master" -> propertiesUtil.getKuduHosts,
        "kudu.table" -> propertiesUtil.getArticlesTranscodingTable)).kudu
    dfArticleTranscod.createOrReplaceTempView(ARTICLE_TRANSCOD_VIEW)

    val dfSiteTranscod: DataFrame = sparkSession.read.options(
      Map(
        "kudu.master" -> propertiesUtil.getKuduHosts,
        "kudu.table" -> propertiesUtil.getStoresTranscodingTable)).kudu
    dfSiteTranscod.createOrReplaceTempView(SITE_TRANSCOD_VIEW)

    val dfreservationActions: DataFrame = sparkSession.read.options(
      Map(
        "kudu.master" -> propertiesUtil.getKuduHosts,
        "kudu.table" -> propertiesUtil.getCustomerOrdersReservationActionsTable)).kudu
    dfreservationActions.createOrReplaceTempView(RESERVATION_ACTIONS_VIEW)
  }
}

class KuduDataSource(implicit sparkSession: SparkSession, val kuduContext: KuduContext) extends Serializable
  with FunctionalLogger {

  def upsertToKudu(dfToPersist: DataFrame, table: String): Unit = {
    Try(kuduContext.upsertRows(dfToPersist, table)) match {
      case Failure(ex) =>
        logNonFunctionalMessage(s"Error to insert DataFrame in Kudu table $table, ${ex.getMessage} ${ex.getStackTrace.mkString("\n")}")
      case Success(_) =>
        logInfo(s"All rows are upserted on $table")
    }
  }

  def deleteFromKudu(dfToDelete: DataFrame, table: String): Unit = {
    Try(kuduContext.deleteRows(dfToDelete, table)) match {
      case Failure(ex) =>
        if(ex.getMessage.contains(Constants.KUDU_DELETE_NON_EXISTENT_PK_ERROR))
          logInfo(s"Some keys could not be found when deleting, but process continues to execute normally")
        else
          logNonFunctionalMessage(s"Error to delete DataFrame from Kudu table $table, ${ex.getMessage}")
      case Success(_) =>
        logInfo(s"All rows are deleted from $table")
    }
  }

  def getArticleTranscod(tableName: String): DataFrame =
    sparkSession
      .sql(
        s"select distinct ${ArticleTranscod.selectedFields.mkString(",")} " +
          s"from $tableName " +
          s"${getClasueWhereIfApplied(ArticleTranscod.whereClause)}")

  def getSiteTranscod(tableName: String): DataFrame =
    sparkSession
      .sql(
        s"select distinct ${SiteTranscod.selectedFields.mkString(",")} " +
          s"from $tableName " +
          s"${getClasueWhereIfApplied(SiteTranscod.whereClause)}")

  def getReservationActions(tableName: String): DataFrame =
    sparkSession
      .sql(
        s"select ${ReservationActions.selectedFields.mkString(",")} " +
          s"from $tableName " +
          s"${getClasueWhereIfApplied(ReservationActions.whereClause)}")

  private def getClasueWhereIfApplied(whereClause: String, tpye: String = Sql.WHERE): String =
    if (whereClause.isEmpty) "" else s"$tpye $whereClause"

}
