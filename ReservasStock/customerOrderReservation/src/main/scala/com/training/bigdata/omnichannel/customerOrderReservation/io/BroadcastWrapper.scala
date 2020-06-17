package com.training.bigdata.omnichannel.customerOrderReservation.io

import com.training.bigdata.omnichannel.customerOrderReservation.utils.log.FunctionalLogger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.commons.lang3.StringUtils.stripAccents

object BroadcastWrapper extends FunctionalLogger {

  private var broadcastArticleTranscodMap: Option[Broadcast[Map[String, String]]] = None
  private var lastUpdatedAtTranscod: Long = System.currentTimeMillis
  private var broadcastSiteTranscodMap: Option[Broadcast[Map[String, String]]] = None
  private var broadcastActionsMap: Option[Broadcast[Map[(String, String), (String, String)]]] = None

  def getTablesBroadcastAndUpdateThemIfExpired(kuduDataSource: KuduDataSource,
                                               articleTranscodTableName: String,
                                               siteTranscodTableName: String,
                                               reservationActionsTableName: String,
                                               millisToRefresh: Long)
                                              (implicit sparkSession: SparkSession): (Broadcast[Map[String, String]], Broadcast[Map[String, String]], Broadcast[Map[(String, String), (String, String)]]) = {

    val currentTimestamp = System.currentTimeMillis
    val diff: Long = currentTimestamp - lastUpdatedAtTranscod

    if (broadcastArticleTranscodMap.isEmpty || broadcastSiteTranscodMap.isEmpty || broadcastActionsMap.isEmpty || diff > millisToRefresh) {
      lastUpdatedAtTranscod = System.currentTimeMillis

      logInfo(s"Creating/Updating BroadCast dictionary from table $articleTranscodTableName")
      if (broadcastArticleTranscodMap.isDefined) broadcastArticleTranscodMap.get.unpersist(true)
      val articleTranscodDictionary: Map[String, String] =
        getDictionaryFromTranscodDataFrame(
          sparkSession,
          kuduDataSource.getArticleTranscod(articleTranscodTableName),
          articleTranscodTableName)
      broadcastArticleTranscodMap = Some(sparkSession.sparkContext.broadcast(articleTranscodDictionary))

      logInfo(s"Creating/Updating BroadCast dictionary from table $siteTranscodTableName")
      if (broadcastSiteTranscodMap.isDefined) broadcastSiteTranscodMap.get.unpersist(true)
      val siteTranscodDictionary: Map[String, String] =
        getDictionaryFromTranscodDataFrame(
          sparkSession,
          kuduDataSource.getSiteTranscod(siteTranscodTableName),
          siteTranscodTableName)
      broadcastSiteTranscodMap = Some(sparkSession.sparkContext.broadcast(siteTranscodDictionary))

      logInfo(s"Creating/Updating BroadCast dictionary from table $reservationActionsTableName")
      if (broadcastActionsMap.isDefined) broadcastActionsMap.get.unpersist(true)
      val actionsDictionary: Map[(String, String), (String, String)] =
        getDictionaryFromConfigDataFrame(
          sparkSession,
          kuduDataSource.getReservationActions(reservationActionsTableName),
          reservationActionsTableName)
      broadcastActionsMap = Some(sparkSession.sparkContext.broadcast(actionsDictionary))

    }
    (broadcastArticleTranscodMap.get, broadcastSiteTranscodMap.get, broadcastActionsMap.get)
  }

  private def getDictionaryFromTranscodDataFrame(
                                  sparkSession: SparkSession,
                                  df: DataFrame,
                                  tableName: String): Map[String, String] = {

    val catalog: List[Row] = getCatalog(df, tableName)
    catalog.map(row => (row.getString(0), row.getString(1))).toMap
  }

  private def getDictionaryFromConfigDataFrame(
                                                sparkSession: SparkSession,
                                                df: DataFrame,
                                                tableName: String): Map[(String, String), (String, String)] = {

    val catalog: List[Row] = getCatalog(df, tableName)
    catalog.map(
      row =>( (row.getString(0), stripAccents(row.getString(1).toUpperCase)), (row.getString(2), row.getString(3)) ) //site_id, order_status, sherpa_order_status, action
    ).toMap
  }

  private def getCatalog(df: DataFrame, tableName: String): List[Row] = {
    val catalog: List[Row] = df.collect().toList

    logInfo(s"Get catalog from $tableName Number of elements in catalog: ${catalog.length}")
    if(catalog.isEmpty) logAlert(s"Not found data on table: [$tableName]")
    catalog
  }

}


