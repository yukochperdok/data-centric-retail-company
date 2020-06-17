package com.training.bigdata.omnichannel.customerOrderReservation.utils

import java.sql.Timestamp
import com.training.bigdata.omnichannel.customerOrderReservation.entities.CustomerOrdersReservations
import com.training.bigdata.omnichannel.customerOrderReservation.utils.log.FunctionalLogger
import com.training.bigdata.omnichannel.customerOrderReservation.utils.Constants.Errors._
import com.training.bigdata.omnichannel.customerOrderReservation.entities.EcommerceOrder._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.commons.lang3.StringUtils.stripAccents

object OperationsUtils extends FunctionalLogger {

  def filterNullsAndLog(column: String, message: String): DataFrame => DataFrame = {

    inputDF: DataFrame =>
      inputDF.filter { row =>
        if (row.getAs[String](column) == null) {
          logAlert(s"$message$row")
          false
        } else true
      }
  }

  val udfLogAndReturnTimestamp: UserDefinedFunction =
    udf[Timestamp, String, Timestamp] {
      (message: String, defaultValue: Timestamp) => {
        logAlert(message)
        defaultValue
      }
    }

  def transcodArticleAndSite(articleCol: String,
                             siteCol: String,
                             articleTranscodMap: Broadcast[Map[String, String]],
                             siteTranscodMap: Broadcast[Map[String, String]]): DataFrame => DataFrame = {

    val transcodArticleAndLogNull =
      udf[String, String, String] {
        (key: String, message: String) =>
          Utils
            .logAlertWhenNoneAndReturn(
              articleTranscodMap.value.get(key),
              s"$message$key").orNull
      }

    val transcodSiteAndLogNull =
      udf[String, String, String] {
        (key: String, message: String) =>
          Utils
            .logAlertWhenNoneAndReturn(
              siteTranscodMap.value.get(key),
              s"$message$key").orNull
      }

    inputDF =>
      inputDF
        .withColumn(
          articleCol,
          transcodArticleAndLogNull(col(articleCol), lit(ARTICLE_NOT_IN_TRANSCOD))
        )
        .withColumn(
          siteCol,
          transcodSiteAndLogNull(col(siteCol), lit(STORE_NOT_IN_TRANSCOD))
        )
        .filter(!isnull(col(articleCol)) && !isnull(col(siteCol)))

  }

  def separateByAction(reservationActions: Broadcast[Map[(String, String), (String, String)]], upsertAction: String, deleteAction: String) : DataFrame => (DataFrame, DataFrame) = df => {

    val action = "action"
    val tupleColumn = "tuple"
    val notFound = "notFound"

    val getActionAndSherpaStatus =
      udf {
        (siteId: String, orderStatus: String) => {
          val upperCaseWithoutAccent: String => String = site => if(site != null) stripAccents(site.toUpperCase) else site
          reservationActions.value.getOrElse((siteId, upperCaseWithoutAccent(orderStatus)), (notFound, notFound))
        }
      }

    val dfWithActionAndStatus = df
      .withColumn(
        tupleColumn,
        getActionAndSherpaStatus(col(siteId), col(orderState))
      )
      .selectExpr(
        "*",
        // Columns in tuple with correct name
        s"$tupleColumn._1 AS ${CustomerOrdersReservations.sherpaOrderStatus}",
        s"$tupleColumn._2 AS $action"
      )
      .filter(col(CustomerOrdersReservations.sherpaOrderStatus) =!= lit(notFound))
      .drop(tupleColumn, orderState)

    (
      dfWithActionAndStatus.where(col(action) === upsertAction).drop(action),
      dfWithActionAndStatus.where(col(action) === deleteAction).drop(action)
    )

  }

  def calculateAmountAndUnitOfMeasurement(implicit propertiesUtil: PropertiesUtil): DataFrame => DataFrame = df => {
    df
      .withColumn(
        CustomerOrdersReservations.amount,
        when(col(variableWeight).isNull || col(variableWeight) === lit(false),
          col(quantity)
        ).otherwise(
          col(grossWeight) / lit(1000)
        )
      )
      .withColumn(
        CustomerOrdersReservations.unitMeasure,
        when(col(variableWeight).isNull || col(variableWeight) === lit(false),
          lit(propertiesUtil.getEA)
        ).otherwise(
          lit(propertiesUtil.getKG)
        )
      )
      .filter(col(CustomerOrdersReservations.amount).isNotNull)
      .drop(grossWeight, variableWeight, quantity)
  }

  def addArticleBasedOnSite: DataFrame => DataFrame = df => {
    df
      .withColumn(
        article,
        when(col(siteId).isin(foodSite:_*),
          concat(col(smsId), lit("0000"))
        ).otherwise(
          col(smsSizeColor)
        )
      )
      .drop(smsId, smsSizeColor)
  }

}
