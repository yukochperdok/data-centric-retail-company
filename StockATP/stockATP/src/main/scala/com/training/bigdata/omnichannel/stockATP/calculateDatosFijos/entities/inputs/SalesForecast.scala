package com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.inputs

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.SurtidoInterface
import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import org.apache.spark.sql.{DataFrame, Dataset}

object SalesForecast {

  object ccTags {
    val dateSaleTag = "dateSale"
    val idArticleTag = "idArticle"
    val idStoreTag = "idStore"
    val regularSalesForecastTag = "regularSalesForecast"
    val promoSalesForecastTag = "promoSalesForecast"
    val salesForecastNTag = "salesForecastN"
  }

  object dbTags {
    val dateSaleTag = "date_sale"
    val idArticleTag = "matnr"
    val idStoreTag = "werks"
    val regularSalesForecastTag = "regular_sales_forecast"
    val promoSalesForecastTag = "promo_sales_forecast"
  }

  object implicits extends SalesForecastImplicits

  trait SalesForecastImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.dateSaleTag -> ccTags.dateSaleTag,
      dbTags.idArticleTag -> ccTags.idArticleTag,
      dbTags.regularSalesForecastTag -> ccTags.regularSalesForecastTag,
      dbTags.promoSalesForecastTag -> ccTags.promoSalesForecastTag,
      dbTags.idStoreTag -> ccTags.idStoreTag
    )

    private lazy val toCCAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)

    implicit def readFromKuduTablesSalesForecast(implicit kuduCon: String): FromDbTable[SalesForecast, Kudu] = {
      new FromKuduTable[SalesForecast] {
        override def fromDB: DataFrame => Dataset[SalesForecast] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.idStoreTag,
            dbTags.dateSaleTag,
            dbTags.idArticleTag,
            dbTags.regularSalesForecastTag,
            dbTags.promoSalesForecastTag).transform(toCCAgg).as[SalesForecast]
        }

        override val schemaName: String = "forecast_user"
        override val tableName: String = "forecast_sales_forecast"

        override def host: String = kuduCon
      }
    }

  }

}

case class SalesForecast(
  idStore: String,
  dateSale: Timestamp,
  idArticle: String,
  regularSalesForecast: Option[Double],
  promoSalesForecast: Option[Double]) extends SurtidoInterface

case class SalesForecastView(
  dateSale: String,
  idArticle: String,
  regularSalesForecast: Double,
  idStore: String) extends SurtidoInterface

