package com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.inputs

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.BaseEntitiesImplicits
import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{Audit, SurtidoInterface}
import com.training.bigdata.omnichannel.stockATP.common.io.tables.{FromDbTable, FromHiveTable, FromKuduTable, Kudu}
import org.apache.spark.sql.{DataFrame, Dataset}


object SalesPlatform {

  object ccTags {
    val dateSaleTag = "dateSale"
    val idArticleTag = "idArticle"
    val idStoreTag = "idStore"
    val regularSalesForecastTag = "regularSalesForecast"
  }

  object dbTags {
    val dateSaleTag = "date_sale"
    val idArticleTag = "matnr"
    val idStoreTag = "werks"
    val regularSalesForecastTag = "regular_sales_forecast"
  }

  object implicits extends SalesPlatformImplicits

  trait SalesPlatformImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.dateSaleTag -> ccTags.dateSaleTag,
      dbTags.idArticleTag -> ccTags.idArticleTag,
      dbTags.idStoreTag -> ccTags.idStoreTag,
      dbTags.regularSalesForecastTag -> ccTags.regularSalesForecastTag
    )

    private lazy val toCCAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)

    implicit def readFromHiveTablesSalesPlatform: FromHiveTable[SalesPlatform] = {
      new FromHiveTable[SalesPlatform] {
        override def fromDB: DataFrame => Dataset[SalesPlatform] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.dateSaleTag,
            dbTags.idArticleTag,
            dbTags.idStoreTag,
            dbTags.regularSalesForecastTag).transform(toCCAgg).as[SalesPlatform]
        }

        override val schemaName: String = "forecast_last"
        override val tableName: String = "forecast_sales_platform"

      }
    }
  }

}

case class SalesPlatform(
  dateSale: Timestamp,
  idArticle: String,
  idStore: String,
  regularSalesForecast: Option[Double]) extends SurtidoInterface


