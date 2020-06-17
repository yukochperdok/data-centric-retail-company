package com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs

import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{Audit, SurtidoInterface}
import org.apache.spark.sql.{DataFrame, Dataset}

object StockConsolidado {

  object ccTags {
    val idArticleTag = "idArticle"
    val idStoreTag = "idStore"
    val stockTypeTag = "stockType"
    val unitMeasureTag = "unitMeasure"
    val stockDispTag = "stockDisp"
  }

  object dbTags {
    val idArticleTag = "matnr"
    val idStoreTag = "werks"
    val stockTypeTag = "stock_type"
    val unitMeasureTag = "unit_measure"
    val amountTag = "amount"
  }


  object implicits extends StockConsolidadoImplicits

  trait StockConsolidadoImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.idArticleTag -> ccTags.idArticleTag,
      dbTags.idStoreTag -> ccTags.idStoreTag,
      dbTags.stockTypeTag -> ccTags.stockTypeTag,
      dbTags.unitMeasureTag -> ccTags.unitMeasureTag,
      dbTags.amountTag -> ccTags.stockDispTag,
      Audit.dbTags.tsUpdateDlkTag -> Audit.ccTags.tsUpdateDlkTag
    )

    private lazy val toCCAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)
    private lazy val toDBAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._2, t._1) }).reduce(_ andThen _)

    implicit def readFromKuduTablesStockConsolidado(implicit kuduCon: String): FromDbTable[StockConsolidado, Kudu] = {
      new FromKuduTable[StockConsolidado] {
        override def fromDB: DataFrame => Dataset[StockConsolidado] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.idArticleTag,
            dbTags.idStoreTag,
            dbTags.stockTypeTag,
            dbTags.unitMeasureTag,
            dbTags.amountTag,
            Audit.dbTags.tsUpdateDlkTag
          ).transform(toCCAgg).as[StockConsolidado]
        }

        override val schemaName: String = "impala::supply_stream"
        override val tableName: String = "supply_stock_type"

        override def host: String = kuduCon
      }
    }
    implicit def writeToKuduTablesStockConsolidado(implicit kuduCon: String): ToDbTable[StockConsolidado, Kudu] = {
      new ToKuduTable[StockConsolidado] {

        override def toDB: Dataset[StockConsolidado] => DataFrame = {
          ds => {
            ds.toDF().transform(toDBAgg)
          }
        }

        override val schemaName: String = "impala::supply_stream"
        override val tableName: String = "supply_stock_type"

        override def host: String = kuduCon

      }
    }

  }

}

case class StockConsolidado(
  override val idArticle: String,
  override val idStore: String,
  val stockType: Int,
  val unitMeasure: String,
  val stockDisp: Option[Double],
  val tsUpdateDlk: Timestamp) extends SurtidoInterface

case class StockConsolidadoView(
  override val idArticle: String,
  override val idStore: String,
  val unitMeasure: String,
  val stockDisp: Double,
  val tsUpdateDlk: Timestamp) extends SurtidoInterface