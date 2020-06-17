package com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{Audit, SurtidoInterface}
import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import org.apache.spark.sql.{DataFrame, Dataset}

object SaleMovements {

  object ccTags {
    val idArticleTag = "idArticle"
    val idStoreTag = "idStore"
    val amountTag = "amount"
    val movementDateTag = "movementDate"
    val movementCodeTag = "movementCode"
  }

  object dbTags {
    val idArticleTag = "matnr"
    val idStoreTag = "werks"
    val amountTag = "amount"
    val movementDateTag = "movement_date"
    val movementCodeTag = "cod_mov"
  }


  object implicits extends SaleMovementsImplicits

  trait SaleMovementsImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.idArticleTag -> ccTags.idArticleTag,
      dbTags.idStoreTag -> ccTags.idStoreTag,
      dbTags.amountTag -> ccTags.amountTag,
      dbTags.movementDateTag -> ccTags.movementDateTag,
      dbTags.movementCodeTag -> ccTags.movementCodeTag,
      Audit.dbTags.tsUpdateDlkTag -> Audit.ccTags.tsUpdateDlkTag
    )

    private lazy val toCCAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)

    implicit def readFromKuduTablesSaleMovements(implicit kuduCon: String): FromDbTable[SaleMovements, Kudu] = {
      new FromKuduTable[SaleMovements] {
        override def fromDB: DataFrame => Dataset[SaleMovements] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.idArticleTag,
            dbTags.idStoreTag,
            dbTags.movementDateTag,
            dbTags.amountTag,
            dbTags.movementCodeTag,
            Audit.dbTags.tsUpdateDlkTag
          ).transform(toCCAgg).as[SaleMovements]
        }

        override val schemaName: String = "impala::supply_stream"
        override val tableName: String = "stock_atp_movements"

        override def host: String = kuduCon
      }
    }
  }

}

case class SaleMovements(
  override val idArticle: String,
  override val idStore: String,
  val movementDate: Timestamp,
  val movementCode: String,
  val amount: Double,
  val tsUpdateDlk: Timestamp) extends SurtidoInterface

case class SaleMovementsView(
  override val idArticle: String,
  override val idStore: String,
  val amount: Double,
  val tsUpdateDlk: Timestamp) extends SurtidoInterface
