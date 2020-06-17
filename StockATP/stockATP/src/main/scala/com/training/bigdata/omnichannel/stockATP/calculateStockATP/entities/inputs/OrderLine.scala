package com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.BaseEntitiesImplicits
import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces._
import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import org.apache.spark.sql.{DataFrame, Dataset}

object OrderLine {

  object ccTags {
    val idOrderTag = "idOrder"
    val idOrderLineTag = "idOrderLine"
    val idArticleTag = "idArticle"
    val idStoreTag = "idStore"
    val canceledOrderFlagTag = "canceledOrderFlag"
    val finalDeliveryFlagTag = "finalDeliveryFlag"
    val returnedToProviderFlagTag = "returnedToProviderFlag"
  }

  object dbTags {
    val idOrderTag = "ebeln"
    val idOrderLineTag = "ebelp"
    val idArticleTag = "matnr"
    val idStoreTag = "werks"
    val canceledOrderFlagTag = "loekz"
    val finalDeliveryFlagTag = "elikz"
    val returnedToProviderFlagTag = "retpo"
  }

  object implicits extends OrderLineImplicits

  trait OrderLineImplicits extends BaseEntitiesImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.idOrderTag -> ccTags.idOrderTag,
      dbTags.idOrderLineTag -> ccTags.idOrderLineTag,
      dbTags.idArticleTag -> ccTags.idArticleTag,
      dbTags.idStoreTag -> ccTags.idStoreTag,
      dbTags.canceledOrderFlagTag -> ccTags.canceledOrderFlagTag,
      dbTags.finalDeliveryFlagTag -> ccTags.finalDeliveryFlagTag,
      dbTags.returnedToProviderFlagTag -> ccTags.returnedToProviderFlagTag,
      Audit.dbTags.tsUpdateDlkTag -> Audit.ccTags.tsUpdateDlkTag
    )

    private lazy val toCCAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)

    implicit def readFromKuduTablesOrderLines(implicit kuduCon: String): FromDbTable[OrderLine, Kudu] = {
      new FromKuduTable[OrderLine] {
        override def fromDB: DataFrame => Dataset[OrderLine] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.idOrderTag,
            dbTags.idOrderLineTag,
            dbTags.idArticleTag,
            dbTags.idStoreTag,
            dbTags.canceledOrderFlagTag,
            dbTags.finalDeliveryFlagTag,
            dbTags.returnedToProviderFlagTag,
            Audit.dbTags.tsUpdateDlkTag).transform(toCCAgg).as[OrderLine]
        }

        override val schemaName: String = "supply_stream"
        override val tableName: String = "ekpo"

        override def host: String = kuduCon
      }
    }

  }

}

case class OrderLine(
  override val idOrder: String,
  override val idOrderLine: String,
  override val idArticle: String,
  override val idStore: String,
  canceledOrderFlag: String,
  finalDeliveryFlag: String,
  returnedToProviderFlag: String,
  tsUpdateDlk: Timestamp) extends OrderInterface with SurtidoInterface

case class OrderLineView(
  override val idOrder: String,
  override val idOrderLine: String,
  override val idArticle: String,
  override val idStore: String,
  tsUpdateDlk: Timestamp) extends OrderInterface with SurtidoInterface
