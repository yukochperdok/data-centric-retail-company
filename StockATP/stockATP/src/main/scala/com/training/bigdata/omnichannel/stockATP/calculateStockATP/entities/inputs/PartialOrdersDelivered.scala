package com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.BaseEntitiesImplicits
import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{Audit, OrderInterface}
import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import org.apache.spark.sql.{DataFrame, Dataset}

object PartialOrdersDelivered {

  object ccTags {
    val idOrderTag = "idOrder"
    val idOrderLineTag = "idOrderLine"
    val partialOrderNumberTag = "partialOrderNumber"
    val wholePartialOrderAmountTag = "wholePartialOrderAmount"
    val deliveredAmountTag = "deliveredAmount"
    val deliveryDateTsTag = "deliveryDateTs"
  }

  object dbTags {
    val idOrderTag = "ebeln"
    val idOrderLineTag = "ebelp"
    val partialOrderNumberTag = "etenr"
    val wholePartialOrderAmountTag = "menge"
    val deliveredAmountTag = "wemng"
    val deliveryDateTsTag = "eindt"
  }

  object implicits extends PartialOrdersDeliveredImplicits

  trait PartialOrdersDeliveredImplicits extends BaseEntitiesImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.idOrderTag -> ccTags.idOrderTag,
      dbTags.idOrderLineTag -> ccTags.idOrderLineTag,
      dbTags.partialOrderNumberTag -> ccTags.partialOrderNumberTag,
      dbTags.wholePartialOrderAmountTag -> ccTags.wholePartialOrderAmountTag,
      dbTags.deliveredAmountTag -> ccTags.deliveredAmountTag,
      dbTags.deliveryDateTsTag -> ccTags.deliveryDateTsTag,
      Audit.dbTags.tsUpdateDlkTag -> Audit.ccTags.tsUpdateDlkTag
    )

    private lazy val toCCAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)

    implicit def readFromKuduTablesPartialOrders(implicit kuduCon: String): FromDbTable[PartialOrdersDelivered, Kudu] = {
      new FromKuduTable[PartialOrdersDelivered] {
        override def fromDB: DataFrame => Dataset[PartialOrdersDelivered] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.idOrderTag,
            dbTags.idOrderLineTag,
            dbTags.partialOrderNumberTag,
            dbTags.wholePartialOrderAmountTag,
            dbTags.deliveredAmountTag,
            dbTags.deliveryDateTsTag,
            Audit.dbTags.tsUpdateDlkTag).transform(toCCAgg).as[PartialOrdersDelivered]
        }

        override val schemaName: String = "supply_stream"
        override val tableName: String = "eket"

        override def host: String = kuduCon
      }
    }

  }

}

case class PartialOrdersDelivered (
  override val idOrder: String,
  override val idOrderLine: String,
  partialOrderNumber: String,
  wholePartialOrderAmount: Option[Double],
  deliveredAmount: Option[Double],
  deliveryDateTs: String,
  tsUpdateDlk: Timestamp) extends OrderInterface

case class PartialOrdersDeliveredView (
  override val idOrder: String,
  override val idOrderLine: String,
  partialOrderNumber: String,
  wholePartialOrderAmount: Double,
  deliveredAmount: Double,
  deliveryDateTs: Timestamp,
  tsUpdateDlk: Timestamp) extends OrderInterface
