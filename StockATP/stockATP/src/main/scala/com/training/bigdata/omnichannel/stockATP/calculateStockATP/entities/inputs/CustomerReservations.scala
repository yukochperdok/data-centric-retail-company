package com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{Audit, SurtidoInterface}
import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import org.apache.spark.sql.{DataFrame, Dataset}

object CustomerReservations {

  object ccTags {
    val idArticleTag = "idArticle"
    val idStoreTag = "idStore"
    val amountTag = "amount"
    val reservationDateTag = "reservationDate"
    val amountNTag = "amountReservationsN"
  }

  object dbTags {
    val idArticleTag = "matnr"
    val idStoreTag = "werks"
    val amountTag = "amount"
    val reservationDateTag = "reservation_date"
  }


  object implicits extends CustomerReservationsImplicits

  trait CustomerReservationsImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.idArticleTag -> ccTags.idArticleTag,
      dbTags.idStoreTag -> ccTags.idStoreTag,
      dbTags.amountTag -> ccTags.amountTag,
      dbTags.reservationDateTag -> ccTags.reservationDateTag,
      Audit.dbTags.tsUpdateDlkTag -> Audit.ccTags.tsUpdateDlkTag
    )

    private lazy val toCCAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)

    implicit def readFromKuduTablesCustomerReservations(implicit kuduCon: String): FromDbTable[CustomerReservations, Kudu] = {
      new FromKuduTable[CustomerReservations] {
        override def fromDB: DataFrame => Dataset[CustomerReservations] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.idArticleTag,
            dbTags.idStoreTag,
            dbTags.reservationDateTag,
            dbTags.amountTag,
            Audit.dbTags.tsUpdateDlkTag
          ).transform(toCCAgg).as[CustomerReservations]
        }

        override val schemaName: String = "impala::omnichannel_user"
        override val tableName: String = "customer_orders_reservations"

        override def host: String = kuduCon
      }
    }
  }
}

case class CustomerReservations(
   override val idArticle: String,
   override val idStore: String,
   reservationDate: Option[Timestamp],
   amount: Option[Double],
   tsUpdateDlk: Timestamp) extends SurtidoInterface

case class CustomerReservationsView(
   override val idArticle: String,
   override val idStore: String,
   reservationDate: String,
   amount: Double,
   tsUpdateDlk: Timestamp) extends SurtidoInterface
