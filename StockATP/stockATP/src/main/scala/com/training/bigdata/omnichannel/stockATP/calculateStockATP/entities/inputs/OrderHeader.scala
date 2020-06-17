package com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.Audit
import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import org.apache.spark.sql.{DataFrame, Dataset}

object OrderHeader {

  object ccTags {
    val idOrderTag = "idOrder"
    val orderTypeTag = "orderType"
  }

  object dbTags {
    val idOrderTag = "ebeln"
    val orderTypeTag = "bsart"
  }

  object implicits extends OrderHeaderImplicits

  trait OrderHeaderImplicits{
    private val db2cc: List[(String, String)] = List(
      dbTags.idOrderTag -> ccTags.idOrderTag,
      dbTags.orderTypeTag -> ccTags.orderTypeTag,
      Audit.dbTags.tsUpdateDlkTag -> Audit.ccTags.tsUpdateDlkTag
    )

    private lazy val toCCAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)
    private lazy val toDBAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._2, t._1) }).reduce(_ andThen _)

    implicit def readFromKuduTablesOrderHeader(implicit kuduCon: String): FromDbTable[OrderHeader, Kudu] = {
      new FromKuduTable[OrderHeader] {
        override def fromDB: DataFrame => Dataset[OrderHeader] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.idOrderTag,
            dbTags.orderTypeTag,
            Audit.dbTags.tsUpdateDlkTag).transform(toCCAgg).as[OrderHeader]
        }

        override val schemaName: String = "supply_stream"
        override val tableName: String = "ekko"

        override def host: String = kuduCon
      }
    }

  }

}

case class OrderHeader(
  idOrder: String,
  orderType: String,
  tsUpdateDlk: Timestamp)
