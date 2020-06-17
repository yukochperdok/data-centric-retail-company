package com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.inputs

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.BaseEntitiesImplicits
import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{Audit, SurtidoInterface}
import org.apache.spark.sql.{DataFrame, Dataset}

object Nsp {

  object ccTags {
    val idArticleTag = "idArticle"
    val idStoreTag = "idStore"
    val lifnrTag = "lifnr"    // Account Number of Vendor or Creditor
    val nspTag = "nsp"
    val tsNspTag = "tsNsp"    // Date from validity
  }

  object dbTags {
    val idArticleTag = "matnr"
    val idStoreTag = "werks"
    val lifnrTag = "lifnr"
    val nspTag = "nsp"
    val tsNspTag = "ts_nsp"
  }

  object implicits extends NspImplicits

  trait NspImplicits extends BaseEntitiesImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.idArticleTag -> ccTags.idArticleTag,
      dbTags.idStoreTag -> ccTags.idStoreTag,
      dbTags.nspTag -> ccTags.nspTag,
      dbTags.tsNspTag -> ccTags.tsNspTag,
      Audit.dbTags.tsInsertDlkTag -> Audit.ccTags.tsInsertDlkTag,
      Audit.dbTags.tsUpdateDlkTag -> Audit.ccTags.tsUpdateDlkTag
    )

    private lazy val toCCAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)

    implicit def readFromKuduTablesNsp(implicit kuduCon: String): FromDbTable[Nsp, Kudu] = {
      new FromKuduTable[Nsp] {
        override def fromDB: DataFrame => Dataset[Nsp] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.idArticleTag,
            dbTags.idStoreTag,
            dbTags.nspTag,
            dbTags.tsNspTag,
            Audit.dbTags.tsInsertDlkTag,
            Audit.dbTags.tsUpdateDlkTag).transform(toCCAgg andThen parseLastUpdate).as[Nsp]
        }

        override val schemaName: String = "supply_user"
        override val tableName: String = "supply_nsp"

        override def host: String = kuduCon
      }
    }

  }

}

case class Nsp(
  idArticle: String,
  idStore: String,
  nsp: Double,
  tsNsp: Timestamp, // Timestamp from validity
  tsUpdateDlk: Timestamp) extends SurtidoInterface

case class NspView(
  idArticle: String,
  idStore: String,
  nsp: Double,
  tsUpdateDlk: Timestamp) extends SurtidoInterface


