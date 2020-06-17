package com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.inputs

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.BaseEntitiesImplicits
import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{Audit, SurtidoInterface}
import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import org.apache.spark.sql.{DataFrame, Dataset}

object Nst {

  object ccTags {
    val idArticleTag = "idArticle"
    val idStoreTag = "idStore"
    val nstTag = "nst"
    val tsNstTag = "tsNst"
  }

  object dbTags {
    val idArticleTag = "matnr"
    val idStoreTag = "werks"
    val nstTag = "nst"
    val tsNstTag = "ts_nst"
  }

  object implicits extends NstImplicits

  trait NstImplicits extends BaseEntitiesImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.idArticleTag -> ccTags.idArticleTag,
      dbTags.idStoreTag -> ccTags.idStoreTag,
      dbTags.nstTag -> ccTags.nstTag,
      dbTags.tsNstTag -> ccTags.tsNstTag,
      Audit.dbTags.tsInsertDlkTag -> Audit.ccTags.tsInsertDlkTag,
      Audit.dbTags.tsUpdateDlkTag -> Audit.ccTags.tsUpdateDlkTag
    )

    private lazy val toCCAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)

    implicit def readFromKuduTablesNst(implicit kuduCon: String): FromDbTable[Nst, Kudu] = {
      new FromKuduTable[Nst] {
        override def fromDB: DataFrame => Dataset[Nst] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.idArticleTag,
            dbTags.idStoreTag,
            dbTags.nstTag,
            dbTags.tsNstTag,
            Audit.dbTags.tsInsertDlkTag,
            Audit.dbTags.tsUpdateDlkTag).transform(toCCAgg andThen parseLastUpdate).as[Nst]
        }

        override val schemaName: String = "supply_user"
        override val tableName: String = "supply_nst"

        override def host: String = kuduCon
      }
    }

  }

}

case class Nst(
  idArticle: String,
  idStore: String,
  nst: Double,
  tsNst: Timestamp, // Timestamp from validity
  tsUpdateDlk: Timestamp) extends SurtidoInterface

case class NstView(
  idArticle: String,
  idStore: String,
  nst: Double,
  tsUpdateDlk: Timestamp) extends SurtidoInterface


