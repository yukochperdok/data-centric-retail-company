package com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.inputs

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.SurtidoInterface
import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import org.apache.spark.sql.{DataFrame, Dataset}

object SurtidoMarc {

  val ACTIVO = "Z2"

  object ccTags {
    val idArticleTag = "idArticle"
    val idStoreTag = "idStore"
    val idStatusTag = "idStatus"   // State, we have to filter ACTIVO
  }

  object dbTags {
    val idArticleTag = "matnr"
    val idStoreTag = "werks"
    val idStatusTag = "mmsta"   // State, we have to filter ACTIVO
  }

  object implicits extends SurtidoMarcImplicits

  trait SurtidoMarcImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.idArticleTag -> ccTags.idArticleTag,
      dbTags.idStoreTag -> ccTags.idStoreTag,
      dbTags.idStatusTag -> ccTags.idStatusTag
    )

    private lazy val toCCAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)

    implicit def readFromKuduTablesSurtidoMarc(implicit kuduCon: String): FromDbTable[SurtidoMarc, Kudu] = {
      new FromKuduTable[SurtidoMarc] {
        override def fromDB: DataFrame => Dataset[SurtidoMarc] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.idArticleTag,
            dbTags.idStoreTag,
            dbTags.idStatusTag).transform(toCCAgg).as[SurtidoMarc]
        }

        override val schemaName: String = "assortment_stream"
        override val tableName: String = "marc"

        override def host: String = kuduCon
      }
    }

  }

}

case class SurtidoMarc(
  idArticle: String,
  idStore: String,
  idStatus: String) extends SurtidoInterface


