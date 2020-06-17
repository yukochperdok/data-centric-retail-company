package com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.BaseEntitiesImplicits
import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{ArticuloInterface, Audit, MaterialInterface}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Identify each articles group (aka list of materials) that contains different sub-articles
  */
object MaterialsArticle {

  object ccTags {
    val idArticleTag = "idArticle"
    val idListMaterialTag = "idListMaterial"
  }

  object dbTags {
    val idArticleTag = "matnr"
    val idListMaterialTag = "stlnr"
  }

  object implicits extends MaterialsArticleImplicits

  trait MaterialsArticleImplicits extends BaseEntitiesImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.idArticleTag -> ccTags.idArticleTag,
      dbTags.idListMaterialTag -> ccTags.idListMaterialTag,
      Audit.dbTags.tsInsertDlkTag -> Audit.ccTags.tsInsertDlkTag,
      Audit.dbTags.tsUpdateDlkTag -> Audit.ccTags.tsUpdateDlkTag
    )

    private lazy val toCCAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)

    implicit def readFromKuduTablesMaterialsArticle(implicit kuduCon: String): FromDbTable[MaterialsArticle, Kudu] = {
      new FromKuduTable[MaterialsArticle] {
        override def fromDB: DataFrame => Dataset[MaterialsArticle] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.idArticleTag,
            dbTags.idListMaterialTag,
            Audit.dbTags.tsInsertDlkTag,
            Audit.dbTags.tsUpdateDlkTag).transform(toCCAgg andThen parseLastUpdate).as[MaterialsArticle]
        }

        override val schemaName: String = "mdata_stream"
        override val tableName: String = "mast"

        override def host: String = kuduCon
      }
    }

  }

}

case class MaterialsArticle(
  override val idArticle: String,
  override val idListMaterial: String,
  val tsUpdateDlk: Timestamp) extends ArticuloInterface with MaterialInterface
