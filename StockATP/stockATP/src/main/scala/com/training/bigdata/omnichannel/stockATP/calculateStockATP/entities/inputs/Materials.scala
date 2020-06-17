package com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.BaseEntitiesImplicits
import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{ArticuloInterface, Audit, MaterialInterface}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Identify all sub-articles that belongs to group of articles (aka material list)
  */
object Materials {

  object ccTags {
    val idListMaterialTag = "idListMaterial"
    val idArticleTag = "idArticle"
    val amountTag = "amount"
  }

  object dbTags {
    val idListMaterialTag = "stlnr"
    val idArticleTag = "idnrk"
    val amountTag = "menge"
  }

  object implicits extends MaterialsImplicits

  trait MaterialsImplicits extends BaseEntitiesImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.idListMaterialTag -> ccTags.idListMaterialTag,
      dbTags.idArticleTag -> ccTags.idArticleTag,
      dbTags.amountTag -> ccTags.amountTag,
      Audit.dbTags.tsInsertDlkTag -> Audit.ccTags.tsInsertDlkTag,
      Audit.dbTags.tsUpdateDlkTag -> Audit.ccTags.tsUpdateDlkTag
    )

    private lazy val toCCAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)

    implicit def readFromKuduTablesMaterials(implicit kuduCon: String): FromDbTable[Materials, Kudu] = {
      new FromKuduTable[Materials] {
        override def fromDB: DataFrame => Dataset[Materials] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.idListMaterialTag,
            dbTags.idArticleTag,
            dbTags.amountTag,
            Audit.dbTags.tsInsertDlkTag,
            Audit.dbTags.tsUpdateDlkTag).transform(toCCAgg andThen parseLastUpdate).as[Materials]
        }

        override val schemaName: String = "mdata_stream"
        override val tableName: String = "stpo"

        override def host: String = kuduCon
      }
    }

  }

}

case class Materials(
  override val idListMaterial: String,
  override val idArticle: String,
  amount: Double,
  tsUpdateDlk: Timestamp) extends MaterialInterface with ArticuloInterface
