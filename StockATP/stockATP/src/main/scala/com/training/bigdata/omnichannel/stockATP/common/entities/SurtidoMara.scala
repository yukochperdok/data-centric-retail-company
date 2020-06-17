package com.training.bigdata.omnichannel.stockATP.common.entities

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{ArticuloInterface, Audit, SubfamilyInterface}
import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import org.apache.spark.sql.{DataFrame, Dataset}

object SurtidoMara {

  final val MARA_SCHEMA = "mdata_stream"
  final val MARA_TABLE = "mara"

  object ccTags {
    val idArticleTag = "idArticle"
    val unityMeasureTag = "unityMeasure"
    val idSubfamilyTag = "idSubfamily"
    val articleTypeTag = "articleType"
    val articleCategoryTag = "articleCategory"
    val salePurchaseIndicatorTag = "salePurchaseIndicator"
    val salesParentArticleTag = "salesParentArticle"
  }

  object dbTags {
    val idArticleTag = "matnr"
    val unityMeasureTag = "meins"
    val idSubfamilyTag = "matkl"
    val articleTypeTag = "mtart"
    val articleCategoryTag = "attyp"
    val salePurchaseIndicatorTag = "zzlogistical_mat"
    val salesParentArticleTag = "zzsalesmaterial"
  }

  object implicits extends SurtidoMaraImplicits

  trait SurtidoMaraImplicits extends BaseEntitiesImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.idArticleTag -> ccTags.idArticleTag,
      dbTags.unityMeasureTag -> ccTags.unityMeasureTag,
      dbTags.idSubfamilyTag -> ccTags.idSubfamilyTag,
      dbTags.articleTypeTag -> ccTags.articleTypeTag,
      dbTags.articleCategoryTag -> ccTags.articleCategoryTag,
      dbTags.salePurchaseIndicatorTag -> ccTags.salePurchaseIndicatorTag,
      dbTags.salesParentArticleTag -> ccTags.salesParentArticleTag,
      Audit.dbTags.tsInsertDlkTag -> Audit.ccTags.tsInsertDlkTag,
      Audit.dbTags.tsUpdateDlkTag -> Audit.ccTags.tsUpdateDlkTag
    )

    private lazy val toCCAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)

    implicit def readFromKuduTablesSurtidoMara(implicit kuduCon: String): FromDbTable[SurtidoMara, Kudu] = {
      new FromKuduTable[SurtidoMara] {
        override def fromDB: DataFrame => Dataset[SurtidoMara] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.idArticleTag,
            dbTags.unityMeasureTag,
            dbTags.idSubfamilyTag,
            dbTags.salePurchaseIndicatorTag
          ).transform(toCCAgg).as[SurtidoMara]
        }

        override val schemaName: String = MARA_SCHEMA
        override val tableName: String = MARA_TABLE

        override def host: String = kuduCon
      }
    }

    implicit def readFromKuduTablesMaraWithArticleTypes(implicit kuduCon: String): FromDbTable[MaraWithArticleTypes, Kudu] = {
      new FromKuduTable[MaraWithArticleTypes] {
        override def fromDB: DataFrame => Dataset[MaraWithArticleTypes] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.idArticleTag,
            dbTags.articleTypeTag,
            dbTags.articleCategoryTag,
            dbTags.salePurchaseIndicatorTag,
            dbTags.salesParentArticleTag,
            Audit.dbTags.tsInsertDlkTag,
            Audit.dbTags.tsUpdateDlkTag
          ).transform(toCCAgg andThen parseLastUpdate).as[MaraWithArticleTypes]
        }

        override val schemaName: String = MARA_SCHEMA
        override val tableName: String = MARA_TABLE

        override def host: String = kuduCon
      }
    }

  }

}

case class SurtidoMara(
  idArticle: String,
  unityMeasure: String,
  idSubfamily: String,
  salePurchaseIndicator: String) extends ArticuloInterface with SubfamilyInterface


case class MaraWithArticleTypes(
  idArticle: String,
  articleType: String,
  articleCategory: String,
  salePurchaseIndicator: String,
  salesParentArticle: String,
  tsUpdateDlk: Timestamp) extends ArticuloInterface