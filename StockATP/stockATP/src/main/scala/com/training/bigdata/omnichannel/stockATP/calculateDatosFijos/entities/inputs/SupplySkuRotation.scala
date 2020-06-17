package com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.inputs

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{Audit, SurtidoInterface}
import org.apache.spark.sql.{DataFrame, Dataset}

object SupplySkuRotation {

  object ccTags {
    val idStoreTag = "idStore"
    val idArticleTag = "idArticle"
    val rotationTag = "rotation"
  }

  object dbTags {
    val idStoreTag = "werks"
    val idArticleTag = "matnr"
    val rotationTag = "rotation"
  }

  object implicits extends SupplySkuRotationImplicits

  trait SupplySkuRotationImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.idStoreTag -> ccTags.idStoreTag,
      dbTags.idArticleTag -> ccTags.idArticleTag,
      dbTags.rotationTag -> ccTags.rotationTag,
      Audit.dbTags.tsUpdateDlkTag -> Audit.ccTags.tsUpdateDlkTag
    )

    private lazy val toCCAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)

    implicit def readFromKuduTablesSupplySkuRotation(implicit kuduCon: String): FromDbTable[SupplySkuRotation, Kudu] = {
      new FromKuduTable[SupplySkuRotation] {
        override def fromDB: DataFrame => Dataset[SupplySkuRotation] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.idStoreTag,
            dbTags.idArticleTag,
            dbTags.rotationTag,
            Audit.dbTags.tsUpdateDlkTag).transform(toCCAgg).as[SupplySkuRotation]
        }

        override val schemaName: String = "supply_user"
        override val tableName: String = "supply_sku_rotation"

        override def host: String = kuduCon
      }
    }

  }

}

case class SupplySkuRotation(
  idStore: String,
  idArticle: String,
  rotation: String,
  tsUpdateDlk: Timestamp) extends SurtidoInterface
