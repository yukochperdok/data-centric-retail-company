package com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.Audit
import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import org.apache.spark.sql.{DataFrame, Dataset}

object StockConf {

  object ccTags {
    val idTag = "id"
    val maxNspTag = "maxNsp"
    val minNspTag = "minNsp"
    val maxNstTag = "maxNst"
    val minNstTag = "minNst"
    val rotationArtAATag = "rotationArtAA"
    val rotationArtATag = "rotationArtA"
    val rotationArtBTag = "rotationArtB"
    val rotationArtCTag = "rotationArtC"
    val numberOfDaysToProjectTag = "numberOfDaysToProject"
  }

  object dbTags {
    val idTag = "id"
    val maxNspTag = "max_nsp"
    val minNspTag = "min_nsp"
    val maxNstTag = "max_nst"
    val minNstTag = "min_nst"
    val rotationArtAATag = "rotation_art_aa"
    val rotationArtATag = "rotation_art_a"
    val rotationArtBTag = "rotation_art_b"
    val rotationArtCTag = "rotation_art_c"
    val numberOfDaysToProjectTag = "day_project"
  }

  object implicits extends StockConfImplicits

  trait StockConfImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.idTag -> ccTags.idTag,
      dbTags.maxNspTag -> ccTags.maxNspTag,
      dbTags.minNspTag -> ccTags.minNspTag,
      dbTags.maxNstTag -> ccTags.maxNstTag,
      dbTags.minNstTag -> ccTags.minNstTag,
      dbTags.rotationArtAATag -> ccTags.rotationArtAATag,
      dbTags.rotationArtATag -> ccTags.rotationArtATag,
      dbTags.rotationArtBTag -> ccTags.rotationArtBTag,
      dbTags.rotationArtCTag -> ccTags.rotationArtCTag,
      dbTags.numberOfDaysToProjectTag -> ccTags.numberOfDaysToProjectTag,
      Audit.dbTags.tsUpdateDlkTag -> Audit.ccTags.tsUpdateDlkTag
    )

    private lazy val toCCAgg = db2cc.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)

    implicit def readFromKuduTablesStockConf(implicit kuduCon: String): FromDbTable[StockConf, Kudu] = {
      new FromKuduTable[StockConf] {
        override def fromDB: DataFrame => Dataset[StockConf] = df => {
          import df.sparkSession.implicits._
          df.select(
            dbTags.maxNspTag,
            dbTags.minNspTag,
            dbTags.maxNstTag,
            dbTags.minNstTag,
            dbTags.rotationArtAATag,
            dbTags.rotationArtATag,
            dbTags.rotationArtBTag,
            dbTags.rotationArtCTag,
            dbTags.numberOfDaysToProjectTag,
            Audit.dbTags.tsUpdateDlkTag
          ).transform(toCCAgg).as[StockConf]
        }

        override val schemaName: String = "omnichannel_stream"
        override val tableName: String = "3b_stock_conf_oms"

        override def host: String = kuduCon
      }
    }

  }

}

case class StockConf(
  maxNsp: Double,
  minNsp: Double,
  maxNst: Double,
  minNst: Double,
  rotationArtAA: Double,
  rotationArtA: Double,
  rotationArtB: Double,
  rotationArtC: Double,
  numberOfDaysToProject: Int,
  tsUpdateDlk: Timestamp)
