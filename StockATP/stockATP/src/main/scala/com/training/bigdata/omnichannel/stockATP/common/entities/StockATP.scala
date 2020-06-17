package com.training.bigdata.omnichannel.stockATP.common.entities

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{Audit, SurtidoInterface}
import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import com.training.bigdata.omnichannel.stockATP.common.util.Constants
import org.apache.spark.sql.DataFrame
import com.training.bigdata.omnichannel.stockATP.common.util.FieldsUtils._

object StockATP {

  object ccTags {
    val idArticleTag = "idArticle"
    val idStoreTag = "idStore"
    //StockConsolidado
    val unityMeasureTag = "unityMeasure"
    //DatosFijos
    val sectorTag = "sector"
    //StoreCapacity
    val isPPTag = "isPP"
    val nDateTag = "nDate"
    //StockATP
    val stockATPNTag = "stockAtpN"
  }

  object dbTags {
    val idArticleTag = "matnr"
    val idStoreTag = "werks"
    //StockConsolidado
    val unityMeasureTag = "unidad_medida"
    //DatosFijos
    val sectorTag = "sector"
    //StoreCapacity
    val isPPTag = "is_pp"
    val nDateTag = "n_date"
    //StockATP
    val stockATPNTag = "stock_atp_n"
  }

  object implicits extends StockATPImplicits

  trait StockATPImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.idArticleTag -> ccTags.idArticleTag,
      dbTags.idStoreTag -> ccTags.idStoreTag,
      //StockConsolidado
      dbTags.unityMeasureTag -> ccTags.unityMeasureTag,
      //DatosFijos
      dbTags.sectorTag -> ccTags.sectorTag,
      //StoreCapacity
      dbTags.isPPTag -> ccTags.isPPTag,
      dbTags.nDateTag -> ccTags.nDateTag,
      //StockATP
      dbTags.stockATPNTag -> ccTags.stockATPNTag,
      Audit.dbTags.tsUpdateDlkTag -> Audit.ccTags.tsUpdateDlkTag,
      Audit.dbTags.userUpdateDlkTag -> Audit.ccTags.userUpdateDlkTag
    )

    val stockAtpNFields: List[(String, String)] =
      generateNFieldsSequence(dbTags.stockATPNTag, Constants.ATP_PLANNING_DAYS)
        .zip(generateNFieldsSequence(ccTags.stockATPNTag, Constants.ATP_PLANNING_DAYS)).toList

    private val copydb: List[(String, String)] = List(
      Audit.dbTags.tsInsertDlkTag -> Audit.dbTags.tsUpdateDlkTag,
      Audit.dbTags.userInsertDlkTag -> Audit.dbTags.userUpdateDlkTag
    )

    private lazy val toCCAgg = (db2cc ++ stockAtpNFields).map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)
    private lazy val toDBAgg = (db2cc ++ stockAtpNFields).map(t => { df: DataFrame => df.withColumnRenamed(t._2, t._1) }).reduce(_ andThen _)
    private lazy val toCopyDB = copydb.map(t => { df: DataFrame => df.withColumn(t._1, df(t._2)) }).reduce(_ andThen _)

    implicit def writeToKuduTablesStockATP(implicit kuduCon: String): ToDbTable[StockATP, Kudu] = {
      new ToKuduTable[StockATP] {

        override def toDBDF: DataFrame => DataFrame = {
          df => {
            df.transform(toDBAgg andThen toCopyDB)
          }
        }

        override val schemaName: String = "impala::omnichannel_user"
        override val tableName: String = "omnichannel_stock_atp"

        override def host: String = kuduCon

      }
    }
  }

}

case class StockATP(
  val idArticle: String,
  val idStore: String,
  val unityMeasure: String,
  val sector: String,
  val isPP: Boolean,
  val stockATPN: Double,
  val tsUpdateDlk: Timestamp,
  val userUpdateDlk: String) extends SurtidoInterface

