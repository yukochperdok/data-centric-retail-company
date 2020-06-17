package com.training.bigdata.omnichannel.stockATP.common.entities

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{Audit, SurtidoInterface}
import com.training.bigdata.omnichannel.stockATP.common.io.tables._
import com.training.bigdata.omnichannel.stockATP.common.util.Constants
import com.training.bigdata.omnichannel.stockATP.common.util.FieldsUtils._
import org.apache.spark.sql.DataFrame

object DatosFijos {

  object ccTags {
    val idArticleTag = "idArticle"
    val idStoreTag = "idStore"
    val dateSaleTag = "dateSale"
    val sectorTag = "sector"
    val unityMeasureTag = "unityMeasure"
    val nspTag = "nsp"
    val nstTag = "nst"
    val rotationTag = "rotation"
    val salesForecastTag = "salesForecast"
    val salesForecastNTag = "salesForecastN"
    val salesForecastN1Tag = "salesForecastN1"
  }

  object dbTags {
    val idArticleTag = "matnr"
    val idStoreTag = "werks"
    val dateSaleTag = "date_sale"
    val sectorTag = "sector"
    val unityMeasureTag = "unidad_medida"
    val nspTag = "nsp"
    val nstTag = "nst"
    val rotationTag = "rotation"
    val salesForecastTag = "sales_forecast"
    val salesForecastNTag = "sales_forecast_n"
    val salesForecastN1Tag = "sales_forecast_n1"
  }

  val defaultFieldsToReadInATP: Seq[String] = Seq(
    ccTags.idArticleTag,
    ccTags.idStoreTag,
    ccTags.dateSaleTag,
    ccTags.sectorTag,
    ccTags.unityMeasureTag,
    ccTags.nspTag,
    ccTags.nstTag,
    ccTags.rotationTag,
    Audit.ccTags.tsUpdateDlkTag
  )

  object implicits extends DatosFijosImplicits

  trait DatosFijosImplicits {
    private val db2cc: List[(String, String)] = List(
      dbTags.idArticleTag -> ccTags.idArticleTag,
      dbTags.idStoreTag -> ccTags.idStoreTag,
      dbTags.dateSaleTag -> ccTags.dateSaleTag,
      dbTags.sectorTag -> ccTags.sectorTag,
      dbTags.unityMeasureTag -> ccTags.unityMeasureTag,
      dbTags.nspTag -> ccTags.nspTag,
      dbTags.nstTag -> ccTags.nstTag,
      dbTags.rotationTag -> ccTags.rotationTag,
      Audit.dbTags.tsInsertDlkTag -> Audit.ccTags.tsInsertDlkTag,
      Audit.dbTags.userInsertDlkTag -> Audit.ccTags.userInsertDlkTag,
      Audit.dbTags.tsUpdateDlkTag -> Audit.ccTags.tsUpdateDlkTag,
      Audit.dbTags.userUpdateDlkTag -> Audit.ccTags.userUpdateDlkTag
    )

    //renames all salesForecastN{X} columns to sales_forecast_n{x}
    val forecastFields: List[(String, String)] =
      generateNFieldsSequence(dbTags.salesForecastNTag, Constants.SALES_FORECAST_DAYS)
        .zip(generateNFieldsSequence(ccTags.salesForecastNTag, Constants.SALES_FORECAST_DAYS)).toList

    private lazy val toCCAgg: DataFrame => DataFrame = (db2cc ++ forecastFields).map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)
    private lazy val toDBAgg: DataFrame => DataFrame =
      (db2cc ++ forecastFields).map(t => { df: DataFrame => df.withColumnRenamed(t._2, t._1) }).reduce(_ andThen _) andThen
        {df => df.selectExpr((db2cc ++ forecastFields).map(_._1):_*) }

    implicit def readFromKuduTablesDatosFijos(implicit kuduCon: String): FromDbTableDF[DatosFijos, Kudu] = {
      new FromKuduTableDF[DatosFijos] {
        override def fromDBDF: DataFrame => DataFrame = df => {
          df.transform(toCCAgg)
        }

        override val schemaName: String = "impala::omnichannel_user"
        override val tableName: String = "omnichannel_datos_fijos"

        override def host: String = kuduCon
      }
    }

    implicit def writeToKuduTablesDatosFijos(implicit kuduCon: String): ToDbTable[DatosFijos, Kudu] = {
      new ToKuduTable[DatosFijos] {

        override def toDBDF: DataFrame => DataFrame = {
          df => {
            df.transform(toDBAgg)
          }
        }

        override val schemaName: String = "impala::omnichannel_user"
        override val tableName: String = "omnichannel_datos_fijos"

        override def host: String = kuduCon

      }
    }

    implicit def readFromHiveTablesDatosFijos: FromDbTableDF[DatosFijos, Hive] = {
      new FromHiveTableDF[DatosFijos] {

        override val schemaName: String = "omnichannel_user"
        override val tableName: String = "omnichannel_datos_fijos"

        override def fromDBDF: DataFrame => DataFrame = df => {
          df.transform(toCCAgg)
        }
      }
    }

    implicit def writeToHiveTablesDatosFijos: ToDbTable[DatosFijos, Hive] = {
      new ToHiveTable[DatosFijos] {

        override val schemaName: String = "omnichannel_user"
        override val tableName: String = "omnichannel_datos_fijos"
        override val numPartitions:Int = 70

        override def toDBDF: DataFrame => DataFrame = {
          df => {
            df.transform(toDBAgg)
          }
        }
      }
    }

  }
}

case class DatosFijos(
  override val idArticle: String,
  override val idStore: String,
  dateSale: String,
  sector: String,
  unityMeasure: String,
  nsp: Option[Double],
  nst: Option[Double],
  rotation: String,
  tsUpdateDlk: Timestamp,
  userUpdateDlk: String,
  salesForecastN: Double,
  salesForecastN1: Double) extends SurtidoInterface

case class DatosFijosView(
  override val idArticle: String,
  override val idStore: String,
  dateSale: String,
  sector: String,
  unityMeasure: String,
  nsp: Option[Double],
  nst: Option[Double],
  rotation: String,
  tsUpdateDlk: Timestamp,
  salesForecastN: Option[Double],
  salesForecastN1: Option[Double]) extends SurtidoInterface

case class DatosFijosParsedView(
  override val idArticle: String,
  override val idStore: String,
  dateSale: String,
  salesForecast: Double,
  sector: String,
  unityMeasure: String,
  nsp: Option[Double],
  nst: Option[Double],
  rotation: String,
  tsUpdateDlk: Timestamp) extends SurtidoInterface