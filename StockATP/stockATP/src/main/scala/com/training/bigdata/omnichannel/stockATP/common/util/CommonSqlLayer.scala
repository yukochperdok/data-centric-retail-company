package com.training.bigdata.omnichannel.stockATP.common.util

import java.sql.Timestamp
import java.time.format.DateTimeFormatter

import com.training.bigdata.omnichannel.stockATP.common.entities.{ControlEjecucion, ControlEjecucionView}
import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import com.training.bigdata.omnichannel.stockATP.common.util.Constants.{INNER, LEFT}

import scala.util.Try

object CommonSqlLayer {

  def filterControlEjecucion(controlEjecucionProcesses: List[String]): Dataset[ControlEjecucionView] => Dataset[ControlEjecucionView] = {
    import org.apache.spark.sql.functions.col
    ds =>
      ds.filter(col(ControlEjecucion.ccTags.idProcess).isin(controlEjecucionProcesses:_*))
  }

  def joinSurtidoByArticleAndLocation[A <: SurtidoInterface, B <: SurtidoInterface](
    aDs: Dataset[A],
    bDs: Dataset[B],
    joinType: String = INNER)(implicit spark: SparkSession): Dataset[(A, B)] = {

    aDs.joinWith(
      bDs,
      aDs(Surtido.ccTags.idArticleTag) === bDs(Surtido.ccTags.idArticleTag)
        and aDs(Surtido.ccTags.idStoreTag) === bDs(Surtido.ccTags.idStoreTag),
      joinType)

  }

  def joinSurtidoByArticleAndLocationDF(
     aDF: DataFrame,
     bDF: DataFrame,
     joinType: String = INNER)(implicit spark: SparkSession): DataFrame = {

    aDF.join(
      bDF,
      Seq(Surtido.ccTags.idArticleTag, Surtido.ccTags.idStoreTag),
      joinType)

  }

  def joinSurtidoBySubfamily[A <: SubfamilyInterface, B <: SubfamilyInterface](
    aDs: Dataset[A],
    bDs: Dataset[B],
    joinType: String = INNER)(implicit spark: SparkSession): Dataset[(A, B)] = {

    aDs.joinWith(
      bDs,
      aDs(Subfamily.ccTags.idSubfamilyTag) === bDs(Subfamily.ccTags.idSubfamilyTag),
      joinType)
  }

  def joinSurtidoByArticle[A <: SurtidoInterface, B <: ArticuloInterface](
    aDs: Dataset[A],
    bDs: Dataset[B],
    joinType: String = INNER)(implicit spark: SparkSession): Dataset[(A, B)] = {

    aDs.joinWith(
      bDs,
      aDs(Surtido.ccTags.idArticleTag) === bDs(Articulo.ccTags.idArticleTag),
      joinType)
  }

  def joinStore[A <: StoreInterface, B <: StoreInterface](
    aDs: Dataset[A],
    bDs: Dataset[B],
    joinType: String = INNER)(implicit spark: SparkSession): Dataset[(A, B)] = {

    aDs.joinWith(
      bDs,
      aDs(Surtido.ccTags.idStoreTag) === bDs(Surtido.ccTags.idStoreTag),
      joinType
    )
  }

  def joinSurtidoWithStore[A <: SurtidoInterface, B <: StoreInterface](
    aDs: Dataset[A],
    bDs: Dataset[B],
    joinType: String = INNER)(implicit spark: SparkSession): Dataset[(A, B)] = {

    aDs.joinWith(
      bDs,
      aDs(Surtido.ccTags.idStoreTag) === bDs(Store.ccTags.idStoreTag),
      joinType
    )
  }

  def joinSurtidoWithStoreDF(
    aDs: DataFrame,
    bDs: DataFrame,
    filterType: String = INNER)(implicit spark: SparkSession): DataFrame = {

    aDs.join(
      bDs,
      Seq(Surtido.ccTags.idStoreTag),
      filterType
    )
  }

  def joinPedidos[A <: OrderInterface, B <: OrderInterface](
    aDs: Dataset[A],
    bDs: Dataset[B],
    joinType: String = INNER)(implicit spark: SparkSession): Dataset[(A, B)] = {

    aDs.joinWith(
      bDs,
        aDs(Order.ccTags.idOrderTag) === bDs(Order.ccTags.idOrderTag)
        and aDs(Order.ccTags.idOrderLineTag) === bDs(Order.ccTags.idOrderLineTag),
      joinType)

  }

  def joinMaterial[A <: MaterialInterface, B <: MaterialInterface](
    aDs: Dataset[A],
    bDs: Dataset[B],
    joinType: String = INNER)(implicit spark: SparkSession): Dataset[(A, B)] = {

    aDs.joinWith(
      bDs,
      aDs(Material.ccTags.idListMaterialTag) === bDs(Material.ccTags.idListMaterialTag),
      joinType
    )
  }

  def addDFAndFillNullWithZeros(addedDF: DataFrame)
    (implicit spark: SparkSession): DataFrame => DataFrame = {

    df =>
      CommonSqlLayer.joinSurtidoByArticleAndLocationDF(df, addedDF, LEFT)
        .transform(AuditUtils.getLastUpdateDlkInInnerOrLeftJoin(addedDF(Audit.ccTags.tsUpdateDlkTag)))
        .na.fill(addedDF.columns.map(col => (col, 0d)).toMap)
  }

  import org.apache.spark.sql.functions.udf
  import com.training.bigdata.omnichannel.stockATP.common.util.Dates.{timestampToStringWithTZ,stringToTimestamp}
  val udfTimeStampToString =
    udf[String, Timestamp, String, String]{
      (ts:Timestamp, separator: String, timezone:String) => timestampToStringWithTZ(ts, separator, timezone)
    }

  val udfStringToTimestamp =
    udf[Timestamp, String, String, String, Timestamp]{
      (date:String, tz: String, format: String, defaultValue: Timestamp) => {
        val formatter = DateTimeFormatter.ofPattern(format)
        Try(stringToTimestamp(date, tz, formatter)).getOrElse(defaultValue)
      }
    }

  val udfCompleteDateString =
    udf[String,String,String]{
      (date: String, pattern: String) =>
        Option(date) match {
          case Some(d) => if (d.matches(pattern)) d.concat(" 00:00:00") else d
          case None => null
        }
    }

}
