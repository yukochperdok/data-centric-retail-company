package com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.util

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.util.Constants._
import com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.inputs._
import com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.joins._
import com.training.bigdata.omnichannel.stockATP.common.entities.{DatosFijos, SurtidoMara}
import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{Audit, Surtido}
import com.training.bigdata.omnichannel.stockATP.common.util.{CommonSqlLayer, Dates, FieldsUtils}
import com.training.bigdata.omnichannel.stockATP.common.util.CommonSqlLayer.udfTimeStampToString
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object SqlOperations {

  /** Treats sales forecast data in order to generate:
    * - date sales field as string with the format needed
    * - regular sales field and promo sales field in order to treat null values
    * - regular sales field and promo sales field in order to get the correct amount for each article and store (regular
    * if no promo is applied - promo sales field is 0 or null - otherwise promo amount).
    *
    * @param todayTs today's timestamp
    * @param futurePlanningDayTs future planning day's timestamp
    * @param timeZone string indicating timezone
    * @param spark spark session
    * @return function that transforms sales forecast dataset to sales forecast view dataset
    */
  def getForecastSalesForTodayAndFuturePlanningDay(
    todayTs: Timestamp,
    futurePlanningDayTs: Timestamp,
    timeZone: String)(implicit spark: SparkSession) : Dataset[SalesForecast] => Dataset[SalesForecastView] = {

    import org.apache.spark.sql.functions._
    import spark.implicits._

    salesForecastDS =>

      salesForecastDS
        .filter(
        col(SalesForecast.ccTags.dateSaleTag) < lit(futurePlanningDayTs) and
          col(SalesForecast.ccTags.dateSaleTag) >= lit(todayTs))
        // PromoSales and RegularSales will be 0.0 if origin value is null
        .withColumn(
          SalesForecast.ccTags.promoSalesForecastTag,
          when(col(SalesForecast.ccTags.promoSalesForecastTag) isNull, 0d)
            .otherwise(col(SalesForecast.ccTags.promoSalesForecastTag)))
        .withColumn(
          SalesForecast.ccTags.regularSalesForecastTag,
          when(col(SalesForecast.ccTags.regularSalesForecastTag) isNull, 0d)
            .otherwise(col(SalesForecast.ccTags.regularSalesForecastTag)))
        // PromoSales will be 0.0 if there is no promotion, otherwise it will include regular sales
        .withColumn(
          SalesForecast.ccTags.regularSalesForecastTag,
          when(col(SalesForecast.ccTags.promoSalesForecastTag) === lit(0d), col(SalesForecast.ccTags.regularSalesForecastTag))
            .otherwise(col(SalesForecast.ccTags.promoSalesForecastTag)))
        .withColumn(SalesForecast.ccTags.dateSaleTag, udfTimeStampToString(col(SalesForecast.ccTags.dateSaleTag), lit(HYPHEN_SEPARATOR), lit(timeZone)))
        .drop(col(SalesForecast.ccTags.promoSalesForecastTag))
        .as[SalesForecastView]
  }

  /** Treats sales platform data in order to generate:
    * - date sales field as string with the format needed
    * - regular sales field in order to treat null values
    *
    * @param todayTs today's timestamp
    * @param futurePlanningDayTs future planning day's timestamp
    * @param timeZone string indicating timezone
    * @param spark spark session
    * @return function that transforms sales platform dataset to sales forecast view dataset
    */
  def getSalesPlatformForTodayAndFuturePlanningDay(
    todayTs: Timestamp,
    futurePlanningDayTs: Timestamp,
    timeZone: String)(implicit spark: SparkSession) : Dataset[SalesPlatform] => Dataset[SalesForecastView] = {

    import org.apache.spark.sql.functions._
    import spark.implicits._

    salesPlatformDS =>

      salesPlatformDS
        .filter(
          col(SalesForecast.ccTags.dateSaleTag) < lit(futurePlanningDayTs) and
            col(SalesForecast.ccTags.dateSaleTag) >= lit(todayTs))
        .withColumn(
          SalesForecast.ccTags.regularSalesForecastTag,
          when(col(SalesForecast.ccTags.regularSalesForecastTag) isNull, 0d)
            .otherwise(col(SalesForecast.ccTags.regularSalesForecastTag)))
        .withColumn(SalesForecast.ccTags.dateSaleTag, udfTimeStampToString(col(SalesForecast.ccTags.dateSaleTag), lit(HYPHEN_SEPARATOR), lit(timeZone)))
        .as[SalesForecastView]
  }

  /*
   * Filter any join with Mara to get only articles from stores that are considered "selling articles". This includes the
   * list of types passed as parameters along with NULLS (both if mara row does not exist and if article type in mara is NULL)
   */
  def filterJoinMarcMara[T](saleArticleList: List[String])(implicit spark: SparkSession): Dataset[(T,SurtidoMara)] => Dataset[(T,SurtidoMara)] = {

    dsTupleMarcMara =>
      dsTupleMarcMara.filter {
        joinRow => Option(joinRow._2)
          .flatMap(mara => Option(mara.salePurchaseIndicator))
          .fold(true)(artType => saleArticleList.contains(artType))
      }
  }

  /*
   * Join Marc and Mara in order to get the sector id of all article-store
   */
  def joinFilteredMarcMara(
    dsSurtidoMarc: Dataset[SurtidoMarc],
    dsSurtidoMara: Dataset[SurtidoMara],
    saleArticleList: List[String])(implicit spark: SparkSession): Dataset[JoinSurtidoMarcMara] = {

    import spark.implicits._

    CommonSqlLayer.joinSurtidoByArticle(dsSurtidoMarc, dsSurtidoMara, LEFT)
      .transform(filterJoinMarcMara(saleArticleList))
      .map { element =>
        JoinSurtidoMarcMara(
          element._1.idArticle,
          element._1.idStore,
          Option(element._2).map(_.unityMeasure).orNull,
          Option(element._2).map(_.idSubfamily).orNull)
      }
  }

  /*
   * Join Marc, Mara and commercial_structure in order to get the sector of all article-store
   */
  def joinMarcMaraSector(
    dsSurtidoMarcMara: Dataset[JoinSurtidoMarcMara],
    dsSurtidoCommercialStructure: Dataset[SurtidoEstructuraComercialView])(implicit spark: SparkSession): Dataset[JoinSurtidoMarcMaraSector] = {

    import spark.implicits._

    CommonSqlLayer.joinSurtidoBySubfamily(dsSurtidoMarcMara, dsSurtidoCommercialStructure, LEFT)
      .map { element =>
        JoinSurtidoMarcMaraSector(
          element._1.idArticle,
          element._1.idStore,
          Option(element._2).map(_.sector).orNull,
          element._1.unityMeasure)
      }
  }

  /*
   * Join between NST and NSP
   */
  def joinNstNsp(
    dsNst: Dataset[NstView],
    dsNsp: Dataset[NspView])(implicit spark: SparkSession): Dataset[JoinNstNsp] = {
    import spark.implicits._

    CommonSqlLayer.joinSurtidoByArticleAndLocation(dsNst, dsNsp, OUTER)
      .map { element =>
        JoinNstNsp(
          Option(element._1).map(_.idArticle).getOrElse(element._2.idArticle),
          Option(element._1).map(_.idStore).getOrElse(element._2.idStore),
          Option(element._1).map(_.nst),
          Option(element._2).map(_.nsp))
      }
  }

  /*
   * Join between all Surtido and the result of joining NST and NSP
   */
  def joinSurtidoNs(
    dsSurtido: Dataset[JoinSurtidoMarcMaraSector],
    dsNstNsp: Dataset[JoinNstNsp])(implicit spark: SparkSession): Dataset[JoinSurtidoAndNs] = {
    import spark.implicits._

    CommonSqlLayer.joinSurtidoByArticleAndLocation(dsSurtido, dsNstNsp, LEFT)
      .map { element =>
        JoinSurtidoAndNs(
          element._1.idArticle,
          element._1.idStore,
          element._1.sector,
          element._1.unityMeasure,
          Option(element._2).flatMap(_.nst),
          Option(element._2).flatMap(_.nsp))
      }
  }

  def joinSurtidoNsAndRotation(
    dsSurtidoAndNS: Dataset[JoinSurtidoAndNs],
    dsRotation: Dataset[SupplySkuRotation])(implicit spark: SparkSession): Dataset[JoinSurtidoNsRotation] = {
    import spark.implicits._

    CommonSqlLayer.joinSurtidoByArticleAndLocation(dsSurtidoAndNS, dsRotation, LEFT)
      .map { element =>
        JoinSurtidoNsRotation(
          element._1.idArticle,
          element._1.idStore,
          element._1.sector,
          element._1.unityMeasure,
          element._1.nst,
          element._1.nsp,
          Option(element._2).map(_.rotation).orNull
        )
      }
  }

  def joinDatosFijos(
     dsSurtidoNsRotation: Dataset[JoinSurtidoNsRotation],
     defaultDateSale: String)(implicit spark: SparkSession): DataFrame => DataFrame = { salesForecastDF => {
      CommonSqlLayer.joinSurtidoByArticleAndLocationDF(dsSurtidoNsRotation.toDF, salesForecastDF, LEFT)
        .na.fill(defaultDateSale, Seq(DatosFijos.ccTags.dateSaleTag))
    }
  }

  def addAuditFields(tsInsert: Timestamp, userInsert: String, tsUpdate: Timestamp, userUpdate: String): DataFrame => DataFrame = {
    df => {
      df.withColumn(Audit.ccTags.tsInsertDlkTag, lit(tsInsert))
        .withColumn(Audit.ccTags.userInsertDlkTag, lit(userInsert))
        .withColumn(Audit.ccTags.tsUpdateDlkTag, lit(tsUpdate))
        .withColumn(Audit.ccTags.userUpdateDlkTag, lit(userUpdate))
    }
  }

  def pivotSalesForecast(
    forecastDateMap: Map[String, String],
    todayDate: String)(implicit spark: SparkSession): Dataset[SalesForecastView] => DataFrame = {

    import org.apache.spark.sql.functions._

    dsSalesForecast =>

      dsSalesForecast.groupBy(
        Surtido.ccTags.idArticleTag,
        Surtido.ccTags.idStoreTag)
        .pivot(
          SalesForecast.ccTags.dateSaleTag, forecastDateMap.keys.toSeq)
        .agg(
          first(SalesForecast.ccTags.regularSalesForecastTag))
        .transform(FieldsUtils.renamePivotedColumns(forecastDateMap, SalesForecast.ccTags.salesForecastNTag))
        .withColumn(
          SalesForecast.ccTags.dateSaleTag, lit(todayDate)
        )

  }

}
