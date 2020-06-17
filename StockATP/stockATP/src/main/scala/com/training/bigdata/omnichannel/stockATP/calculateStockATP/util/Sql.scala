package com.training.bigdata.omnichannel.stockATP.calculateStockATP.util

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.util.Constants._
import com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs._
import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{SurtidoInterface, _}
import com.training.bigdata.omnichannel.stockATP.common.entities.{DatosFijos, MaraWithArticleTypes, StockATP}
import com.training.bigdata.omnichannel.stockATP.common.util.Dates.timestampToStringWithTZ
import com.training.bigdata.omnichannel.stockATP.common.util._
import com.training.bigdata.omnichannel.stockATP.common.util.LensAppConfig._
import com.training.bigdata.omnichannel.stockATP.common.util.debug.DebugUtils
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, DoubleType, StructType}

case class TempSalesForecast(
  idArticle: String,
  idStore: String,
  dateSale: String,
  tsUpdateDlk: Long,
  salesForecastN: Double = 0d,
  salesForecastN1: Double = 0d) extends SurtidoInterface

case class TempEntregaParcial(
  override val idOrder: String,
  override val idOrderLine: String,
  val numEntregaParcial: String,
  val aggEntrega: Double,
  val aggEntregaAyer: Double,
  val tsUpdateDlk: Timestamp) extends OrderInterface

object Sql {

  def filterSaleMovementsByMovementCode(implicit spark: SparkSession, conf: AppConfig): Dataset[SaleMovements] => Dataset[SaleMovements] = {
    saleMovements =>
      saleMovements
        .filter(col(SaleMovements.ccTags.movementCodeTag) === lit(conf.getMovementCode))
  }

  def getSaleMovementsForDay(dateAtMidnightTs: Timestamp)
  (implicit spark: SparkSession, conf: AppConfig): Dataset[SaleMovements] => Dataset[SaleMovementsView] = {
    import spark.implicits._

    saleMovements => {
      val nextDayAtMidnightTs = Dates.sumDaysToTimestamp(dateAtMidnightTs, 1)
      saleMovements
        .filter(
          col(SaleMovements.ccTags.movementDateTag) >= lit(dateAtMidnightTs)
            && col(SaleMovements.ccTags.movementDateTag) < lit(nextDayAtMidnightTs)
        )
        .drop(col(SaleMovements.ccTags.movementDateTag))
        .drop(col(SaleMovements.ccTags.movementCodeTag))
        .as[SaleMovementsView]
    }
  }

  def filterStockConsolidadoAndManageNulls
  (implicit spark: SparkSession, conf: AppConfig): Dataset[StockConsolidado] => Dataset[StockConsolidadoView] = {
    import spark.implicits._

    stock => {
      val stockTypeOMS = conf.getStockTypeOMS
      stock
        .filter(col(StockConsolidado.ccTags.stockTypeTag) === lit(stockTypeOMS))
        .withColumn(
          StockConsolidado.ccTags.stockDispTag,
          when(col(StockConsolidado.ccTags.stockDispTag) isNull, lit(0d))
            .otherwise(col(StockConsolidado.ccTags.stockDispTag)))
        .drop(col(StockConsolidado.ccTags.stockTypeTag))
        .as[StockConsolidadoView]
    }
  }

  def filterDatosFijos(
    days: Int,
    dayNTs: Timestamp,
    previousDayToNTs: Timestamp)(implicit spark: SparkSession, conf: AppConfig): DataFrame => DataFrame = {

    df =>

      val dayNStr = Dates.timestampToStringWithTZ(dayNTs, HYPHEN_SEPARATOR, conf.getTimezone)
      val previousDayToNStr =  Dates.timestampToStringWithTZ(previousDayToNTs, HYPHEN_SEPARATOR, conf.getTimezone)

      // We take two days more because we are going to calculate stock atp for one day more because of the change of day
      // and we need to take into account the change of day of datos fijos
      val forecastFields: Seq[String] = FieldsUtils.generateNFieldsSequence(DatosFijos.ccTags.salesForecastNTag, days + 2)
      val fieldsToSelectDatosFijos: Seq[String] = DatosFijos.defaultFieldsToReadInATP ++ forecastFields

      val renamingForecastFieldsMap: Seq[(String, String)] = forecastFields.init.zip(forecastFields.tail)

      val parseForecastNullValues: DataFrame => DataFrame = forecastFields.map(field => { df: DataFrame =>
        df.withColumn(
          field,
          when(col(field) isNull, lit(0d))
            .otherwise(col(field)))})
        .reduce(_ andThen _)

      val adaptingForecastToSaleDate: DataFrame => DataFrame = renamingForecastFieldsMap.map{ case (forecastTagToday, forecastTagTomorrow) => {
         df: DataFrame =>
          df.withColumn(forecastTagToday,
            when(col(DatosFijos.ccTags.dateSaleTag) === lit(dayNStr), col(forecastTagToday))
              .otherwise(col(forecastTagTomorrow)))
        }
      }.reduce(_ andThen _)

      df
        .select(fieldsToSelectDatosFijos.head, fieldsToSelectDatosFijos.tail: _*)
        .filter(col(DatosFijos.ccTags.dateSaleTag) === lit(dayNStr) or
            col(DatosFijos.ccTags.dateSaleTag) === lit(previousDayToNStr))
        .transform(parseForecastNullValues andThen adaptingForecastToSaleDate)
        .drop(forecastFields.last)

  }


  def filterOrderHeader
  (implicit spark: SparkSession, conf: AppConfig): Dataset[OrderHeader] => Dataset[OrderHeader] = {
    orderHeader => {
      val typeOrdersList = conf.getTypeOrdersList
      orderHeader.filter(col(OrderHeader.ccTags.orderTypeTag).isin(typeOrdersList.map(lit(_)): _*))
    }
  }

  def filterOrderLines(implicit spark: SparkSession, conf: AppConfig): Dataset[OrderLine] => Dataset[OrderLineView] = {
    import spark.implicits._

    orderLine => {
      val returnedToProviderFlag = conf.getStockToBeDeliveredParameters.returnedToProviderFlag
      val alreadyDeliveredFlag = conf.getStockToBeDeliveredParameters.alreadyDeliveredFlag
      val canceledFlagList = conf.getStockToBeDeliveredParameters.canceledFlagList
      orderLine
        .filter(
          col(OrderLine.ccTags.canceledOrderFlagTag).isNull ||
          (!col(OrderLine.ccTags.canceledOrderFlagTag).isin(canceledFlagList.map(lit(_)): _*))
        )
        .filter(
          !(col(OrderLine.ccTags.returnedToProviderFlagTag) eqNullSafe lit(returnedToProviderFlag)))
        .filter(
          !(col(OrderLine.ccTags.finalDeliveryFlagTag) eqNullSafe lit(alreadyDeliveredFlag)))
        .as[OrderLineView]
    }
  }

  def filterPendingPartialOrdersDeliveredBetweenDays
  (firstDay: Timestamp, lastDay: Timestamp)
  (implicit spark: SparkSession, conf: AppConfig): Dataset[PartialOrdersDelivered] => Dataset[PartialOrdersDelivered] = {
    partialOrdersDelivered =>

      val firstDayStrWithoutSeparator: String = Dates.timestampToStringWithTZ(firstDay, NO_SEPARATOR, conf.getTimezone)
      val endDayStrWithoutSeparator: String = Dates.timestampToStringWithTZ(lastDay, NO_SEPARATOR, conf.getTimezone)

      partialOrdersDelivered.filter(
        col(PartialOrdersDelivered.ccTags.deliveryDateTsTag) >= firstDayStrWithoutSeparator &&
          col(PartialOrdersDelivered.ccTags.deliveryDateTsTag) < endDayStrWithoutSeparator &&
          // If the whole partial order has been delivered, the delivered amount should be the whole partial order amount
          col(PartialOrdersDelivered.ccTags.deliveredAmountTag) =!= col(PartialOrdersDelivered.ccTags.wholePartialOrderAmountTag)
      )
  }

  def transformPartialOrdersDelivered
    (implicit spark: SparkSession, conf: AppConfig): Dataset[PartialOrdersDelivered] => Dataset[PartialOrdersDeliveredView] = {
    import spark.implicits._
    import com.training.bigdata.omnichannel.stockATP.common.util.CommonSqlLayer.{udfCompleteDateString, udfStringToTimestamp}

    partialOrdersDelivered =>
      partialOrdersDelivered
        .withColumn(
          PartialOrdersDelivered.ccTags.deliveredAmountTag,
          when(col(PartialOrdersDelivered.ccTags.deliveredAmountTag) isNull, lit(0d))
            .otherwise(col(PartialOrdersDelivered.ccTags.deliveredAmountTag)))
        .withColumn(
          PartialOrdersDelivered.ccTags.wholePartialOrderAmountTag,
          when(col(PartialOrdersDelivered.ccTags.wholePartialOrderAmountTag) isNull, lit(0d))
            .otherwise(col(PartialOrdersDelivered.ccTags.wholePartialOrderAmountTag)))
        .withColumn(
          PartialOrdersDelivered.ccTags.deliveryDateTsTag,
          udfCompleteDateString(col(PartialOrdersDelivered.ccTags.deliveryDateTsTag), lit(ONLY_DIGITS_DATE_REGEX))
        )
        .withColumn(
          PartialOrdersDelivered.ccTags.deliveryDateTsTag,
          udfStringToTimestamp(
            col(PartialOrdersDelivered.ccTags.deliveryDateTsTag),
            lit(conf.getTimezone),
            lit(IN_PROCESS_ORDERS_DATETIME_FORMAT),
            lit(DEFAULT_ORDER_TIMESTAMP))
        )
        .as[PartialOrdersDeliveredView]
  }

  def joinOrderLinesById(
    dsOrderHeader: Dataset[OrderHeader],
    dsOrderLine: Dataset[OrderLineView])(implicit spark: SparkSession): Dataset[OrderLineView] = {
    import spark.implicits._

    val dsJoinOrderLines: Dataset[OrderLineView] =
      dsOrderHeader
        .joinWith(dsOrderLine, dsOrderHeader(OrderHeader.ccTags.idOrderTag) === dsOrderLine(OrderLine.ccTags.idOrderTag), INNER)
        .map(t => OrderLineView(
          t._1.idOrder,
          t._2.idOrderLine,
          t._2.idArticle,
          t._2.idStore,
          if(t._1.tsUpdateDlk.after(t._2.tsUpdateDlk)) t._1.tsUpdateDlk else t._2.tsUpdateDlk
        ))

    dsJoinOrderLines
  }

  def getArticleAndStoreOfOrdersNotCanceled(dsOrderLines: Dataset[OrderLineView])
    (implicit spark: SparkSession): Dataset[PartialOrdersDeliveredView] => Dataset[ActivePartialOrdersWithArticleAndStore] = {

    dsPartialOrdersDelivered =>

      import spark.implicits._

      val dsJoinOrdersWithDeliveries: Dataset[ActivePartialOrdersWithArticleAndStore] =
        CommonSqlLayer.joinPedidos(dsOrderLines, dsPartialOrdersDelivered, INNER)
          .map(t => ActivePartialOrdersWithArticleAndStore(
            t._1.idArticle,
            t._1.idStore,
            t._2.deliveryDateTs,
            t._2.wholePartialOrderAmount,
            t._2.deliveredAmount,
            if(t._1.tsUpdateDlk.after(t._2.tsUpdateDlk)) t._1.tsUpdateDlk else t._2.tsUpdateDlk
          ))

      dsJoinOrdersWithDeliveries
  }

  def getMaximumBetween: (Double, Double) => Double = { case (a, b) => math.max(a, b) }

  val getMaximumBetweenUdf: UserDefinedFunction = udf(getMaximumBetween)

  def getStockToBeDeliveredByArticleStoreAndDay(implicit spark: SparkSession):
    Dataset[ActivePartialOrdersWithArticleAndStore] => Dataset[StockToBeDelivered] = {

    dsStockToBeDeliveredWithLogisticVariable =>
      import spark.implicits._

      val dsAggregatedStockToBeDeliveredByArticleAndStore: Dataset[StockToBeDelivered] =
        dsStockToBeDeliveredWithLogisticVariable
          .withColumn(
            StockToBeDelivered.ccTags.dayTag,
            date_format(col(ActivePartialOrdersWithArticleAndStore.ccTags.deliveryDateTsTag), DEFAULT_DAY_FORMAT)
          ).groupBy(
            ActivePartialOrdersWithArticleAndStore.ccTags.idArticleTag,
            ActivePartialOrdersWithArticleAndStore.ccTags.idStoreTag,
            StockToBeDelivered.ccTags.dayTag
          )
          .agg(
            sum(
              getMaximumBetweenUdf(
                col(ActivePartialOrdersWithArticleAndStore.ccTags.wholePartialOrderAmountTag) - col(ActivePartialOrdersWithArticleAndStore.ccTags.deliveredAmountTag),
                lit(0d)
              )
            ) alias StockToBeDelivered.ccTags.amountToBeDeliveredTag,
            max(Audit.ccTags.tsUpdateDlkTag) alias Audit.ccTags.tsUpdateDlkTag
          ).as[StockToBeDelivered]

      dsAggregatedStockToBeDeliveredByArticleAndStore
  }

  def aggregateSaleMovementsByArticleAndStore(implicit spark: SparkSession): Dataset[SaleMovementsView] => Dataset[SaleMovementsView] = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    dsSaleMovements =>

      dsSaleMovements
        .groupBy(
          Surtido.ccTags.idArticleTag,
          Surtido.ccTags.idStoreTag)
        .agg(
          sum(col(SaleMovements.ccTags.amountTag)) alias SaleMovements.ccTags.amountTag,
          max(Audit.ccTags.tsUpdateDlkTag) alias Audit.ccTags.tsUpdateDlkTag
        ).as[SaleMovementsView]

  }

  def getBoxAndPrepackContentInformation(
    dsMaterialsArticle: Dataset[MaterialsArticle],
    dsMaterials: Dataset[Materials])(implicit spark: SparkSession): Dataset[BoxAndPrepackInformation] = {
    import spark.implicits._

    val dsJoinMaterials: Dataset[BoxAndPrepackInformation] =
      CommonSqlLayer.joinMaterial(dsMaterialsArticle, dsMaterials, INNER)
        .map(t => BoxAndPrepackInformation(
          t._1.idListMaterial,
          t._1.idArticle, // Purchase article id
          t._2.idArticle, // Sales equivalent article id
          Option(t._2.amount).getOrElse(0d),
          if(t._1.tsUpdateDlk.after(t._2.tsUpdateDlk)) t._1.tsUpdateDlk else t._2.tsUpdateDlk
        ))

    dsJoinMaterials
  }

  def addInformationOfArticlesToBeDelivered(dsMara: Dataset[MaraWithArticleTypes])(implicit spark: SparkSession, conf: AppConfig)
    : Dataset[StockToBeDelivered] => Dataset[StockToBeDeliveredWithLogisticVariableWithMara] = {

    dsAggOrders =>
      import spark.implicits._

      CommonSqlLayer.joinSurtidoByArticle(dsAggOrders, dsMara, INNER)
        .map(t =>
          StockToBeDeliveredWithLogisticVariableWithMara(
            t._1.idArticle,
            t._1.idStore,
            t._1.day,
            t._1.amountToBeDelivered,
            t._2.articleType,
            t._2.articleCategory,
            t._2.salePurchaseIndicator,
            t._2.salesParentArticle,
            if(t._1.tsUpdateDlk.after(t._2.tsUpdateDlk)) t._1.tsUpdateDlk else t._2.tsUpdateDlk
          )
        )
  }

  def filterBoxPrepack(implicit spark: SparkSession, conf: AppConfig):
    Dataset[StockToBeDeliveredWithLogisticVariableWithMara] => Dataset[StockToBeDeliveredWithLogisticVariableWithMara] = {
    import spark.implicits._

    dsAggOrdersWithMara =>
      dsAggOrdersWithMara.filter(
        aggOrdersWithMara =>
          OrdersUtils.isBoxOrPrepack(
            ArticleTypeAndCategory(aggOrdersWithMara.articleType,aggOrdersWithMara.articleCategory),
            conf.getBoxOrPrepackArticleList
          )
      ).as[StockToBeDeliveredWithLogisticVariableWithMara]
  }

  def filterSaleArticleOrPuchaseArticleWithUniqueSaleArticle
  (implicit spark: SparkSession, conf: AppConfig): Dataset[StockToBeDeliveredWithLogisticVariableWithMara] => Dataset[StockToBeDeliveredWithLogisticVariableWithMara] = {
    import spark.implicits._

    dsAggOrdersWithMara =>
      dsAggOrdersWithMara.filter(
        aggOrdersWithMara =>
          OrdersUtils.isASaleArticleOrAPuchaseArticleWithUniqueSaleArticle(
            conf.getSaleArticlesList,
            aggOrdersWithMara.salePurchaseIndicator,
            ArticleTypeAndCategory(aggOrdersWithMara.articleType,aggOrdersWithMara.articleCategory),
            aggOrdersWithMara.salesParentArticleId,
            conf.getPuchaseArticleWithUniqueSaleArticleList
          )
      ).as[StockToBeDeliveredWithLogisticVariableWithMara]
  }

  def filterCustomerReservationsBetweenDays
  (firstDay: Timestamp, lastDay: Timestamp): Dataset[CustomerReservations] => Dataset[CustomerReservations] = {
    dsCustomerReservations =>
      dsCustomerReservations.filter(
        col(CustomerReservations.ccTags.reservationDateTag) >= firstDay &&
          col(CustomerReservations.ccTags.reservationDateTag) < lastDay
      )
  }

  def nullAmountsToZeroAndDateAsStringInCustomerReservations
  (implicit spark: SparkSession): Dataset[CustomerReservations] => Dataset[CustomerReservationsView] = {
    dsCustomerReservations =>
      import spark.implicits._

      dsCustomerReservations
        .withColumn(CustomerReservations.ccTags.reservationDateTag, date_format(col(CustomerReservations.ccTags.reservationDateTag), "yyyyMMdd"))
        .na.fill(0d, Seq(CustomerReservations.ccTags.amountTag))
        .as[CustomerReservationsView]
  }

  def addBoxAndPrepackOrdersContent(
    dsJoinMaterials: Dataset[BoxAndPrepackInformation])
                                   (implicit spark: SparkSession, conf: AppConfig): Dataset[StockToBeDeliveredWithLogisticVariableWithMara] => Dataset[StockToBeDeliveredOfBoxOrPrepack] = {
    import spark.implicits._

    /* TODO: If we need optimize joinOrdersWithMaraAndMaterials and calculateAggOrdersBoxPrepack methods we could mix them:
  .map(t =>
      AggOrdersWithMara(
        t._1.idArticle,
        t._1.idStore,
        t._1.aggPedidos * t._2.amount,
        t._1.aggPedidosAyer * t._2.amount,
        t._1.articleType,
        t._1.articleCategory,
        t._1.salePurchaseIndicator,
        t._2.idSaleArticle,
        if(t._1.tsUpdateDlk.after(t._2.tsUpdateDlk)) t._1.tsUpdateDlk else t._2.tsUpdateDlk
      )
    )
*/
    dsAggOrdersWithMara =>
      // Join by purchase article
      CommonSqlLayer.joinSurtidoByArticle(dsAggOrdersWithMara, dsJoinMaterials, INNER)
        .map(t =>
          StockToBeDeliveredOfBoxOrPrepack(
            t._1.idArticle,
            t._1.idStore,
            t._1.day,
            t._1.amountToBeDelivered,
            t._1.articleType,
            t._1.articleCategory,
            t._1.salePurchaseIndicator,
            t._2.salesParentArticleId,
            t._2.amountOfSalesParentArticleIncluded,
            if(t._1.tsUpdateDlk.after(t._2.tsUpdateDlk)) t._1.tsUpdateDlk else t._2.tsUpdateDlk
          )
        )
  }

  def calculateAmountOfSalesArticlesIncludedInBoxOrPrepack
  (implicit spark: SparkSession): Dataset[StockToBeDeliveredOfBoxOrPrepack] => Dataset[StockToBeDeliveredWithLogisticVariableWithMara] = {
    import spark.implicits._

    dsAggOrdersWithMaraAndMaterials =>
      dsAggOrdersWithMaraAndMaterials
        .withColumn(
          StockToBeDeliveredWithLogisticVariableWithMara.ccTags.amountToBeDeliveredTag,
          col(StockToBeDeliveredOfBoxOrPrepack.ccTags.numberOfBoxOrPrepackToBeDeliveredTag) * col(StockToBeDeliveredOfBoxOrPrepack.ccTags.amountOfSalesArticleIncludedTag)
        )
        .drop(StockToBeDeliveredOfBoxOrPrepack.ccTags.numberOfBoxOrPrepackToBeDeliveredTag)
        .drop(StockToBeDeliveredOfBoxOrPrepack.ccTags.amountOfSalesArticleIncludedTag)
       .as[StockToBeDeliveredWithLogisticVariableWithMara]
  }

  def unionAllStockToBeDeliveredWithLogisticalVariableAndMara(
    dsBoxAndPrepackOrdersWithSalesArticleInformation: Dataset[StockToBeDeliveredWithLogisticVariableWithMara])
    (implicit spark: SparkSession): Dataset[StockToBeDeliveredWithLogisticVariableWithMara] => Dataset[StockToBeDeliveredWithLogisticVariableWithMara] = {

    dsAggOrdersWithMara =>
      dsAggOrdersWithMara.unionByName(dsBoxAndPrepackOrdersWithSalesArticleInformation)

  }

  def adaptArticleToSaleArticle(implicit spark: SparkSession, conf: AppConfig):
    Dataset[StockToBeDeliveredWithLogisticVariableWithMara] => Dataset[StockToBeDelivered] = {
    import spark.implicits._

    dsAggOrdersWithMara =>
      dsAggOrdersWithMara
        .map(aggOrder =>
          StockToBeDelivered(
            if (OrdersUtils.isASaleArticle(conf.getSaleArticlesList, aggOrder.salePurchaseIndicator))
              aggOrder.idArticle
            else
              aggOrder.salesParentArticleId,
            aggOrder.idStore,
            aggOrder.day,
            aggOrder.amountToBeDelivered,
            aggOrder.tsUpdateDlk
          )
        )

  }

  def pivotAmountForAllProjectionDaysByArticleAndStore[T <: SurtidoInterface](dayAtMidnightTs: Timestamp, numberOfDaysToProjectPlus1: Int,
     columnToPivot: String, amountColumn: String, pivotedColumnBaseName: String, firstDayIndex: Int)
    (implicit spark: SparkSession): Dataset[T] => DataFrame = {

    dsSurtido =>

      val tsUpdateDlkWindow = Window
        .partitionBy(
          dsSurtido(Surtido.ccTags.idArticleTag),
          dsSurtido(Surtido.ccTags.idStoreTag))
        .orderBy(dsSurtido(Audit.ccTags.tsUpdateDlkTag).desc)

      val projectionDaysMap: Map[String, String] =
        Dates.getFuturePlanningDaysMap(dayAtMidnightTs, NO_SEPARATOR, plusDays = numberOfDaysToProjectPlus1 + 1, firstDayIndex = firstDayIndex)

      dsSurtido
        .withColumn(
          Audit.ccTags.tsUpdateDlkTag,
          max(dsSurtido(Audit.ccTags.tsUpdateDlkTag)).over(tsUpdateDlkWindow))
        .groupBy(
          Surtido.ccTags.idArticleTag,
          Surtido.ccTags.idStoreTag,
          Audit.ccTags.tsUpdateDlkTag)
        .pivot(
          columnToPivot, projectionDaysMap.keys.toSeq)
        .agg(
          sum(col(amountColumn) alias amountColumn))
        .transform(
          FieldsUtils.renamePivotedColumns(projectionDaysMap, pivotedColumnBaseName))
        .na.fill(0d)

  }

  def addConsolidatedStockToAggregatedSalesMovements(
    dsConsolidatedStock: Dataset[StockConsolidadoView])(implicit spark: SparkSession):
    Dataset[SaleMovementsView] => Dataset[JoinConsolidatedStockAndAggregatedSaleMovements] = {
    import spark.implicits._

    dsAggSaleMovements =>

      CommonSqlLayer.joinSurtidoByArticleAndLocation(dsConsolidatedStock, dsAggSaleMovements, OUTER)
        .map(t => JoinConsolidatedStockAndAggregatedSaleMovements(
          Option(t._1).map(_.idArticle).getOrElse(t._2.idArticle),
          Option(t._1).map(_.idStore).getOrElse(t._2.idStore),
          Option(t._1).map(_.unitMeasure).getOrElse(null),
          Option(t._1).map(_.stockDisp).getOrElse(0),
          Option(t._2).map(_.amount).getOrElse(0),
          (Option(t._1), Option(t._2)) match {
            case (Some(t1), None) => t1.tsUpdateDlk
            case (None, Some(t2)) => t2.tsUpdateDlk
            case (Some(t1), Some(t2)) => if(t1.tsUpdateDlk.after(t2.tsUpdateDlk)) t1.tsUpdateDlk else t2.tsUpdateDlk
            /* This situation isn't possible, but fix warning*/
            case _ => new Timestamp(System.currentTimeMillis())
          }
        ))
  }

  def addFixedDataToConsolidatedStockAndMovements(dfFixedData: DataFrame)(implicit spark: SparkSession)
    : Dataset[JoinConsolidatedStockAndAggregatedSaleMovements] => DataFrame = {

    dsConsolidatedStockAndAggSaleMovs =>
    val dfConsolidatedStockAndAggSaleMovs =
      dsConsolidatedStockAndAggSaleMovs
        .withColumnRenamed(DatosFijos.ccTags.unityMeasureTag, TempTags.tempUnitMeasureTag)
        .toDF()

    val dfJoinFixedDataAndConsolidatedStockAndAggregatedSaleMovements: DataFrame =
      CommonSqlLayer.joinSurtidoByArticleAndLocationDF(dfFixedData, dfConsolidatedStockAndAggSaleMovs, LEFT)
        .transform(AuditUtils.getLastUpdateDlkInInnerOrLeftJoin(dfConsolidatedStockAndAggSaleMovs(Audit.ccTags.tsUpdateDlkTag)))
        .withColumn(
          DatosFijos.ccTags.unityMeasureTag,
          when(col(TempTags.tempUnitMeasureTag) isNotNull, col(TempTags.tempUnitMeasureTag))
            .otherwise(col(DatosFijos.ccTags.unityMeasureTag)))
        .withColumn(
          JoinConsolidatedStockAndAggregatedSaleMovements.ccTags.availableStockTag,
          when(col(JoinConsolidatedStockAndAggregatedSaleMovements.ccTags.availableStockTag) isNull, lit(0d))
            .otherwise(col(JoinConsolidatedStockAndAggregatedSaleMovements.ccTags.availableStockTag)))
        .withColumn(
          SaleMovements.ccTags.amountTag,
          when(col(SaleMovements.ccTags.amountTag) isNull, lit(0d))
            .otherwise(col(SaleMovements.ccTags.amountTag)))
        .drop(col(TempTags.tempUnitMeasureTag))
        .drop(col(DatosFijos.ccTags.dateSaleTag))


    dfJoinFixedDataAndConsolidatedStockAndAggregatedSaleMovements
  }

  def calculateNspAndNst(stockConfig: StockConf)(implicit spark: SparkSession, conf: AppConfig) : DataFrame => DataFrame = {

    df =>
      df
        .withColumn(
          DatosFijos.ccTags.nspTag,
            when(col(DatosFijos.ccTags.nspTag) <= lit(stockConfig.maxNsp) && col(DatosFijos.ccTags.nspTag) >= lit(stockConfig.minNsp), col(DatosFijos.ccTags.nspTag) / 100 )
           .otherwise(conf.getCommonParameters.defaultNsp / 100))
        .withColumn(
         DatosFijos.ccTags.nstTag,
           when(col(DatosFijos.ccTags.nstTag) <= lit(stockConfig.maxNst) && col(DatosFijos.ccTags.nstTag) >= lit(stockConfig.minNst), col(DatosFijos.ccTags.nstTag) / 100 )
           .otherwise(conf.getCommonParameters.defaultNst / 100))
        .withColumn(TempTags.nsTag, col(DatosFijos.ccTags.nspTag) * col(DatosFijos.ccTags.nstTag))
        .drop(DatosFijos.ccTags.nspTag, DatosFijos.ccTags.nstTag)
  }

  def aggregationPP(
    dsStoreCapacity: Dataset[StoreCapacityView],
    listCapacitiesPP: Seq[Int])(implicit spark: SparkSession): Dataset[AggPP] = {
    import spark.implicits._

    val isPP = udf( (s:Seq[Int]) => !s.intersect(listCapacitiesPP).isEmpty )

    val dsAggPP = dsStoreCapacity
      .groupBy(
        Surtido.ccTags.idStoreTag
      ).agg(
        collect_list(StoreCapacity.ccTags.idCapacityTag).as(AggPP.ccTags.listCapacities),
        max(Audit.ccTags.tsUpdateDlkTag) alias Audit.ccTags.tsUpdateDlkTag
      )
      .withColumn(AggPP.ccTags.isPP, isPP(col(AggPP.ccTags.listCapacities)))
      .drop(col(AggPP.ccTags.listCapacities))
      .as[AggPP]

    dsAggPP
  }

  def joinStoreTypeAndAggPP(
    dsStoreType: Dataset[StoreTypeView],
    dsAggPP: Dataset[AggPP])(implicit spark: SparkSession): Dataset[JoinStoreTypeAndAggPP] = {
    import spark.implicits._

    /**
      * If joinStore return a Dataframe we could change it by:
      *   val dsJoinStoreTypeAndAggPP: DataFrame =
                  joinStore(dsStoreType,dsAggPP,OUTER)
                  .drop("listCapacities")
                  .na.fill(false, Seq("isPP"))
                  .na.fill(true, Seq("typeStore"))
      */

    val dsJoinStoreTypeAndAggPP =
      CommonSqlLayer.joinStore(dsStoreType,dsAggPP,OUTER)
        .map{t =>
          if(t._1 != null && t._2 != null) JoinStoreTypeAndAggPP(t._1.idStore,t._1.typeStore,t._2.isPP, if(t._1.tsUpdateDlk.after(t._2.tsUpdateDlk)) t._1.tsUpdateDlk else t._2.tsUpdateDlk)
          else if (t._1 != null) JoinStoreTypeAndAggPP(t._1.idStore,t._1.typeStore,false, t._1.tsUpdateDlk )
          else /*if (t._2 != null)*/ JoinStoreTypeAndAggPP(t._2.idStore,true,t._2.isPP, t._2.tsUpdateDlk)

        }

    dsJoinStoreTypeAndAggPP
  }

  def addStoreTypeAndPreparationInformationToPrecalculatedStockData(
    dfJoinStockConsAndDatosFijosAndAggPedidos: DataFrame,
    dsJoinStoreTypeAndAggPP: Dataset[JoinStoreTypeAndAggPP],
    stockConf: StockConf,
    updatedTimestamp: Timestamp,
    dateRegistroEjecucion: Option[Timestamp],
    updateUser:String)(implicit spark: SparkSession, conf: AppConfig): DataFrame = {

    val dfJoinStoreTypeAndAggPP: DataFrame = dsJoinStoreTypeAndAggPP.toDF

    val dfStockATP: DataFrame =
      CommonSqlLayer.joinSurtidoWithStoreDF(dfJoinStockConsAndDatosFijosAndAggPedidos, dfJoinStoreTypeAndAggPP, LEFT)
        .transform(AuditUtils.getLastUpdateDlkInInnerOrLeftJoin(dfJoinStoreTypeAndAggPP(Audit.ccTags.tsUpdateDlkTag)))
        .withColumn(
          Audit.ccTags.tsUpdateDlkTag,
          when(col(Audit.ccTags.tsUpdateDlkTag) > lit(stockConf.tsUpdateDlk), col(Audit.ccTags.tsUpdateDlkTag))
            .otherwise(lit(stockConf.tsUpdateDlk)))
        // TODO: Fix Modification control. right now it is ignored for stock to be delivered to work
        //.transform(filterRegistriesToProcess(dateRegistroEjecucion))
        .withColumn(Audit.ccTags.tsUpdateDlkTag, lit(updatedTimestamp))
        .withColumn(
          DatosFijos.ccTags.unityMeasureTag,
          when(
            col(DatosFijos.ccTags.unityMeasureTag) isNull,
            lit(conf.getCommonParameters.defaultUnidadMedida))
          .otherwise(col(DatosFijos.ccTags.unityMeasureTag)))
        .withColumn(
          StoreType.ccTags.typeStoreTag,
          when(
            col(StoreType.ccTags.typeStoreTag) isNull, lit(true))
            .otherwise(col(StoreType.ccTags.typeStoreTag)))
        .withColumn(
          AggPP.ccTags.isPP,
          when(
            col(AggPP.ccTags.isPP) isNull, lit(false))
            .otherwise(col(AggPP.ccTags.isPP)))
        .withColumn(Audit.ccTags.userUpdateDlkTag, lit(updateUser))

    dfStockATP
  }

  def filterRegistriesToProcess(oTsLastExecution: Option[Timestamp]): DataFrame => DataFrame = {
    df =>
      oTsLastExecution match{
        case Some(tsLastExecution) =>
          df.filter(col(Audit.ccTags.tsUpdateDlkTag) >= tsLastExecution)
        case None => df
      }
  }

  def roundDownAndMaxWithZero(num: Double): Double =
    math.max(math.floor(num).toInt, 0)

  def round3DecimalsAndMaxWithZero(num: Double): Double =
    math.max(BigDecimal(num).setScale(3, BigDecimal.RoundingMode.HALF_DOWN).toDouble, 0)

  val udfRoundBasedOnUnityMeasureAndMaxWithZero =
    udf((num: Double, unityMeasure: String) => {
      if(unityMeasure==UNITY_MEASURE_TO_ROUND_DOWN)
        roundDownAndMaxWithZero(num)
      else
        round3DecimalsAndMaxWithZero(num)
    } )

  def roundBasedOnUnityMeasureAndMaxWithZero(columnToRoundDown: String, unityMeasureColumn: String): DataFrame => DataFrame = {
    df =>
      df.withColumn(columnToRoundDown, udfRoundBasedOnUnityMeasureAndMaxWithZero(col(columnToRoundDown), col(unityMeasureColumn)))
  }

  def getStockToBeDeliveredBasedOnSector(previousDayToNColumnName: String, dayNColumnName: String)(implicit spark: SparkSession) : DataFrame => DataFrame = {
    df =>
      df
        .withColumn(TempTags.tempStockToBeDelivered,
          when(col(DatosFijos.ccTags.sectorTag) === FRESH_PRODUCTS_SECTOR, col(dayNColumnName) * col(TempTags.nsTag))
            .otherwise(col(previousDayToNColumnName) * col(TempTags.nsTag)))
  }

  def calculateStockATP(
    stockConfig:StockConf,
    currentDateTs: Timestamp)(implicit conf: AppConfig, debugOptions: DebugParameters, spark: SparkSession): DataFrame => DataFrame = {
      stockAtpDF =>
        val getRotation: DataFrame => DataFrame =
          df =>
            df
              .withColumn(
                DatosFijos.ccTags.rotationTag,
                when(col(DatosFijos.ccTags.rotationTag) isNull, lower(lit(conf.getCommonParameters.defaultRotation)))
                  .otherwise(lower(col(DatosFijos.ccTags.rotationTag))))
              .withColumn(
              DatosFijos.ccTags.rotationTag,
              when(col(DatosFijos.ccTags.rotationTag) === TIPO_ROTACION_AA, lit(stockConfig.rotationArtAA / 100)).otherwise(
                when(col(DatosFijos.ccTags.rotationTag) === TIPO_ROTACION_A, lit(stockConfig.rotationArtA / 100)).otherwise(
                  when(col(DatosFijos.ccTags.rotationTag) === TIPO_ROTACION_B, lit(stockConfig.rotationArtB / 100)).otherwise(
                    when(col(DatosFijos.ccTags.rotationTag) === TIPO_ROTACION_C, lit(stockConfig.rotationArtC / 100)).otherwise(
                      stockConfig.rotationArtB / 100
               )))))

        val getAdjustedSalesForecast: DataFrame => DataFrame =
          df =>
            df.withColumn(
                TempTags.tempAdjustedSalesForecastTag,
                when(col(StoreType.ccTags.typeStoreTag) === true, col(DatosFijos.ccTags.salesForecastNTag) - col(SaleMovements.ccTags.amountTag)).otherwise(lit(0d)))
              .withColumn(
                TempTags.tempAdjustedSalesForecastTag,
                when(col(TempTags.tempAdjustedSalesForecastTag) < lit(0d), lit(0d)).otherwise(col(TempTags.tempAdjustedSalesForecastTag)))


        val getStockToBeDelivered: DataFrame => DataFrame =
          getStockToBeDeliveredBasedOnSector(StockToBeDelivered.ccTags.amountToBeDeliveredPreviousDayToNTag, StockToBeDelivered.ccTags.amountToBeDeliveredNTag)

        val finalCalc: DataFrame => DataFrame =
          df =>
            df
              .withColumn(StockATP.ccTags.nDateTag, lit(currentDateTs))
              .withColumn(
                StockATP.ccTags.stockATPNTag,
                (col(JoinConsolidatedStockAndAggregatedSaleMovements.ccTags.availableStockTag) * col(DatosFijos.ccTags.rotationTag))
                  - col(TempTags.tempAdjustedSalesForecastTag)
                  + col(TempTags.tempStockToBeDelivered)
                  - col(CustomerReservations.ccTags.amountNTag))

        stockAtpDF
          .transform(
            getRotation andThen
            getAdjustedSalesForecast andThen
            getStockToBeDelivered andThen
            finalCalc andThen
              roundBasedOnUnityMeasureAndMaxWithZero(StockATP.ccTags.stockATPNTag, DatosFijos.ccTags.unityMeasureTag) andThen
            DebugUtils.logDebugTransform("dfStockAtpNBeforeDroppingColumns"))
          .drop(SaleMovements.ccTags.amountTag)
          .drop(StoreType.ccTags.typeStoreTag)
          .drop(TempTags.tempAdjustedSalesForecastTag)
          .drop(TempTags.tempStockToBeDelivered)
          .drop(StockToBeDelivered.ccTags.amountToBeDeliveredPreviousDayToNTag)
          .drop(JoinConsolidatedStockAndAggregatedSaleMovements.ccTags.availableStockTag)
          .drop(DatosFijos.ccTags.rotationTag)
          .drop(DatosFijos.ccTags.salesForecastNTag)
          .drop(CustomerReservations.ccTags.amountNTag)
          .drop(DatosFijos.ccTags.nspTag)
          .drop(DatosFijos.ccTags.nstTag)

  }

  def calculateStockATPNX(stockConfig: StockConf)(implicit spark: SparkSession, conf: AppConfig): DataFrame => DataFrame = {
    df =>
      val stockToBeDeliveredFields: Seq[(String, String)] =
        FieldsUtils.generateNFieldsSequenceGroupedByPreviousAndActual(
          StockToBeDelivered.ccTags.amountToBeDeliveredNTag,
          stockConfig.numberOfDaysToProject + 1,
          0
        )
      val forecastFields: Seq[String] = FieldsUtils.generateNFieldsSequence(DatosFijos.ccTags.salesForecastNTag, stockConfig.numberOfDaysToProject + 1).tail
      val stockAtpNXFields: Seq[String] = FieldsUtils.generateNFieldsSequence(StockATP.ccTags.stockATPNTag, stockConfig.numberOfDaysToProject + 1).tail
      val stockAtpNullFields: Seq[String] = FieldsUtils.generateNFieldsSequence(StockATP.ccTags.stockATPNTag, ATP_PLANNING_DAYS, stockConfig.numberOfDaysToProject + 2)
      val customerReservationFields: Seq[String] = FieldsUtils.generateNFieldsSequence(CustomerReservations.ccTags.amountNTag, stockConfig.numberOfDaysToProject + 1).tail
      val stockATPNXCalculationFields: Seq[(String, String, String, String)] =
        FieldsUtils.zip3(forecastFields, customerReservationFields, stockToBeDeliveredFields)

      def addFieldsToSchema(df: DataFrame, newFields: List[String]): StructType = {
        var schema: StructType = df.schema
        newFields.foreach(newField => schema = schema.add(newField, DataTypes.DoubleType, true))
        schema
      }
      val encoder = RowEncoder.apply(addFieldsToSchema(df, stockAtpNXFields.toList ++ stockAtpNullFields.toList))

      val nullFields: Seq[Option[Double]] = stockAtpNullFields.map(_ => None)
      val calculateStockATPNXValues: Seq[(String, String, String, String)] => Row => Row =
        stockMap => row => {
          val stockATPN = row.getAs[Double](StockATP.ccTags.stockATPNTag)
          val addedCols: Seq[Option[Double]] = stockMap.foldLeft((Seq.empty[Option[Double]], stockATPN)) {
            case ((stockATPAcc, previousStockATP), (salesForecast, customerReservations, stockToBeDeliveredDayBefore, stockToBeDeliveredDay)) =>

            val stockToBeDelivered: Double =
              if(row.getAs[String](StockATP.ccTags.sectorTag) == FRESH_PRODUCTS_SECTOR)
                row.getAs[Double](stockToBeDeliveredDay) * row.getAs[Double](TempTags.nsTag)
              else
                row.getAs[Double](stockToBeDeliveredDayBefore) * row.getAs[Double](TempTags.nsTag)

            val newStock: Double = previousStockATP -
              row.getAs[Double](salesForecast) -
              row.getAs[Double](customerReservations) +
              stockToBeDelivered

            val roundedStock: Double =
              if(row.getAs[String](StockATP.ccTags.unityMeasureTag) == UNITY_MEASURE_TO_ROUND_DOWN)
                roundDownAndMaxWithZero(newStock)
              else
                round3DecimalsAndMaxWithZero(newStock)

            (stockATPAcc ++ Seq(Option(roundedStock)), roundedStock)
          }._1 ++ nullFields

          Row.merge(row, Row.fromSeq(addedCols.map(_.getOrElse(null))))
        }

      df
        .map(calculateStockATPNXValues(stockATPNXCalculationFields))(encoder)
        .drop(forecastFields ++
          customerReservationFields ++
          stockToBeDeliveredFields.map(_._1) :+
          stockToBeDeliveredFields.last._2 :+
          TempTags.nsTag: _*)
  }

}
