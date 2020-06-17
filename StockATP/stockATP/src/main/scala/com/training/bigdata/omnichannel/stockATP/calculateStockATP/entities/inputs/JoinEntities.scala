package com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs

import java.sql.Timestamp

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces._

object StockToBeDelivered {
  object ccTags {
    val idArticleTag = "idArticle"
    val idStoreTag =  "idStore"
    // This tag uses special character "-" for automatic fields generation purposes.
    // It will not cause any problem, unless the correspondant dataframe is persisted.
    val amountToBeDeliveredPreviousDayToNTag = "amountToBeDeliveredN-1"
    val amountToBeDeliveredNTag = "amountToBeDeliveredN"
    val amountToBeDeliveredTag = "amountToBeDelivered"
    val dayTag = "day"
  }
}
case class StockToBeDelivered(
  override val idArticle: String,
  override val idStore: String,
  day: String,
  amountToBeDelivered: Double,
  tsUpdateDlk: Timestamp) extends SurtidoInterface


object StockToBeDeliveredWithLogisticVariableWithMara {
  object ccTags {
    val articleTypeTag = "articleType"
    val articleCategoryTag = "articleCategory"
    val salePurchaseIndicatorTag = "salePurchaseIndicator"
    val salesParentArticleIdTag = "salesParentArticleId"
    val dayTag = "day"
    val amountToBeDeliveredTag = "amountToBeDelivered"
  }
}
case class StockToBeDeliveredWithLogisticVariableWithMara(
  override val idArticle: String,
  override val idStore: String,
  day: String,
  amountToBeDelivered: Double,
  articleType: String,
  articleCategory: String,
  salePurchaseIndicator: String,
  salesParentArticleId: String,
  tsUpdateDlk: Timestamp
) extends SurtidoInterface


object StockToBeDeliveredOfBoxOrPrepack {
  object ccTags {
    val dayTag = "day"
    val numberOfBoxOrPrepackToBeDeliveredTag = "numberOfBoxOrPrepackToBeDelivered"
    val amountOfSalesArticleIncludedTag = "amountOfSalesArticleIncluded"
    val salesParentArticleIdTag = "salesParentArticleId"
  }
}
case class StockToBeDeliveredOfBoxOrPrepack(
  override val idArticle: String,
  override val idStore: String,
  day: String,
  numberOfBoxOrPrepackToBeDelivered: Double,
  articleType: String,
  articleCategory: String,
  salePurchaseIndicator: String,
  salesParentArticleId: String,
  amountOfSalesArticleIncluded: Double,
  tsUpdateDlk: Timestamp
) extends SurtidoInterface


object AggPP {
  object ccTags {
    val listCapacities = "listCapacities"
    val isPP = "isPP"
  }
}

case class AggPP(
  override val idStore: String,
  val isPP: Boolean,
  val tsUpdateDlk: Timestamp) extends StoreInterface


object ActivePartialOrdersWithArticleAndStore {
  object ccTags {
    val wholePartialOrderAmountTag = "wholePartialOrderAmount"
    val deliveredAmountTag = "deliveredAmount"
    val deliveryDateTsTag = "deliveryDateTs"
    val idArticleTag = "idArticle"
    val idStoreTag = "idStore"
  }
}
case class ActivePartialOrdersWithArticleAndStore(
  override val idArticle: String,
  override val idStore: String,
  deliveryDateTs: Timestamp,
  wholePartialOrderAmount: Double,
  deliveredAmount: Double,
  tsUpdateDlk: Timestamp) extends SurtidoInterface

case class BoxAndPrepackInformation(
  override val idListMaterial: String,
  override val idArticle: String,
  salesParentArticleId: String,
  amountOfSalesParentArticleIncluded: Double,
  tsUpdateDlk: Timestamp) extends MaterialInterface with ArticuloInterface

object JoinConsolidatedStockAndAggregatedSaleMovements {
  object ccTags {
    val availableStockTag = "availableStock"
  }
}

case class JoinConsolidatedStockAndAggregatedSaleMovements(
  override val idArticle: String,
  override val idStore: String,
  val unityMeasure: String,
  val availableStock: Double,
  val amount: Double,
  val tsUpdateDlk: Timestamp) extends SurtidoInterface

case class JoinStoreTypeAndAggPP(
  override val idStore: String,
  val typeStore: Boolean,
  val isPP: Boolean,
  val tsUpdateDlk: Timestamp) extends StoreInterface

case class JoinAllConfig(
  override val idStore: String,
  val typeStore: Boolean,
  val isPP: Boolean,
  val maxNsp: Double,
  val minNsp: Double,
  val maxNst: Double,
  val minNst: Double,
  val rotationArtAA: Double,
  val rotationArtA: Double,
  val rotationArtB: Double,
  val rotationArtC: Double,
  val dayProjectFood: Int,
  val dayProjectNoFood: Int) extends StoreInterface
