package com.training.bigdata.omnichannel.customerOrderReservation.entities

import org.apache.spark.sql.DataFrame

object Audit {
  val tsInsertDlk = "ts_insert_dlk"
  val tsUpdateDlk = "ts_update_dlk"
  val userInsertDlk = "user_insert_dlk"
  val userUpdateDlk = "user_update_dlk"
}

object EcommerceOrder {

  val foodSite = Seq("bodegaSite", "basicSite")
  val nonFoodSite = Seq("c4nonfood")
  val shippingGroupId = "shippingGroupId"
  val omsOrderId = "omsOrderId"
  val siteId = "siteId"
  val commerceItems = "commerceItems"
  val shippingGroups = "shippingGroups"
  val orderState = "state"
  val article = "article"
  val smsSizeColor = "smsSizeColor"
  val smsId = "smsId"
  val store = "locationId"
  val creationDate = "creationDate"
  val pickingDate = "pickingDate"
  val deliveryDate = "deliveryDate"
  val quantity = "quantity"
  val variableWeight = "variableWeight"
  val grossWeight = "grossWeight"
  val orderType = "orderType"

  val simpleFields = List(
    omsOrderId,
    siteId,
    creationDate,
    orderType,
    orderState
  )

  val commerceItemsFields = List(
    s"$commerceItems.$smsSizeColor",
    s"$commerceItems.$smsId",
    s"$commerceItems.$store",
    s"$commerceItems.$variableWeight",
    s"$commerceItems.$quantity",
    s"$commerceItems.$grossWeight",
    s"$commerceItems.$shippingGroupId"
  )

  val shippingGroupsFields = List(
    s"$shippingGroups.$shippingGroupId",
    s"$shippingGroups.$pickingDate",
    s"$shippingGroups.deliveryInformation.$deliveryDate"
  )
}

object CustomerOrdersReservations {
  val article = "matnr"
  val store = "werks"
  val orderId = "order_id"
  val siteId = "site_id"
  val orderDate = "order_date"
  val orderPreparationDate = "order_preparation_date"
  val orderDeliveryDate = "order_delivery_date"
  val unitMeasure = "unit_measure"
  val amount = "amount"
  val sherpaOrderStatus = "sherpa_order_status"

  val pkFields = List(article, store, orderId, siteId)

  val dbTagsConversion: List[(String, String)] = List(
    EcommerceOrder.article -> article,
    EcommerceOrder.store -> store,
    EcommerceOrder.omsOrderId -> orderId,
    EcommerceOrder.siteId -> siteId,
    EcommerceOrder.pickingDate -> orderDate,
    EcommerceOrder.creationDate -> orderPreparationDate,
    EcommerceOrder.deliveryDate -> orderDeliveryDate
  )

  lazy val toDBFields: DataFrame => DataFrame =
    dbTagsConversion.map(t => { df: DataFrame => df.withColumnRenamed(t._1, t._2) }).reduce(_ andThen _)
}

object ArticleTranscod {
  final val SELL_TRANSCOD_ID = "0000000002"
  final val ARTICLE_TRANSCOD_VIEW = "article_transcodification_view"

  object dbTags {
    val codeSap = "code_sap"
    val codeLegacy = "code_legacy"
    val idTranscod = "id_transcod"
  }
  import dbTags._
  val selectedFields = Seq(codeLegacy, codeSap)
  val whereClause = s"$idTranscod = '$SELL_TRANSCOD_ID'"

}

object SiteTranscod {
  final val FILTERED_DISTRIBUTION_CHANNEL = "40"
  final val SITE_TRANSCOD_VIEW = "site_transcodification_view"

  object dbTags {
    val codeSap = "werks"
    val codeLegacy = "zsiteatica"
    val distributionChannel = "vtweg"
  }
  import dbTags._
  val selectedFields = Seq(codeLegacy, codeSap)
  val whereClause = s"$distributionChannel != '$FILTERED_DISTRIBUTION_CHANNEL'"

}

object ReservationActions {
  final val RESERVATION_ACTIONS_VIEW = "reservation_actions_view"

  object dbTags {
    val site_id = "site_id"
    val order_status = "order_status"
    val sherpa_order_status = "sherpa_order_status"
    val action = "action"
  }
  import dbTags._
  val selectedFields = Seq(site_id, order_status, sherpa_order_status, action)
  val whereClause = ""

}