package com.training.bigdata.omnichannel.stockATP.common.entities.interfaces

import java.sql.Timestamp

trait Debugger{
  def getDebugField(name: String): Option[String]
}

object Debugger{
  object ccTags {
    val idArticle = Surtido.ccTags.idArticleTag
    val idStore = Surtido.ccTags.idStoreTag
    val idOrder = Order.ccTags.idOrderTag
    val idOrderLine = Order.ccTags.idOrderLineTag
    val idSubfamily = Subfamily.ccTags.idSubfamilyTag
    val idListMaterial = Material.ccTags.idListMaterialTag
  }
}

trait ArticuloInterface{
  val idArticle: String
}

object Articulo {
 object ccTags {
   val idArticleTag = "idArticle"
 }
}


trait SurtidoInterface{
  val idArticle: String
  val idStore: String
}

object Surtido {
  object ccTags {
    val idArticleTag = "idArticle"
    val idStoreTag = "idStore"
  }
}


trait StoreInterface{
  val idStore: String
}

object Store {
  object ccTags {
    val idStoreTag = "idStore"
  }
}


trait OrderInterface{
  val idOrder: String
  val idOrderLine: String
}

object Order {
  object ccTags {
    val idOrderTag = "idOrder"
    val idOrderLineTag = "idOrderLine"
  }
}


trait SubfamilyInterface {
  val idSubfamily: String
}

object Subfamily {
  object ccTags {
    val idSubfamilyTag = "idSubfamily"
  }
}

trait MaterialInterface {
  val idListMaterial: String
}

object Material {
  object ccTags {
    val idListMaterialTag = "idListMaterial"
  }
}

trait UpdateInterface {
  val tsUpdateDlk: Timestamp
}
object UpdateInterface {

}

trait InsertInterface extends UpdateInterface{
  val tsInsertDlk: Timestamp
}
object InsertInterface {

}

object Audit {
  object ccTags {
    val tsInsertDlkTag = "tsInsertDlk"
    val tsUpdateDlkTag = "tsUpdateDlk"
    val userInsertDlkTag = "userInsertDlk"
    val userUpdateDlkTag = "userUpdateDlk"
  }
  object dbTags {
    val tsInsertDlkTag = "ts_insert_dlk"
    val tsUpdateDlkTag = "ts_update_dlk"
    val userInsertDlkTag = "user_insert_dlk"
    val userUpdateDlkTag = "user_update_dlk"

  }
}

object TempTags {
  val tempTsUpdateDlkTag = "tempTsUpdateDlk"
  val tempUnitMeasureTag = "tempUnitMeasure"
  val tempAdjustedSalesForecastTag = "tempAdjustedSalesForecast"
  val tempStockToBeDelivered = "tempStockToBeDelivered"
  val nsTag = "ns"
}

