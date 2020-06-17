package com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities

import com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs.CustomerReservations.CustomerReservationsImplicits
import com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs.SaleMovements.SaleMovementsImplicits
import com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs.PartialOrdersDelivered.PartialOrdersDeliveredImplicits
import com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs.OrderLine.OrderLineImplicits
import com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs.Materials.MaterialsImplicits
import com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs.MaterialsArticle.MaterialsArticleImplicits
import com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs.OrderHeader.OrderHeaderImplicits
import com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs.StockConf.StockConfImplicits
import com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs.StockConsolidado.StockConsolidadoImplicits
import com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs.StoreCapacity.StoreCapacityImplicits
import com.training.bigdata.omnichannel.stockATP.calculateStockATP.entities.inputs.StoreType.StoreTypeImplicits

package object inputs {
  object implicits extends InputsImplicits

  trait InputsImplicits
    extends StockConfImplicits
      with StoreCapacityImplicits
      with StoreTypeImplicits
      with StockConsolidadoImplicits
      with SaleMovementsImplicits
      with OrderHeaderImplicits
      with OrderLineImplicits
      with PartialOrdersDeliveredImplicits
      with MaterialsArticleImplicits
      with MaterialsImplicits
      with CustomerReservationsImplicits
}
