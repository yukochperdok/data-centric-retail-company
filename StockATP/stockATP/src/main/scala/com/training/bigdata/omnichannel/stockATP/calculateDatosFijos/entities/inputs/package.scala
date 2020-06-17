package com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities

import com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.inputs.Nsp.NspImplicits
import com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.inputs.Nst.NstImplicits
import com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.inputs.SupplySkuRotation.SupplySkuRotationImplicits
import com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.inputs.SalesForecast.SalesForecastImplicits
import com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.inputs.SalesPlatform.SalesPlatformImplicits
import com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.inputs.SurtidoEstructuraComercial.SurtidoEstructuraComercialImplicits
import com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.inputs.SurtidoMarc.SurtidoMarcImplicits


package object inputs {
  object implicits extends InputsImplicits

  trait InputsImplicits
    extends SupplySkuRotationImplicits
      with SalesForecastImplicits
      with SalesPlatformImplicits
      with NstImplicits
      with NspImplicits
      with SurtidoMarcImplicits
      with SurtidoEstructuraComercialImplicits

}
