package com.training.bigdata.omnichannel.stockATP.common.entities

import com.training.bigdata.omnichannel.stockATP.common.entities.DatosFijos.DatosFijosImplicits
import com.training.bigdata.omnichannel.stockATP.common.entities.ControlEjecucion.ControlEjecucionImplicits
import com.training.bigdata.omnichannel.stockATP.common.entities.StockATP.StockATPImplicits
import com.training.bigdata.omnichannel.stockATP.common.entities.SurtidoMara.SurtidoMaraImplicits

package object common_entities {
  object common_implicits extends CommonEntitiesImplicits

  trait CommonEntitiesImplicits
    extends StockATPImplicits
    with DatosFijosImplicits
    with ControlEjecucionImplicits
    with SurtidoMaraImplicits
}
