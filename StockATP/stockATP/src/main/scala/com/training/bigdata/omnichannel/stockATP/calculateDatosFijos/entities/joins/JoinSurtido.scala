package com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.joins

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.{SubfamilyInterface, SurtidoInterface}

case class JoinSurtidoMarcMara(
  idArticle: String,
  idStore: String,
  unityMeasure: String,
  idSubfamily: String
) extends SurtidoInterface with SubfamilyInterface

case class JoinSurtidoMarcMaraSector(
  idArticle: String,
  idStore: String,
  sector: String,
  unityMeasure: String) extends SurtidoInterface
