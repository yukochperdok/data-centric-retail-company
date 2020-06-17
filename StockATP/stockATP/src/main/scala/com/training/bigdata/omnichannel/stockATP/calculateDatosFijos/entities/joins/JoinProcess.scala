package com.training.bigdata.omnichannel.stockATP.calculateDatosFijos.entities.joins

import com.training.bigdata.omnichannel.stockATP.common.entities.interfaces.SurtidoInterface

case class JoinNstNsp(
  idArticle: String,
  idStore: String,
  nst: Option[Double],
  nsp: Option[Double]) extends SurtidoInterface

case class JoinSurtidoAndNs(
  idArticle: String,
  idStore: String,
  sector: String,
  unityMeasure: String,
  nst: Option[Double],
  nsp: Option[Double]) extends SurtidoInterface

case class JoinSurtidoNsRotation(
  idArticle: String,
  idStore: String,
  sector: String,
  unityMeasure: String,
  nst: Option[Double],
  nsp: Option[Double],
  rotation: String) extends SurtidoInterface

case class JoinSurtidoNsRotationSalesForecast(
  idArticle: String,
  idStore: String,
  sector: String,
  unityMeasure: String,
  nst: Option[Double],
  nsp: Option[Double],
  rotation: String,
  dateSale: String,
  regularSalesForecast: Int) extends SurtidoInterface