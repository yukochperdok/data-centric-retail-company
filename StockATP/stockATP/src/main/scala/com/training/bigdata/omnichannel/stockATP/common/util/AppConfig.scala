package com.training.bigdata.omnichannel.stockATP.common.util

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import pureconfig.{CamelCase, ConfigFieldMapping, ProductHint, loadConfigOrThrow}


object AppConfig {
  implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
  def fromConfig(conf:Config = ConfigFactory.load()):AppConfig = loadConfigOrThrow[AppConfig](conf)
  def apply(path:String): AppConfig = fromConfig(ConfigFactory.parseFile(new File(path)))
}

case class AppConfig(application: Application)

case class Application(
  cmdb: ProjectCMDB,
  process: List[Process],
  topic: List[Topic],
  parameters: Parameters
)
case class ProjectCMDB(
  CMDBapp: String,
  CMDBMod: String,
  Confluence: String,
  BitBucket: String
)
case class Process(
  name: String,
  description: String
)
case class Topic(
  name: String,
  description: String,
  max_message_bytes: Int,
  format: String
)
case class Parameters(
  common: CommonParameters,
  stockATP: StockATPParameters,
  calculateDatosFijos: DatosFijosParameters
)

case class CommonParameters(
  kuduHost: String,
  timezone: String = "Europe/Paris",
  defaultUnidadMedida: String = "EA",
  defaultNst: Double = 100,
  defaultNsp: Double = 100,
  defaultRotation: String = "B",
  defaultRegularSales: Int = 0
)

case class DebugParameters(
  activateExplain: Boolean = false,
  activateDebugDataset: Boolean = false,
  activateShowDataset: Boolean = false,
  filters: Map[String, String] = Map.empty[String, String]
)

case class StockATPParameters(
  listCapacitiesPP: List[Int] = List(5,6,7,8,9),
  stockTypeOMS: Int = 1,
  movementCode: String = Constants.DEFAULT_SALES_MOVEMENT_CODE,
  pedidosCurso: PedidosCursoParameters,
  listPuchaseArticleWithUniqueSaleArticle: List[ArticleTypeAndCategory] =
    Constants.PURCHASE_ARTICLE_TYPE_AND_CATEGORY_WITH_UNIQUE_SALE_ARTICLE,
  listBoxOrPrepackArticle: List[ArticleTypeAndCategory] =
    Constants.BOX_AND_PREPACK_TYPE_AND_CATEGORY,
  listTypeOrders: List[String] = Constants.DEFAULT_LIST_TYPE_ORDERS,
  controlEjecucionProcesses: List[String] = List(),
  debug: Option[DebugParameters] = Option(DebugParameters())
)
case class PedidosCursoParameters(
  canceledFlagList: List[String] = Constants.DEFAULT_FLAGS_ANULADO_BORRADO,
  returnedToProviderFlag: String = Constants.DEFAULT_FLAG_DEVOLUCION,
  alreadyDeliveredFlag: String = Constants.DEFAULT_FLAG_ENTREGADO
)
case class DatosFijosParameters (
  listSaleArticles : List[String] = List("", "S"),
  debug: Option[DebugParameters] = Option(DebugParameters())
)


import monocle.Lens
import monocle.macros.GenLens

object LensAppConfig{
  val applicationLens: Lens[AppConfig,Application] = GenLens[AppConfig](_.application)
  val projectLens: Lens[AppConfig,ProjectCMDB] = GenLens[AppConfig](_.application.cmdb)
  val processLens: Lens[AppConfig,List[Process]] = GenLens[AppConfig](_.application.process)
  val topicLens: Lens[AppConfig,List[Topic]] = GenLens[AppConfig](_.application.topic)
  val parametersLens: Lens[AppConfig,Parameters] = GenLens[AppConfig](_.application.parameters)
  val commonParametersLens: Lens[AppConfig,CommonParameters] = GenLens[AppConfig](_.application.parameters.common)
  val stockATPParametersLens: Lens[AppConfig,StockATPParameters] = GenLens[AppConfig](_.application.parameters.stockATP)
  val datosFijosParametersLens: Lens[AppConfig,DatosFijosParameters] = GenLens[AppConfig](_.application.parameters.calculateDatosFijos)

  val debugStockATPParametersLens: Lens[AppConfig,Option[DebugParameters]] =
    GenLens[AppConfig](_.application.parameters.stockATP.debug)
  val debugDatosFijosParametersLens: Lens[AppConfig,Option[DebugParameters]] =
    GenLens[AppConfig](_.application.parameters.calculateDatosFijos.debug)

  val controlEjecucionProcessesLens: Lens[StockATPParameters,List[String]] = GenLens[StockATPParameters](_.controlEjecucionProcesses)
  val controlEjecucionProcessesComposeLens: Lens[AppConfig,List[String]] = stockATPParametersLens composeLens controlEjecucionProcessesLens

  val kuduHostLens: Lens[CommonParameters, String] = GenLens[CommonParameters](_.kuduHost)
  val kuduHostComposeLens: Lens[AppConfig, String] = commonParametersLens composeLens kuduHostLens

  val timeZoneLens: Lens[CommonParameters, String] = GenLens[CommonParameters](_.timezone)
  val timeZoneComposeLens: Lens[AppConfig, String] = commonParametersLens composeLens timeZoneLens

  val defaultNstLens: Lens[CommonParameters, Double] = GenLens[CommonParameters](_.defaultNst)
  val defaultNstComposeLens: Lens[AppConfig, Double] = commonParametersLens composeLens defaultNstLens

  val defaultNspLens: Lens[CommonParameters, Double] = GenLens[CommonParameters](_.defaultNsp)
  val defaultNspComposeLens: Lens[AppConfig, Double] = commonParametersLens composeLens defaultNspLens

  val listCapacitiesPPLens: Lens[StockATPParameters,List[Int]] = GenLens[StockATPParameters](_.listCapacitiesPP)
  val listCapacitiesPPComposeLens: Lens[AppConfig,List[Int]] = stockATPParametersLens composeLens listCapacitiesPPLens

  val stockTypeOMSLens: Lens[StockATPParameters,Int] = GenLens[StockATPParameters](_.stockTypeOMS)
  val stockTypeOMSComposeLens: Lens[AppConfig,Int] = stockATPParametersLens composeLens stockTypeOMSLens

  val movementCodeLens: Lens[StockATPParameters,String] = GenLens[StockATPParameters](_.movementCode)
  val movementCodeComposeLens: Lens[AppConfig,String] = stockATPParametersLens composeLens movementCodeLens

  val pedidosCursoParametersLens: Lens[StockATPParameters,PedidosCursoParameters] = GenLens[StockATPParameters](_.pedidosCurso)
  val pedidosCursoParametersComposeLens: Lens[AppConfig,PedidosCursoParameters] = stockATPParametersLens composeLens pedidosCursoParametersLens

  val listPuchaseArticleWithUniqueSaleArticleLens: Lens[StockATPParameters,List[ArticleTypeAndCategory]] = GenLens[StockATPParameters](_.listPuchaseArticleWithUniqueSaleArticle)
  val listPuchaseArticleWithUniqueSaleArticleComposeLens: Lens[AppConfig,List[ArticleTypeAndCategory]] = stockATPParametersLens composeLens listPuchaseArticleWithUniqueSaleArticleLens

  val listBoxOrPrepackArticleLens: Lens[StockATPParameters,List[ArticleTypeAndCategory]] = GenLens[StockATPParameters](_.listBoxOrPrepackArticle)
  val listBoxOrPrepackArticleComposeLens: Lens[AppConfig,List[ArticleTypeAndCategory]] = stockATPParametersLens composeLens listBoxOrPrepackArticleLens

  val listTypeOrdersLens: Lens[StockATPParameters,List[String]] = GenLens[StockATPParameters](_.listTypeOrders)
  val listTypeOrdersComposeLens: Lens[AppConfig,List[String]] = stockATPParametersLens composeLens listTypeOrdersLens

  val listSaleArticlesLens: Lens[DatosFijosParameters,List[String]] = GenLens[DatosFijosParameters](_.listSaleArticles)
  val listSaleArticlesComposeLens: Lens[AppConfig,List[String]] = datosFijosParametersLens composeLens listSaleArticlesLens

  implicit class AppConfigToValues (conf:AppConfig) {
    def getApplication: Application = {
      applicationLens.get(conf)
    }

    def setApplication (application: Application): AppConfig = {
      applicationLens.set(application)(conf)
    }

    def getProjectCMDB: ProjectCMDB = {
      projectLens.get(conf)
    }

    def setProjectCMDB(project: ProjectCMDB): AppConfig = {
      projectLens.set(project)(conf)
    }

    def getListProcess: List[Process] = {
      processLens.get(conf)
    }

    def setListProcess(listProcces: List[Process]): AppConfig = {
      processLens.set(listProcces)(conf)
    }

    def getListTopic: List[Topic] = {
      topicLens.get(conf)
    }

    def setListTopic(listTopic: List[Topic]): AppConfig = {
      topicLens.set(listTopic)(conf)
    }

    def getParameters: Parameters = {
      parametersLens.get(conf)
    }

    def setParameters(parameters: Parameters): AppConfig = {
      parametersLens.set(parameters)(conf)
    }

    def getCommonParameters: CommonParameters = {
      commonParametersLens.get(conf)
    }

    def setCommonParameters(commonParameters: CommonParameters): AppConfig = {
      commonParametersLens.set(commonParameters)(conf)
    }

    def getKuduHost: String = {
      kuduHostComposeLens.get(conf)
    }

    def setKuduHost(kuduHost:String): AppConfig = {
      kuduHostComposeLens.set(kuduHost)(conf)
    }

    def getTimezone: String = {
      timeZoneComposeLens.get(conf)
    }

    def setTimezone(timezone: String): AppConfig = {
      timeZoneComposeLens.set(timezone)(conf)
    }

    def getDefaultNst: Double = {
      defaultNstComposeLens.get(conf)
    }

    def setDefaultNst(nst: Double): AppConfig = {
      defaultNstComposeLens.set(nst)(conf)
    }

    def getDefaultNsp: Double = {
      defaultNspComposeLens.get(conf)
    }

    def setDefaultNsp(nsp: Double): AppConfig = {
      defaultNspComposeLens.set(nsp)(conf)
    }

    def getStockATPParameters: StockATPParameters = {
      stockATPParametersLens.get(conf)
    }

    def setStockATPParameters(stockATPParameters: StockATPParameters): AppConfig = {
      stockATPParametersLens.set(stockATPParameters)(conf)
    }

    def getDebugStockATPParameters: Option[DebugParameters] = {
      debugStockATPParametersLens.get(conf)
    }

    def setDebugStockATPParameters(debugStockATPParameters: DebugParameters): AppConfig = {
      debugStockATPParametersLens.set(Option(debugStockATPParameters))(conf)
    }

    def getDebugDatosFijosParameters: Option[DebugParameters] = {
      debugDatosFijosParametersLens.get(conf)
    }

    def setDebugDatosFijosParameters(debugDatosFijosParameters: DebugParameters): AppConfig = {
      debugDatosFijosParametersLens.set(Option(debugDatosFijosParameters))(conf)
    }

    def getControlEjecucionProcesses: List[String] = {
      controlEjecucionProcessesComposeLens.get(conf)
    }

    def setControlEjecucionProcesses(controlEjecucionProcesses: List[String]): AppConfig = {
      controlEjecucionProcessesComposeLens.set(controlEjecucionProcesses)(conf)
    }

    def getStockToBeDeliveredParameters: PedidosCursoParameters = {
      pedidosCursoParametersComposeLens.get(conf)
    }

    def setPedidosCursoParameters(pedidosCursoParameters: PedidosCursoParameters): AppConfig = {
      pedidosCursoParametersComposeLens.set(pedidosCursoParameters)(conf)
    }

    def getPuchaseArticleWithUniqueSaleArticleList: List[ArticleTypeAndCategory] = {
      listPuchaseArticleWithUniqueSaleArticleComposeLens.get(conf)
    }

    def setPuchaseArticleWithUniqueSaleArticleList(listPuchaseArticleWithUniqueSaleArticle: List[ArticleTypeAndCategory]): AppConfig = {
      listPuchaseArticleWithUniqueSaleArticleComposeLens.set(listPuchaseArticleWithUniqueSaleArticle)(conf)
    }

    def getBoxOrPrepackArticleList: List[ArticleTypeAndCategory] = {
      listBoxOrPrepackArticleComposeLens.get(conf)
    }

    def setBoxOrPrepackArticleList(listBoxOrPrepackArticle: List[ArticleTypeAndCategory]): AppConfig = {
      listBoxOrPrepackArticleComposeLens.set(listBoxOrPrepackArticle)(conf)
    }

    def getTypeOrdersList: List[String] = {
      listTypeOrdersComposeLens.get(conf)
    }

    def setTypeOrdersList(listTypeOrders: List[String]): AppConfig = {
      listTypeOrdersComposeLens.set(listTypeOrders)(conf)
    }

    def getListCapacitiesPP: List[Int] = {
      listCapacitiesPPComposeLens.get(conf)
    }

    def setListCapacitiesPP(list: List[Int]): AppConfig = {
      listCapacitiesPPComposeLens.set(list)(conf)
    }

    def getStockTypeOMS: Int = {
      stockTypeOMSComposeLens.get(conf)
    }

    def setStockTypeOMS(stockTypeOMS: Int): AppConfig = {
      stockTypeOMSComposeLens.set(stockTypeOMS)(conf)
    }

    def getMovementCode: String = {
      movementCodeComposeLens.get(conf)
    }

    def setMovementCode(movementCode: String): AppConfig = {
      movementCodeComposeLens.set(movementCode)(conf)
    }

    def getSaleArticlesList: List[String] = {
      listSaleArticlesComposeLens.get(conf)
    }

    def setSaleArticleList(list: List[String]): AppConfig = {
      listSaleArticlesComposeLens.set(list)(conf)
    }

    def getDatosFijosParameters: DatosFijosParameters = {
      datosFijosParametersLens.get(conf)
    }

    def setDatosFijosParameters(datosFijosParameters: DatosFijosParameters): AppConfig = {
      datosFijosParametersLens.set(datosFijosParameters)(conf)
    }
  }
}