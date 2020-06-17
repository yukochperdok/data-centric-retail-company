package com.training.bigdata.omnichannel.stockATP.common.util

import com.training.bigdata.omnichannel.stockATP.common.util.tag.TagTest._
import org.scalatest.{FlatSpecLike, Matchers}
import com.training.bigdata.omnichannel.stockATP.common.util.LensAppConfig._
import pureconfig.error.ConfigReaderException

trait DefaultConf {
  final val defaultProjectCMDB =
    ProjectCMDB("app","mod","url","bitbucket")
  val defaultProcess: List[Process] =
    Process("process1","des_process1")::Nil
  val defaultTopic: List[Topic] =
    Topic("topic1","des_topic1",1000000,"parquet")::Nil
  val defaultCommonParam =
    CommonParameters(
      kuduHost = "kuduHost",
      timezone = "Europe/Paris",
      defaultUnidadMedida = "EA",
      defaultNst = 100,
      defaultNsp = 100,
      defaultRotation = "B",
      defaultRegularSales = 0
    )
  val defaultDebug = Option(DebugParameters())
  val defaultPedidosCursoParam =
    PedidosCursoParameters(
      Constants.DEFAULT_FLAGS_ANULADO_BORRADO,
      Constants.DEFAULT_FLAG_DEVOLUCION,
      Constants.DEFAULT_FLAG_ENTREGADO)

  val purchaseArticleWithUniqueSaleArticleList = List(
    ArticleTypeAndCategory("ZVLG", "12")
  )
  val boxAndPrepackTypeAndCategoryList = List(
    ArticleTypeAndCategory("ZTEX", "11"),
    ArticleTypeAndCategory("ZBOX", "12")
  )

  val defaultControlEjecucionProcesses =
    List(
      "stockATP",
      "calculateDatosFijos",
      "stockConsolidado",
      "stockGLC",
      "enCursoATICAGLC",
      "enCursoGLC",
      "enCursoATICA"
    )

  val defaultStockATPParam =
    StockATPParameters(
      List(5,6,7,8,9),
      1,
      Constants.DEFAULT_SALES_MOVEMENT_CODE,
      defaultPedidosCursoParam,
      purchaseArticleWithUniqueSaleArticleList,
      boxAndPrepackTypeAndCategoryList,
      Constants.DEFAULT_LIST_TYPE_ORDERS,
      defaultControlEjecucionProcesses,
      defaultDebug
    )

  val defaultDatosFijosParam =
    DatosFijosParameters(
      List("", "S"),
      defaultDebug
    )

  val defaultParameters =
    Parameters(
      defaultCommonParam,
      defaultStockATPParam,
      defaultDatosFijosParam
    )

  val defaultApplication =
    Application(
      defaultProjectCMDB,
      defaultProcess,
      defaultTopic,
      defaultParameters
    )

  val defaultConfig =
    AppConfig(
      defaultApplication
    )
}

class AppConfigTest extends FlatSpecLike with Matchers with DefaultConf {

  final val APPCONFIG = "application.conf"
  final val APPCONFIG_LIST = "application_list.conf"
  final val APPCONFIG_ERRORS = "application_errors.conf"
  final val APPCONFIG_DEBUG = "application_with_debug.conf"

  "The AppConfig" must "be retrieved correctly a default config" taggedAs (UnitTestTag) in {
    val appConfig = AppConfig(getClass.getClassLoader.getResource(APPCONFIG).getPath)

    appConfig shouldBe defaultConfig
    appConfig.getApplication shouldBe defaultApplication
    appConfig.getProjectCMDB shouldBe defaultProjectCMDB
    appConfig.getListProcess shouldBe defaultProcess
    appConfig.getListTopic shouldBe defaultTopic
    appConfig.getParameters shouldBe defaultParameters
    appConfig.getCommonParameters shouldBe defaultCommonParam
    appConfig.getStockATPParameters shouldBe defaultStockATPParam
    appConfig.getDatosFijosParameters shouldBe defaultDatosFijosParam

  }

  it must "be retrieved correctly a list of process" taggedAs (UnitTestTag) in {
    val appConfig = AppConfig(getClass.getClassLoader.getResource(APPCONFIG_LIST).getPath)

    appConfig.getListProcess should contain theSameElementsAs(
      Process("process1","des_process1")::
      Process("process2","des_process2")::
      Process("process3","des_process3")::
      Process("process4","des_process4")::
      Nil)
  }

  it must "be retrieved correctly a list of topics" taggedAs (UnitTestTag) in {
    val appConfig = AppConfig(getClass.getClassLoader.getResource(APPCONFIG_LIST).getPath)

    appConfig.getListTopic should contain theSameElementsAs(
      Topic("topic1","des_topic1",1000000,"parquet")::
      Topic("topic2","des_topic2",1000000,"parquet")::
      Topic("topic3","des_topic3",1000000,"parquet")::
      Topic("topic4","des_topic4",1000000,"parquet")::
      Nil)
  }

  it must "not fail if not debug keys are found" taggedAs (UnitTestTag) in {
    val appConfig = AppConfig(getClass.getClassLoader.getResource(APPCONFIG).getPath)
    appConfig.getDebugStockATPParameters should be (Some(DebugParameters()))
    appConfig.getDebugDatosFijosParameters should be (Some(DebugParameters()))
  }

  it must "be retrieved correctly a debug options" taggedAs (UnitTestTag) in {
    val appConfig = AppConfig(getClass.getClassLoader.getResource(APPCONFIG_DEBUG).getPath)
    val filters1 = Map("idArticle"->"1","idStore"->"2")
    val filters2 = Map("idArticle"->"3","idStore"->"4")
    appConfig.getDebugStockATPParameters should be (Some(DebugParameters(true,true,true,filters1)))
    appConfig.getDebugDatosFijosParameters should be (Some(DebugParameters(true,true,true,filters2)))
  }

  it must "fail if not all keys are found" taggedAs (UnitTestTag) in {
    val caught = intercept[ConfigReaderException[AppConfig]] {
      AppConfig(getClass.getClassLoader.getResource(APPCONFIG_ERRORS).getPath)
    }
    info(caught.getMessage)
    assert(caught.getMessage.indexOf("application.parameters.common") != -1)
  }

  it should "have mechanisms to set values" taggedAs (UnitTestTag) in {
    val appConfig = AppConfig(getClass.getClassLoader.getResource(APPCONFIG).getPath)

    val otherTimezone = "Europe/London"
    val otherKuduHost = "otherKuduHost"
    val otherListTopic: List[Topic] = List.empty[Topic]
    val otherListProcess: List[Process] = Process("process1","des_process2") :: Nil
    val otherPedidosCursoParameters = PedidosCursoParameters(List("",""),"","")

    val otherPurchaseArticleWithUniqueSaleArticleList = List(
      ArticleTypeAndCategory("ZZ1", "211")
    )
    val otherBoxAndPrepackTypeAndCategoryList = List(
      ArticleTypeAndCategory("ZZ2", "221"),
      ArticleTypeAndCategory("ZZ3", "312")
    )

    val otherTypeOrderList = "ZACT" :: "ZADT" :: Nil

    val otherListControlEjecucionProcesses2 = List("otherProcess1")

    val otherStockATPParameters =
      StockATPParameters(
        List(1,2,3),
        1,
        "600",
        otherPedidosCursoParameters,
        otherPurchaseArticleWithUniqueSaleArticleList,
        otherBoxAndPrepackTypeAndCategoryList,
        otherTypeOrderList,
        otherListControlEjecucionProcesses2
      )

    val otherProjectCMDB = ProjectCMDB("app2","mod2","url2","bitbucket2")

    val otherAppConfig =
      defaultConfig
      .setTimezone(otherTimezone)
      .setKuduHost(otherKuduHost)
      .setListTopic(otherListTopic)
      .setListProcess(otherListProcess)
      .setStockATPParameters(otherStockATPParameters)
      .setProjectCMDB(otherProjectCMDB)


    appConfig
      .setTimezone(otherTimezone)
      .setKuduHost(otherKuduHost)
      .setListTopic(otherListTopic)
      .setListProcess(otherListProcess)
      .setStockATPParameters(otherStockATPParameters)
      .setProjectCMDB(otherProjectCMDB) should be (otherAppConfig)
  }
}
