package com.training.bigdata.mdata.INEaddresses.acceptance.masters

import com.training.bigdata.mdata.INEaddresses.common.entities.interfaces.Audit
import com.training.bigdata.mdata.INEaddresses.common.util.SparkLocalTest
import com.training.bigdata.mdata.INEaddresses.common.util.runner.MastersRunner
import com.training.bigdata.mdata.INEaddresses.logging.CustomAppender
import com.training.bigdata.mdata.INEaddresses.services.KuduService.LoadFunctions
import com.training.bigdata.mdata.INEaddresses.services.{KuduService => Kudu}
import com.holdenkarau.spark.testing.JavaDatasetSuiteBase
import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.DataFrame
import org.scalatest.concurrent.Eventually
import org.scalatest.{FlatSpec, Matchers}
import com.training.bigdata.mdata.INEaddresses.common.util.io.Tables.{DataFramesReadOperations, Kudu => K}
import com.training.bigdata.mdata.INEaddresses.common.util.io.Tables.Implementations.KuduDs

class MastersAcceptanceSteps extends FlatSpec with Matchers with ScalaDsl with EN
  with Eventually with SparkLocalTest {

  val assertDataFrameEqualsWithoutTsAuditColumns: (DataFrame, DataFrame) => Unit = {
    (dfExpected: DataFrame, dfFinal: DataFrame) => {
      (new JavaDatasetSuiteBase).assertDataFrameEquals(
        dfExpected.drop(Audit.tsUpdateDlk).drop(Audit.tsInsertDlk),
        dfFinal.drop(Audit.tsUpdateDlk).drop(Audit.tsInsertDlk))
    }
  }

  var scenarioLocation = ""
  private val baseDataDir: String = "data/"
  private val baseProcessDir = "/masters/"

  Given("a (\\w+) with only previous data of ccaa") { implicit scenario: String =>

    Kudu.cleanKuduTables

    val createKuduTables: LoadFunctions = Seq(
      Kudu.createKuduTable(Kudu.CCAA_TABLE,Kudu.ccaaTable._1),
      Kudu.createKuduTable(Kudu.POSTAL_CODES_TABLE,Kudu.postalCodesTable._1),
      Kudu.createKuduTable(Kudu.TOWNS_TABLE,Kudu.townsTable._1)
    )

    val loadKuduTables: LoadFunctions = Seq(
      Kudu.loadKuduTable(Kudu.CCAA_TABLE,Kudu.ccaaTable._2),
      Kudu.loadKuduTable(Kudu.POSTAL_CODES_TABLE,Kudu.postalCodesTable._2),
      Kudu.loadKuduTable(Kudu.TOWNS_TABLE,Kudu.townsTable._2)
    )

    Kudu.createInitialKudu(createKuduTables)
    Kudu.loadInitialKudu(loadKuduTables)

    scenarioLocation = scenario
  }

  Given("a (\\w+) with only previous data of street map") { implicit scenario: String =>

    Kudu.cleanKuduTables

    val createKuduTables: LoadFunctions = Seq(
      Kudu.createKuduTable(Kudu.STREET_MAP_TABLE,Kudu.streetMapTable._1),
      Kudu.createKuduTable(Kudu.POSTAL_CODES_TABLE,Kudu.postalCodesTable._1),
      Kudu.createKuduTable(Kudu.TOWNS_TABLE,Kudu.townsTable._1)
    )

    val loadKuduTables: LoadFunctions = Seq(
      Kudu.loadKuduTable(Kudu.STREET_MAP_TABLE,Kudu.streetMapTable._2),
      Kudu.loadKuduTable(Kudu.POSTAL_CODES_TABLE,Kudu.postalCodesTable._2),
      Kudu.loadKuduTable(Kudu.TOWNS_TABLE,Kudu.townsTable._2)
    )

    Kudu.createInitialKudu(createKuduTables)
    Kudu.loadInitialKudu(loadKuduTables)

    scenarioLocation = scenario
  }

  Given("a (\\w+) with previous data of street map and ccaa") { implicit scenario: String =>

    Kudu.cleanKuduTables

    val createKuduTables: LoadFunctions = Seq(
      Kudu.createKuduTable(Kudu.CCAA_TABLE,Kudu.ccaaTable._1),
      Kudu.createKuduTable(Kudu.STREET_MAP_TABLE,Kudu.streetMapTable._1),
      Kudu.createKuduTable(Kudu.POSTAL_CODES_TABLE,Kudu.postalCodesTable._1),
      Kudu.createKuduTable(Kudu.TOWNS_TABLE,Kudu.townsTable._1)
    )

    val loadKuduTables: LoadFunctions = Seq(
      Kudu.loadKuduTable(Kudu.CCAA_TABLE,Kudu.ccaaTable._2),
      Kudu.loadKuduTable(Kudu.STREET_MAP_TABLE,Kudu.streetMapTable._2),
      Kudu.loadKuduTable(Kudu.POSTAL_CODES_TABLE,Kudu.postalCodesTable._2),
      Kudu.loadKuduTable(Kudu.TOWNS_TABLE,Kudu.townsTable._2)
    )

    Kudu.createInitialKudu(createKuduTables)
    Kudu.loadInitialKudu(loadKuduTables)

    scenarioLocation = scenario
  }

  When("any .* entity is .*available in (\\w+).*") { filePath: String =>
    MastersRunner.main(Array("localhost", "false", getClass.getClassLoader.getResource(s"$baseDataDir$baseProcessDir$scenarioLocation/$filePath").getPath))
  }

  Then("an (.+) should be thrown and .+ should be written (.+) and (.+)") {
    (alarm: String, townsCsv: String, postalCodesCsv: String) =>

    val expectedTowns = spark.createDataFrame(
      Kudu.createDataFrameByOutputCsv(
        baseProcessDir,
        scenarioLocation,
        townsCsv,
        Kudu.townsTable(scenarioLocation)._3).rdd,
      Kudu.townsTable(scenarioLocation)._3)
    val expectedPostalCodes = spark.createDataFrame(
      Kudu.createDataFrameByOutputCsv(
        baseProcessDir,
        scenarioLocation,
        postalCodesCsv,
        Kudu.postalCodesTable(scenarioLocation)._3).rdd,
      Kudu.postalCodesTable(scenarioLocation)._3)

    val resultTowns  = Kudu.TOWNS_TABLE.readFrom[K]("localhost").get
    val resultPostalCodes = Kudu.POSTAL_CODES_TABLE.readFrom[K]("localhost").get


    assertDataFrameEqualsWithoutTsAuditColumns(expectedTowns, resultTowns)
    assertDataFrameEqualsWithoutTsAuditColumns(expectedPostalCodes, resultPostalCodes)

    CustomAppender.listOfEvents.find( event => event.getMessage.toString.contains(alarm)) shouldBe defined
  }

  Then("all postal codes and towns should be inserted plus audit fields in (.+) and (.+)") {
    (townsCsv: String, postalCodesCsv: String) =>

      val expectedTowns = spark.createDataFrame(
        Kudu.createDataFrameByOutputCsv(
          baseProcessDir,
          scenarioLocation,
          townsCsv,
          Kudu.townsTable(scenarioLocation)._3).rdd,
        Kudu.townsTable(scenarioLocation)._3)
      val expectedPostalCodes = spark.createDataFrame(
        Kudu.createDataFrameByOutputCsv(
          baseProcessDir,
          scenarioLocation,
          postalCodesCsv,
          Kudu.postalCodesTable(scenarioLocation)._3).rdd,
        Kudu.postalCodesTable(scenarioLocation)._3)

    val resultTowns  = Kudu.TOWNS_TABLE.readFrom[K]("localhost").get
    val resultPostalCodes = Kudu.POSTAL_CODES_TABLE.readFrom[K]("localhost").get

    assertDataFrameEqualsWithoutTsAuditColumns(expectedTowns, resultTowns)
    assertDataFrameEqualsWithoutTsAuditColumns(expectedPostalCodes, resultPostalCodes)

  }

  Then("process should be traced with (.+) and those provinces should not be written in (.+) and (.+)") {

    (logMessage: String, townsCsv: String, postalCodesCsv: String) =>
      val expectedTowns = spark.createDataFrame(
        Kudu.createDataFrameByOutputCsv(
          baseProcessDir,
          scenarioLocation,
          townsCsv,
          Kudu.townsTable(scenarioLocation)._3).rdd,
        Kudu.townsTable(scenarioLocation)._3)
      val expectedPostalCodes = spark.createDataFrame(
        Kudu.createDataFrameByOutputCsv(
          baseProcessDir,
          scenarioLocation,
          postalCodesCsv,
          Kudu.postalCodesTable(scenarioLocation)._3).rdd,
        Kudu.postalCodesTable(scenarioLocation)._3)

      val resultTowns  = Kudu.TOWNS_TABLE.readFrom[K]("localhost").get
      val resultPostalCodes = Kudu.POSTAL_CODES_TABLE.readFrom[K]("localhost").get

      assertDataFrameEqualsWithoutTsAuditColumns(expectedTowns, resultTowns)
      assertDataFrameEqualsWithoutTsAuditColumns(expectedPostalCodes, resultPostalCodes)

      CustomAppender.listOfEvents.find( event => event.getMessage.toString.contains(logMessage)) shouldBe defined

  }

}
