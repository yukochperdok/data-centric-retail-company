package com.training.bigdata.mdata.INEaddresses.acceptance.INEFilesIngestion

import com.training.bigdata.mdata.INEaddresses.common.entities.interfaces.Audit
import com.training.bigdata.mdata.INEaddresses.common.util.runner.INEFilesIngestionRunner
import com.training.bigdata.mdata.INEaddresses.logging.CustomAppender
import com.training.bigdata.mdata.INEaddresses.services.{HiveService => Hive, KuduService => Kudu}
import com.training.bigdata.mdata.INEaddresses.services.HiveService.LoadFunctions
import com.holdenkarau.spark.testing.DatasetSuiteBase
import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.DataFrame
import org.scalatest.concurrent.Eventually
import org.scalatest.{FlatSpec, Matchers}

class ZipCodesStreetMapSteps extends FlatSpec with Matchers with ScalaDsl with EN
  with Eventually with DatasetSuiteBase {

  val assertDataFrameEqualsWithoutTsAuditColumns: (DataFrame, DataFrame, String) => Unit = {
    (dfExpected: DataFrame, dfFinal: DataFrame, orderColumn: String) => {
      assertDataFrameEquals(
        dfExpected.drop(Audit.tsUpdateDlk).drop(Audit.tsInsertDlk).orderBy(orderColumn),
        dfFinal.drop(Audit.tsUpdateDlk).drop(Audit.tsInsertDlk).orderBy(orderColumn))
    }
  }

  var scenarioLocation = ""
  private val baseDataDir: String = "data/"
  private val baseOutputDir: String = "/output/"
  private val baseProcessDir = "/INEFilesIngestion/"

  Given("a (\\w+) with previous data of street stretches") { implicit scenario: String =>

    val loadsTableHiveFun: LoadFunctions = Seq(
      Hive.createDatabase(
        Hive.MASTERDATA_RAW_DATABASE
      ),
      Hive.createDatabase(
        Hive.MASTERDATA_LAST_DATABASE
      ),
      Hive.createAndLoadHivePartitionedTable(
        Hive.STREET_STRETCHES_HISTORIC_RAW_TABLE,
        Hive.streetStretchesHistoricRawTable._1, Hive.streetStretchesHistoricRawTable._3, Hive.streetStretchesHistoricRawTable._2),
      Hive.createAndLoadHiveTable(
        Hive.STREET_STRETCHES_HISTORIC_LAST_TABLE,
        Hive.streetStretchesHistoricLastTable._1, Hive.streetStretchesHistoricLastTable._2)
    )

    Hive.initHive(loadsTableHiveFun)

    Hive.spark.sql(s"REFRESH TABLE ${Hive.STREET_STRETCHES_HISTORIC_RAW_TABLE}")
    Hive.spark.sql(s"REFRESH TABLE ${Hive.STREET_STRETCHES_HISTORIC_LAST_TABLE}")

    val initialHistoricRaw  = Hive.spark.sql(s"SELECT * FROM ${Hive.STREET_STRETCHES_HISTORIC_RAW_TABLE}")
    val initialHistoricLast = Hive.spark.sql(s"SELECT * FROM ${Hive.STREET_STRETCHES_HISTORIC_LAST_TABLE}")

    scenarioLocation = scenario
  }

  When("a.*street stretches file .* in (\\w+)") { filePath: String =>
    INEFilesIngestionRunner.main(Array("localhost", "false", getClass.getClassLoader.getResource(s"$baseDataDir$baseProcessDir$scenarioLocation/$filePath").getPath))
  }

  When("a .*street stretches file .* in (\\w+) with historify flag active") { filePath: String =>
    INEFilesIngestionRunner.main(Array("localhost", "false", getClass.getClassLoader.getResource(s"$baseDataDir$baseProcessDir$scenarioLocation/$filePath").getPath, "stretches"))
  }

  Then("all street stretches should be historified with all file information.*plus audit fields in (.+), .*in (.+),.* in (.+)") {
    (historicRawCsv: String, historicLastCsv: String, zipCodesStreetMapCsv: String) =>

      val expectedHistoricRaw   = Hive.createDataFrameByOutputCsv(
        baseProcessDir,
        scenarioLocation,
        historicRawCsv,
        Hive.streetStretchesHistoricRawTable(scenarioLocation)._1
      )
      val expectedHistoricLast  = Hive.createDataFrameByOutputCsv(
        baseProcessDir,
        scenarioLocation,
        historicLastCsv,
        Hive.streetStretchesHistoricLastTable(scenarioLocation)._1
      )

    val resultHistoricRaw  = Hive.spark.sql(s"SELECT * FROM ${Hive.STREET_STRETCHES_HISTORIC_RAW_TABLE}")
    val resultHistoricLast = Hive.spark.sql(s"SELECT * FROM ${Hive.STREET_STRETCHES_HISTORIC_LAST_TABLE}")

    assertDataFrameEqualsWithoutTsAuditColumns(expectedHistoricRaw, resultHistoricRaw, "previous_bland")
    assertDataFrameEqualsWithoutTsAuditColumns(expectedHistoricLast, resultHistoricLast, "previous_bland")
  }

  Then("all street stretches should be historified, the zip codes street map should be generated and process should be traced with (.+)") {
    lastProcessLog: String =>

    CustomAppender.listOfEvents.find( event => event.getMessage.toString.contains(lastProcessLog)) shouldBe defined
  }

  Then("an (.+) should be thrown and nothing should be written in (.+) or (.+) or (.+)") {
    (expectedLogMessage: String, historicRawCsv: String, historicLastCsv: String, zipCodesStreetMapCsv: String) =>

      val expectedHistoricRaw   = Hive.createDataFrameByOutputCsv(
        baseProcessDir,
        scenarioLocation,
        historicRawCsv,
        Hive.streetStretchesHistoricRawTable(scenarioLocation)._1
      )
      val expectedHistoricLast  = Hive.createDataFrameByOutputCsv(
        baseProcessDir,
        scenarioLocation,
        historicLastCsv,
        Hive.streetStretchesHistoricLastTable(scenarioLocation)._1
      )

    val resultHistoricRaw  = Hive.spark.sql(s"SELECT * FROM ${Hive.STREET_STRETCHES_HISTORIC_RAW_TABLE}")
    val resultHistoricLast = Hive.spark.sql(s"SELECT * FROM ${Hive.STREET_STRETCHES_HISTORIC_LAST_TABLE}")

    assertDataFrameEqualsWithoutTsAuditColumns(expectedHistoricRaw, resultHistoricRaw, "previous_bland")
    assertDataFrameEqualsWithoutTsAuditColumns(expectedHistoricLast, resultHistoricLast, "previous_bland")

    CustomAppender.listOfEvents.find( event => event.getMessage.toString.contains(expectedLogMessage)) shouldBe defined
  }

  private def cleanKuduTables(tables: Seq[String]): Unit = {
    tables.map(Kudu.kuduClient.deleteTable)
  }

}
