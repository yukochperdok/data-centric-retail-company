package com.training.bigdata.omnichannel.customerOrderReservation.acceptance

import com.training.bigdata.omnichannel.customerOrderReservation.events.CustomerOrderReservationEvent.Order
import com.training.bigdata.omnichannel.customerOrderReservation.producer.KafkaAvroProducer
import com.training.bigdata.omnichannel.customerOrderReservation.services.KafkaProducerService._
import com.training.bigdata.omnichannel.customerOrderReservation.services.KuduService._
import com.training.bigdata.omnichannel.customerOrderReservation.services.SparkStreamingService._
import com.training.bigdata.omnichannel.customerOrderReservation.tables.MockTables.AcceptanceTests._
import com.training.bigdata.omnichannel.customerOrderReservation.tables.MockTables.createDataFrameByCsv
import com.training.bigdata.omnichannel.customerOrderReservation.entities.Audit
import com.training.bigdata.omnichannel.customerOrderReservation.entities.CustomerOrdersReservations.pkFields
import com.training.bigdata.omnichannel.customerOrderReservation.utils.log.CustomAppender
import cucumber.api.scala.{EN, ScalaDsl}
import com.holdenkarau.spark.testing.JavaDatasetSuiteBase
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.spark.SparkContextForTests
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{FlatSpec, Matchers}

class AcceptanceSteps extends FlatSpec with Matchers with ScalaDsl with EN
  with Eventually with SparkContextForTests {

  implicit val spark: SparkSession = sparkSession

  Before { _ =>
    initProducer(propertiesUtil)
  }

  After { _ =>
    cleanKuduTables(Seq(
      CUSTOMER_ORDER_RESERVATION_TABLE,
      CUSTOMER_ORDER_RESERVATION_ACTIONS_TABLE,
      SITE_TRANSCOD_TABLE,
      ARTICLE_TRANSCOD_TABLE
    ))
    CustomAppender.listOfEvents.clear()
    closeProducer()
  }

  val assertDataFrameEqualsWithoutTsAuditColumns: (DataFrame, DataFrame) => Unit = {
    (dfExpected: DataFrame, dfFinal: DataFrame) => {
      (new JavaDatasetSuiteBase).assertDataFrameEquals(
        dfExpected.drop(Audit.tsUpdateDlk).drop(Audit.tsInsertDlk).orderBy(pkFields.head, pkFields.tail:_*),
        dfFinal.drop(Audit.tsUpdateDlk).drop(Audit.tsInsertDlk).orderBy(pkFields.head, pkFields.tail:_*))
    }
  }

  private def cleanKuduTables(tables: Seq[String]): Unit = {
    tables.foreach( table =>
      kuduContext.deleteTable(table)
    )
  }


  private def getLoadTablesFunction(implicit specificScenarioLocation: String) =
    Seq(
      createAndLoadKuduTable(kuduContext, sparkSession, propertiesUtil)(KUDU_CUSTOMER_ORDER_RESERVATION_CSV, CUSTOMER_ORDER_RESERVATION_TABLE),
      createAndLoadKuduTable(kuduContext, sparkSession, propertiesUtil)(KUDU_CUSTOMER_ORDER_RESERVATION_ACTIONS_CSV, CUSTOMER_ORDER_RESERVATION_ACTIONS_TABLE),
      createAndLoadKuduTable(kuduContext, sparkSession, propertiesUtil)(KUDU_ARTICLE_TRANSCOD_CSV, ARTICLE_TRANSCOD_TABLE),
      createAndLoadKuduTable(kuduContext, sparkSession, propertiesUtil)(KUDU_SITE_TRANSCOD_CSV, SITE_TRANSCOD_TABLE)
    )

  /////////////////////////////////////////////////////////////////////////////
  ////////////////////////////MOCKS AND EXPECTED///////////////////////////////
  /////////////////////////////////////////////////////////////////////////////

  Given("events from (\\w+)") { scenarioLocation: String =>
    implicit val scenario: String = scenarioLocation

    initKudu(getLoadTablesFunction)
    kuduContext.tableExists(CUSTOMER_ORDER_RESERVATION_TABLE) shouldBe true
    kuduContext.tableExists(CUSTOMER_ORDER_RESERVATION_ACTIONS_TABLE) shouldBe true
    kuduContext.tableExists(SITE_TRANSCOD_TABLE) shouldBe true
    kuduContext.tableExists(ARTICLE_TRANSCOD_TABLE) shouldBe true
    countRecords(CUSTOMER_ORDER_RESERVATION_ACTIONS_TABLE) shouldBe 32

    AdminUtils.topicExists(ZkUtils.apply(zkUrl, 30000, 30000, false), propertiesUtil.getReservationsTopic) shouldBe true
  }

  When("the streaming is executed given that (\\w+)") { scenario: String =>
    implicit val scenarioLocation: String = scenario

    KafkaAvroProducer.processPipeline(new KafkaAvroProducer[Order](Some(kafkaProducer)), Some(KAFKA_ORDER_EVENTS_JSON))
    fixedClock.addTime(batchDuration)

  }

  Then("it will .* reservations info in customer orders reservations table (\\w+)") { scenarioLocation: String =>
    implicit val scenario: String = scenarioLocation

    eventually(timeout(Span(6, Seconds)), interval(Span(3, Seconds))) {

      val result: DataFrame = readTable(CUSTOMER_ORDER_RESERVATION_TABLE)

      val expected: DataFrame = createDataFrameByCsv(KUDU_CUSTOMER_ORDER_RESERVATION_OUTPUT_CSV._2, result.schema)

      assertDataFrameEqualsWithoutTsAuditColumns(expected, result)
    }
  }

  Then("it will insert reservations info in customer orders reservations table (\\w+) and trace error (.+)") {
    (scenarioLocation: String, expectedLogMessage: String) =>

    implicit val scenario: String = scenarioLocation

    eventually(timeout(Span(6, Seconds)), interval(Span(3, Seconds))) {

      val result: DataFrame = readTable(CUSTOMER_ORDER_RESERVATION_TABLE)

      val expected: DataFrame = createDataFrameByCsv(KUDU_CUSTOMER_ORDER_RESERVATION_OUTPUT_CSV._2, result.schema)

      assertDataFrameEqualsWithoutTsAuditColumns(expected, result)

      CustomAppender.listOfEvents.find( event => event.getMessage.toString.contains(expectedLogMessage)) shouldBe defined
    }
  }


}
