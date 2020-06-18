package com.training.bigdata.mdata.INEaddresses.common.util.io

import com.training.bigdata.mdata.INEaddresses.common.util.SparkLocalTest
import com.training.bigdata.mdata.INEaddresses.common.util.tag.TagTest.AcceptanceTestsSuite
import com.training.bigdata.mdata.INEaddresses.common.util.io.Tables.{DataFramesReadOperations, DataFramesWriteOperations, Hive, Kudu, OperationType}
import com.training.bigdata.mdata.INEaddresses.docker.DockerKuduService
import com.training.bigdata.mdata.INEaddresses.services.KuduService._
import com.training.bigdata.mdata.INEaddresses.services.HiveService.{countRecords, createAndLoadHiveTable, initHive}
import com.whisk.docker.impl.dockerjava.DockerKitDockerJava
import com.whisk.docker.scalatest.DockerTestKit
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterEach, Matchers, WordSpec}
import scala.concurrent.duration._

import scala.concurrent.Await
import scala.util.Failure

class TablesTest extends WordSpec
  with Matchers
  with SparkLocalTest
  with DockerTestKit
  with DockerKitDockerJava
  with DockerKuduService
  with BeforeAndAfterEach{

  implicit val sparkSession = spark
  private implicit val specificLocationPath: String = "/integration"
  private val KUDU_TABLE = "impala::domain.kudu_table"
  private val HIVE_TABLE = "hive_table"
  private val NOT_EXIST_TABLE = "notExistTableName"
  private val functionHive = Seq(createAndLoadHiveTable(HIVE_TABLE, TEST_HIVE_TABLE_CSV._1, TEST_HIVE_TABLE_CSV._2))
  private lazy val functionCreateKudu = Seq(createKuduTable(KUDU_TABLE, TEST_KUDU_TABLE_CSV._1))
  private lazy val functionInitialLoadKudu = Seq(loadKuduTable(KUDU_TABLE, TEST_KUDU_TABLE_CSV._2))

  private def cleanKuduTables(tables: Seq[String]): Unit = {
    import com.training.bigdata.mdata.INEaddresses.common.util.io.Tables.Implementations.KuduDs
    tables.map{
      table =>
        val readDF: DataFrame = table.readFrom[Kudu]().get
        if (!readDF.rdd.isEmpty()) readDF.select("fullName").deleteFrom[Kudu](table)
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    Await.result(containerManager.states.head.isRunning(), 7 seconds)
    initKudu
    createInitialKudu(functionCreateKudu)

    initHive(functionHive)
  }

  override def afterAll(): Unit = {
    closeKudu()
    super.afterAll()
  }

  override def afterEach(): Unit = {
    super.beforeEach()
    cleanKuduTables(
      KUDU_TABLE ::
      Nil
    )
  }


  "kudu tables" should {
    "be created" taggedAs AcceptanceTestsSuite in {
      kuduClient.getTablesList.getTablesList.size() shouldBe 1
      kuduClient.tableExists(KUDU_TABLE) shouldBe true
      countKuduRecords(KUDU_TABLE) shouldBe 0
    }
    "be loaded" taggedAs AcceptanceTestsSuite in {
      loadInitialKudu(functionInitialLoadKudu)
      kuduClient.getTablesList.getTablesList.size() shouldBe 1
      kuduClient.tableExists(KUDU_TABLE) shouldBe true
      countKuduRecords(KUDU_TABLE) shouldBe 1
    }
  }

  "hive tables" should {
    "be created" taggedAs AcceptanceTestsSuite in {
      countRecords(HIVE_TABLE) shouldBe 1
    }
  }

  "Tables Component for Hive" should {

    import com.training.bigdata.mdata.INEaddresses.common.util.io.Tables.Implementations.HiveDs

    "Read the specific DataFrame" taggedAs AcceptanceTestsSuite in {
      val triedFrame = HIVE_TABLE.readFrom[Hive]()
      triedFrame should be a 'success
      triedFrame.get.except(getExpectedDf).count() shouldBe 0
    }

    "Read the not exist DataFrame" taggedAs AcceptanceTestsSuite in {
      val triedFrame = NOT_EXIST_TABLE.readFrom[Hive]()
      triedFrame should be a 'failure
      triedFrame match {
        case Failure(exception) => {
          val runtimeException = intercept[RuntimeException](throw exception)
          runtimeException.getMessage should be(s"Table $NOT_EXIST_TABLE does not exist in hive")
        }
        case _ => assert(false)
      }
    }

    "Insert the specific DataFrame" taggedAs AcceptanceTestsSuite in {
      val sampleDF: DataFrame = getDfMock
      val triedBoolean = sampleDF.writeTo[Hive](HIVE_TABLE)
      triedBoolean should be a 'success
      triedBoolean.get shouldBe true
      HIVE_TABLE.readFrom[Hive]()
        .get
        .collect()
        .map( row => (row.get(0),row.get(1))) should contain theSameElementsAs(seqDfMock)
    }

    "Insert not exist DataFrame" taggedAs AcceptanceTestsSuite in {
      val sampleDF: DataFrame = getNotExistDf
      val triedBoolean = sampleDF.writeTo[Hive](NOT_EXIST_TABLE)
      triedBoolean should be a 'failure
      triedBoolean match {
        case Failure(exception) => {
          val runtimeException = intercept[RuntimeException](throw exception)
          runtimeException.getMessage should be(s"Table $NOT_EXIST_TABLE does not exist in hive")
        }
        case _ => assert(false)
      }
    }

    "Not allowed Upsert" taggedAs AcceptanceTestsSuite in {
      val sampleDF: DataFrame = getDfMock
      val triedBoolean = sampleDF.writeTo[Hive](HIVE_TABLE, "localhost", OperationType.Upsert)
      triedBoolean should be a 'failure
      triedBoolean match {
        case Failure(exception) => {
          val runtimeException = intercept[RuntimeException](throw exception)
          runtimeException.getMessage should be(s"Not allowed operation")
        }
        case _ => assert(false)
      }
    }

    "Not allowed InsertIgnoreRows" taggedAs AcceptanceTestsSuite in {
      val sampleDF: DataFrame = getDfMock
      val triedBoolean = sampleDF.writeTo[Hive](HIVE_TABLE, "localhost", OperationType.InsertIgnoreRows)
      triedBoolean should be a 'failure
      triedBoolean match {
        case Failure(exception) => {
          val runtimeException = intercept[RuntimeException](throw exception)
          runtimeException.getMessage should be(s"Not allowed operation")
        }
        case _ => assert(false)
      }
    }

    "Not allowed Delete" taggedAs AcceptanceTestsSuite in {
      val sampleDF: DataFrame = getDfMock
      val triedBoolean = sampleDF.deleteFrom[Hive](HIVE_TABLE)
      triedBoolean should be a 'failure
      triedBoolean match {
        case Failure(exception) => {
          val runtimeException = intercept[RuntimeException](throw exception)
          runtimeException.getMessage should be(s"Not allowed operation")
        }
        case _ => assert(false)
      }
    }

  }

  "Tables Component for Kudu" should {

    import com.training.bigdata.mdata.INEaddresses.common.util.io.Tables.Implementations.KuduDs

    "Read the specific DataFrame" taggedAs AcceptanceTestsSuite in {

      loadInitialKudu(functionInitialLoadKudu)
      val triedFrame = KUDU_TABLE.readFrom[Kudu]("localhost")
      triedFrame should be a 'success
      triedFrame.get.except(getExpectedDf).count() shouldBe 0
    }

    "Read the not exist DataFrame" taggedAs AcceptanceTestsSuite in {
      val triedFrame = NOT_EXIST_TABLE.readFrom[Kudu]()
      triedFrame should be a 'failure
      triedFrame match {
        case Failure(exception) => {
          val runtimeException = intercept[RuntimeException](throw exception)
          runtimeException.getMessage should be(s"Table $NOT_EXIST_TABLE does not exist in kudu")
        }
        case _ => assert(false)
      }
    }

    "Insert the specific DataFrame" taggedAs AcceptanceTestsSuite in {
      val sampleDF: DataFrame = getDfMock
      val triedBoolean = sampleDF.writeTo[Kudu](KUDU_TABLE, operationMode = OperationType.Insert)
      triedBoolean should be a 'success
      triedBoolean.get shouldBe true
      KUDU_TABLE.readFrom[Kudu]()
        .get
        .collect()
        .map( row => (row.get(0),row.get(1))) should contain theSameElementsAs(seqDfMock)
    }

    "Insert not exist DataFrame" taggedAs AcceptanceTestsSuite in {
      val sampleDF: DataFrame = getNotExistDf
      val triedBoolean = sampleDF.writeTo[Kudu](NOT_EXIST_TABLE, operationMode = OperationType.Insert)
      triedBoolean should be a 'failure
      triedBoolean match {
        case Failure(exception) => {
          val runtimeException = intercept[RuntimeException](throw exception)
          runtimeException.getMessage should be(s"Table $NOT_EXIST_TABLE does not exist in kudu")
        }
        case _ => assert(false)
      }
    }

    "Upsert the specific DataFrame" taggedAs AcceptanceTestsSuite in {
      val sampleDF: DataFrame = getDfMock
      sampleDF.writeTo[Kudu](KUDU_TABLE, operationMode = OperationType.Insert)

      val upsertedRowDF: DataFrame = getDfUpsertedRowMock
      upsertedRowDF.writeTo[Kudu](KUDU_TABLE, operationMode = OperationType.Upsert)

      KUDU_TABLE.readFrom[Kudu]()
        .get
        .collect()
        .map( row => (row.get(0),row.get(1))) should contain theSameElementsAs(seqDfUpsertedMock)
    }

    "Upsert not exist DataFrame" taggedAs AcceptanceTestsSuite in {
      val sampleDF: DataFrame = getNotExistDf
      val triedBoolean = sampleDF.writeTo[Kudu](NOT_EXIST_TABLE, operationMode = OperationType.Upsert)
      triedBoolean should be a 'failure
      triedBoolean match {
        case Failure(exception) => {
          val runtimeException = intercept[RuntimeException](throw exception)
          runtimeException.getMessage should be(s"Table $NOT_EXIST_TABLE does not exist in kudu")
        }
        case _ => assert(false)
      }
    }

    "Update some row in the specific DataFrame" taggedAs AcceptanceTestsSuite in {
      val sampleDF: DataFrame = getDfMock
      sampleDF.writeTo[Kudu](KUDU_TABLE, operationMode = OperationType.Insert)

      val upsertedRowDF: DataFrame = getDfUpsertedRowMock
      upsertedRowDF.writeTo[Kudu](KUDU_TABLE, operationMode = OperationType.Update)

      KUDU_TABLE.readFrom[Kudu]()
        .get
        .collect()
        .map( row => (row.get(0),row.get(1))) should contain theSameElementsAs(seqDfUpsertedMock)
    }

    "Update some row in not exist DataFrame" taggedAs AcceptanceTestsSuite in {
      val sampleDF: DataFrame = getNotExistDf
      val triedBoolean = sampleDF.writeTo[Kudu](NOT_EXIST_TABLE, operationMode = OperationType.Update)
      triedBoolean should be a 'failure
      triedBoolean match {
        case Failure(exception) => {
          val runtimeException = intercept[RuntimeException](throw exception)
          runtimeException.getMessage should be(s"Table $NOT_EXIST_TABLE does not exist in kudu")
        }
        case _ => assert(false)
      }
    }

    "Delete the specific row in DataFrame" taggedAs AcceptanceTestsSuite in {
      val sampleDF: DataFrame = getDfMock
      sampleDF.writeTo[Kudu](KUDU_TABLE, operationMode = OperationType.Insert)

      val deletedRowDF: DataFrame = getDfDeletedRowMock
      deletedRowDF.select("fullName").deleteFrom[Kudu](KUDU_TABLE)

      KUDU_TABLE.readFrom[Kudu]()
        .get
        .collect()
        .map( row => (row.get(0),row.get(1))) should contain theSameElementsAs(seqDfDeletedMock)
    }

    "Delete a row in not exist DataFrame" taggedAs AcceptanceTestsSuite in {
      val sampleDF: DataFrame = getNotExistDf
      val triedBoolean = sampleDF.deleteFrom[Kudu](NOT_EXIST_TABLE)
      triedBoolean should be a 'failure
      triedBoolean match {
        case Failure(exception) => {
          val runtimeException = intercept[RuntimeException](throw exception)
          runtimeException.getMessage should be(s"Table $NOT_EXIST_TABLE does not exist in kudu")
        }
        case _ => assert(false)
      }
    }

    "Insert the specific DataFrame ignoring if it exists" taggedAs AcceptanceTestsSuite in {
      val sampleDF: DataFrame = getDfMock
      sampleDF.writeTo[Kudu](KUDU_TABLE, operationMode = OperationType.Insert)

      val insertedRowDF: DataFrame = getDfInsertedRowMock
      insertedRowDF.writeTo[Kudu](KUDU_TABLE, operationMode = OperationType.InsertIgnoreRows)

      KUDU_TABLE.readFrom[Kudu]()
        .get
        .collect()
        .map( row => (row.get(0),row.get(1))) should contain theSameElementsAs(seqDfInsertedMock)

      insertedRowDF.writeTo[Kudu](KUDU_TABLE, operationMode = OperationType.InsertIgnoreRows)

      KUDU_TABLE.readFrom[Kudu]()
        .get
        .collect()
        .map( row => (row.get(0),row.get(1))) should contain theSameElementsAs(seqDfInsertedMock)
    }

    "Insert a row in not exist DataFrame ignoring if it exists" taggedAs AcceptanceTestsSuite in {
      val sampleDF: DataFrame = getNotExistDf
      val triedBoolean = sampleDF.writeTo[Kudu](NOT_EXIST_TABLE, operationMode = OperationType.InsertIgnoreRows)
      triedBoolean should be a 'failure
      triedBoolean match {
        case Failure(exception) => {
          val runtimeException = intercept[RuntimeException](throw exception)
          runtimeException.getMessage should be(s"Table $NOT_EXIST_TABLE does not exist in kudu")
        }
        case _ => assert(false)
      }
    }
  }

  private val seqDfMock =
    Seq(
      ("Carlos", "Ok"),
      ("Naomi", "Ok"),
      ("Nacho", "Ok"),
      ("ElOtro", "KO")
    )

  private val seqExpectedDf =
    Seq(("Alvaro", "The best"))

  private val seqNotExistDf =
    Seq(("fail", "fail", "fail"))

  private val seqDfUpsertedRowDF =
    Seq(("ElOtro", "Ok"))

  private val seqDfUpsertedMock =
    Seq(
      ("Carlos", "Ok"),
      ("Naomi", "Ok"),
      ("Nacho", "Ok"),
      ("ElOtro", "Ok")
    )

  private val seqDfDeletedRowDF =
    Seq(("ElOtro", "KO"))

  private val seqDfDeletedMock =
    Seq(
      ("Carlos", "Ok"),
      ("Naomi", "Ok"),
      ("Nacho", "Ok")
    )

  private val seqDfInsertedRowDF =
    Seq(("Yisus", "ok"))

  private val seqDfInsertedMock =
    Seq(
      ("Carlos", "Ok"),
      ("Naomi", "Ok"),
      ("Nacho", "Ok"),
      ("ElOtro", "KO"),
      ("Yisus", "ok")
    )

  import spark.implicits._

  private def getDfMock = {
    seqDfMock.toDF("fullName", "desc")
  }

  private def getExpectedDf = {
    seqExpectedDf.toDF("fullName", "desc")
  }

  private def getNotExistDf = {
    seqNotExistDf.toDF("failure1", "failure2", "failure3")
  }

  private def getDfUpsertedRowMock = {
    seqDfUpsertedRowDF.toDF("fullName", "desc")
  }

  private def getDfUpsertedMock = {
    seqDfUpsertedMock.toDF("fullName", "desc")
  }

  private def getDfDeletedRowMock = {
    seqDfDeletedRowDF.toDF("fullName", "desc")
  }

  private def getDfDeletedMock = {
    seqDfDeletedMock.toDF("fullName", "desc")
  }

  private def getDfInsertedRowMock = {
    seqDfInsertedRowDF.toDF("fullName", "desc")
  }

  private def getDfInsertedMock = {
    seqDfInsertedMock.toDF("fullName", "desc")
  }
}
