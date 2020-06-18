package com.training.bigdata.arquitectura.ingestion.datalake

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.training.bigdata.arquitectura.ingestion.datalake.config.AppConfig
import com.training.bigdata.arquitectura.ingestion.datalake.services.KuduService._
import com.training.bigdata.arquitectura.ingestion.datalake.services.KafkaService._
import com.training.bigdata.arquitectura.ingestion.datalake.tags.NarrowTestsSuite
import com.training.bigdata.arquitectura.ingestion.datalake.utils.docker.{DockerKafkaService, DockerKuduService}
import com.whisk.docker.impl.dockerjava.DockerKitDockerJava
import com.whisk.docker.scalatest.DockerTestKit
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Clock, StreamingContext}
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time._

import scala.io.Source


@NarrowTestsSuite
class KafkaToKuduTest extends FlatSpec with Matchers with Eventually
  with DockerTestKit with DockerKitDockerJava with DockerKuduService with DockerKafkaService {

  implicit val pc = PatienceConfig(Span(20, Seconds), Span(1, Second))

  implicit var ssc: StreamingContext = _

  val configPath = getClass.getClassLoader.getResource(".").getPath

  val table1Key = "TABLE1"
  val table2Key = "TABLE2"
  val table3Key = "TABLE3"
  val marcKey = "MARC"

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  sdf.setTimeZone(TimeZone.getTimeZone("Europe/Madrid"))


  override def beforeAll(): Unit = {
    super.beforeAll()
    initKudu()
    initKafka()
  }

  override def afterAll(): Unit = {
    ssc.stop(stopSparkContext = true, stopGracefully = false)
    kuduClient.close()
    kafkaProducer.close()
    super.afterAll()
  }


  "kudu table" should "be created" in {
    kuduClient.getTablesList.getTablesList.size() shouldBe 7
    kuduClient.getTablesList.getTablesList.contains(table1Name) shouldBe true
    kuduClient.getTablesList.getTablesList.contains(table2Name) shouldBe true
    kuduClient.getTablesList.getTablesList.contains(table1_hName) shouldBe true
    kuduClient.getTablesList.getTablesList.contains(table2_hName) shouldBe true
    kuduClient.getTablesList.getTablesList.contains(table3_001Name) shouldBe true
    kuduClient.getTablesList.getTablesList.contains(table3_002Name) shouldBe true
    kuduClient.getTablesList.getTablesList.contains(marcName) shouldBe true
  }

  "kafka topics" should "be created" in {
    AdminUtils.topicExists(ZkUtils.apply("localhost:2181", 30000, 30000, false), topic1Name) shouldBe true
    AdminUtils.topicExists(ZkUtils.apply("localhost:2181", 30000, 30000, false), topic2Name) shouldBe true
  }

  "KafkaToKudu" should "insert Kafka messages in Kudu" in {

    val appConf = AppConfig(s"${configPath}/application.conf")

    val sparkConf = new SparkConf().setMaster(appConf.application.parameters("sparkMaster"))
      .setAppName("arquitectura-ingestion-streaming").set("spark.streaming.clock", "org.apache.spark.FixedClock")

    val batchDuration = org.apache.spark.streaming.Seconds(appConf.application.parameters("sparkBatchDuration").toLong)

    ssc = new StreamingContext(sparkConf, batchDuration)

    val fixedClock = Clock.getFixedClock(ssc)


    KafkaToKudu.processPipeline(appConf, ssc)


    println("### 1 ###")

    // empty value
    val record0_1 = Source.fromFile(s"${configPath}/records/table0_1_Ko.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic1Name, null, record0_1))
    // event?
    val record0_2 = Source.fromFile(s"${configPath}/records/table0_2_Ko.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic1Name, null, record0_2))
    // table TABLE_xxx?
    val record0_3 = Source.fromFile(s"${configPath}/records/table0_3_Ko.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic1Name, null, record0_3))
    // event="a"?
    val record0_4 = Source.fromFile(s"${configPath}/records/table0_4_Ko.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic1Name, null, record0_4))

    // Data keys do NOT match table columns (errores x2 table1 & table1_h)
    val record1_0_a = Source.fromFile(s"${configPath}/records/table1_0_a_Ko.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic1Name, table1Key, record1_0_a))
    // 't1_v2' is NOT a number (errores x2 table1 & table1_h)
    val record1_0_b = Source.fromFile(s"${configPath}/records/table1_0_b_Ko.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic1Name, table1Key, record1_0_b))
    // 't1_v1' is NOT a String (errores x2 table1 & table1_h)
    val record1_0_c = Source.fromFile(s"${configPath}/records/table1_0_c_Ko.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic1Name, table1Key, record1_0_c))
    // Row error for primary key (OJO: ERROR que va a table1_h)
    val record1_0_d = Source.fromFile(s"${configPath}/records/table1_0_d_Ko.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic1Name, table1Key, record1_0_d))
    // insert OK
    val record1_1 = Source.fromFile(s"${configPath}/records/table1_1_Ok.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic1Name, table1Key, record1_1))
    // Missing data for key: 't1_v2' (errores x2 table1 & table1_h)
    val record1_2 = Source.fromFile(s"${configPath}/records/table1_2_Ko.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic1Name, table1Key, record1_2))
    // Column cannot be set to null (errores x2 table1 & table1_h)
    val record1_3 = Source.fromFile(s"${configPath}/records/table1_3_Ko.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic1Name, table1Key, record1_3))
    // Row error for primary key (OJO: ERROR que va a table1_h)
    val record1_4 = Source.fromFile(s"${configPath}/records/table1_4_Ko.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic1Name, table1Key, record1_4))
    // Row error for primary key (OJO: ERROR que va a table1_h)
    val record1_5 = Source.fromFile(s"${configPath}/records/table1_5_Ko.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic1Name, table1Key, record1_5))

    eventually (timeout(Span(30, Seconds)), interval(Span(5, Seconds))) {
      fixedClock.addTime(batchDuration)
      countRecords(table1Name) shouldBe 1
      countRecords(table1_hName) shouldBe 4
    }

    val recordsTable1 = getRecords(table1Name)
    val recordTable1 = recordsTable1(0)
    recordTable1("t1_k1") shouldBe "column t1_k1"
    recordTable1("t1_k2") shouldBe 33
    recordTable1("t1_v1") shouldBe "column t1_v1"
    recordTable1("t1_v2") shouldBe 2.01
    recordTable1("t1_v3") shouldBe 3
    recordTable1("t1_v4") shouldBe true
    val t1_v5 = sdf.parse("2018-12-19 14:00:00").getTime * 1000L
    recordTable1("t1_v5").asInstanceOf[Long] shouldBe t1_v5
    recordTable1("aaa_bbb_ccc") shouldBe "column /AAA/BBB/CCC (áéíóú & ñ)"
    val tsInsert = recordTable1("ts_insert_dlk")


    println("### 2 ###")

    // update Ok
    val record1_1Bis = Source.fromFile(s"${configPath}/records/table1_1Bis_Ok.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic1Name, table1Key, record1_1Bis))
    // upsert OK
    val record2_1 = Source.fromFile(s"${configPath}/records/table2_1_Ok.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic2Name, table2Key, record2_1))
    // Missing data for key: 't2_k2' & Column cannot be set to null
    val record2_2 = Source.fromFile(s"${configPath}/records/table2_2_delete_Ko.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic2Name, table2Key, record2_2))
    // (t2_k1): Column cannot be set to null (errores x2 table2 & table2_h)
    val record2_3 = Source.fromFile(s"${configPath}/records/table2_3_delete_Ko.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic2Name, table2Key, record2_3))

    eventually (timeout(Span(50, Seconds)), interval(Span(5, Seconds))) {
      fixedClock.addTime(batchDuration)
      countRecords(table2Name) shouldBe 1
      countRecords(table2_hName) shouldBe 1
      countRecords(table1_hName) shouldBe 5
    }

    val recordsTable2_hist = getRecords(table2_hName)
    val recordTable2_hist = recordsTable2_hist(0)
    recordTable2_hist("t2_k1") shouldBe "column t2_k1"
    recordTable2_hist("t2_k2") shouldBe 55
    recordTable2_hist("event") shouldBe "upsert"

    val recordsTable1Bis = getRecords(table1Name)
    recordsTable1Bis.size shouldBe 1
    val recordTable1Bis = recordsTable1Bis(0)
    recordTable1Bis("t1_k1") shouldBe "column t1_k1"
    recordTable1Bis("t1_k2") shouldBe 33
    recordTable1Bis("t1_v1") shouldBe "column t1_v1 Bis"
    recordTable1Bis("t1_v2") shouldBe 10.000001
    recordTable1Bis("t1_v3") shouldBe 333333
    recordTable1Bis("t1_v4") shouldBe false
    val t1_v5_bis = sdf.parse("2018-08-31 14:00:00").getTime * 1000L
    recordTable1Bis("t1_v5").asInstanceOf[Long] shouldBe t1_v5_bis
    recordTable1Bis("aaa_bbb_ccc") shouldBe None
    recordTable1Bis("ts_insert_dlk") shouldBe tsInsert
    recordTable1Bis("ts_insert_dlk") should not be recordTable1Bis("ts_update_dlk")


    println("### 3 ###")

    val record2_4 = Source.fromFile(s"${configPath}/records/table2_4_delete_Ok.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic2Name, table2Key, record2_4))

    eventually (timeout(Span(20, Seconds)), interval(Span(5, Seconds))) {
      fixedClock.addTime(batchDuration)
      countRecords(table2Name) shouldBe 0
      countRecords(table2_hName) shouldBe 2
    }


    println("### 4 ###")

    val record3_1 = Source.fromFile(s"${configPath}/records/table3_1_Ok.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic1Name, table3Key, record3_1))

    eventually (timeout(Span(20, Seconds)), interval(Span(5, Seconds))) {
      fixedClock.addTime(batchDuration)
      countRecords(table3_001Name) shouldBe 1
      countRecords(table3_002Name) shouldBe 1
    }


    println("### 5 ###")

    val record3_2 = Source.fromFile(s"${configPath}/records/table3_2_delete_Ok.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic1Name, table3Key, record3_2))

    eventually (timeout(Span(20, Seconds)), interval(Span(5, Seconds))) {
      fixedClock.addTime(batchDuration)
      countRecords(table3_001Name) shouldBe 0
      countRecords(table3_002Name) shouldBe 0
    }


    println("### 6 ###")

    val t0 = System.currentTimeMillis * 1000L

    val marcRecord = Source.fromFile(s"${configPath}/records/table_marc_Ok.json").getLines.mkString
    kafkaProducer.send(new ProducerRecord[String, String](topic1Name, marcKey, marcRecord))

    eventually (timeout(Span(20, Seconds)), interval(Span(5, Seconds))) {
      fixedClock.addTime(batchDuration)
      countRecords(marcName) shouldBe 1
    }

    val t1 = System.currentTimeMillis * 1000L

    val marcRecords = getRecords(marcName)
    marcRecords.size shouldBe 1
    // println(t0 + "<=" + marcRecords(0)("date_write_event").asInstanceOf[Long] + "<=" + t1)
    marcRecords(0)("date_write_event").asInstanceOf[Long] >= t0 shouldBe true
    marcRecords(0)("date_write_event").asInstanceOf[Long] <= t1 shouldBe true

  }

}