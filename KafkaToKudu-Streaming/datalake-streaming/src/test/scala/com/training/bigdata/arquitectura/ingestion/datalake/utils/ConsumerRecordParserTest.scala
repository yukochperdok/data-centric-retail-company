package com.training.bigdata.arquitectura.ingestion.datalake.utils

import com.training.bigdata.arquitectura.ingestion.datalake.streams.DataException
import com.training.bigdata.arquitectura.ingestion.datalake.streams.writer.UPSERT
import com.training.bigdata.arquitectura.ingestion.datalake.tags.UnitTestsSuite
import com.training.bigdata.arquitectura.ingestion.datalake.utils.ConsumerRecordParser._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.scalatest._


@UnitTestsSuite
class ConsumerRecordParserTest extends FlatSpec with Matchers {

  "parseConsumerRecord" should "parse valid json" in {
    val json = "{\n  " +
      "\"table\":\"TABLE1\",\n  " +
      "\"event\":\"u\",\n  " +
      "\"data\":{\n" +
      "    \"t1_k1\":\"column t1_k1\",\n" +
      "    \"t1_k2\":33,\n" +
      "    \"t1_v1\":\"column t1_v1\",\n" +
      "    \"t1_v2\":2.01,\n" +
      "    \"t1_v3\":3,\n" +
      "    \"t1_v4\":true,\n" +
      "    \"t1_v5\":\"2018-12-19 14:00:00\",\n" +
      "    \"t1_v6\":\"88\",\n" +
      "    \"t1_v7\":\"false\",\n" +
      "    \"t1_v8\":1547024503964,\n" +
      "    \"/AAA/BBB/CCC\":\"column /AAA/BBB/CCC (áéíóú & ñ)\"\n" +
      "  }\n" +
      "}"

    val consumerRecord = new ConsumerRecord[String, String]("topic", 0, 0, "key", json)

    val either = parseConsumerRecord(consumerRecord)
    either.isRight shouldBe true

    val parsedRecord = either.right.get
    parsedRecord._1 shouldBe "TABLE1"
    parsedRecord._2 shouldBe UPSERT

    val data = parsedRecord._3
    data.size() shouldBe 11
    data.getKeys() should equal (Set("t1_k1","t1_k2","t1_v1","t1_v2","t1_v3","t1_v4","t1_v5","t1_v6","t1_v7","t1_v8","/AAA/BBB/CCC"))

    data.getString("t1_k1") shouldBe "column t1_k1"
    data.getInt("t1_k2") shouldBe 33
    data.getString("t1_v1") shouldBe "column t1_v1"
    data.getDouble("t1_v2") shouldBe 2.01
    data.getShort("t1_v3") shouldBe 3
    data.getBoolean("t1_v4") shouldBe true
    data.getString("t1_v5") shouldBe "2018-12-19 14:00:00"
    data.getString("t1_v6") shouldBe "88"
    data.getString("t1_v7") shouldBe "false"
    data.getLong("t1_v8") shouldBe 1547024503964L
    data.getString("/AAA/BBB/CCC") shouldBe "column /AAA/BBB/CCC (áéíóú & ñ)"

    data.getStringOrLong("t1_v5") shouldBe "2018-12-19 14:00:00"
    data.getStringOrLong("t1_v8") shouldBe 1547024503964L

    // Integer as string
    an [DataException] should be thrownBy data.getString("t1_k2")
    // Double as string
    an [DataException] should be thrownBy data.getString("t1_v2")
    // Double as integer
    an [DataException] should be thrownBy data.getInt("t1_v2")
    // String as integer
    an [DataException] should be thrownBy data.getInt("t1_v6")
    // String as double
    an [DataException] should be thrownBy data.getDouble("t1_v6")
    // String as boolean
    an [DataException] should be thrownBy data.getBoolean("t1_v7")

    // String or Long
    an [DataException] should be thrownBy data.getStringOrLong("t1_v2")
    an [DataException] should be thrownBy data.getStringOrLong("t1_v4")
  }

  "parseConsumerRecord" should "fails when json is malformed" in {
    val json1 = "{}"
    val consumerRecord1 = new ConsumerRecord[String, String]("topic", 0, 0, "key", json1)
    parseConsumerRecord(consumerRecord1).isLeft shouldBe true

    val json2 = "{\"table\":\"TABLE0\",\"data\":{\"c0\":\"column 0\",\"c1\":\"column 1\"}}"
    val consumerRecord2 = new ConsumerRecord[String, String]("topic", 0, 0, "key", json2)
    parseConsumerRecord(consumerRecord2).isLeft shouldBe true

    val json3 = "{\"data\":{\"c0\":\"column 0\",\"c1\":\"column 1\"}}"
    val consumerRecord3 = new ConsumerRecord[String, String]("topic", 0, 0, "key", json3)
    parseConsumerRecord(consumerRecord3).isLeft shouldBe true

    val json4 = "{\"table\":\"TABLE0\",\"event\":\"u\",\"data\":\"\"}"
    val consumerRecord4 = new ConsumerRecord[String, String]("topic", 0, 0, "key", json4)
    parseConsumerRecord(consumerRecord4).isLeft shouldBe true
  }

  "DataObject returned by MessageJsonParser" should "have + & ++ operations" in {
    val json = "{\n  " +
      "\"table\":\"TABLE1\",\n  " +
      "\"event\":\"u\",\n  " +
      "\"data\":{\n" +
      "    \"t1_k1\":\"column t1_k1\",\n" +
      "    \"t1_v1\":\"column t1_v1\"\n" +
      "  }\n" +
      "}"

    val consumerRecord = new ConsumerRecord[String, String]("topic", 0, 0, "key", json)

    val either = parseConsumerRecord(consumerRecord)
    either.isRight shouldBe true

    val parsedRecord = either.right.get
    val data = parsedRecord._3
    data.size() shouldBe 2

    val timestamp1 = System.currentTimeMillis
    val timestamp2 = timestamp1 - (10 * 60 * 1000L) // current time minus 10 minutes
    val timestamp3 = timestamp1 - (15 * 60 * 1000L) // current time minus 15 minutes

    val audit = Map[String, Any](
      "user_insert_dlk" -> "user_insert", "ts_insert_dlk" -> timestamp1,
      "user_update_dlk" -> "user_update", "ts_update_dlk" -> timestamp2
    )

    val data1 = data ++ audit
    data1.size() shouldBe 6
    data1.getString("t1_k1") shouldBe "column t1_k1"
    data1.getString("t1_v1") shouldBe "column t1_v1"
    data1.getString("user_insert_dlk") shouldBe "user_insert"
    data1.getString("user_update_dlk") shouldBe "user_update"
    data1.getLong("ts_insert_dlk") shouldBe timestamp1
    data1.getLong("ts_update_dlk") shouldBe timestamp2

    val data2 = (data + ("date_write_event" -> timestamp3)) ++ audit
    data2.size() shouldBe 7
    data2.getString("t1_k1") shouldBe "column t1_k1"
    data2.getString("t1_v1") shouldBe "column t1_v1"
    data2.getString("user_insert_dlk") shouldBe "user_insert"
    data2.getString("user_update_dlk") shouldBe "user_update"
    data2.getLong("ts_insert_dlk") shouldBe timestamp1
    data2.getLong("ts_update_dlk") shouldBe timestamp2
    data2.getLong("date_write_event") shouldBe timestamp3

    an [DataException] should be thrownBy (data + ("double" -> 2.3))
    an [DataException] should be thrownBy (data + ("boolean" -> false))
  }

  "parseConsumerRecord" should "not parse json" in {
    val json = "{\n  " +
      "\"table\":\"TABLE1\",\n  " +
      "\"event\":\"notValidOperation\",\n  " +
      "\"data\":{\n" +
      "    \"t1_k1\":\"column t1_k1\",\n" +
      "    \"t1_k2\":33,\n" +
      "    \"t1_v1\":\"column t1_v1\",\n" +
      "    \"t1_v2\":2.01,\n" +
      "    \"t1_v3\":3,\n" +
      "    \"t1_v4\":true,\n" +
      "    \"t1_v5\":\"2018-12-19 14:00:00\",\n" +
      "    \"t1_v6\":\"88\",\n" +
      "    \"t1_v7\":\"false\",\n" +
      "    \"t1_v8\":1547024503964,\n" +
      "    \"/AAA/BBB/CCC\":\"column /AAA/BBB/CCC (áéíóú & ñ)\"\n" +
      "  }\n" +
      "}"

    val consumerRecord = new ConsumerRecord[String, String]("topic", 0, 0, "key", json)

    val either = parseConsumerRecord(consumerRecord)
    either.isLeft shouldBe true

    either match{
      case Left(exception) =>
        exception.message.contains("- Table:") shouldBe true
    }

  }


}
