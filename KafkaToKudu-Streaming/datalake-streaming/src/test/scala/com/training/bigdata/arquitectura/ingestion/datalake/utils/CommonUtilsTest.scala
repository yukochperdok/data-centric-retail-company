package com.training.bigdata.arquitectura.ingestion.datalake.utils

import com.training.bigdata.arquitectura.ingestion.datalake.config.Topic
import com.training.bigdata.arquitectura.ingestion.datalake.streams.DataException
import com.training.bigdata.arquitectura.ingestion.datalake.streams.writer.{DELETE, INSERT, UPDATE, UPSERT}
import com.training.bigdata.arquitectura.ingestion.datalake.tags.UnitTestsSuite
import com.training.bigdata.arquitectura.ingestion.datalake.utils.CommonUtils._
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}
import org.scalatest._

import scala.collection.JavaConverters._


@UnitTestsSuite
class CommonUtilsTest extends FlatSpec with Matchers {

  "getTopicToSchemaMappings" should "map topics and kudu schemas" in {
    val mapKafkaTopicToKuduSchema = List(Topic("topic1","des_topic1","xxx_stream",9999,"json"), Topic("topic2","des_topic2","yyy_stream",9999,"json"))
    val result = Map("topic1" -> "xxx_stream", "topic2" -> "yyy_stream")
    getTopicToSchemaMappings(mapKafkaTopicToKuduSchema) should equal (result)
  }

  "normalizeMapKeys" should "normalize map keys" in {
    val originalKeys = List("KEY1", "/AAA/BBB/CCC", "ts_insert_dlk", "ts_update_dlk", "user_insert_dlk", "user_update_dlk")
    val columns = List(
      new ColumnSchemaBuilder("key1", Type.STRING).key(true).build(),
      new ColumnSchemaBuilder("aaa_bbb_ccc", Type.STRING).build(),
      new ColumnSchemaBuilder("ts_insert_dlk", Type.UNIXTIME_MICROS).nullable(false).build,
      new ColumnSchemaBuilder("ts_update_dlk", Type.UNIXTIME_MICROS).nullable(true).build,
      new ColumnSchemaBuilder("user_insert_dlk", Type.STRING).nullable(false).build,
      new ColumnSchemaBuilder("user_update_dlk", Type.STRING).nullable(true).build
    )
    val schema = new Schema(columns.asJava)
    val resultUpsertInsert = Map(
      "key1" -> "KEY1",
      "aaa_bbb_ccc" -> "/AAA/BBB/CCC",
      "ts_insert_dlk" -> "ts_insert_dlk",
      "ts_update_dlk" -> "ts_update_dlk",
      "user_insert_dlk" -> "user_insert_dlk",
      "user_update_dlk" -> "user_update_dlk"
    )
    val resultUpdate = Map(
      "key1" -> "KEY1",
      "aaa_bbb_ccc" -> "/AAA/BBB/CCC",
      "ts_update_dlk" -> "ts_update_dlk",
      "user_update_dlk" -> "user_update_dlk"
    )
    val resultDelete = Map("key1" -> "KEY1")
    normalizeMapKeys(UPSERT, originalKeys, schema) should equal (resultUpsertInsert)
    normalizeMapKeys(INSERT, originalKeys, schema) should equal (resultUpsertInsert)
    normalizeMapKeys(UPDATE, originalKeys, schema) should equal (resultUpdate)
    normalizeMapKeys(DELETE, originalKeys, schema) should equal (resultDelete)
  }

  "normalizeMapKeys" should "fails when columns do not match" in {
    val originalKeys = List("KEY1", "/AAA/BBB/CCC", "ts_insert_dlk", "ts_update_dlk", "user_insert_dlk", "user_update_dlk")
    val columns = List(
      new ColumnSchemaBuilder("key1", Type.STRING).key(true).build(),
      new ColumnSchemaBuilder("key2", Type.STRING).key(true).build(),
      new ColumnSchemaBuilder("aaa_bbb_ccc", Type.STRING).build(),
      new ColumnSchemaBuilder("ts_insert_dlk", Type.UNIXTIME_MICROS).nullable(false).build,
      new ColumnSchemaBuilder("ts_update_dlk", Type.UNIXTIME_MICROS).nullable(true).build,
      new ColumnSchemaBuilder("user_insert_dlk", Type.STRING).nullable(false).build,
      new ColumnSchemaBuilder("user_update_dlk", Type.STRING).nullable(true).build
    )
    val schema = new Schema(columns.asJava)
    an [DataException] should be thrownBy normalizeMapKeys(UPSERT, originalKeys, schema)
    an [DataException] should be thrownBy normalizeMapKeys(INSERT, originalKeys, schema)
    an [DataException] should be thrownBy normalizeMapKeys(UPDATE, originalKeys, schema)
    an [DataException] should be thrownBy normalizeMapKeys(DELETE, originalKeys, schema)
  }

}
