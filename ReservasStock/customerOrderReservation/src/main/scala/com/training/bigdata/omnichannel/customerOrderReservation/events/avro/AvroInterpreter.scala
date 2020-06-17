package com.training.bigdata.omnichannel.customerOrderReservation.events.avro

import com.databricks.spark.avro.{SchemaConverterUtils, SchemaConverters}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.{explode, col}

object AvroInterpreter {

  def getAvroSchema(schemaRegistryURL: String, topic: String): Schema = {
    val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryURL, 100)
    val schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(topic + "-value")
    val schemaReg = schemaMetadata.getSchema
    val avroSchema = new Schema.Parser().parse(schemaReg)
    avroSchema
  }

  def parseAvroToDF(rdd: RDD[ConsumerRecord[String, GenericRecord]], avroSchema: Schema)
               (implicit sparkSession: SparkSession) : DataFrame = {

    val structAvroType: StructType =
      SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]

    val convertRow: GenericRecord => Row =
      SchemaConverterUtils.converterSQL(avroSchema, structAvroType)

    sparkSession
      .createDataFrame(rdd.map(record => convertRow(record.value())), structAvroType)
  }

  def explodeColumnsAndSelectFields(inputDF: DataFrame, arrayToExplode: String, fieldsToExplode: List[String], otherFields: List[String] = List()): DataFrame = {
    inputDF
        .withColumn(arrayToExplode, explode(col(arrayToExplode)))
        .select(otherFields.head, otherFields.tail ++ fieldsToExplode:_*)
  }

}
