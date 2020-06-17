package com.training.bigdata.omnichannel.customerOrderReservation.consumer

import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object KafkaStreamReader {
  private[consumer] val DEFAULT_KAFKA_SERVER: String = "localhost:9092"
  private[consumer] val DEFAULT_SCHEMA_REGISTRY: String = "localhost:8081"
  private[consumer] val DEFAULT_GROUP_ID: String = "test-consumer-group"
  private[consumer] val DEFAULT_KAFKA_STARTING_OFFSETS: String = "latest"
  private[consumer] val DEFAULT_KAFKA_SECURITY:  Boolean = true
  private[consumer] val DEFAULT_ENABLE_AUTOCOMMIT:  Boolean = false

  def readStream(
    inputTopics: String,
    kafkaServers: String = DEFAULT_KAFKA_SERVER,
    schemaRegistryURL: String = DEFAULT_SCHEMA_REGISTRY,
    groupId: String = DEFAULT_GROUP_ID,
    startingOffsets: String = DEFAULT_KAFKA_STARTING_OFFSETS,
    securedKafka: Boolean = DEFAULT_KAFKA_SECURITY,
    enableAutoCommit: java.lang.Boolean = DEFAULT_ENABLE_AUTOCOMMIT)
    (implicit ssc: StreamingContext): InputDStream[ConsumerRecord[String, GenericRecord]] = {

    val topicsSet = inputTopics.split(",").toSet

    val commonParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[KafkaAvroDeserializer],
      AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryURL,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> startingOffsets,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> enableAutoCommit
    )

    val additionalSecuredParams = securedKafka match {
      case true =>
        Map(
          CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> SecurityProtocol.SASL_PLAINTEXT.name,
          SaslConfigs.SASL_KERBEROS_SERVICE_NAME -> "kafka"
        )
      case false => Map.empty
    }

    val kafkaParams = commonParams ++ additionalSecuredParams

    KafkaUtils.createDirectStream[String, GenericRecord](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, GenericRecord](topicsSet, kafkaParams))

  }
}
