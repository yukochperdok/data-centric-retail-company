package com.training.bigdata.omnichannel.customerOrderReservation.services

import java.util.{Properties, UUID}

import com.training.bigdata.omnichannel.customerOrderReservation.utils.PropertiesUtil
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.producer.KafkaProducer
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.kafka.common.serialization.StringSerializer

/**
  * This service is a Docker Kafka Service with a producer in order to generate kafka Events
  */
object KafkaProducerService {

  var kafkaProducer: KafkaProducer[String, GenericRecord] = _

  val zkUrl = "localhost:2181"

  def closeProducer(): Unit = {
    kafkaProducer.flush()
    kafkaProducer.close()
  }

  //FIXME: This method could be modified in order to accept all kafka producer configuration as parameters. At the moment, the only parameter is the topic
  def initProducer(implicit propertiesUtil: PropertiesUtil): Unit = {

    println("###############################################################")
    println(s"################  KAFKA INITIALIZING ########################")
    println("###############################################################")

    val zkUtils = ZkUtils.apply(zkUrl, 30000, 30000, false)
    if(!AdminUtils.topicExists(zkUtils, propertiesUtil.getReservationsTopic))
      AdminUtils.createTopic(zkUtils, propertiesUtil.getReservationsTopic, 3, 1)

    val propsProducer = new Properties()
    propsProducer.put(BOOTSTRAP_SERVERS_CONFIG, propertiesUtil.getKafkaServers)
    propsProducer.put(ACKS_CONFIG, "all")
    propsProducer.put(RETRIES_CONFIG, Integer.valueOf(0))
    propsProducer.put(BATCH_SIZE_CONFIG, Integer.valueOf(16384))
    propsProducer.put(LINGER_MS_CONFIG, Integer.valueOf(1))
    propsProducer.put(BUFFER_MEMORY_CONFIG, Integer.valueOf(33554432))
    propsProducer.put(KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    propsProducer.put(VALUE_SERIALIZER_CLASS_CONFIG,  classOf[KafkaAvroSerializer])
    propsProducer.put(SCHEMA_REGISTRY_URL_CONFIG, propertiesUtil.getSchemaRegistryURL)
    propsProducer.put(CLIENT_ID_CONFIG, UUID.randomUUID().toString)

    kafkaProducer = new KafkaProducer[String, GenericRecord](propsProducer)

  }

}
