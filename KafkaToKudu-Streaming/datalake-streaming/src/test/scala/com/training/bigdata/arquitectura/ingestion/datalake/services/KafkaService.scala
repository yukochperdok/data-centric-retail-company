package com.training.bigdata.arquitectura.ingestion.datalake.services

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.producer.KafkaProducer


object KafkaService {

  val topic1Name = "topic1"
  val topic2Name = "topic2"

  var kafkaProducer: KafkaProducer[String, String] = _

  def initKafka(): Unit = {
    AdminUtils.createTopic(ZkUtils.apply("localhost:2181", 30000, 30000, false), topic1Name, 3, 1)
    AdminUtils.createTopic(ZkUtils.apply("localhost:2181", 30000, 30000, false), topic2Name, 3, 1)

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", Integer.valueOf(0))
    props.put("batch.size", Integer.valueOf(16384))
    props.put("linger.ms", Integer.valueOf(1))
    props.put("buffer.memory", Integer.valueOf(33554432))
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    kafkaProducer = new KafkaProducer[String, String](props)
  }

}
