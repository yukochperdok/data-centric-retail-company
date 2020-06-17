package com.training.bigdata.omnichannel.customerOrderReservation.services

import com.training.bigdata.omnichannel.customerOrderReservation.utils.PropertiesUtil
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._
import org.apache.avro.Schema

import scala.io.Source

object SchemaRegistryService {

  val schemaRegistryPort = 8081
  val schemaRegistryHost = "localhost"
  val schemaRegistryUrl = s"http://$schemaRegistryHost:$schemaRegistryPort"

  private val wireMockServer = new WireMockServer(wireMockConfig().port(schemaRegistryPort))

  val configPath: String = getClass.getClassLoader.getResource(".").getPath

  def closeSchemaRegistry(): Unit = wireMockServer.shutdown()

  def initSchemaRegistry(implicit propertiesUtil: PropertiesUtil): Unit = {

    val topic = propertiesUtil.getReservationsTopic

    wireMockServer.start()
    WireMock.configureFor(schemaRegistryHost, schemaRegistryPort)

    val schema = new Schema.Parser().parse(Source.fromURL(getClass.getResource(s"/$topic-value.avsc")).mkString)
    val escapedSchema = schema.toString().replaceAll("\"", "\\\\\\\"")

    //println(s"SCHEMA -> ${schema.toString(false)}")

    stubFor(post(s"/subjects/$topic-value/versions")
      .willReturn(aResponse()
        .withStatus(200)
        .withHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
        .withBody(
          "{" +
            "\"id\":1" +
            "}"
        )
      )
    )

    stubFor(get(s"/subjects/$topic-value/versions/latest")
      .willReturn(aResponse()
        .withStatus(200)
        .withBody(
          "{" +
            "\"subject\":\"" + topic + "-value\"," +
            "\"version\":1," +
            "\"id\":1," +
            "\"schema\":\"" + escapedSchema + "\"" +
            "}"
        )
      )
    )

    stubFor(get(s"/schemas/ids/1")
      .willReturn(aResponse()
        .withStatus(200)
        .withBody(
          "{" +
            "\"schema\":\"" + escapedSchema + "\"" +
            "}"
        )
      )
    )
  }
}
