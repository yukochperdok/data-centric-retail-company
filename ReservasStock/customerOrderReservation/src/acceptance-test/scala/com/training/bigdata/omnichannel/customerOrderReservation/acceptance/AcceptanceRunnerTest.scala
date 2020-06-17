package com.training.bigdata.omnichannel.customerOrderReservation.acceptance

import com.training.bigdata.omnichannel.customerOrderReservation.docker.{DockerKafkaService, DockerKuduService, DockerTestKit}
import com.training.bigdata.omnichannel.customerOrderReservation.services.KuduService._
import com.training.bigdata.omnichannel.customerOrderReservation.services.SparkStreamingService._
import com.training.bigdata.omnichannel.customerOrderReservation.services.SchemaRegistryService._
import com.whisk.docker.impl.dockerjava.DockerKitDockerJava
import cucumber.api.CucumberOptions
import cucumber.api.junit.Cucumber
import org.apache.spark.SparkContextForTests
import org.junit.runner.RunWith
import org.junit.{AfterClass, BeforeClass}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

object AcceptanceRunnerTest extends FlatSpec with Matchers with DockerTestKit with DockerKitDockerJava with DockerKuduService with DockerKafkaService with SparkContextForTests {

  @BeforeClass
  override def beforeAll(): Unit = {
    super.beforeAll()
    // DISCLAIMER: It means that is the maximum timeout to check if docker container is running. Should be outside in properties context framework.
    // Maybe in the future.
    Await.result(containerManager.states.head.isRunning(), 7 seconds)
    startStreaming(sparkSession)
    initSchemaRegistry(propertiesUtil)
  }

  @AfterClass
  override def afterAll(): Unit = {

    closeSchemaRegistry()
    stopStreaming()
    super.afterAll()
  }
}

@RunWith(classOf[Cucumber])
@CucumberOptions(
  features = Array("classpath:features"),
  glue = Array("classpath:com/training/bigdata/omnichannel/customerOrderReservation/acceptance"),
  tags = Array("@acc"),
  monochrome = true,
  plugin = Array("pretty",
    "html:target/cucumber",
    "json:target/cucumber/test-report.json",
    "junit:target/cucumber/test-report.xml")
)
class AcceptanceRunnerTest
