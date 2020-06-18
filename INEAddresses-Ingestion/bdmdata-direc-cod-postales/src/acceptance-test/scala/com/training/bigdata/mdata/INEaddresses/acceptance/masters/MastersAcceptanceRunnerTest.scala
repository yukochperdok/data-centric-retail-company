package com.training.bigdata.mdata.INEaddresses.acceptance.masters

import com.training.bigdata.mdata.INEaddresses.docker.{DockerKuduService, DockerTestKit}
import com.training.bigdata.mdata.INEaddresses.services.KuduService._
import com.whisk.docker.impl.dockerjava.DockerKitDockerJava
import cucumber.api.CucumberOptions
import cucumber.api.junit.Cucumber
import org.junit.runner.RunWith
import org.junit.{AfterClass, BeforeClass}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

object MastersAcceptanceRunnerTest extends FlatSpec with Matchers with DockerTestKit with DockerKitDockerJava with DockerKuduService {

  @BeforeClass
  override def beforeAll(): Unit = {
    super.beforeAll()
    // DISCLAIMER: It means that is the maximum timeout to check if docker container is running. Should be outside in properties contex framework.
    // Maybe in the future.
    Await.result(containerManager.states.head.isRunning(), 7 seconds)
    initKudu
  }

  @AfterClass
  override def afterAll(): Unit = {
    closeKudu
    super.afterAll()
  }
}

@RunWith(classOf[Cucumber])
@CucumberOptions(
  features = Array("classpath:features/masters"),
  glue = Array("classpath:com/training/bigdata/mdata/INEaddresses/acceptance/masters"),
  tags = Array("@acc"),
  monochrome = true,
  plugin = Array("pretty",
    "html:target/cucumber",
    "json:target/cucumber/test-report.json",
    "junit:target/cucumber/test-report.xml")
)
class MastersAcceptanceRunnerTest
