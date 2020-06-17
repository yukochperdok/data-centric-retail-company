package com.training.bigdata.omnichannel.customerOrderReservation.docker

import com.whisk.docker.DockerKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait DockerTestKit extends BeforeAndAfterAll with ScalaFutures with DockerKit { self: Suite =>

  def dockerInitPatienceInterval =
    PatienceConfig(scaled(Span(20, Seconds)), scaled(Span(10, Millis)))

  def dockerPullImagesPatienceInterval =
    PatienceConfig(scaled(Span(1200, Seconds)), scaled(Span(250, Millis)))

  override def beforeAll(): Unit = {
    super.beforeAll()
    startAllOrFail()
  }

  override def afterAll(): Unit = {
    stopAllQuietly()
    super.afterAll()

  }
}
