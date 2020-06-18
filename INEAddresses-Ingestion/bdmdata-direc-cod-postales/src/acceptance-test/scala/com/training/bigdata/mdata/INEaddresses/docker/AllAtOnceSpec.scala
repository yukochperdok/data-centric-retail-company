package com.training.bigdata.mdata.INEaddresses.docker

import com.training.bigdata.mdata.INEaddresses.common.util.tag.TagTest.AcceptanceTestsSuite
import com.whisk.docker.impl.dockerjava.DockerKitDockerJava
import org.scalatest.time.{Second, Seconds, Span}
import org.scalatest.{FlatSpec, Matchers}

class AllAtOnceSpec extends FlatSpec with Matchers
  with DockerTestKit
  with DockerKitDockerJava
  with DockerKuduService {

  //This vals are DockerKit properties with the automatically functionality about "docker pull".
//  override val PullImagesTimeout = 120.minutes
//  override val StartContainersTimeout = 120.seconds
//  override val StopContainersTimeout = 120.seconds

  implicit val pc = PatienceConfig(Span(20, Seconds), Span(1, Second))


  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }


  "all containers" should "be ready at the same time" taggedAs AcceptanceTestsSuite in {
    dockerContainers.map(_.image).foreach(println)
    dockerContainers.forall(_.isReady().futureValue) shouldBe true
  }

}
