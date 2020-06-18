package com.training.bigdata.mdata.INEaddresses.docker

import com.training.bigdata.mdata.INEaddresses.common.util.tag.TagTest.AcceptanceTestsSuite
import com.whisk.docker.impl.dockerjava.DockerKitDockerJava
import org.apache.kudu.client.KuduClient
import org.scalatest.time.{Second, Seconds, Span}
import org.scalatest.{FlatSpec, Matchers}

class KuduServiceSpec extends FlatSpec with Matchers with DockerTestKit with DockerKitDockerJava
  with DockerKuduService {

  implicit val pc = PatienceConfig(Span(20, Seconds), Span(1, Second))

  "kudu container" should "be ready" taggedAs AcceptanceTestsSuite in {
    isContainerReady(kuduContainer).futureValue shouldBe true
    kuduContainer.isRunning().futureValue shouldBe true
    kuduContainer.bindPorts.get(7051) should not be empty
    kuduContainer.getIpAddresses().futureValue should not be Seq.empty
  }

  "kudu" should "be ready" taggedAs AcceptanceTestsSuite in {
    val kuduClient = new KuduClient.KuduClientBuilder("localhost").build()
    kuduClient.getTablesList.getTablesList.size() shouldBe 0
    kuduClient.close()
  }
}
