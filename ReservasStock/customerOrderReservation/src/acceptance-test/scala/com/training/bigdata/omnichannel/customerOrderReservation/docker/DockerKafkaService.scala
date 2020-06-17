package com.training.bigdata.omnichannel.customerOrderReservation.docker

import com.whisk.docker.{DockerContainer, DockerKit, DockerReadyChecker}


trait DockerKafkaService extends DockerKit {

  def KafkaAdvertisedPort = 9092
  val ZookeeperDefaultPort = 2181

  val kafkaContainer: DockerContainer =
    DockerContainer(image = "spotify/kafka", name = Some("spotify-kafka"))
    .withPorts(KafkaAdvertisedPort -> Some(KafkaAdvertisedPort), ZookeeperDefaultPort -> Some(ZookeeperDefaultPort))
    .withEnv(s"ADVERTISED_PORT=$KafkaAdvertisedPort", s"ADVERTISED_HOST=${dockerExecutor.host}")
    //.withLogLineReceiver(new LogLineReceiver(true, println)) -- It's about trace logs. This line could be used to see any problems in kafkacontainer.
    .withReadyChecker(DockerReadyChecker.LogLineContains("kafka entered RUNNING state"))

  abstract override def dockerContainers: List[DockerContainer] = kafkaContainer :: super.dockerContainers

}
