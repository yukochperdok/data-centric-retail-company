package com.training.bigdata.arquitectura.ingestion.datalake.config

import com.training.bigdata.arquitectura.ingestion.datalake.tags.UnitTestsSuite
import org.scalatest.FlatSpec
import org.scalatest.Matchers


@UnitTestsSuite
class AppConfigTest extends FlatSpec with Matchers {

  trait DefaultConf {
    final val defaultCmdb =
      Cmdb("app","mod","confluence","bitbucket")

    final val defaultProcess: List[Process] =
      Process("process1","des_process1")::Nil

    final val defaultTopic: List[Topic] =
      Topic("topic1","des_topic1","xxx_stream",9999,"json")::Topic("topic2","des_topic2","yyy_stream",9999,"json")::Nil

    final val defaultParameters = Map(
      "sparkMaster" -> "local[2]",
      "kuduHost" -> "localhost",
      "kafkaConsumerGroup" -> "test-gid",
      "kafkaServers" -> "localhost:9092",
      "kafkaWithKerberos" -> "false",
      "sparkBatchDuration" -> "10",
      "kuduMaxNumberOfColumns" -> "8",
      "gracefulShutdownFile" -> "sparkStreaming.dontRemove.file",
      "datalakeUser" -> "test",
      "historyTable" -> "xxx_stream.table1,yyy_stream.table2"
    )

    final val defaultApplication =
      Application(
        defaultCmdb,
        defaultProcess,
        defaultTopic,
        defaultParameters
      )

    final val defaultConfig =
      AppConfig(
        defaultApplication
      )
  }

  "The config" should "be retreaved correctly a default config" in new DefaultConf {
    AppConfig(getClass.getClassLoader.getResource("application.conf").getPath) shouldBe (defaultConfig)
  }

}
