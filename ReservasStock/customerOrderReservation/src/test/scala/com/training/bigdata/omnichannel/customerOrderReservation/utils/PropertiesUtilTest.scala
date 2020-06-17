package com.training.bigdata.omnichannel.customerOrderReservation.utils

import com.training.bigdata.omnichannel.customerOrderReservation.utils.tag.TagTest.UnitTestTag
import com.training.bigdata.omnichannel.customerOrderReservation.utils.LensAppConfig._
import org.apache.kudu.spark.kudu.KuduContext
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, FlatSpecLike, Matchers}

class PropertiesUtilTest extends FlatSpecLike with BeforeAndAfter with Matchers with MockitoSugar {

  before {
    System.setProperty("config.file", "")
  }

  "PropertiesUtil" must "retrieve correctly both application.conf and config.file" taggedAs UnitTestTag in {
    System.setProperty("config.file", getClass.getClassLoader.getResource(s"./${PropertiesUtil.DEFAULT_CONFIG_FILE_NAME}").getPath)
    val user: String = "user"
    val propertiesUtil = PropertiesUtil(user, getClass.getClassLoader.getResource(s"./${PropertiesUtil.DEFAULT_APPLICATION_FILE_NAME}").getPath)
    propertiesUtil.getListProcess shouldBe
      Process("process1","des_process1")::Nil
    propertiesUtil.getListTopic shouldBe
      Topic("topicToConsume","des_topic1",1000000,"parquet")::Nil
    propertiesUtil.getBatchDuration shouldBe 1
    propertiesUtil.getReservationsTopic shouldBe "topic1"
    propertiesUtil.getReservationsGroupId shouldBe "groupID1"
    propertiesUtil.getTimezone shouldBe "timezone"
    propertiesUtil.getKuduHosts shouldBe "kuduHost"
    propertiesUtil.getKafkaServers shouldBe "localhost:9092"
    propertiesUtil.getSchemaRegistryURL shouldBe "localhost:8081"
    propertiesUtil.getUser shouldBe user
    propertiesUtil.getCustomerOrdersReservationTable shouldBe "impala::omnichannel_user.customer_orders_reservations"
    propertiesUtil.getCustomerOrdersReservationActionsTable shouldBe "impala::omnichannel_user.customer_orders_reservations_actions"
    propertiesUtil.getArticlesTranscodingTable shouldBe "impala::mdata_stream.zxtranscod"
    propertiesUtil.getStoresTranscodingTable shouldBe "impala::mdata_stream.zxtranscod_site"
    propertiesUtil.getUpsertAction shouldBe "ALTA"
    propertiesUtil.getDeleteAction shouldBe "BAJA"
    propertiesUtil.getEA shouldBe "EA"
    propertiesUtil.getKG shouldBe "KG"
    propertiesUtil.getOrderTypesToIgnore shouldBe List("Marketplace")
    propertiesUtil.getRefreshTranscodTablesMillis shouldBe 3600000
  }

  it must "retrieve correctly the attributes of AppConfig's class" taggedAs UnitTestTag in new DefaultConf {
    System.setProperty("config.file", getClass.getClassLoader.getResource(s"./${PropertiesUtil.DEFAULT_CONFIG_FILE_NAME}").getPath)

    val listTopicModif: List[Topic] =
      Topic(
        "topic1",
        "desc_topic1",
        1000000,
        "parquet")::
      Topic(
        "topic2",
        "desc_topic2",
        1000000,
        "parquet")::
       Nil

    val appConfig: AppConfig =
      defaultConfig
        .setListTopic(listTopicModif)
        .setParameters(
          Parameters(
            "kuduHost_modif",
            "kafkaServers_modif",
            "schemaRegistryURL_modif",
            "timezone_modif",
            isKafkaSecured = false
          )
        )
    val user: String = "user"
    val propertiesUtil = PropertiesUtil(user, appConfig)
    propertiesUtil.getListProcess shouldBe
      Process("process1","des_process1")::Nil
    propertiesUtil.getListTopic shouldBe listTopicModif
    propertiesUtil.getBatchDuration shouldBe 1
    propertiesUtil.getReservationsTopic shouldBe "topic1"
    propertiesUtil.getReservationsGroupId shouldBe "groupID1"
    propertiesUtil.getKuduHosts shouldBe "kuduHost_modif"
    propertiesUtil.getKafkaServers shouldBe "kafkaServers_modif"
    propertiesUtil.getSchemaRegistryURL shouldBe "schemaRegistryURL_modif"
    propertiesUtil.getTimezone shouldBe "timezone_modif"
    propertiesUtil.isKafkaSecured shouldBe false
    propertiesUtil.getUser shouldBe user
    propertiesUtil.getCustomerOrdersReservationTable shouldBe "impala::omnichannel_user.customer_orders_reservations"
    propertiesUtil.getCustomerOrdersReservationActionsTable shouldBe "impala::omnichannel_user.customer_orders_reservations_actions"
    propertiesUtil.getArticlesTranscodingTable shouldBe "impala::mdata_stream.zxtranscod"
    propertiesUtil.getStoresTranscodingTable shouldBe "impala::mdata_stream.zxtranscod_site"
    propertiesUtil.getUpsertAction shouldBe "ALTA"
    propertiesUtil.getDeleteAction shouldBe "BAJA"
    propertiesUtil.getEA shouldBe "EA"
    propertiesUtil.getKG shouldBe "KG"
    propertiesUtil.getOrderTypesToIgnore shouldBe List("Marketplace")
    propertiesUtil.getRefreshTranscodTablesMillis shouldBe 3600000

  }

  it must "fail if not config files are found" taggedAs UnitTestTag in {
    val user: String = "user"
    val caught = intercept[Exception] {
      PropertiesUtil(user)
    }
    info(caught.getMessage)
    assert(caught.getMessage.indexOf("application.cmdb") == -1)
  }

  it must "fail if not config files are found and property is retrieved" taggedAs UnitTestTag in {
    val user: String = "user"
    val caught2 = intercept[Exception] {
      PropertiesUtil(user, getClass.getClassLoader.getResource(s"./${PropertiesUtil.DEFAULT_APPLICATION_FILE_NAME}").getPath)
    }
    info(caught2.getMessage)
    assert(caught2.getMessage.indexOf("app.refreshTranscodTablesMillis") == -1)
  }

  "PropertiesUtils.getTableExists" must "return the tableName with impala:: prefix when the given table exists with that prefix" taggedAs UnitTestTag in {
    System.setProperty("config.file", getClass.getClassLoader.getResource(s"./${PropertiesUtil.DEFAULT_APPLICATION_FILE_NAME}").getPath)
    val user: String = "user"
    val propertiesUtil = PropertiesUtil(user, getClass.getClassLoader.getResource(s"./${PropertiesUtil.DEFAULT_APPLICATION_FILE_NAME}").getPath)
    val kuduContext = mock[KuduContext]
    //mocking the tableExists return value
    Mockito.doReturn(true).when(kuduContext).tableExists("impala::schema.table_test")
    propertiesUtil.getTableExists("schema.table_test", kuduContext) shouldBe "impala::schema.table_test"
  }

  it must "return the tableName when the given table exists without the impala:: prefix" taggedAs UnitTestTag in {
    System.setProperty("config.file", getClass.getClassLoader.getResource(s"./${PropertiesUtil.DEFAULT_APPLICATION_FILE_NAME}").getPath)
    val user: String = "user"
    val propertiesUtil = PropertiesUtil(user, getClass.getClassLoader.getResource(s"./${PropertiesUtil.DEFAULT_APPLICATION_FILE_NAME}").getPath)
    val kuduContext = mock[KuduContext]
    //mocking the tableExists return value
    Mockito.doReturn(true).when(kuduContext).tableExists("schema.table_test")
    propertiesUtil.getTableExists("schema.table_test", kuduContext) shouldBe "schema.table_test"
  }

  it must "return the tableName when none of the possible names exists" taggedAs UnitTestTag in {
    System.setProperty("config.file", getClass.getClassLoader.getResource(s"./${PropertiesUtil.DEFAULT_APPLICATION_FILE_NAME}").getPath)
    val user: String = "user"
    val propertiesUtil = PropertiesUtil(user, getClass.getClassLoader.getResource(s"./${PropertiesUtil.DEFAULT_APPLICATION_FILE_NAME}").getPath)
    val kuduContext = mock[KuduContext]
    propertiesUtil.getTableExists("schema.table_test", kuduContext) shouldBe "schema.table_test"
  }

}
