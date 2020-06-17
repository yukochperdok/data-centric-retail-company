package com.training.bigdata.omnichannel.customerOrderReservation.utils

import com.training.bigdata.omnichannel.customerOrderReservation.utils.Constants._
import com.training.bigdata.omnichannel.customerOrderReservation.utils.LensAppConfig._
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
import org.apache.kudu.spark.kudu.KuduContext

class PropertiesUtil(
  val user: String,
  val appConfig: AppConfig,
  val config: Config) extends Serializable {

  import scala.collection.JavaConversions._

  def getKuduHosts: String = appConfig.getKuduHost
  def getParameters: Parameters = appConfig.getParameters
  def getListTopic: List[Topic] = appConfig.getListTopic
  def getListProcess: List[Process] = appConfig.getListProcess
  def getProjectCMDB: ProjectCMDB = appConfig.getProjectCMDB
  def getKafkaServers: String = appConfig.getKafkaServers
  def getSchemaRegistryURL: String = appConfig.getSchemaRegistryURL
  def getTimezone: String = appConfig.getTimezone
  def isKafkaSecured: Boolean = appConfig.isKafkaSecured
  def getRefreshTranscodTablesMillis: Long = config.getLong("app.refreshTranscodTablesMillis")
  def getGracefulStopHDFSDir: String = config.getString("app.gracefulStopHDFSDir")
  def getCheckpoint: String = config.getString("spark.checkpoint")

  def getCustomerOrdersReservationTable: String = config.getString("kudu.customer_orders_reservations_table")
  def getCustomerOrdersReservationActionsTable: String = config.getString("kudu.customer_orders_reservations_actions_table")
  def getArticlesTranscodingTable: String = config.getString("kudu.articles_transcoding_table")
  def getStoresTranscodingTable: String = config.getString("kudu.stores_transcoding_table")

  def getOrderTypesToIgnore: List[String] = config.getStringList("constants.orderTypesToIgnore").toList
  def getUpsertAction: String = config.getString("constants.upsertAction")
  def getDeleteAction: String = config.getString("constants.deleteAction")
  def getKG: String = config.getString("constants.KG")
  def getEA: String = config.getString("constants.EA")

  def getReservationsGroupId: String = config.getString("kafka.sources.reservations.groupId")
  def getReservationsTopic: String = config.getString("kafka.sources.reservations.topic")
  def getBatchDuration: Int = config.getInt("spark.batchDuration")
  def getUser: String = user


  def getTableExists(tableName : String, kuduContext: KuduContext) : String = {
    val possibleNames = List(tableName, IMPALA_PREFIX + tableName, tableName.toLowerCase, IMPALA_PREFIX + tableName.toLowerCase)
    val actualName = possibleNames.find(
      kuduContext.tableExists
    )
    actualName.getOrElse(tableName)
  }

  override def toString: String = {
    s"App Config - [getRefreshTranscodTablesMillis: $getRefreshTranscodTablesMillis]\n" +
    s"App Config - [getKuduHosts: $getKuduHosts]\n" +
    s"App Config - [getParameters: $getParameters]\n" +
    s"App Config - [getListTopic: $getListTopic]\n" +
    s"App Config - [getListProcess: $getListProcess]\n" +
    s"App Config - [getProjectCMDB: $getProjectCMDB]\n" +
    s"App Config - [getKafkaServers: $getKafkaServers]\n" +
    s"App Config - [getSchemaRegistryURL: $getSchemaRegistryURL]\n" +
    s"App Config - [getTimezone: $getTimezone]\n" +
    s"App Config - [isKafkaSecured: $isKafkaSecured]\n" +
    s"Config - [getGracefulStopHDFSDir: $getGracefulStopHDFSDir]\n" +
    s"Config - [getCheckpoint: $getCheckpoint]\n" +
    s"Config - [getCustomerOrdersReservationTable: $getCustomerOrdersReservationTable]\n" +
    s"Config - [getCustomerOrdersReservationActionsTable: $getCustomerOrdersReservationActionsTable]\n" +
    s"Config - [getArticlesTranscodingTable: $getArticlesTranscodingTable]\n" +
    s"Config - [getStoresTranscodingTable: $getStoresTranscodingTable]\n" +
    s"Config - [getOrderTypesToIgnore: $getOrderTypesToIgnore]\n" +
    s"Config - [getUpsertAction: $getUpsertAction]\n" +
    s"Config - [getDeleteAction: $getDeleteAction]\n" +
    s"Config - [getKG: $getKG]\n" +
    s"Config - [getEA: $getEA]\n" +
    s"Config - [getReservationsGroupId: $getReservationsGroupId]\n" +
    s"Config - [getDigitalTicketTopic: $getReservationsTopic]\n" +
    s"Config - [getRefreshTranscodTablesMillis: $getRefreshTranscodTablesMillis]\n" +
    s"Config - [getBatchDuration: $getBatchDuration]"
  }
}

/**
  * This class loads both the default config file from classpath and application.conf.
  */
object PropertiesUtil {

  private[utils] final val DEFAULT_APPLICATION_FILE_NAME = "application.conf"
  private[utils] final val DEFAULT_CONFIG_FILE_NAME = "customerOrderReservationStreaming.conf"

  def apply(user: String, _appConfigFile: String = DEFAULT_APPLICATION_FILE_NAME): PropertiesUtil =
    apply(user, AppConfig(_appConfigFile))

  def apply(user: String, _appConfig: AppConfig): PropertiesUtil =
    new PropertiesUtil (
      user,
      _appConfig,
      ConfigFactory.load(ConfigParseOptions.defaults().setAllowMissing(false)))
}
