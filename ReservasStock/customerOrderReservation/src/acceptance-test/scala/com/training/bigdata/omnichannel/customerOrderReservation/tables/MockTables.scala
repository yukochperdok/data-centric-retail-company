package com.training.bigdata.omnichannel.customerOrderReservation.tables

import com.training.bigdata.omnichannel.customerOrderReservation.tables.MockTables.AcceptanceTests.CUSTOMER_ORDER_RESERVATION_SCHEMA
import org.apache.spark.SparkContextForTests
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

object MockTables extends SparkContextForTests {

  val configPath: String = getClass.getClassLoader.getResource(".").getPath
  private val baseDataDir: String = "/data/"
  private val baseInputDir: String = "/input"
  private val baseOutputDir: String = "/output"
  private val baseSchemasDir: String = "schemas"

  /*
   * ACCEPTANCE TESTS
   */
  object AcceptanceTests {

    // KUDU TABLE NAMES
    val CUSTOMER_ORDER_RESERVATION_TABLE: String = "omnichannel_stream.customer_orders_reservations"
    val CUSTOMER_ORDER_RESERVATION_ACTIONS_TABLE: String = "omnichannel_user.customer_orders_reservations_actions"
    val ARTICLE_TRANSCOD_TABLE: String = "mdata_stream.zxtranscod"
    val SITE_TRANSCOD_TABLE: String = "mdata_stream.zxtranscod_site"

    // CSV/JSON NAMES
    val ORDER_EVENTS = "order_events.json"
    val CUSTOMER_ORDER_RESERVATION_CSV = "customer_orders_reservations.csv"
    val CUSTOMER_ORDER_RESERVATION_ACTIONS_CSV = "customer_orders_reservations_actions.csv"
    val ARTICLE_TRANSCOD_CSV = "zxtranscod_data.csv"
    val SITE_TRANSCOD_CSV = "zxtranscod_site_data.csv"

    // SCHEMA NAMES
    val CUSTOMER_ORDER_RESERVATION_SCHEMA = "customer_orders_reservations_schema.csv"
    val CUSTOMER_ORDER_RESERVATION_ACTION_SCHEMA = "customer_orders_reservations_actions_schema.csv"
    val ARTICLE_TRANSCOD_SCHEMA = "zxtranscod_schema.csv"
    val SITE_TRANSCOD_SCHEMA = "zxtranscod_site_schema.csv"

    def KAFKA_ORDER_EVENTS_JSON(implicit specificLocationPath: String): String =
      s"$configPath$baseDataDir$specificLocationPath$baseInputDir/$ORDER_EVENTS"

    def KUDU_CUSTOMER_ORDER_RESERVATION_CSV(implicit specificLocationPath: String): (String, String) =
      (s"$configPath$baseDataDir$baseSchemasDir/$CUSTOMER_ORDER_RESERVATION_SCHEMA", s"$configPath$baseDataDir$specificLocationPath$baseInputDir/$CUSTOMER_ORDER_RESERVATION_CSV")

    def KUDU_CUSTOMER_ORDER_RESERVATION_OUTPUT_CSV(implicit specificLocationPath: String): (String, String) =
      (s"$configPath$baseDataDir$baseSchemasDir/$CUSTOMER_ORDER_RESERVATION_SCHEMA", s"$configPath$baseDataDir$specificLocationPath$baseOutputDir/$CUSTOMER_ORDER_RESERVATION_CSV")

    def KUDU_CUSTOMER_ORDER_RESERVATION_ACTIONS_CSV(implicit specificLocationPath: String): (String, String) =
      (s"$configPath$baseDataDir$baseSchemasDir/$CUSTOMER_ORDER_RESERVATION_ACTION_SCHEMA", s"$configPath$baseDataDir$specificLocationPath$baseInputDir/$CUSTOMER_ORDER_RESERVATION_ACTIONS_CSV")

    def KUDU_ARTICLE_TRANSCOD_CSV(implicit specificLocationPath: String): (String, String) =
      (s"$configPath$baseDataDir$baseSchemasDir/$ARTICLE_TRANSCOD_SCHEMA", s"$configPath$baseDataDir$specificLocationPath$baseInputDir/$ARTICLE_TRANSCOD_CSV")

    def KUDU_SITE_TRANSCOD_CSV(implicit specificLocationPath: String): (String, String) =
      (s"$configPath$baseDataDir$baseSchemasDir/$SITE_TRANSCOD_SCHEMA", s"$configPath$baseDataDir$specificLocationPath$baseInputDir/$SITE_TRANSCOD_CSV")
  }

  /*
   * INTEGRATION TESTS
   */
  object IntegrationTests {

    // KUDU TABLE NAMES
    val ARTICLE_TRANSCOD_TABLE: String = "impala::mdata_stream.zxtranscod"
    val ARTICLE_TRANSCOD_VIEW: String = "article_transcodification_view"
    val SITE_TRANSCOD_TABLE: String = "impala::mdata_stream.zxtranscod_site"
    val SITE_TRANSCOD_VIEW: String = "site_transcodification_view"
    val RESERVATION_ACTIONS_TABLE: String = "impala::omnichannel_user.customer_orders_reservations_actions"
    val RESERVATION_ACTIONS_VIEW = "reservation_actions_view"
    val CUSTOMER_ORDER_RESERVATION_TABLE: String = "omnichannel_stream.customer_orders_reservations"
    val CUSTOMER_ORDER_RESERVATION_EXPECTED_TABLE: String = "omnichannel_stream.customer_orders_reservations_exp"

    // CSV NAMES
    val ARTICLE_TRANSCOD_CSV = "zxtranscod_data.csv"
    val SITE_TRANSCOD_CSV = "zxtranscod_site_data.csv"
    val CUSTOMER_ORDER_RESERVATION_CSV = "customer_orders_reservations.csv"
    val CUSTOMER_ORDER_RESERVATION_EXPECTED_CSV = "customer_orders_reservations_data_expected.csv"
    val CUSTOMER_ORDER_RESERVATION_ACTIONS_CSV = "customer_orders_reservations_actions.csv"

    // SCHEMA NAMES
    val ARTICLE_TRANSCOD_SCHEMA = "zxtranscod_schema.csv"
    val SITE_TRANSCOD_SCHEMA = "zxtranscod_site_schema.csv"
    val CUSTOMER_ORDER_RESERVATION_SCHEMA = "customer_orders_reservations_schema.csv"
    val CUSTOMER_ORDER_RESERVATION_ACTION_SCHEMA = "customer_orders_reservations_actions_schema.csv"

    def KUDU_ARTICLE_TRANSCOD_CSV(implicit specificLocationPath: String): (String, String) =
      (s"$configPath$baseDataDir$baseSchemasDir/$ARTICLE_TRANSCOD_SCHEMA", s"$configPath$baseDataDir$specificLocationPath$baseInputDir/$ARTICLE_TRANSCOD_CSV")

    def KUDU_SITE_TRANSCOD_CSV(implicit specificLocationPath: String): (String, String) =
      (s"$configPath$baseDataDir$baseSchemasDir/$SITE_TRANSCOD_SCHEMA", s"$configPath$baseDataDir$specificLocationPath$baseInputDir/$SITE_TRANSCOD_CSV")

    def KUDU_CUSTOMER_ORDER_RESERVATION_ACTIONS_CSV(implicit specificLocationPath: String): (String, String) =
      (s"$configPath$baseDataDir$baseSchemasDir/$CUSTOMER_ORDER_RESERVATION_ACTION_SCHEMA", s"$configPath$baseDataDir$specificLocationPath$baseInputDir/$CUSTOMER_ORDER_RESERVATION_ACTIONS_CSV")

    def KUDU_CUSTOMER_ORDER_RESERVATION_CSV(implicit specificLocationPath: String): (String, String) =
      (s"$configPath$baseDataDir$baseSchemasDir/$CUSTOMER_ORDER_RESERVATION_SCHEMA", s"$configPath$baseDataDir$specificLocationPath$baseInputDir/$CUSTOMER_ORDER_RESERVATION_CSV")

    def KUDU_CUSTOMER_ORDER_RESERVATION_OUTPUT_CSV(implicit specificLocationPath: String): (String, String) =
      (s"$configPath$baseDataDir$baseSchemasDir/$CUSTOMER_ORDER_RESERVATION_SCHEMA", s"$configPath$baseDataDir$specificLocationPath$baseOutputDir/$CUSTOMER_ORDER_RESERVATION_EXPECTED_CSV")

  }

  val createDataFrameByCsv: (String, StructType) => DataFrame = (file, schema) => {
    sparkSession.createDataFrame(
      sparkSession.read
        .schema(schema)
        .format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .load(file).rdd,
      schema)
  }
}

