package com.training.bigdata.mdata.INEaddresses.tables

import com.training.bigdata.mdata.INEaddresses.common.util.SparkLocalTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

abstract class MockTables extends SparkLocalTest {

  type LoadFunctions = Seq[() => Unit]

  private val configPath: String = getClass.getClassLoader.getResource(".").getPath
  private val baseDataDir: String = "/data/"
  private val baseInputDir: String = "/input/"
  private val baseOutputDir: String = "/output/"
  private val streetStretchesBaseProcessDir = "/INEFilesIngestion/"
  private val mastersBaseProcessDir = "/masters/"
  private val integrationPath = "/integration"
  private val schemaPath = "/schemas/"
  private val historicRawTable = "/historified_raw_table.csv"
  private val historicLastTable = "/historified_last_table.csv"
  private val ccaaEntity = "/ccaa_table.csv"
  private val streetMapEntity = "/street_map_table.csv"
  private val postalCodesEntity = "/postal_codes_table.csv"
  private val townsEntity = "/towns_table.csv"

  private val integration_test_hive_table = StructType(Array(
    StructField("fullName", StringType, nullable = false),
    StructField("desc", StringType, nullable = true)))

  val MASTERDATA_USER_DATABASE = "mdata_user"
  val MASTERDATA_RAW_DATABASE = "mdata_raw"
  val MASTERDATA_LAST_DATABASE = "mdata_last"
  val STREET_STRETCHES_HISTORIC_RAW_TABLE = s"$MASTERDATA_RAW_DATABASE.national_street_stretches"
  val STREET_STRETCHES_HISTORIC_LAST_TABLE = s"$MASTERDATA_LAST_DATABASE.national_street_stretches"
  val CCAA_TABLE = s"impala::$MASTERDATA_USER_DATABASE.ccaa_regions"
  val POSTAL_CODES_TABLE = s"impala::$MASTERDATA_USER_DATABASE.zip_codes"
  val TOWNS_TABLE = s"impala::$MASTERDATA_USER_DATABASE.towns"
  val STREET_MAP_TABLE = s"impala::$MASTERDATA_USER_DATABASE.zip_codes_street_map"

  private val townsTableStructType = StructType(Array(
    StructField("town_code",          StringType, nullable = false),
    StructField("land1",              StringType, nullable = false),
    StructField("spras",              StringType, nullable = false),
    StructField("town_name",          StringType, nullable = false),
    StructField("bland",              StringType, nullable = false),
    StructField("codauto",            StringType, nullable = false),
    StructField("ts_insert_dlk",   TimestampType, nullable = false),
    StructField("user_insert_dlk",    StringType, nullable = false),
    StructField("ts_update_dlk",   TimestampType, nullable = false),
    StructField("user_update_dlk",    StringType, nullable = false),
    StructField("ine_modification_date",     TimestampType, nullable = false)
  ))
  private val postalCodesTableStructType = StructType(Array(
    StructField("zip_code",           StringType, nullable = false),
    StructField("town_code",          StringType, nullable = false),
    StructField("land1",              StringType, nullable = false),
    StructField("bland",              StringType, nullable = false),
    StructField("codauto",            StringType, nullable = false),
    StructField("ts_insert_dlk",   TimestampType, nullable = false),
    StructField("user_insert_dlk",    StringType, nullable = false),
    StructField("ts_update_dlk",   TimestampType, nullable = false),
    StructField("user_update_dlk",    StringType, nullable = false),
    StructField("ine_modification_date",     TimestampType, nullable = false)
  ))
  private val streetStretchesHistoricTableRawStructType = StructType(Array(
    StructField("previous_bland",               StringType, nullable = true),
    StructField("previous_local_council_code",  StringType, nullable = true),
    StructField("previous_district",            StringType, nullable = true),
    StructField("previous_section",             StringType, nullable = true),
    StructField("previous_section_letter",      StringType, nullable = true, metadata = Metadata.fromJson("""{"HIVE_TYPE_STRING":"char(1)"}""")),
    StructField("previous_subsection",          StringType, nullable = true),
    StructField("previous_cun",                 StringType, nullable = true),
    StructField("previous_street_code",        IntegerType, nullable = true),
    StructField("previous_pseudostreet_code",  IntegerType, nullable = true),
    StructField("previous_square",              StringType, nullable = true),
    StructField("previous_zip_code",            StringType, nullable = true),
    StructField("previous_numbering_type",      StringType, nullable = true),
    StructField("previous_initial_number",     IntegerType, nullable = true),
    StructField("previous_initial_letter",      StringType, nullable = true, metadata = Metadata.fromJson("""{"HIVE_TYPE_STRING":"char(1)"}""")),
    StructField("previous_end_number",         IntegerType, nullable = true),
    StructField("previous_end_letter",          StringType, nullable = true, metadata = Metadata.fromJson("""{"HIVE_TYPE_STRING":"char(1)"}""")),
    StructField("information_type",             StringType, nullable = true, metadata = Metadata.fromJson("""{"HIVE_TYPE_STRING":"char(1)"}""")),
    StructField("refund_cause",                 StringType, nullable = true),
    StructField("INE_modification_date",     TimestampType, nullable = true),
    StructField("INE_modification_code",        StringType, nullable = true, metadata = Metadata.fromJson("""{"HIVE_TYPE_STRING":"char(1)"}""")),
    StructField("district",                     StringType, nullable = true),
    StructField("section",                      StringType, nullable = true),
    StructField("section_letter",               StringType, nullable = true, metadata = Metadata.fromJson("""{"HIVE_TYPE_STRING":"char(1)"}""")),
    StructField("subsection",                   StringType, nullable = true),
    StructField("cun",                          StringType, nullable = true),
    StructField("group_entity_short_name",      StringType, nullable = true),
    StructField("town_name",                    StringType, nullable = true),
    StructField("last_granularity_short_name",  StringType, nullable = true),
    StructField("street_code",                 IntegerType, nullable = true),
    StructField("street_name",                  StringType, nullable = true),
    StructField("pseudostreet_code",           IntegerType, nullable = true),
    StructField("pseudostreet_name",            StringType, nullable = true),
    StructField("cadastral_square",             StringType, nullable = true),
    StructField("zip_code",                     StringType, nullable = true),
    StructField("numbering_type",               StringType, nullable = true),
    StructField("initial_number",              IntegerType, nullable = true),
    StructField("initial_letter",               StringType, nullable = true, metadata = Metadata.fromJson("""{"HIVE_TYPE_STRING":"char(1)"}""")),
    StructField("end_number",                  IntegerType, nullable = true),
    StructField("end_letter",                   StringType, nullable = true, metadata = Metadata.fromJson("""{"HIVE_TYPE_STRING":"char(1)"}"""")),
    StructField("ts_insert_dlk",             TimestampType, nullable = true),
    StructField("user_insert_dlk",              StringType, nullable = true),
    StructField("ts_update_dlk",             TimestampType, nullable = true),
    StructField("user_update_dlk" ,             StringType, nullable = true),
    StructField("year",                        IntegerType, nullable = true),
    StructField("month",                       IntegerType, nullable = true)
  ))

  private val createHistoricTableRawSQL: String = s"CREATE TABLE IF NOT EXISTS $STREET_STRETCHES_HISTORIC_RAW_TABLE " +
    "("+
    "previous_bland                     STRING   , " +
    "previous_local_council_code        STRING   , " +
    "previous_district                  STRING   , " +
    "previous_section                   STRING   , " +
    "previous_section_letter            CHAR(1)  , " +
    "previous_subsection                STRING   , " +
    "previous_cun                       STRING   , " +
    "previous_street_code               INT      , " +
    "previous_pseudostreet_code         INT      , " +
    "previous_square                    STRING   , " +
    "previous_zip_code                  STRING   , " +
    "previous_numbering_type            STRING   , " +
    "previous_initial_number            INT      , " +
    "previous_initial_letter            CHAR(1)  , " +
    "previous_end_number                INT      , " +
    "previous_end_letter                CHAR(1)  , " +
    "information_type                   CHAR(1)  , " +
    "refund_cause                       STRING   , " +
    "INE_modification_date              TIMESTAMP, " +
    "INE_modification_code              CHAR(1)  , " +
    "district                           STRING   , " +
    "section                            STRING   , " +
    "section_letter                     CHAR(1)  , " +
    "subsection                         STRING   , " +
    "cun                                STRING   , " +
    "group_entity_short_name            STRING   , " +
    "town_name                          STRING   , " +
    "last_granularity_short_name        STRING   , " +
    "street_code                        INT      , " +
    "street_name                        STRING   , " +
    "pseudostreet_code                  INT      , " +
    "pseudostreet_name                  STRING   , " +
    "cadastral_square                   STRING   , " +
    "zip_code                           STRING   , " +
    "numbering_type                     STRING   , " +
    "initial_number                     INT      , " +
    "initial_letter                     CHAR(1)  , " +
    "end_number                         INT      , " +
    "end_letter                         CHAR(1)  , " +
    "ts_insert_dlk                      TIMESTAMP, " +
    "user_insert_dlk                    STRING   , " +
    "ts_update_dlk                      TIMESTAMP, " +
    "user_update_dlk                    STRING    " +
    ")"+
    "PARTITIONED BY"+
    "("+
    "year                               INT      , " +
    "month                              INT       " +
    ")"+
    "COMMENT 'INE street stretches historification'"+
    "STORED AS PARQUET"

  private val streetStretchesHistoricTableLastStructType = StructType(Array(
    StructField("previous_bland",               StringType, nullable = true),
    StructField("previous_local_council_code",  StringType, nullable = true),
    StructField("previous_district",            StringType, nullable = true),
    StructField("previous_section",             StringType, nullable = true),
    StructField("previous_section_letter",      StringType, nullable = true),
    StructField("previous_subsection",          StringType, nullable = true),
    StructField("previous_cun",                 StringType, nullable = true),
    StructField("previous_street_code",        IntegerType, nullable = true),
    StructField("previous_pseudostreet_code",  IntegerType, nullable = true),
    StructField("previous_square",              StringType, nullable = true),
    StructField("previous_zip_code",            StringType, nullable = true),
    StructField("previous_numbering_type",      StringType, nullable = true),
    StructField("previous_initial_number",     IntegerType, nullable = true),
    StructField("previous_initial_letter",      StringType, nullable = true),
    StructField("previous_end_number",         IntegerType, nullable = true),
    StructField("previous_end_letter",          StringType, nullable = true),
    StructField("information_type",             StringType, nullable = true),
    StructField("refund_cause",                 StringType, nullable = true),
    StructField("INE_modification_date",     TimestampType, nullable = true),
    StructField("INE_modification_code",        StringType, nullable = true),
    StructField("district",                     StringType, nullable = true),
    StructField("section",                      StringType, nullable = true),
    StructField("section_letter",               StringType, nullable = true),
    StructField("subsection",                   StringType, nullable = true),
    StructField("cun",                          StringType, nullable = true),
    StructField("group_entity_short_name",      StringType, nullable = true),
    StructField("town_name",                    StringType, nullable = true),
    StructField("last_granularity_short_name",  StringType, nullable = true),
    StructField("street_code",                 IntegerType, nullable = true),
    StructField("street_name",                  StringType, nullable = true),
    StructField("pseudostreet_code",           IntegerType, nullable = true),
    StructField("pseudostreet_name",            StringType, nullable = true),
    StructField("cadastral_square",             StringType, nullable = true),
    StructField("zip_code",                     StringType, nullable = true),
    StructField("numbering_type",               StringType, nullable = true),
    StructField("initial_number",              IntegerType, nullable = true),
    StructField("initial_letter",               StringType, nullable = true),
    StructField("end_number",                  IntegerType, nullable = true),
    StructField("end_letter",                   StringType, nullable = true),
    StructField("ts_insert_dlk",             TimestampType, nullable = true),
    StructField("user_insert_dlk",              StringType, nullable = true),
    StructField("ts_update_dlk",             TimestampType, nullable = true),
    StructField("user_update_dlk" ,             StringType, nullable = true)
  ))

  def applyFuncTables(funsToAplyLoads: LoadFunctions): Unit = {
    funsToAplyLoads.foreach(_.apply())
  }

  def TEST_KUDU_TABLE_CSV: (String, String) =
    (s"$configPath$baseDataDir$integrationPath/kudu_table_schema.csv", s"$configPath$baseDataDir$integrationPath/kudu_table_data.csv")

  def TEST_HIVE_TABLE_CSV: (StructType, String) =
    (integration_test_hive_table,s"$configPath$baseDataDir$integrationPath/hive_table.csv")

  def streetStretchesHistoricRawTable(implicit scenarioPath: String): (StructType, String, String) =
    (streetStretchesHistoricTableRawStructType, s"$configPath$baseDataDir$streetStretchesBaseProcessDir$scenarioPath$baseInputDir$historicRawTable", createHistoricTableRawSQL)

  def streetStretchesHistoricLastTable(implicit scenarioPath: String): (StructType, String) =
    (streetStretchesHistoricTableLastStructType, s"$configPath$baseDataDir$streetStretchesBaseProcessDir$scenarioPath$baseInputDir$historicLastTable")

  def ccaaTable(implicit scenarioPath: String): (String, String) =
    (s"$configPath$baseDataDir$mastersBaseProcessDir$schemaPath$ccaaEntity", s"$configPath$baseDataDir$mastersBaseProcessDir$scenarioPath$baseInputDir$ccaaEntity")

  def streetMapTable(implicit scenarioPath: String): (String, String) =
    (s"$configPath$baseDataDir$mastersBaseProcessDir$schemaPath$streetMapEntity", s"$configPath$baseDataDir$mastersBaseProcessDir$scenarioPath$baseInputDir$streetMapEntity")

  def postalCodesTable(implicit scenarioPath: String): (String, String, StructType) =
    (
      s"$configPath$baseDataDir$mastersBaseProcessDir$schemaPath$postalCodesEntity",
      s"$configPath$baseDataDir$mastersBaseProcessDir$scenarioPath$baseInputDir$postalCodesEntity",
      postalCodesTableStructType)

  def townsTable(implicit scenarioPath: String): (String, String, StructType) =
    (
      s"$configPath$baseDataDir$mastersBaseProcessDir$schemaPath$townsEntity",
      s"$configPath$baseDataDir$mastersBaseProcessDir$scenarioPath$baseInputDir$townsEntity",
      townsTableStructType)

  val createDataFrameByOutputCsv: (String, String, String, StructType) => DataFrame = (processDir, scenarioPath, csvFile, schema) => {
    spark.read
      .schema(schema)
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "false")
      .load(s"$configPath$baseDataDir$processDir$scenarioPath$baseOutputDir$csvFile")
  }

}