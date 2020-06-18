package com.training.bigdata.mdata.INEaddresses.services

import com.training.bigdata.mdata.INEaddresses.common.util.SparkLocalTest
import com.training.bigdata.mdata.INEaddresses.tables.MockTables
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType

object HiveService extends MockTables with SparkLocalTest {

  lazy val createDatabase: String => () => Unit = databaseName => {

    spark.sql(
      s"DROP DATABASE IF EXISTS $databaseName CASCADE"
    )
    spark.sql(
      s"CREATE DATABASE IF NOT EXISTS $databaseName"
    )
    () => Unit
  }

  lazy val createAndLoadHivePartitionedTable: (String, StructType, String, String) => () => Unit =
    (tableName, schema, createSQLSentence, data) => {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(createSQLSentence)
      val dfHiveTable = spark.read.schema(schema)
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(data)
      () => dfHiveTable.write.mode(SaveMode.Overwrite).insertInto(tableName)
    }

  lazy val createAndLoadHiveTable: (String, StructType, String) => () => Unit =
    (tableName, schema, data) => {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      val dfHiveTable = spark.read.schema(schema)
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(data)
      () => dfHiveTable.write.saveAsTable(tableName)
    }

  def initHive(tablesToload: LoadFunctions)(implicit specificLocationPath: String): Unit = {

    println("###############################################################")
    println(s"##########  HIVE   SCENARIO: $specificLocationPath ########################")
    println("###############################################################")

    applyFuncTables(tablesToload)
  }

  def countRecords(tableName: String): Int = {
    spark.sql(s"SELECT * FROM $tableName").count().toInt
  }
}
