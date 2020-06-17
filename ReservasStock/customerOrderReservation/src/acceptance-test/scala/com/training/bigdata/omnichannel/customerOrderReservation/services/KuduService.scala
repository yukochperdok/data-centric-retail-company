package com.training.bigdata.omnichannel.customerOrderReservation.services

import java.time.format.DateTimeFormatter

import com.training.bigdata.omnichannel.customerOrderReservation.io.KuduDataSource
import com.training.bigdata.omnichannel.customerOrderReservation.utils.{DateUtils, PropertiesUtil}
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.spark.kudu._
import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.io.Source

object KuduService {

  type KuduLoadFunctions = Seq[() => Unit]

  var kuduDataSource: KuduDataSource = _

  def loadKuduTables(tablesToLoad: KuduLoadFunctions): Unit = {
    tablesToLoad.foreach(_.apply())
  }

  def createAndLoadKuduTable(implicit kuduContext: KuduContext,
                             sparkSession: SparkSession,
                             propertiesUtil: PropertiesUtil): ((String, String), String) => () => Unit =
    (schemaData, tableName) => () => createAndLoad(tableName, schemaData)

  private def createAndLoad(tableName: String, schemaData: (String, String))(implicit kuduContext: KuduContext,
                                                                             sparkSession: SparkSession,
                                                                             propertiesUtil: PropertiesUtil): Unit = {
    createKuduTable(tableName, schemaData._1)
    loadKuduTable(tableName, schemaData._2)
  }

  def initKudu(tablesToload: KuduLoadFunctions)(implicit specificLocationPath: String,
                                                kuduContext: KuduContext,
                                                sparkSession: SparkSession,
                                                propertiesUtil: PropertiesUtil): Unit = {

    println("###############################################################")
    println(s"##########  KUDU   SCENARIO: $specificLocationPath ########################")
    println("###############################################################")

    kuduDataSource = new KuduDataSource
    loadKuduTables(tablesToload)
  }

  private def createKuduTable(tableName: String, schema_file: String)(implicit kuduContext: KuduContext, spark: SparkSession): Unit = {
    val schema = new Schema(getColumnsSchemas(schema_file).asJava)

    val createTableOptions = new CreateTableOptions
    createTableOptions.setNumReplicas(1)
    createTableOptions.addHashPartitions(getPrimaryKeyColumns(schema).asJava, 3)
    kuduContext.createTable(tableName, schema, createTableOptions)
  }

  def readTable(tableName: String)(implicit spark: SparkSession, propertiesUtil: PropertiesUtil): DataFrame = {
    spark.read.options(
      Map("kudu.master" -> propertiesUtil.getKuduHosts,
          "kudu.table" -> tableName)).kudu
  }

  private def loadKuduTable(tableName: String, data_file: String)(implicit kuduContext: KuduContext,
                                                                  spark: SparkSession,
                                                                  propertiesUtil: PropertiesUtil): Unit = {

    val schema: StructType = readTable(tableName).schema


    def toRow(values: Array[String], schema: StructType): Row = {
      Row.fromSeq(schema.fields.zipWithIndex.map { case(field, index) =>
        field.dataType match {
          case StringType => values(index).toString
          case IntegerType => values(index).toInt
          case LongType => values(index).toLong
          case DoubleType => values(index).toDouble
          case BooleanType => values(index).toBoolean
          case TimestampType => DateUtils.stringToTimestamp(values(index).toString, formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
          case _ => throw new IllegalArgumentException(s"Type doesn't found or recognized. Type:  ${field.dataType}")
        }
      })
    }

    val linesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Source.fromFile(data_file).getLines.drop(1).map(_.split(",")).map(toRow(_, schema)).toList
      ),
      schema
    )
    kuduDataSource.upsertToKudu(linesDF, tableName)
  }

  private def getColumnsSchemas(file: String): List[ColumnSchema] = {
    val lines = Source.fromFile(file).getLines.toList
    val header = lines.head.split(",").zipWithIndex.toMap

    lines.drop(1).map(line => {
      val values = line.split(",")
      new ColumnSchema.ColumnSchemaBuilder(values(header("name")), getType(values(header("type"))))
        .key(values(header("primary_key")).toBoolean).nullable(values(header("nullable")).toBoolean)
        .build
    })
  }

  def getColumnsNames(file: String): List[String] = {
    val lines = Source.fromFile(file).getLines.toList
    val header = lines.head.split(",").zipWithIndex.toMap

    lines.drop(1).map(line => {
      val values = line.split(",")
      values(header("name"))
    })
  }

  private def getPrimaryKeyColumns(schema: Schema): List[String] = {
    schema.getPrimaryKeyColumns.asScala.map(_.getName).toList
  }

  private def getType(kuduType: String): Type = {
    kuduType match {
      case "string" => Type.STRING
      case "boolean" => Type.BOOL
      case "int" => Type.INT32
      case "bigint" => Type.INT64
      case "double" => Type.DOUBLE
      case "timestamp" => Type.UNIXTIME_MICROS
      case _ => throw new IllegalArgumentException(s"Type $kuduType doesn't found or not recognized")
    }
  }

  def countRecords(tableName: String)(implicit spark: SparkSession, propertiesUtil: PropertiesUtil): Int = {
    readTable(tableName).count().toInt
  }

}

