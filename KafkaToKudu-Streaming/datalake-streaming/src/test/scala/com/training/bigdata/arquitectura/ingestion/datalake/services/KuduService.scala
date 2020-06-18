package com.training.bigdata.arquitectura.ingestion.datalake.services

import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.apache.kudu.client.{CreateTableOptions, KuduClient}

import collection.JavaConversions._
import scala.collection.JavaConverters._

import scala.collection.mutable.ListBuffer


object KuduService {

  val table1Name = "xxx_stream.table1"
  val table2Name = "impala::yyy_stream.table2"
  val table3_001Name = "xxx_stream.table3"
  val table3_002Name = "xxx_stream.table3_t002"
  val marcName = "xxx_stream.marc"

  //History
  val table1_hName = "xxx_stream.table1_h"
  val table2_hName = "impala::yyy_stream.table2_h"

  var kuduClient: KuduClient = _

  def initKudu(): Unit = {
    kuduClient = new KuduClient.KuduClientBuilder("localhost:7051").build()

    // TABLE: 1
    val columns1 = ListBuffer(
      new ColumnSchema.ColumnSchemaBuilder("t1_k1", Type.STRING).key(true).nullable(false).build,
      new ColumnSchema.ColumnSchemaBuilder("t1_k2", Type.INT32).key(true).nullable(false).build,
      new ColumnSchema.ColumnSchemaBuilder("t1_v1", Type.STRING).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("t1_v2", Type.DOUBLE).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("t1_v3", Type.INT64).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("t1_v4", Type.BOOL).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("t1_v5", Type.UNIXTIME_MICROS).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("aaa_bbb_ccc", Type.STRING).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("ts_insert_dlk", Type.UNIXTIME_MICROS).nullable(false).build,
      new ColumnSchema.ColumnSchemaBuilder("ts_update_dlk", Type.UNIXTIME_MICROS).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("user_insert_dlk", Type.STRING).nullable(false).build,
      new ColumnSchema.ColumnSchemaBuilder("user_update_dlk", Type.STRING).nullable(true).build
    )

    val hashKeys1 = List("t1_k1", "t1_k2").asJava

    val createTableOptions1 = new CreateTableOptions()
    createTableOptions1.addHashPartitions(hashKeys1, 3)
    createTableOptions1.setNumReplicas(1)

    val schema1 = new Schema(columns1.asJava)

    kuduClient.createTable(table1Name, schema1, createTableOptions1)


    // TABLE: 2
    val columns2 = ListBuffer(
      new ColumnSchema.ColumnSchemaBuilder("t2_k1", Type.STRING).key(true).nullable(false).build,
      new ColumnSchema.ColumnSchemaBuilder("t2_k2", Type.INT32).key(true).nullable(false).build,
      new ColumnSchema.ColumnSchemaBuilder("t2_v1", Type.STRING).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("t2_v2", Type.DOUBLE).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("t2_v3", Type.INT64).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("t2_v4", Type.BOOL).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("ts_insert_dlk", Type.UNIXTIME_MICROS).nullable(false).build,
      new ColumnSchema.ColumnSchemaBuilder("ts_update_dlk", Type.UNIXTIME_MICROS).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("user_insert_dlk", Type.STRING).nullable(false).build,
      new ColumnSchema.ColumnSchemaBuilder("user_update_dlk", Type.STRING).nullable(true).build
    )

    val hashKeys2 = List("t2_k1", "t2_k2").asJava

    val createTableOptions2 = new CreateTableOptions()
    createTableOptions2.addHashPartitions(hashKeys2, 3)
    createTableOptions2.setNumReplicas(1)

    val schema2 = new Schema(columns2.asJava)

    kuduClient.createTable(table2Name, schema2, createTableOptions2)


    //TABLES: 1_h & 2_h
    val uuidColumn = new ColumnSchema.ColumnSchemaBuilder("uuid", Type.STRING).key(true).nullable(false).build
    val eventColumn = new ColumnSchema.ColumnSchemaBuilder("event", Type.STRING).nullable(true).build

    val schema1_hist = new Schema((uuidColumn +=: columns1 += eventColumn).toList.asJava)
    val schema2_hist = new Schema((uuidColumn +=: columns2 += eventColumn).toList.asJava)

    kuduClient.createTable(table1_hName, schema1_hist, createTableOptions1)
    kuduClient.createTable(table2_hName, schema2_hist, createTableOptions2)


    // TABLES: 3_001 & 3_002
    val columns3_001 = List(
      new ColumnSchema.ColumnSchemaBuilder("t3_k", Type.STRING).key(true).nullable(false).build,
      new ColumnSchema.ColumnSchemaBuilder("t3_v1", Type.STRING).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("t3_v2", Type.STRING).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("t3_v3", Type.STRING).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("t3_v4", Type.STRING).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("t3_v5", Type.STRING).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("ts_insert_dlk", Type.UNIXTIME_MICROS).nullable(false).build,
      new ColumnSchema.ColumnSchemaBuilder("ts_update_dlk", Type.UNIXTIME_MICROS).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("user_insert_dlk", Type.STRING).nullable(false).build,
      new ColumnSchema.ColumnSchemaBuilder("user_update_dlk", Type.STRING).nullable(true).build
    )

    val columns3_002 = List(
      new ColumnSchema.ColumnSchemaBuilder("t3_k", Type.STRING).key(true).nullable(false).build,
      new ColumnSchema.ColumnSchemaBuilder("t3_v6", Type.STRING).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("t3_v7", Type.STRING).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("t3_v8", Type.STRING).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("t3_v9", Type.STRING).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("t3_v10", Type.STRING).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("ts_insert_dlk", Type.UNIXTIME_MICROS).nullable(false).build,
      new ColumnSchema.ColumnSchemaBuilder("ts_update_dlk", Type.UNIXTIME_MICROS).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("user_insert_dlk", Type.STRING).nullable(false).build,
      new ColumnSchema.ColumnSchemaBuilder("user_update_dlk", Type.STRING).nullable(true).build
    )

    val hashKeys3 = List("t3_k").asJava

    val createTableOptions3 = new CreateTableOptions()
    createTableOptions3.addHashPartitions(hashKeys3, 3)
    createTableOptions3.setNumReplicas(1)

    val schema3_001 = new Schema(columns3_001.asJava)
    val schema3_002 = new Schema(columns3_002.asJava)

    kuduClient.createTable(table3_001Name, schema3_001, createTableOptions3)
    kuduClient.createTable(table3_002Name, schema3_002, createTableOptions3)


    // TABLE: MARC
    val marcColumns = List(
      new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING).key(true).nullable(false).build,
      new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("date_write_event", Type.UNIXTIME_MICROS).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("ts_insert_dlk", Type.UNIXTIME_MICROS).nullable(false).build,
      new ColumnSchema.ColumnSchemaBuilder("ts_update_dlk", Type.UNIXTIME_MICROS).nullable(true).build,
      new ColumnSchema.ColumnSchemaBuilder("user_insert_dlk", Type.STRING).nullable(false).build,
      new ColumnSchema.ColumnSchemaBuilder("user_update_dlk", Type.STRING).nullable(true).build
    )

    val marcHashKeys = List("key").asJava

    val marcCreateTableOptions = new CreateTableOptions()
    marcCreateTableOptions.addHashPartitions(marcHashKeys, 3)
    marcCreateTableOptions.setNumReplicas(1)

    val marcSchema = new Schema(marcColumns.asJava)

    kuduClient.createTable(marcName, marcSchema, marcCreateTableOptions)

  }

  def countRecords(tableName: String): Int = {
    val table = kuduClient.openTable(tableName)
    val scanner = kuduClient.newScannerBuilder(table).build()
    var count = 0
    while(scanner.hasMoreRows) {
      val results = scanner.nextRows()
//      while(results.hasNext) {
//        println(">>> " + results.next().rowToString + " <<<")
//      }
      count += results.getNumRows
    }
    scanner.close()
    count
  }

  def getRecords(tableName: String): List[scala.collection.mutable.Map[String, Any]] = {
    val table = kuduClient.openTable(tableName)
    val scanner = kuduClient.newScannerBuilder(table).build()
    val columns = table.getSchema.getColumns
    var records = List[scala.collection.mutable.Map[String, Any]]()
    while(scanner.hasMoreRows) {
      val rows = scanner.nextRows()
      while(rows.hasNext) {
        val row = rows.next()
        val result = scala.collection.mutable.Map[String, Any]()
        for(column: ColumnSchema <- columns) {
          row.isNull(column.getName) match {
            case true => result += (column.getName -> None)
            case false =>
              column.getType match {
                case Type.STRING => result += (column.getName -> row.getString(column.getName))
                case Type.INT8 => result += (column.getName -> row.getByte(column.getName))
                case Type.INT16 => result += (column.getName -> row.getShort(column.getName))
                case Type.INT32 => result += (column.getName -> row.getInt(column.getName))
                case Type.INT64 => result += (column.getName -> row.getLong(column.getName))
                case Type.DOUBLE => result += (column.getName -> row.getDouble(column.getName))
                case Type.BOOL => result += (column.getName -> row.getBoolean(column.getName))
                case Type.UNIXTIME_MICROS => result += (column.getName -> row.getLong(column.getName))
                case _ => throw new IllegalArgumentException("Type not implemented: " + column.getType)
              }
          }
        }
        records = result :: records
      }
    }
    scanner.close()
    records
  }

}
